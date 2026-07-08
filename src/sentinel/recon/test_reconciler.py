"""Reconciler integration tests — every failure mode funnels into the one
recovery mechanism, including the full crash-and-restart sequence."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest


from sentinel.broker.sim import BrokerScript, ScriptedBroker
from sentinel.domain import Authority, EconomicOrderIntent, OrderState, Side
from sentinel.ledger import LedgerStore
from sentinel.oms import CommandGateway, InstrumentHeld, OrderEngine, WriterCoordinator
from sentinel.recon import Reconciler, ReconciliationDivergence


def intent(key="K1", qty="4", side=Side.BUY, authority=Authority.ENTRY,
           instrument="IDX-OPT"):
    return EconomicOrderIntent(
        intent_id=uuid4(),
        idempotency_key=key,
        instrument=instrument,
        side=side,
        qty=Decimal(qty),
        limit_price=Decimal("4.20"),
        authority=authority,
        trace_id=uuid4(),
    )


def rig(pool, script: BrokerScript | None = None):
    store = LedgerStore(pool)
    sim = ScriptedBroker(script or BrokerScript())
    coord = WriterCoordinator()
    engine = OrderEngine(store, sim, coord)
    gateway = CommandGateway(store, engine)
    recon = Reconciler(store, sim, coord)
    return store, sim, engine, gateway, recon


async def pump(sim, engine, steps=1):
    for _ in range(steps):
        sim.step()
        for event in sim.take_events():
            await engine.on_broker_event(event)


# ------------------------------------------------ UNKNOWN -> found (the crux)


async def test_unknown_resolves_to_discovered_working_order(pool):
    """The R1.14 crux: timeout with no id, broker accepted anyway. Recon
    discovers the order by client id and adopts it — never resubmits."""
    script = BrokerScript().on_submit("K1", timeout=True, accept_on_timeout=True)
    store, sim, engine, gateway, recon = rig(pool, script)

    stored = await gateway.place(uuid4(), intent())
    assert stored.core.state is OrderState.UNKNOWN

    resolved = (await recon.drain(engine.needs_reconcile))[0]
    assert resolved.core.state is OrderState.WORKING
    assert resolved.core.broker_order_id == "SIM-1"   # the id we never received
    # Exactly one order exists broker-side: adoption, not resubmission.
    assert (await sim.query_order("K1")).broker_order_id == "SIM-1"


async def test_unknown_resolves_absent_to_unexposed_and_frees_instrument(pool):
    """Timeout where the broker truly never accepted: conclusively absent ->
    CANCELED with zero fills, and the R1.4 hold lifts."""
    script = BrokerScript().on_submit("K1", timeout=True)  # NOT accepted
    store, sim, engine, gateway, recon = rig(pool, script)

    await gateway.place(uuid4(), intent(key="K1"))
    with pytest.raises(InstrumentHeld):
        await gateway.place(uuid4(), intent(key="K2"))    # held while UNKNOWN

    resolved = (await recon.drain(engine.needs_reconcile))[0]
    assert resolved.core.state is OrderState.CANCELED
    assert resolved.core.filled_qty == 0
    assert await store.get_position("IDX-OPT") == 0      # zero exposure

    ok = await gateway.place(uuid4(), intent(key="K2"))  # hold lifted
    assert ok.core.state is OrderState.WORKING


# --------------------------------------------------------------- backfill


async def test_backfill_recovers_fills_we_never_heard(pool):
    """Broker executed while we were deaf (no events pumped). Recon backfills
    every missed execution exactly once and resolves to broker truth."""
    script = BrokerScript().on_submit("K1", timeout=True, accept_on_timeout=True)
    script.fill("K1", qty="1", price="4.20", at_step=1)
    script.fill("K1", qty="2", price="4.25", at_step=2)
    store, sim, engine, gateway, recon = rig(pool, script)

    await gateway.place(uuid4(), intent())
    sim.step()
    sim.step()
    sim.take_events()  # deliveries discarded: we never heard them

    resolved = (await recon.drain(engine.needs_reconcile))[0]
    assert resolved.core.state is OrderState.PARTIAL
    assert resolved.core.filled_qty == 3
    assert await store.get_position("IDX-OPT") == 3
    assert await pool.fetchval("SELECT count(*) FROM fills") == 2  # exactly once


async def test_backfill_is_idempotent_with_already_applied_fills(pool):
    """Fills partially heard before reconciliation: backfill dedups against
    them — position never double-counts."""
    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    script.fill("K1", qty="1", price="4.25", at_step=2)
    store, sim, engine, gateway, recon = rig(pool, script)

    await gateway.place(uuid4(), intent())
    await pump(sim, engine)          # fill 1 heard and applied
    sim.step()
    sim.take_events()                # fill 2 executed but never heard

    resolved = await recon.reconcile_order("K1")
    assert resolved.core.filled_qty == 3
    assert await store.get_position("IDX-OPT") == 3
    n = await pool.fetchval("SELECT count(*) FROM events WHERE kind='FILL_APPLIED'")
    assert n == 2                    # one live, one backfilled — no duplicates


# ------------------------------------------------------- late fill resolution


async def test_late_fill_reconciles_to_broker_truth(pool):
    """Completes the R1.7 story: late fill parked the order in RECONCILING;
    recon backfills it and resolves CANCELED with the true filled qty."""
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1, deliver_at_step=4)
    script.on_cancel("K1", confirm_after_steps=1)
    store, sim, engine, gateway, recon = rig(pool, script)

    await gateway.place(uuid4(), intent())
    sim.step()                                     # executes; delivery deferred
    await gateway.cancel(uuid4(), "K1", uuid4())
    await pump(sim, engine, steps=1)               # cancel confirm
    await pump(sim, engine, steps=2)               # late fill -> RECONCILING

    resolved = (await recon.drain(engine.needs_reconcile))[0]
    assert resolved.core.state is OrderState.CANCELED
    assert resolved.core.filled_qty == 1           # the late execution counted
    assert await store.get_position("IDX-OPT") == 1


# ------------------------------------------------------------- divergence


async def test_absent_broker_order_with_local_fills_resolves_terminal(pool):
    """An order the broker's query no longer returns (-2013) but which has real
    local fills is an AGED-OUT order, not phantom exposure — our fills are all
    broker-sourced. Resolve it terminal preserving the fills (position stands);
    do NOT halt. Exposure is guarded by POSITION reconciliation, not by an
    order-query miss. (Regression: this used to halt the whole OMS on real,
    fully-filled orders that merely aged out of demo-fapi's query window.)"""

    class AbsentBroker:
        async def query_order(self, client_order_id):
            return None

    store, sim, engine, gateway, recon = rig(pool)
    await gateway.place(uuid4(), intent(qty="2"))

    from sentinel.domain import FillReceived

    stored = await store.load_order("K1")
    await store.apply_event(
        stored, FillReceived(exec_id="E1", qty=Decimal(1), price=Decimal("4.2")),
        uuid4(),
    )

    recon2 = Reconciler(store, AbsentBroker(), WriterCoordinator())
    resolved = await recon2.reconcile_order("K1")            # no halt
    assert resolved.core.state is OrderState.CANCELED
    assert resolved.core.filled_qty == Decimal(1)           # real fill preserved
    assert await store.get_position("IDX-OPT") == 1         # position stands, not doubled


async def test_stale_sweep_recovers_fill_lost_in_stream_gap(pool):
    """The BTCUSDT prod incident: a MARKET protective exit fills at the venue,
    but the user stream dropped mid-gap and Binance does NOT replay missed
    events — the order sits WORKING/filled=0 with NO reactive trigger, pinning
    its position as committed-to-exits and blocking every SL/TP flatten. The
    staleness query is the missing trigger; existing reconcile machinery does
    the rest."""
    script = BrokerScript()
    script.fill("K1", qty="4", price="4.20", at_step=1)
    store, sim, engine, gateway, recon = rig(pool, script)

    await gateway.place(uuid4(), intent())     # ACKed -> WORKING locally
    sim.step()
    sim.take_events()                          # fill happened; report LOST

    working = await store.load_order("K1")
    assert working.core.state is OrderState.WORKING   # stranded, no trigger

    # Fresh orders are left alone...
    assert await store.load_stale_nonterminal(120) == []
    # ...but once untouched past the staleness window, the sweep finds it.
    await pool.execute(
        "UPDATE orders SET updated_at = now() - interval '10 minutes'"
    )
    stale = await store.load_stale_nonterminal(120)
    assert [s.core.client_order_id for s in stale] == ["K1"]

    for s in stale:                            # what the sweep loop does
        await engine.needs_reconcile.put(s.core.client_order_id)
    resolved = (await recon.drain(engine.needs_reconcile))[0]
    assert resolved.core.state is OrderState.FILLED    # broker truth restored
    assert resolved.core.filled_qty == 4
    assert await store.get_position("IDX-OPT") == 4    # exposure unpinned


def _view(state, filled, fills):
    from sentinel.broker.adapter import BrokerOrderView
    return BrokerOrderView("K1", "B1", state, Decimal(filled), tuple(fills))


def _fill(exec_id, qty, price="4.2"):
    from sentinel.broker.adapter import BrokerFill
    return BrokerFill("K1", exec_id, Decimal(qty), Decimal(price))


async def test_backfill_retries_a_lagging_trades_feed_then_reconciles(pool, monkeypatch):
    """A resting order still filling during recovery can report a broker
    executedQty AHEAD of its own userTrades (the feed lags). The reconciler
    re-queries and lets the trades catch up instead of halting on a phantom gap
    — the boot-halt we hit twice (ADA, AVAX)."""
    from sentinel.broker import BrokerOrderState
    from sentinel.recon import reconciler as recon_module
    monkeypatch.setattr(recon_module, "_FILL_BACKFILL_BACKOFF_S", 0.0)

    e1, e2 = _fill("E1", "1"), _fill("E2", "2", "4.25")

    class LaggingBroker:
        def __init__(self): self.calls = 0
        async def query_order(self, coid):
            self.calls += 1
            if self.calls == 1:           # executedQty 3, only 1 trade reported yet
                return _view(BrokerOrderState.PARTIAL, "3", [e1])
            return _view(BrokerOrderState.FILLED, "3", [e1, e2])   # trades caught up

    store, sim, engine, gateway, recon = rig(pool)
    await gateway.place(uuid4(), intent(qty="3"))
    b = LaggingBroker()
    resolved = await Reconciler(store, b, WriterCoordinator()).reconcile_order("K1")
    assert resolved.core.filled_qty == 3      # reconciled, no halt
    assert b.calls >= 2                        # it re-queried rather than halting


async def test_persistent_fill_gap_still_halts_after_retries(pool, monkeypatch):
    """If the trades NEVER catch up to executedQty, it's a genuine missing-fills
    gap — halt after the retries, never absorb."""
    from sentinel.broker import BrokerOrderState
    from sentinel.recon import reconciler as recon_module
    monkeypatch.setattr(recon_module, "_FILL_BACKFILL_BACKOFF_S", 0.0)

    e1 = _fill("E1", "1")

    class ShortBroker:
        def __init__(self): self.calls = 0
        async def query_order(self, coid):
            self.calls += 1
            return _view(BrokerOrderState.PARTIAL, "3", [e1])   # always 1 short

    store, sim, engine, gateway, recon = rig(pool)
    await gateway.place(uuid4(), intent(qty="3"))
    b = ShortBroker()
    with pytest.raises(ReconciliationDivergence):
        await Reconciler(store, b, WriterCoordinator()).reconcile_order("K1")
    assert b.calls == recon_module._FILL_BACKFILL_RETRIES + 1   # initial + retries


# ------------------------------------------ lock scope: broker I/O outside it


class _GatedBroker:
    """query_order that parks in "broker HTTP" until released — lets a test
    hold a reconcile mid-I/O and probe what else can run meanwhile."""

    def __init__(self, view):
        self._view = view
        self.entered = asyncio.Event()   # set once reconcile is inside the query
        self.release = asyncio.Event()   # test sets it to let the query return

    async def query_order(self, client_order_id):
        self.entered.set()
        await self.release.wait()
        return self._view


def _shared_coord_rig(pool, gated_broker):
    """Engine and reconciler sharing ONE coordinator (as in prod), with the
    reconciler's broker gated so tests can hold it mid-query."""
    store = LedgerStore(pool)
    coord = WriterCoordinator()
    engine = OrderEngine(store, ScriptedBroker(BrokerScript()), coord)
    gateway = CommandGateway(store, engine)
    recon = Reconciler(store, gated_broker, coord)
    return store, engine, gateway, recon


async def test_fill_event_applies_while_reconcile_is_in_broker_http(pool):
    """The prod convoy: reconcile used to hold the instrument lock across
    multi-second broker HTTP, so live fill applies queued behind it
    (event_apply p99 3.41s vs ~13ms steady state). Broker I/O now runs
    OUTSIDE the lock: a fill event must apply to completion WHILE the
    reconcile is still parked inside its query."""
    from sentinel.broker import BrokerOrderState

    broker = _GatedBroker(_view(BrokerOrderState.PARTIAL, "1", [_fill("E1", "1")]))
    store, engine, gateway, recon = _shared_coord_rig(pool, broker)
    await gateway.place(uuid4(), intent(qty="4"))            # K1 WORKING

    reconcile = asyncio.create_task(recon.reconcile_order("K1"))
    await asyncio.wait_for(broker.entered.wait(), timeout=1.0)  # parked in HTTP

    try:
        # The live fill must NOT convoy behind the parked reconcile. (With the
        # lock held across the query this deadlocks until release -> timeout.)
        await asyncio.wait_for(engine.on_broker_event(_fill("E1", "1")),
                               timeout=1.0)
        assert not reconcile.done()                # reconcile still in its query
        fresh = await store.load_order("K1")
        assert fresh.core.filled_qty == 1          # fill landed while it waited
    finally:
        broker.release.set()

    resolved = await reconcile
    assert resolved.core.state is OrderState.PARTIAL
    assert resolved.core.filled_qty == 1           # E1 deduped, never doubled
    assert await store.get_position("IDX-OPT") == 1
    assert await pool.fetchval("SELECT count(*) FROM fills") == 1


async def test_reconcile_adopts_terminal_resolution_from_another_path(pool):
    """Between phase A (broker query, no lock) and phase B (apply, locked),
    another path may resolve the order — here a live fill completes it to
    FILLED. Reconcile must return that terminal order as-is, never re-opening
    it (no RECONCILE_STARTED / RECONCILE_RESOLVED appended)."""
    from sentinel.broker import BrokerOrderState

    broker = _GatedBroker(_view(BrokerOrderState.FILLED, "4", [_fill("E1", "4")]))
    store, engine, gateway, recon = _shared_coord_rig(pool, broker)
    await gateway.place(uuid4(), intent(qty="4"))            # K1 WORKING

    reconcile = asyncio.create_task(recon.reconcile_order("K1"))
    await asyncio.wait_for(broker.entered.wait(), timeout=1.0)  # parked in HTTP

    try:
        await engine.on_broker_event(_fill("E1", "4"))       # completes: FILLED
    finally:
        broker.release.set()

    resolved = await reconcile
    assert resolved.core.state is OrderState.FILLED          # adopted, not redone
    assert resolved.core.filled_qty == 4
    assert await store.get_position("IDX-OPT") == 4
    n = await pool.fetchval(
        "SELECT count(*) FROM events "
        "WHERE kind IN ('RECONCILE_STARTED', 'RECONCILE_RESOLVED')"
    )
    assert n == 0                                            # never re-opened


# --------------------------------------------------------- startup recovery


async def test_startup_recovery_full_sequence(pool):
    """R1.11 end-to-end: a mixed book crashes; restart rebuilds projections,
    reconciles every non-terminal order, and converges to broker truth.

    Book at crash (one instrument per order — R1.9 allows one live entry
    per instrument, a constraint this test originally violated and the
    guard correctly refused):
      DONE  (IDX-A) — filled cleanly before the crash (terminal: untouched)
      LIVE  (IDX-B) — working; one fill heard, second executed but undelivered
      STUCK (IDX-C) — timeout, broker accepted, filled 1 while we were down
      GHOST (IDX-D) — timeout, broker never accepted
    """
    script = BrokerScript()
    script.on_submit("STUCK", timeout=True, accept_on_timeout=True)
    script.fill("STUCK", qty="1", price="4.10", at_step=3)
    script.on_submit("GHOST", timeout=True)
    script.fill("DONE", qty="1", price="4.00", at_step=1)
    script.fill("LIVE", qty="1", price="4.20", at_step=2)
    script.fill("LIVE", qty="1", price="4.30", at_step=3)
    store, sim, engine, gateway, recon = rig(pool, script)

    await gateway.place(uuid4(), intent(key="DONE", qty="1", instrument="IDX-A"))
    await pump(sim, engine)                       # DONE fills terminally
    await gateway.place(uuid4(), intent(key="LIVE", qty="4", instrument="IDX-B"))
    await pump(sim, engine)                       # LIVE hears fill 1
    await gateway.place(uuid4(), intent(key="STUCK", qty="4", instrument="IDX-C"))
    await gateway.place(uuid4(), intent(key="GHOST", qty="1", instrument="IDX-D"))

    sim.step()                                    # step 3: STUCK + LIVE execute
    sim.take_events()                             # ...but nothing is delivered

    # CRASH: corrupt projections; new store/recon objects (fresh process),
    # same database, same broker truth.
    await pool.execute("UPDATE orders SET filled_qty = 0 WHERE state != 'CREATED'")
    await pool.execute("UPDATE positions SET qty = 99")
    store2 = LedgerStore(pool)
    recon2 = Reconciler(store2, sim, WriterCoordinator())

    report = await recon2.startup_recovery()

    assert set(report.reconciled) == {"STUCK", "GHOST", "LIVE"}  # DONE untouched
    assert report.resolved_states["STUCK"] is OrderState.PARTIAL   # found + fill
    assert report.resolved_states["GHOST"] is OrderState.CANCELED  # unexposed
    assert report.resolved_states["LIVE"] is OrderState.PARTIAL

    stuck = await store2.load_order("STUCK")
    assert stuck.core.broker_order_id is not None  # discovered id adopted
    assert stuck.core.filled_qty == 1
    live = await store2.load_order("LIVE")
    assert live.core.filled_qty == 2               # heard 1 + backfilled 1

    # Exact per-instrument convergence with broker truth.
    for instrument, expected in [("IDX-A", 1), ("IDX-B", 2), ("IDX-C", 1)]:
        assert await store2.get_position(instrument) == expected
        assert (await sim.query_positions())[instrument] == expected
    assert await store2.get_position("IDX-D") == 0   # GHOST: zero exposure

    # The book is clean: nothing UNKNOWN or RECONCILING survives recovery.
    for instrument in ("IDX-A", "IDX-B", "IDX-C", "IDX-D"):
        assert not await store2.has_unresolved(instrument)


# ------------------------------------------------ position reconciliation (R1.12)

class _PosBroker:
    """Minimal broker exposing only what position reconcile needs."""
    def __init__(self, positions):
        self._positions = positions
    async def open_positions(self):
        return self._positions


async def test_position_reconcile_imports_exchange_baseline(pool):
    from sentinel.broker import BrokerPosition
    store = LedgerStore(pool)
    broker = _PosBroker({"RCN-A": BrokerPosition(qty=Decimal("-0.0211"),
                                                 entry_price=Decimal("62702"))})
    recon = Reconciler(store, broker, WriterCoordinator())

    assert await store.get_position("RCN-A") == 0             # ledger starts flat
    assert await recon.reconcile_positions() == ["RCN-A"]     # imports the delta
    assert await store.get_position("RCN-A") == Decimal("-0.0211")   # adopts exchange
    # idempotent: a second pass sees no divergence, imports nothing.
    assert await recon.reconcile_positions() == []
    assert await store.get_position("RCN-A") == Decimal("-0.0211")


async def test_position_reconcile_converges_partial_ledger(pool):
    from sentinel.broker import BrokerPosition
    store = LedgerStore(pool)
    # exchange holds MORE than our ledger will after one import step; reconcile
    # must land exactly on the exchange qty.
    broker = _PosBroker({"RCN-B": BrokerPosition(qty=Decimal("5"),
                                                 entry_price=Decimal("100"))})
    recon = Reconciler(store, broker, WriterCoordinator())
    await recon.reconcile_positions()
    assert await store.get_position("RCN-B") == Decimal("5")


async def test_position_reconcile_skips_when_broker_has_no_positions_api(pool):
    store = LedgerStore(pool)
    broker = ScriptedBroker(BrokerScript())          # no open_positions method
    recon = Reconciler(store, broker, WriterCoordinator())
    assert await recon.reconcile_positions() == []

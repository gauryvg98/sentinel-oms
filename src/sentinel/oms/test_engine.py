"""OMS engine integration tests: gateway -> guards -> ledger -> simulated
broker -> event application. Each test is a miniature integrity scenario."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest


from sentinel.broker.sim import BrokerScript, ScriptedBroker
from sentinel.domain import Authority, EconomicOrderIntent, OrderState, Side
from sentinel.ledger import LedgerStore
from sentinel.oms import (
    CommandGateway,
    DuplicateEntryBlocked,
    InstrumentHeld,
    NothingToExit,
    OrderEngine,
)


def intent(key="K1", qty="4", side=Side.BUY, authority=Authority.ENTRY):
    return EconomicOrderIntent(
        intent_id=uuid4(),
        idempotency_key=key,
        instrument="IDX-OPT",
        side=side,
        qty=Decimal(qty),
        limit_price=Decimal("4.20"),
        authority=authority,
        trace_id=uuid4(),
    )


def rig(pool, script: BrokerScript | None = None):
    store = LedgerStore(pool)
    sim = ScriptedBroker(script or BrokerScript())
    engine = OrderEngine(store, sim)
    gateway = CommandGateway(store, engine)
    return store, sim, engine, gateway


async def pump(sim: ScriptedBroker, engine: OrderEngine, steps: int = 1):
    """Advance virtual time and apply everything the broker delivered."""
    for _ in range(steps):
        sim.step()
        for event in sim.take_events():
            await engine.on_broker_event(event)


# ----------------------------------------------------------- place + idempotency


async def test_place_happy_path_reaches_working(pool):
    _, _, _, gateway = rig(pool)
    stored = await gateway.place(uuid4(), intent())
    assert stored.core.state is OrderState.WORKING
    assert stored.core.broker_order_id == "SIM-1"


async def test_duplicate_command_does_not_double_submit(pool):
    """R1.2 end-to-end: same command id replayed -> same order, one submission."""
    store, sim, _, gateway = rig(pool)
    cid = uuid4()
    first = await gateway.place(cid, intent())
    replay = await gateway.place(cid, intent())
    assert replay.core.order_id == first.core.order_id
    view = await sim.query_order("K1")
    assert view.broker_order_id == first.core.broker_order_id
    n_submits = await pool.fetchval(
        "SELECT count(*) FROM events WHERE kind = 'SUBMISSION_STARTED'"
    )
    assert n_submits == 1


async def test_duplicate_intent_key_different_command_is_one_order(pool):
    _, _, _, gateway = rig(pool)
    a = await gateway.place(uuid4(), intent(key="SAME"))
    b = await gateway.place(uuid4(), intent(key="SAME"))
    assert a.core.order_id == b.core.order_id


# --------------------------------------------------------------- R1.3 / R1.4


async def test_timeout_parks_unknown_and_queues_reconcile(pool):
    script = BrokerScript().on_submit("K1", timeout=True, accept_on_timeout=True)
    _, _, engine, gateway = rig(pool, script)
    stored = await gateway.place(uuid4(), intent())
    assert stored.core.state is OrderState.UNKNOWN
    assert stored.core.broker_order_id is None          # we never got the id
    assert await engine.needs_reconcile.get() == "K1"   # handed to recon


async def test_unknown_holds_instrument_against_new_entries(pool):
    """R1.4: while K1 is UNKNOWN, a new entry on the instrument is refused."""
    script = BrokerScript().on_submit("K1", timeout=True, accept_on_timeout=True)
    _, _, _, gateway = rig(pool, script)
    await gateway.place(uuid4(), intent(key="K1"))
    with pytest.raises(InstrumentHeld):
        await gateway.place(uuid4(), intent(key="K2"))


async def test_reject_frees_the_instrument(pool):
    script = BrokerScript().on_submit("K1", reject="no permission")
    _, _, _, gateway = rig(pool, script)
    stored = await gateway.place(uuid4(), intent(key="K1"))
    assert stored.core.state is OrderState.REJECTED
    ok = await gateway.place(uuid4(), intent(key="K2"))  # terminal reject holds nothing
    assert ok.core.state is OrderState.WORKING


# ---------------------------------------------------------------------- R1.9


async def test_second_live_entry_is_blocked(pool):
    _, _, _, gateway = rig(pool)
    await gateway.place(uuid4(), intent(key="K1"))
    with pytest.raises(DuplicateEntryBlocked):
        await gateway.place(uuid4(), intent(key="K2"))


# ------------------------------------------------------------- fills + cancel


async def test_fills_apply_and_duplicate_delivery_is_noop(pool):
    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    script.redeliver("K1-E1", at_step=2)
    store, sim, engine, gateway = rig(pool, script)

    await gateway.place(uuid4(), intent())
    await pump(sim, engine, steps=2)  # fill, then its duplicate delivery

    stored = await store.load_order("K1")
    assert stored.core.state is OrderState.PARTIAL
    assert stored.core.filled_qty == 2
    assert await store.get_position("IDX-OPT") == 2  # counted exactly once


async def test_cancel_race_full_lifecycle(pool):
    """R1.6 end-to-end: 2 fills, cancel, a 3rd fill inside the window,
    confirm -> CANCELED with exactly 3 filled."""
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1)
    script.fill("K1", qty="1", price="4.22", at_step=2)
    script.fill("K1", qty="1", price="4.30", at_step=3)
    script.on_cancel("K1", confirm_after_steps=2)
    store, sim, engine, gateway = rig(pool, script)

    await gateway.place(uuid4(), intent())          # 4 contracts
    await pump(sim, engine, steps=2)                # fills 1, 2
    await gateway.cancel(uuid4(), "K1", uuid4())    # window opens
    await pump(sim, engine, steps=2)                # fill 3 in window, then confirm

    stored = await store.load_order("K1")
    assert stored.core.state is OrderState.CANCELED
    assert stored.core.filled_qty == 3
    assert stored.core.remaining_qty == 1           # conclusively remaining
    assert await store.get_position("IDX-OPT") == 3


async def test_limit_order_rests_persists_price_partials_then_cancels(pool):
    """End-to-end limit lifecycle through the real stack: a resting limit
    order keeps its price durably (open_entry sees it), partially fills over
    time (staying live), and cancels cleanly — the machinery peg-to-touch
    drives."""
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1)   # partial: 1 of 4
    script.on_cancel("K1", confirm_after_steps=1)
    store, sim, engine, gateway = rig(pool, script)

    stored = await gateway.place(uuid4(), intent())        # limit @ 4.20, qty 4
    assert stored.core.state is OrderState.WORKING

    oe = await store.open_entry("IDX-OPT")                 # price is durable
    assert oe["key"] == "K1" and oe["limit_price"] == Decimal("4.20")

    await pump(sim, engine, steps=1)                        # one partial fill
    resting = await store.open_entry("IDX-OPT")
    assert resting["state"] == "PARTIAL" and resting["filled"] == Decimal("1")

    await gateway.cancel(uuid4(), "K1", uuid4())            # peg would do this to re-price
    await pump(sim, engine, steps=1)                        # confirm
    assert (await store.load_order("K1")).core.state is OrderState.CANCELED
    assert await store.open_entry("IDX-OPT") is None        # no live entry -> guard clears


async def test_late_fill_reopens_via_reconciliation(pool):
    """R1.7: a fill delivered after CANCELED moves the order to RECONCILING
    and queues it — never applied blind, never dropped."""
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1, deliver_at_step=4)
    script.on_cancel("K1", confirm_after_steps=1)
    store, sim, engine, gateway = rig(pool, script)

    await gateway.place(uuid4(), intent())
    sim.step()                                      # executes; delivery deferred
    await gateway.cancel(uuid4(), "K1", uuid4())
    await pump(sim, engine, steps=1)                # cancel confirm arrives
    assert (await store.load_order("K1")).core.state is OrderState.CANCELED

    await pump(sim, engine, steps=2)                # late fill arrives
    stored = await store.load_order("K1")
    assert stored.core.state is OrderState.RECONCILING
    assert await engine.needs_reconcile.get() == "K1"


# --------------------------------------------------------------------- R1.10


async def test_exit_clamps_to_reconciled_position(pool):
    script = BrokerScript()
    script.fill("K1", qty="3", price="4.20", at_step=1)
    store, sim, engine, gateway = rig(pool, script)
    await gateway.place(uuid4(), intent(key="K1", qty="3"))
    await pump(sim, engine)                          # position: +3

    exit_intent = intent(
        key="X1", qty="5", side=Side.SELL, authority=Authority.PROTECTIVE_EXIT
    )
    stored = await gateway.place(uuid4(), exit_intent)
    assert stored.core.qty == 3                      # clamped 5 -> 3, never over


async def test_stacked_exits_cannot_over_exit(pool):
    script = BrokerScript()
    script.fill("K1", qty="3", price="4.20", at_step=1)
    _, sim, engine, gateway = rig(pool, script)
    await gateway.place(uuid4(), intent(key="K1", qty="3"))
    await pump(sim, engine)

    await gateway.place(
        uuid4(),
        intent(key="X1", qty="3", side=Side.SELL, authority=Authority.PROTECTIVE_EXIT),
    )
    with pytest.raises(NothingToExit):               # 3 held, 3 already committed
        await gateway.place(
            uuid4(),
            intent(key="X2", qty="1", side=Side.SELL,
                   authority=Authority.PROTECTIVE_EXIT),
        )


# --------------------------------------------------------------------- R1.13


async def test_exits_work_while_entries_are_held(pool):
    """The protective-exit path must stay open exactly when entries are blocked:
    position exists, then an UNKNOWN entry holds the instrument — the exit
    still goes through."""
    script = BrokerScript()
    script.fill("K1", qty="3", price="4.20", at_step=1)
    script.on_submit("K2", timeout=True, accept_on_timeout=True)
    _, sim, engine, gateway = rig(pool, script)

    await gateway.place(uuid4(), intent(key="K1", qty="3"))
    await pump(sim, engine)                          # position: +3
    await gateway.place(uuid4(), intent(key="K2"))   # -> UNKNOWN, instrument held

    with pytest.raises(InstrumentHeld):
        await gateway.place(uuid4(), intent(key="K3"))  # entries: blocked

    stored = await gateway.place(                       # exits: still open
        uuid4(),
        intent(key="X1", qty="3", side=Side.SELL,
               authority=Authority.PROTECTIVE_EXIT),
    )
    assert stored.core.state is OrderState.WORKING

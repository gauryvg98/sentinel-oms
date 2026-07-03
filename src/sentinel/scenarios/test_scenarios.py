"""Acceptance scenarios R1.1–R1.14 (docs/REQUIREMENTS.md).

One named test per requirement. These are the system's contract: each test's
name, docstring, and assertions map 1:1 to a requirement, so the
scenario->evidence table in any report writes itself.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest


from sentinel.broker.sim import BrokerScript
from sentinel.domain import Authority, OrderState, Side
from sentinel.oms import DuplicateEntryBlocked, InstrumentHeld, NothingToExit

from .harness import build, intent


async def test_r1_01_durable_intent_before_submission(pool):
    """R1.1: intent is durable BEFORE the broker sees anything — even a
    timeout with zero exposure leaves a complete, recoverable record."""
    stack = build(pool, BrokerScript().on_submit("K1", timeout=True))
    await stack.gateway.place(uuid4(), intent())

    stored = await stack.store.load_order("K1")
    assert stored is not None                       # the record exists...
    assert await stack.sim.query_order("K1") is None  # ...the exposure doesn't
    seqs = {
        r["kind"]: r["seq"]
        for r in await pool.fetch("SELECT kind, seq FROM events ORDER BY seq")
    }
    assert seqs["INTENT_PERSISTED"] < seqs["SUBMISSION_STARTED"]


async def test_r1_02_duplicates_cannot_create_duplicate_exposure(pool):
    """R1.2: same command id AND same idempotency key, replayed after the
    original progressed — one order, one submission, one broker-side order."""
    stack = build(pool)
    cid = uuid4()
    first = await stack.gateway.place(cid, intent())
    await stack.gateway.place(cid, intent())            # command replay
    await stack.gateway.place(uuid4(), intent())        # intent-key replay

    assert await pool.fetchval("SELECT count(*) FROM orders") == 1
    assert await pool.fetchval(
        "SELECT count(*) FROM events WHERE kind='SUBMISSION_STARTED'"
    ) == 1
    assert (await stack.sim.query_order("K1")).broker_order_id \
        == first.core.broker_order_id


async def test_r1_03_timeout_is_unknown_and_never_retried(pool):
    """R1.3: timeout -> UNKNOWN, and recovery adopts — the ledger shows ONE
    submission attempt across the entire lifecycle."""
    stack = build(
        pool, BrokerScript().on_submit("K1", timeout=True, accept_on_timeout=True)
    )
    stored = await stack.gateway.place(uuid4(), intent())
    assert stored.core.state is OrderState.UNKNOWN

    resolved = (await stack.recon.drain(stack.engine.needs_reconcile))[0]
    assert resolved.core.state is OrderState.WORKING
    assert await pool.fetchval(
        "SELECT count(*) FROM events WHERE kind='SUBMISSION_STARTED'"
    ) == 1                                              # no second attempt exists


async def test_r1_04_unknown_blocks_conflicting_submissions(pool):
    """R1.4: UNKNOWN holds the instrument; reconciliation lifts the hold."""
    stack = build(pool, BrokerScript().on_submit("K1", timeout=True))
    await stack.gateway.place(uuid4(), intent(key="K1"))

    with pytest.raises(InstrumentHeld):
        await stack.gateway.place(uuid4(), intent(key="K2"))

    await stack.recon.drain(stack.engine.needs_reconcile)   # absent -> unexposed
    ok = await stack.gateway.place(uuid4(), intent(key="K2"))
    assert ok.core.state is OrderState.WORKING


async def test_r1_05_partial_fill_accounting_is_exact(pool):
    """R1.5: filled/remaining track exactly across partial fills."""
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1)
    script.fill("K1", qty="2", price="4.25", at_step=2)
    stack = build(pool, script)
    await stack.gateway.place(uuid4(), intent(qty="4"))
    await stack.pump(2)

    stored = await stack.store.load_order("K1")
    assert stored.core.filled_qty == 3 and stored.core.remaining_qty == 1
    assert await stack.store.get_position("IDX-OPT") == 3


async def test_r1_06_fill_during_cancel_pending_is_accounted(pool):
    """R1.6: a fill inside the cancel window is accepted; the cancel then
    applies only to what conclusively remains."""
    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    script.fill("K1", qty="1", price="4.30", at_step=2)   # inside the window
    script.on_cancel("K1", confirm_after_steps=2)
    stack = build(pool, script)

    await stack.gateway.place(uuid4(), intent(qty="4"))
    await stack.pump(1)
    await stack.gateway.cancel(uuid4(), "K1", uuid4())
    await stack.pump(2)

    stored = await stack.store.load_order("K1")
    assert stored.core.state is OrderState.CANCELED
    assert stored.core.filled_qty == 3                    # race fill counted


async def test_r1_07_late_fill_is_never_lost_and_never_double_applied(pool):
    """R1.7: a fill delivered after presumed-terminal reopens the order via
    reconciliation; position converges to broker truth exactly once."""
    script = BrokerScript()
    script.fill("K1", qty="1", price="4.20", at_step=1, deliver_at_step=4)
    script.on_cancel("K1", confirm_after_steps=1)
    stack = build(pool, script)

    await stack.gateway.place(uuid4(), intent())
    stack.sim.step()                                      # executes, undelivered
    await stack.gateway.cancel(uuid4(), "K1", uuid4())
    await stack.pump(1)                                   # CANCELED locally
    await stack.pump(2)                                   # late fill arrives

    await stack.recon.drain(stack.engine.needs_reconcile)
    stored = await stack.store.load_order("K1")
    assert stored.core.filled_qty == 1
    assert await stack.store.get_position("IDX-OPT") == 1
    assert await pool.fetchval("SELECT count(*) FROM fills") == 1  # exactly once


async def test_r1_08_replace_uses_only_conclusively_remaining_quantity(pool):
    """R1.8: while the cancel is pending, the remaining quantity is NOT
    conclusive — and the duplicate-entry guard structurally refuses a
    replacement. Only after the cancel confirms does 'remaining' become a
    fact you can size against."""
    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    script.fill("K1", qty="1", price="4.30", at_step=2)   # races the cancel
    script.on_cancel("K1", confirm_after_steps=2)
    stack = build(pool, script)

    await stack.gateway.place(uuid4(), intent(qty="4"))
    await stack.pump(1)
    await stack.gateway.cancel(uuid4(), "K1", uuid4())

    with pytest.raises(DuplicateEntryBlocked):
        # Sizing "remaining = 2" here would be WRONG (a fill is in flight).
        await stack.gateway.place(uuid4(), intent(key="K1-R", qty="2"))

    await stack.pump(2)                                   # fill 3, then confirm
    stored = await stack.store.load_order("K1")
    assert stored.core.state is OrderState.CANCELED
    remaining = stored.core.remaining_qty                 # NOW conclusive: 1
    assert remaining == 1

    replacement = await stack.gateway.place(
        uuid4(), intent(key="K1-R", qty=str(remaining))
    )
    assert replacement.core.state is OrderState.WORKING
    assert replacement.core.qty == 1


async def test_r1_09_duplicate_entry_prevention(pool):
    """R1.9: two intents targeting the same exposure resolve to one order."""
    stack = build(pool)
    await stack.gateway.place(uuid4(), intent(key="K1"))
    with pytest.raises(DuplicateEntryBlocked):
        await stack.gateway.place(uuid4(), intent(key="K2"))
    assert await pool.fetchval("SELECT count(*) FROM orders") == 1


async def test_r1_10_never_over_exit(pool):
    """R1.10: exits clamp to reconciled position; stacked exits cannot sum
    past it."""
    script = BrokerScript()
    script.fill("K1", qty="3", price="4.20", at_step=1)
    stack = build(pool, script)
    await stack.gateway.place(uuid4(), intent(key="K1", qty="3"))
    await stack.pump(1)

    first = await stack.gateway.place(
        uuid4(),
        intent(key="X1", qty="5", side=Side.SELL,
               authority=Authority.PROTECTIVE_EXIT),
    )
    assert first.core.qty == 3                            # clamped 5 -> 3
    with pytest.raises(NothingToExit):
        await stack.gateway.place(
            uuid4(),
            intent(key="X2", qty="1", side=Side.SELL,
                   authority=Authority.PROTECTIVE_EXIT),
        )


async def test_r1_11_crash_and_restart_recovery(pool):
    """R1.11: kill mid-flight, corrupt projections, restart -> orders, fills,
    holds and positions rebuilt from PostgreSQL and reconciled with broker."""
    script = BrokerScript().on_submit("K1", timeout=True, accept_on_timeout=True)
    script.fill("K1", qty="2", price="4.20", at_step=1)
    stack = build(pool, script)
    await stack.gateway.place(uuid4(), intent(qty="4"))   # -> UNKNOWN (held)
    stack.sim.step()
    stack.sim.take_events()                               # fills never heard

    await pool.execute("UPDATE orders SET filled_qty=0, state='UNKNOWN'")
    await pool.execute("UPDATE positions SET qty=42")
    fresh = stack.restart()

    report = await fresh.recon.startup_recovery()
    assert report.resolved_states["K1"] is OrderState.PARTIAL
    stored = await fresh.store.load_order("K1")
    assert stored.core.filled_qty == 2
    assert stored.core.broker_order_id is not None        # discovered id
    assert await fresh.store.get_position("IDX-OPT") == 2
    assert not await fresh.store.has_unresolved("IDX-OPT")  # hold lifted


async def test_r1_12_broker_wins_position_disagreement(pool):
    """R1.12: local believes CANCEL_PENDING/2-filled; broker says CANCELED
    with 3. Reconciliation adopts broker truth through an auditable event."""
    script = BrokerScript()
    script.fill("K1", qty="2", price="4.20", at_step=1)
    script.fill("K1", qty="1", price="4.30", at_step=2)
    script.on_cancel("K1", confirm_after_steps=2)
    stack = build(pool, script)

    await stack.gateway.place(uuid4(), intent(qty="4"))
    await stack.pump(1)
    await stack.gateway.cancel(uuid4(), "K1", uuid4())
    stack.sim.step()                                      # fill 3: never heard
    stack.sim.step()                                      # confirm: never heard
    stack.sim.take_events()

    local = await stack.store.load_order("K1")
    assert local.core.state is OrderState.CANCEL_PENDING  # stale belief
    assert local.core.filled_qty == 2

    resolved = await stack.recon.reconcile_order("K1")
    assert resolved.core.state is OrderState.CANCELED     # broker's word
    assert resolved.core.filled_qty == 3
    assert await stack.store.get_position("IDX-OPT") == 3


async def test_r1_13_protective_exits_survive_entry_failure(pool):
    """R1.13: with the instrument held by an UNKNOWN entry and new entries
    refused, protection still arms and works end-to-end."""
    script = BrokerScript()
    script.fill("K1", qty="3", price="4.20", at_step=1)
    script.on_submit("K2", timeout=True, accept_on_timeout=True)
    stack = build(pool, script)

    await stack.gateway.place(uuid4(), intent(key="K1", qty="3"))
    await stack.pump(1)
    await stack.gateway.place(uuid4(), intent(key="K2"))  # held

    with pytest.raises(InstrumentHeld):
        await stack.gateway.place(uuid4(), intent(key="K3"))

    report = await stack.protect.ensure_protection()
    assert report.placed[0].core.state is OrderState.WORKING
    assert report.placed[0].core.qty == 3


async def test_r1_14_compound_end_to_end(pool):
    """R1.14 — the full compound scenario, one deterministic test:

    four-contract intent -> submission timeout with no broker id -> broker
    accepted anyway -> two partial fills -> cancel request -> a third fill
    during cancel-pending -> process crash -> restart -> local/broker
    disagreement -> reconciliation to three filled contracts -> and proof of:
    no duplicate entry, correct remaining quantity, no over-exit,
    position-protection restored.
    """
    script = BrokerScript()
    script.on_submit("K1", timeout=True, accept_on_timeout=True)
    script.fill("K1", qty="1", price="4.20", at_step=1)
    script.fill("K1", qty="1", price="4.25", at_step=2)
    script.fill("K1", qty="1", price="4.30", at_step=3)   # during cancel window
    script.on_cancel("K1", confirm_after_steps=2)
    stack = build(pool, script)

    # 1-3: intent, timeout, hidden acceptance
    cid = uuid4()
    stored = await stack.gateway.place(cid, intent(qty="4"))
    assert stored.core.state is OrderState.UNKNOWN
    assert stored.core.broker_order_id is None

    resolved = (await stack.recon.drain(stack.engine.needs_reconcile))[0]
    assert resolved.core.state is OrderState.WORKING       # adopted, not resent
    assert resolved.core.broker_order_id == "SIM-1"

    # 4: two partial fills
    await stack.pump(2)
    assert (await stack.store.load_order("K1")).core.filled_qty == 2

    # 5-6: cancel request; third fill lands inside the window
    await stack.gateway.cancel(uuid4(), "K1", uuid4())
    await stack.pump(1)                                    # fill 3 applied
    assert (await stack.store.load_order("K1")).core.filled_qty == 3

    # 7: CRASH — before the cancel confirmation is ever heard. Projections
    # corrupted; broker-side the cancel confirms while we are down.
    await pool.execute("UPDATE orders SET filled_qty=1, state='WORKING'")
    await pool.execute("UPDATE positions SET qty=99")
    stack.sim.step()                                       # confirm: unheard
    stack.sim.take_events()

    # 8-10: restart -> disagreement -> reconciliation to three
    fresh = stack.restart()
    report = await fresh.recon.startup_recovery()
    assert report.resolved_states["K1"] is OrderState.CANCELED
    final = await fresh.store.load_order("K1")
    assert final.core.filled_qty == 3                      # three contracts
    assert final.core.remaining_qty == 1                   # correct remainder
    assert await fresh.store.get_position("IDX-OPT") == 3
    assert (await fresh.sim.query_positions())["IDX-OPT"] == 3  # convergence

    # 11: no duplicate entry — replaying the original command changes nothing
    replay = await fresh.gateway.place(cid, intent(qty="4"))
    assert replay.core.order_id == final.core.order_id
    assert await pool.fetchval(
        "SELECT count(*) FROM events WHERE kind='SUBMISSION_STARTED'"
    ) == 1

    # 13-14: protection restored for exactly the held position; over-exit
    # is impossible on top of it
    rearm = await fresh.protect.ensure_protection()
    assert len(rearm.placed) == 1
    assert rearm.placed[0].core.qty == 3
    with pytest.raises(NothingToExit):
        await fresh.gateway.place(
            uuid4(),
            intent(key="X-OVER", qty="1", side=Side.SELL,
                   authority=Authority.PROTECTIVE_EXIT),
        )

    # Audit spine: monotonic sequence, trace ids on every event.
    rows = await pool.fetch("SELECT seq, trace_id FROM events ORDER BY seq")
    assert all(r["trace_id"] is not None for r in rows)
    seqs = [r["seq"] for r in rows]
    assert seqs == sorted(seqs)

    # The book is clean; the system may accept commands again.
    assert not await fresh.store.has_unresolved("IDX-OPT")

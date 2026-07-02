"""Ledger integration tests against a real PostgreSQL (testcontainers).

Fixtures (container, fresh-db pool, docker skip) come from sentinel/conftest.py.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

pytest.importorskip("testcontainers.postgres", reason="testcontainers not installed")

from sentinel.domain import (
    Authority,
    BrokerAcked,
    CancelConfirmed,
    CancelRequested,
    EconomicOrderIntent,
    FillReceived,
    OrderState,
    ReconcileResolved,
    ReconcileStarted,
    Side,
    SubmissionStarted,
    SubmissionTimedOut,
)
from sentinel.ledger import FillOutcome, LedgerStore, apply_migrations


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


def fill(qty: str, exec_id: str, price: str = "4.20") -> FillReceived:
    return FillReceived(exec_id=exec_id, qty=Decimal(qty), price=Decimal(price))


TRACE = uuid4()


# ------------------------------------------------------------- R1.2 commands


async def test_duplicate_command_is_rejected_durably(pool):
    store = LedgerStore(pool)
    cid = uuid4()
    assert await store.record_command(cid, TRACE, "PLACE", {"k": "v"}) is True
    assert await store.record_command(cid, TRACE, "PLACE", {"k": "v"}) is False


# --------------------------------------------------------- R1.1 durable intent


async def test_create_order_is_idempotent(pool):
    store = LedgerStore(pool)
    a = await store.create_order(intent(key="SAME"))
    b = await store.create_order(intent(key="SAME"))
    assert a.core.order_id == b.core.order_id
    assert b.core.state is OrderState.CREATED


async def test_migrations_are_idempotent(pool):
    async with pool.acquire() as conn:
        assert await apply_migrations(conn) == []  # second run applies nothing


# ------------------------------------------------- atomic event + projection


async def test_event_and_projection_commit_together(pool):
    store = LedgerStore(pool)
    o = await store.create_order(intent())
    o = await store.apply_event(o, SubmissionStarted(), TRACE)
    o = await store.apply_event(o, BrokerAcked(broker_order_id="B1"), TRACE)

    reloaded = await store.load_order("K1")
    assert reloaded.core.state is OrderState.WORKING
    assert reloaded.core.broker_order_id == "B1"
    events = await pool.fetch("SELECT kind FROM events ORDER BY seq")
    assert [e["kind"] for e in events] == [
        "INTENT_PERSISTED",
        "SUBMISSION_STARTED",
        "BROKER_ACKED",
    ]


# ------------------------------------------------------ R1.5/R1.7 fill dedup


async def test_duplicate_fill_is_exactly_once(pool):
    store = LedgerStore(pool)
    o = await store.create_order(intent())
    o = await store.apply_event(o, SubmissionStarted(), TRACE)
    o = await store.apply_event(o, BrokerAcked(broker_order_id="B1"), TRACE)

    o = await store.apply_event(o, fill("2", "EXEC-1"), TRACE)
    assert o.core.filled_qty == 2

    # Same execution delivered again (reconnect replay): full no-op.
    result = await store.apply_event(o, fill("2", "EXEC-1"), TRACE)
    assert result is FillOutcome.DUPLICATE

    reloaded = await store.load_order("K1")
    assert reloaded.core.filled_qty == 2
    assert await store.get_position("IDX-OPT") == 2
    n_fill_events = await pool.fetchval(
        "SELECT count(*) FROM events WHERE kind = 'FILL_APPLIED'"
    )
    assert n_fill_events == 1


async def test_position_is_side_signed(pool):
    store = LedgerStore(pool)
    buy = await store.create_order(intent(key="B", qty="3", side=Side.BUY))
    buy = await store.apply_event(buy, SubmissionStarted(), TRACE)
    buy = await store.apply_event(buy, BrokerAcked(broker_order_id="B1"), TRACE)
    await store.apply_event(buy, fill("3", "E-B"), TRACE)

    sell = await store.create_order(
        intent(key="S", qty="1", side=Side.SELL, authority=Authority.PROTECTIVE_EXIT)
    )
    sell = await store.apply_event(sell, SubmissionStarted(), TRACE)
    sell = await store.apply_event(sell, BrokerAcked(broker_order_id="B2"), TRACE)
    await store.apply_event(sell, fill("1", "E-S"), TRACE)

    assert await store.get_position("IDX-OPT") == 2  # +3 - 1


# ------------------------------------------------------------ R1.11 rebuild


async def test_rebuild_reproduces_projections_exactly(pool):
    """Run the 3.14-shaped lifecycle, corrupt every projection, rebuild from
    the ledger, and get identical state back. The ledger wins."""
    store = LedgerStore(pool)
    o = await store.create_order(intent())  # 4 contracts
    o = await store.apply_event(o, SubmissionStarted(), TRACE)
    o = await store.apply_event(o, SubmissionTimedOut(), TRACE)          # UNKNOWN
    o = await store.apply_event(o, ReconcileStarted(cause="timeout"), TRACE)
    o = await store.apply_event(                                          # found working
        o,
        ReconcileResolved(
            resolved_state=OrderState.WORKING,
            broker_order_id="B-FOUND",
            filled_qty=Decimal(0),
        ),
        TRACE,
    )
    o = await store.apply_event(o, fill("1", "E1"), TRACE)
    o = await store.apply_event(o, fill("1", "E2", price="4.25"), TRACE)
    o = await store.apply_event(o, CancelRequested(), TRACE)
    o = await store.apply_event(o, fill("1", "E3", price="4.30"), TRACE)  # during cancel
    o = await store.apply_event(o, CancelConfirmed(), TRACE)

    before = await store.load_order("K1")
    pos_before = await store.get_position("IDX-OPT")
    assert before.core.state is OrderState.CANCELED
    assert before.core.filled_qty == 3 and pos_before == 3

    # Corrupt projections thoroughly — the crash-recovery worst case.
    await pool.execute("UPDATE orders SET state='WORKING', filled_qty=0")
    await pool.execute("DELETE FROM fills WHERE exec_id='E3'")
    await pool.execute("UPDATE positions SET qty = 99")

    replayed = await store.rebuild_projections()
    assert replayed == 10  # intent + 9 lifecycle events

    after = await store.load_order("K1")
    assert after.core.state is OrderState.CANCELED
    assert after.core.filled_qty == 3
    assert after.core.broker_order_id == "B-FOUND"
    assert await store.get_position("IDX-OPT") == 3
    assert await pool.fetchval("SELECT count(*) FROM fills") == 3


async def test_rebuild_is_deterministic(pool):
    store = LedgerStore(pool)
    o = await store.create_order(intent(qty="2"))
    o = await store.apply_event(o, SubmissionStarted(), TRACE)
    o = await store.apply_event(o, BrokerAcked(broker_order_id="B1"), TRACE)
    await store.apply_event(o, fill("2", "E1"), TRACE)

    await store.rebuild_projections()
    first = await store.load_order("K1")
    await store.rebuild_projections()
    second = await store.load_order("K1")
    assert first == second


# ----------------------------------------------------- nonterminal recovery


async def test_load_nonterminal_orders_finds_crash_survivors(pool):
    """Startup reconciliation's worklist: everything not conclusively done."""
    store = LedgerStore(pool)
    done = await store.create_order(intent(key="DONE", qty="1"))
    done = await store.apply_event(done, SubmissionStarted(), TRACE)
    done = await store.apply_event(done, BrokerAcked(broker_order_id="B1"), TRACE)
    await store.apply_event(done, fill("1", "E1"), TRACE)                # FILLED

    stuck = await store.create_order(intent(key="STUCK", qty="1"))
    stuck = await store.apply_event(stuck, SubmissionStarted(), TRACE)
    await store.apply_event(stuck, SubmissionTimedOut(), TRACE)          # UNKNOWN

    fresh = await store.create_order(intent(key="FRESH", qty="1"))       # CREATED

    survivors = {s.core.client_order_id for s in await store.load_nonterminal_orders()}
    assert survivors == {"STUCK", "FRESH"}

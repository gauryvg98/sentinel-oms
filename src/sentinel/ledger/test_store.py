"""Ledger integration tests against a real PostgreSQL (shared container).

Fixtures (container, fresh-db pool, docker skip) come from sentinel/conftest.py.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest


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


async def test_limit_price_is_persisted_surfaced_and_survives_rebuild(pool):
    """A limit order's price is durable metadata: readable via recent_orders /
    open_entry and reproduced from the event log on rebuild (it rides in the
    INTENT_PERSISTED payload, not through transition())."""
    store = LedgerStore(pool)
    lim = intent(key="LIM")  # intent() sets limit_price = 4.20
    await store.create_order(lim)
    mkt = EconomicOrderIntent(
        intent_id=uuid4(), idempotency_key="MKT", instrument="OTHER-OPT",
        side=Side.BUY, qty=Decimal("1"), limit_price=None,
        authority=Authority.ENTRY, trace_id=uuid4(),
    )
    await store.create_order(mkt)

    by_key = {o["key"]: o for o in await store.recent_orders(10)}
    assert by_key["LIM"]["limit_price"] == "4.2"
    assert by_key["MKT"]["limit_price"] is None       # market carries no price

    assert (await store.open_entry("IDX-OPT"))["limit_price"] == Decimal("4.20")
    assert (await store.open_entry("OTHER-OPT"))["limit_price"] is None

    await store.rebuild_projections()                  # re-derive from the log
    after = {o["key"]: o for o in await store.recent_orders(10)}
    assert after["LIM"]["limit_price"] == "4.2"        # restored from payload
    assert after["MKT"]["limit_price"] is None


async def test_recent_orders_ordered_by_seq_not_reset_prone_updated_at(pool):
    """recent_orders must rank by last_event_seq (authoritative, reproduced on
    rebuild), NOT updated_at (DEFAULT now(), reset by rebuild's TRUNCATE). Else
    a recovery collapses every updated_at to one instant and the working-list
    LIMIT window returns an arbitrary slice, dropping live orders."""
    store = LedgerStore(pool)
    a = await store.create_order(intent(key="A"))
    b = await store.create_order(intent(key="B"))
    b = await store.apply_event(b, SubmissionStarted(), TRACE)   # B: higher seq

    # Poison time so A LOOKS most-recent-by-clock; seq ordering must ignore it.
    await pool.execute(
        "UPDATE orders SET updated_at = now() + interval '1 day' "
        "WHERE client_order_id = 'A'"
    )
    keys = [o["key"] for o in await store.recent_orders(10)]
    assert keys.index("B") < keys.index("A")        # B (higher seq) still first

    await store.rebuild_projections()                # collapses every updated_at
    keys2 = [o["key"] for o in await store.recent_orders(10)]
    assert keys2.index("B") < keys2.index("A")       # stable across recovery


async def test_marker_time_is_event_time_and_survives_rebuild(pool):
    """recent_fills must place markers by the append-only event log's time,
    not the fills projection's occurred_at. The projection is TRUNCATEd and
    re-inserted on every rebuild (occurred_at -> now()), so if markers used it,
    every fill would jump to 'now' after a reboot and stack on one candle."""
    store = LedgerStore(pool)
    o = await store.create_order(intent())
    o = await store.apply_event(o, SubmissionStarted(), TRACE)
    o = await store.apply_event(o, BrokerAcked(broker_order_id="B1"), TRACE)
    o = await store.apply_event(o, fill("2", "EXEC-1"), TRACE)

    before = await store.recent_fills("IDX-OPT")
    assert len(before) == 1 and before[0]["side"] == "BUY"
    t_before = before[0]["t"]

    # The projection's own occurred_at is the reset-prone one — prove the
    # marker time does NOT track it by moving it far into the future.
    await pool.execute("UPDATE fills SET occurred_at = now() + interval '5 days'")
    after_poison = await store.recent_fills("IDX-OPT")
    assert after_poison[0]["t"] == t_before          # unaffected: uses event time

    # And it survives a full rebuild (which truncates + re-inserts fills).
    await store.rebuild_projections()
    after_rebuild = await store.recent_fills("IDX-OPT")
    assert after_rebuild[0]["t"] == t_before          # stable across reboot


async def test_recent_fills_since_bounds_by_event_time(pool):
    store = LedgerStore(pool)
    o = await store.create_order(intent())
    o = await store.apply_event(o, SubmissionStarted(), TRACE)
    o = await store.apply_event(o, BrokerAcked(broker_order_id="B1"), TRACE)
    o = await store.apply_event(o, fill("2", "EXEC-1"), TRACE)

    t = (await store.recent_fills("IDX-OPT"))[0]["t"]
    assert len(await store.recent_fills("IDX-OPT", since=t - 60)) == 1   # in window
    assert len(await store.recent_fills("IDX-OPT", since=t + 60)) == 0   # after it


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


# ----------------------------------------------------- starting-equity baseline


async def test_starting_equity_absent_on_fresh_db(pool):
    """A fresh (or wiped) ledger has no baseline -> None, so the account snapshot
    falls back to ledger P&L until first boot captures it."""
    store = LedgerStore(pool)
    assert await store.get_starting_equity() is None


async def test_starting_equity_captured_once_and_stable(pool):
    """First capture persists the baseline exactly; a second call (restart) is a
    no-op that keeps the ORIGINAL — the anchor never drifts with the account."""
    store = LedgerStore(pool)
    stored = await store.set_starting_equity_if_absent(Decimal("10000.00"))
    assert stored == Decimal("10000.00")
    assert await store.get_starting_equity() == Decimal("10000.00")
    # A later boot at a different equity must NOT overwrite the baseline.
    again = await store.set_starting_equity_if_absent(Decimal("9748.13"))
    assert again == Decimal("10000.00")
    assert await store.get_starting_equity() == Decimal("10000.00")


async def test_starting_equity_preserves_cents(pool):
    """Persisted via integer cents in the BIGINT checkpoint column — round-trips
    exactly, so the wallet-anchored net isn't off by fractions of a dollar."""
    store = LedgerStore(pool)
    await store.set_starting_equity_if_absent(Decimal("9748.13"))
    baseline = await store.get_starting_equity()
    assert baseline == Decimal("9748.13")
    # equity − starting_equity is the wallet-anchored net; verify the arithmetic.
    equity = Decimal("9497.62")
    assert equity - baseline == Decimal("-250.51")

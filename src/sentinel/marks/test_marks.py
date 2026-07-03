"""Mark feed + P&L tests: determinism, average-cost accounting, shorts, flips."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from sentinel.domain import (
    Authority,
    BrokerAcked,
    EconomicOrderIntent,
    FillReceived,
    Side,
    SubmissionStarted,
)
from sentinel.ledger import LedgerStore
from sentinel.marks import SimMarkFeed
from sentinel.marks.pnl import compute_pnl

TRACE = uuid4()


# ------------------------------------------------------------------ sim feed


def test_seeded_feed_is_deterministic():
    def run():
        feed = SimMarkFeed(seed=7)
        feed.add_instrument("IDX-OPT", "4.20")
        out = []
        for _ in range(10):
            feed.tick()
            out.append(feed.latest("IDX-OPT").price)
        return out

    assert run() == run()


def test_feed_tracks_freshness():
    feed = SimMarkFeed(seed=1)
    feed.add_instrument("IDX-OPT", "4.20")
    feed.tick(dt=2.5)
    assert feed.latest("IDX-OPT").ts == 2.5


# ---------------------------------------------------------------------- P&L


async def _filled_order(store, key, side, qty, price, execs):
    o = await store.create_order(
        EconomicOrderIntent(
            intent_id=uuid4(), idempotency_key=key, instrument="IDX-OPT",
            side=side, qty=Decimal(qty), limit_price=None,
            authority=Authority.ENTRY if side is Side.BUY
            else Authority.PROTECTIVE_EXIT,
            trace_id=TRACE,
        )
    )
    o = await store.apply_event(o, SubmissionStarted(), TRACE)
    o = await store.apply_event(o, BrokerAcked(broker_order_id=f"B-{key}"), TRACE)
    for i, (fqty, fprice) in enumerate(execs):
        o = await store.apply_event(
            o, FillReceived(exec_id=f"{key}-E{i}", qty=Decimal(fqty),
                            price=Decimal(fprice)),
            TRACE,
        )
    return o


async def test_avg_cost_and_realized_long(pool):
    store = LedgerStore(pool)
    # Buy 2 @ 4.00 and 2 @ 4.20 -> avg 4.10; sell 3 @ 4.50 -> realize 1.20
    await _filled_order(store, "B1", Side.BUY, "4",
                        None, [("2", "4.00"), ("2", "4.20")])
    await _filled_order(store, "S1", Side.SELL, "3", None, [("3", "4.50")])

    pnl = (await compute_pnl(pool))["IDX-OPT"]
    assert pnl.position == 1
    assert pnl.avg_cost == Decimal("4.10")
    assert pnl.realized == Decimal("1.20")            # (4.50-4.10)*3


async def test_unrealized_against_mark(pool):
    store = LedgerStore(pool)
    await _filled_order(store, "B1", Side.BUY, "2", None, [("2", "4.00")])
    feed = SimMarkFeed(seed=1)
    feed.add_instrument("IDX-OPT", "5.00")

    pnl = (await compute_pnl(pool, feed))["IDX-OPT"]
    assert pnl.mark == Decimal("5.00")
    assert pnl.unrealized == Decimal("2.00")          # (5.00-4.00)*2


async def test_short_realizes_symmetrically(pool):
    store = LedgerStore(pool)
    # Short 2 @ 4.50, cover 2 @ 4.20 -> realized +0.60, flat.
    await _filled_order(store, "S1", Side.SELL, "2", None, [("2", "4.50")])
    await _filled_order(store, "B1", Side.BUY, "2", None, [("2", "4.20")])

    pnl = (await compute_pnl(pool))["IDX-OPT"]
    assert pnl.position == 0
    assert pnl.realized == Decimal("0.60")
    assert pnl.avg_cost is None


async def test_flip_through_zero_reopens_at_new_price(pool):
    store = LedgerStore(pool)
    # Long 1 @ 4.00; sell 3 @ 4.40 -> realize 0.40, now short 2 @ 4.40.
    await _filled_order(store, "B1", Side.BUY, "1", None, [("1", "4.00")])
    await _filled_order(store, "S1", Side.SELL, "3", None, [("3", "4.40")])

    pnl = (await compute_pnl(pool))["IDX-OPT"]
    assert pnl.position == -2
    assert pnl.realized == Decimal("0.40")
    assert pnl.avg_cost == Decimal("4.40")

"""Bot.card display tests — the net-% (net_roe) fix.

net_roe is a return on CAPITAL, not on the current (possibly shrinking/closed)
position: realized P&L doesn't belong to whatever the open position happens to be
right now. Dividing all-time realized+unrealized by the current cost basis read
absurd (a small position after a big realized run showed +74%). The fix divides
by eq_share (the bot's pool share == its capital). roe (running %) stays on the
position cost basis — "how's the OPEN position doing".

Pure: a bare stub carries exactly the attributes Bot.card touches; _pnl is
stubbed async so there's no DB.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from sentinel.ui.server import Bot


class _PnL:
    def __init__(self, position, avg_cost, realized, unrealized):
        self.position = position
        self.avg_cost = avg_cost
        self.realized = realized
        self.unrealized = unrealized


def _bot(pnl, eq_share):
    """A minimal object that answers exactly what Bot.card reads, with the real
    Bot.card / _spark / _liq_view methods bound to it."""
    market = SimpleNamespace(
        latest=lambda sym: SimpleNamespace(price=Decimal("100")),
        candles=[], price_age_s=0, interval="1m", symbol="BTCUSDT")
    runner = SimpleNamespace(snapshot=lambda: {
        "running": True, "stance": "LONG", "last_action": None,
        "name": "sma", "interval": "1m"})

    async def _recent_orders(n, sym):
        return []

    app = SimpleNamespace(store=SimpleNamespace(recent_orders=_recent_orders))
    stub = SimpleNamespace(
        symbol="BTCUSDT", market=market, runner=runner, app=app,
        strategies={"sma": None}, size={"usdt": Decimal("15")},
        current={"name": "sma"}, venue=SimpleNamespace(leverage=1),
        liq_price=None, equity_fn=(lambda: eq_share) if eq_share is not None else None,
        spec=SimpleNamespace(lot_step=Decimal("0.001"), price_tick=Decimal("0.1"),
                             min_qty=Decimal("0.001"), min_notional=Decimal("5")))

    async def _pnl():
        return pnl

    stub._pnl = _pnl
    stub._spark = Bot._spark.__get__(stub, Bot)
    stub._liq_view = Bot._liq_view.__get__(stub, Bot)
    stub.card = Bot.card.__get__(stub, Bot)
    return stub


async def test_net_roe_is_return_on_capital_not_shrinking_position():
    # A big realized run on a now-TINY position: basis (position cost) is small,
    # capital (eq_share) is large. net_roe must be net/capital, NOT net/basis.
    pnl = _PnL(position=Decimal("0.001"), avg_cost=Decimal("100"),
               realized=Decimal("740"), unrealized=Decimal("0"))
    card = await _bot(pnl, eq_share=Decimal("1000")).card()
    # net = 740 on 1000 capital = +74% on capital (sane), NOT 740/0.10 = +740000%.
    assert card["net"] == "740.00"
    assert card["net_roe"] == "74.00"


async def test_running_roe_stays_on_position_basis():
    # roe (running %) is unrealized / current cost basis — unchanged by the fix.
    pnl = _PnL(position=Decimal("1"), avg_cost=Decimal("100"),
               realized=Decimal("0"), unrealized=Decimal("10"))
    card = await _bot(pnl, eq_share=Decimal("1000")).card()
    assert card["roe"] == "10.00"                 # 10 unrealized / 100 basis
    assert card["net_roe"] == "1.00"              # 10 net / 1000 capital


async def test_net_roe_none_when_no_capital_view():
    # eq_share None (fixed-notional mode) or 0 -> net_roe None, never a divide-by-zero.
    pnl = _PnL(position=Decimal("1"), avg_cost=Decimal("100"),
               realized=Decimal("50"), unrealized=Decimal("0"))
    card = await _bot(pnl, eq_share=None).card()
    assert card["net_roe"] is None
    card0 = await _bot(pnl, eq_share=Decimal("0")).card()
    assert card0["net_roe"] is None

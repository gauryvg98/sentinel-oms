"""MarketData.latest() staleness gate — don't mark P&L against a dead price."""

from __future__ import annotations

import time
from decimal import Decimal

from sentinel.ui.market import MAX_MARK_AGE_S, MarketData


def _mkt(price="62500", age_s=0.0):
    m = MarketData("BTCUSDT")
    m._price = Decimal(price)                 # noqa: SLF001
    m._price_ts = time.time() - age_s         # noqa: SLF001
    return m


def test_fresh_mark_is_returned():
    m = _mkt(age_s=1.0)
    mark = m.latest("BTCUSDT")
    assert mark is not None and mark.price == Decimal("62500")


def test_stale_mark_is_none():
    m = _mkt(age_s=MAX_MARK_AGE_S + 5)
    assert m.latest("BTCUSDT") is None        # dead feed -> no mark -> no unrealized


def test_wrong_symbol_or_no_price_is_none():
    assert _mkt().latest("ETHUSDT") is None
    assert MarketData("BTCUSDT").latest("BTCUSDT") is None   # never priced

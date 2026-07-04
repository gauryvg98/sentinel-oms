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


def test_book_sets_bid_ask_and_a_mid_mark():
    m = MarketData("BTCUSDT")
    m._ingest_book({"b": "62490.00", "a": "62510.00"})       # noqa: SLF001
    assert m.best_bid() == Decimal("62490.00")
    assert m.best_ask() == Decimal("62510.00")
    mark = m.latest("BTCUSDT")
    assert mark is not None and mark.price == Decimal("62500.00")   # mid, fresh


def test_stale_book_yields_no_bid_ask():
    m = MarketData("BTCUSDT")
    m._ingest_book({"b": "1", "a": "2"})                     # noqa: SLF001
    m._book_ts = time.time() - MAX_MARK_AGE_S - 5            # noqa: SLF001
    assert m.best_bid() is None and m.best_ask() is None      # don't peg on a dead book


def test_malformed_book_is_ignored():
    m = MarketData("BTCUSDT")
    m._ingest_book({"b": "62490.00"})                        # noqa: SLF001 — no ask
    assert m.best_bid() is None                               # nothing committed


def test_kline_keeps_the_mark_alive_without_a_book():
    m = MarketData("BTCUSDT")
    m._ingest_kline({"t": 1_700_000_000_000, "o": "1", "h": "2",  # noqa: SLF001
                     "l": "1", "c": "62345.00"})
    assert m.latest("BTCUSDT").price == Decimal("62345.00")
    assert len(m.candles) == 1

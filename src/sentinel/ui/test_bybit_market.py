"""Bybit market-data ingest tests: ticker -> bid/ask/mid, kline append/update,
delta-merge of partial ticker frames."""

from __future__ import annotations

from decimal import Decimal

from sentinel.ui.bybit_market import BybitBarFeed, BybitMarketData


def test_ticker_sets_bid_ask_and_mid_mark():
    m = BybitMarketData("BTCUSDT")
    m._ingest({"topic": "tickers.BTCUSDT",
               "data": {"bid1Price": "100", "ask1Price": "102"}})
    assert m.best_bid() == Decimal("100") and m.best_ask() == Decimal("102")
    assert m.latest("BTCUSDT").price == Decimal("101")           # mid


def test_delta_frame_merges_only_the_changed_side():
    m = BybitMarketData("BTCUSDT")
    m._ingest({"topic": "tickers.BTCUSDT",
               "data": {"bid1Price": "100", "ask1Price": "102"}})
    m._ingest({"topic": "tickers.BTCUSDT", "data": {"ask1Price": "103"}})  # ask only
    assert m.best_bid() == Decimal("100") and m.best_ask() == Decimal("103")


def test_kline_appends_new_bar_then_updates_the_forming_one():
    m = BybitMarketData("BTCUSDT")
    m._ingest({"topic": "kline.1.BTCUSDT",
               "data": [{"start": 60_000, "open": "1", "high": "2", "low": "0.5",
                         "close": "1.5"}]})
    m._ingest({"topic": "kline.1.BTCUSDT",
               "data": [{"start": 120_000, "open": "1.5", "high": "3", "low": "1.4",
                         "close": "2.9"}]})
    assert [c["t"] for c in m.candles] == [60, 120]              # seconds
    m._ingest({"topic": "kline.1.BTCUSDT",
               "data": [{"start": 120_000, "open": "1.5", "high": "3.2", "low": "1.4",
                         "close": "3.1"}]})
    assert len(m.candles) == 2 and m.candles[-1]["c"] == 3.1     # updated in place


def test_barfeed_ingest():
    bf = BybitBarFeed("BTCUSDT", "5m")
    bf._ingest({"start": 300_000, "open": "1", "high": "2", "low": "1", "close": "1.8"})
    assert bf.candles == [{"t": 300, "o": 1.0, "h": 2.0, "l": 1.0, "c": 1.8}]

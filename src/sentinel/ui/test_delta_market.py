"""Delta market-data ingest tests: v2/ticker -> bid/ask/mid, candlestick
append/update (microsecond start times -> seconds), partial-quote merge."""

from __future__ import annotations

from decimal import Decimal

from sentinel.ui.delta_market import DeltaBarFeed, DeltaMarketData


def test_ticker_sets_bid_ask_and_mid_mark():
    m = DeltaMarketData("BTCUSD")
    m._ingest({"type": "v2/ticker", "symbol": "BTCUSD",
               "quotes": {"best_bid": "100", "best_ask": "102"}})
    assert m.best_bid() == Decimal("100") and m.best_ask() == Decimal("102")
    assert m.latest("BTCUSD").price == Decimal("101")            # mid


def test_partial_quote_frame_merges_only_the_changed_side():
    m = DeltaMarketData("BTCUSD")
    m._ingest({"type": "v2/ticker",
               "quotes": {"best_bid": "100", "best_ask": "102"}})
    m._ingest({"type": "v2/ticker", "quotes": {"best_ask": "103"}})  # ask only
    assert m.best_bid() == Decimal("100") and m.best_ask() == Decimal("103")


def test_candlestick_appends_new_bar_then_updates_the_forming_one():
    m = DeltaMarketData("BTCUSD")
    m._ingest({"type": "candlestick_1m", "candle_start_time": 60_000_000,
               "open": "1", "high": "2", "low": "0.5", "close": "1.5"})
    m._ingest({"type": "candlestick_1m", "candle_start_time": 120_000_000,
               "open": "1.5", "high": "3", "low": "1.4", "close": "2.9"})
    assert [c["t"] for c in m.candles] == [60, 120]              # µs -> seconds
    m._ingest({"type": "candlestick_1m", "candle_start_time": 120_000_000,
               "open": "1.5", "high": "3.2", "low": "1.4", "close": "3.1"})
    assert len(m.candles) == 2 and m.candles[-1]["c"] == 3.1     # updated in place


def test_subscription_ack_frame_is_ignored():
    m = DeltaMarketData("BTCUSD")
    m._ingest({"type": "candlestick_1m", "success": True})       # no candle fields
    assert m.candles == []


def test_barfeed_ingest():
    bf = DeltaBarFeed("BTCUSD", "5m")
    bf._ingest({"type": "candlestick_5m", "candle_start_time": 300_000_000,
                "open": "1", "high": "2", "low": "1", "close": "1.8"})
    assert bf.candles == [{"t": 300, "o": 1.0, "h": 2.0, "l": 1.0, "c": 1.8}]

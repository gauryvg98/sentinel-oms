"""MarketHub dispatch routing — pure, no network."""

from decimal import Decimal

from sentinel.ui.bars import BarFeed
from sentinel.ui.market import MarketData
from sentinel.ui.market_hub import MarketHub


def _md(sym: str, iv: str = "1m") -> MarketData:
    md = MarketData(sym)
    md.interval = iv
    md.on_change = None
    return md


async def test_dispatch_routes_book_and_kline_to_both_feeds():
    hub = MarketHub("wss://x")
    md, bf = _md("BTCUSDT"), BarFeed("BTCUSDT", "1m")
    hub.register(md, bf)
    # @bookTicker -> mark/book on the MarketData, returns md so caller bumps
    r = hub._dispatch("btcusdt@bookTicker", {"b": "100.0", "a": "100.2"})
    assert r is md and md.best_bid() == Decimal("100.0")
    # @kline_1m with md.interval == bf.interval -> BOTH feeds get the candle
    k = {"t": 1_700_000_000_000, "o": "1", "h": "2", "l": "0.5", "c": "1.5"}
    r = hub._dispatch("btcusdt@kline_1m", {"k": k})
    assert r is md and len(md.candles) == 1 and len(bf.candles) == 1


async def test_kline_feeds_only_bars_when_chart_interval_differs():
    hub = MarketHub("wss://x")
    md, bf = _md("ETHUSDC", iv="5m"), BarFeed("ETHUSDC", "1m")
    hub.register(md, bf)
    k = {"t": 1_700_000_000_000, "o": "1", "h": "2", "l": "0.5", "c": "1.5"}
    # a 1m kline is the STRATEGY bar, not the 5m chart -> bars only, no md bump
    r = hub._dispatch("ethusdc@kline_1m", {"k": k})
    assert r is None and len(bf.candles) == 1 and len(md.candles) == 0


async def test_wanted_streams_dedupe_shared_kline():
    hub = MarketHub("wss://x")
    hub.register(_md("BTCUSDT", "1m"), BarFeed("BTCUSDT", "1m"))
    # md@kline_1m and bf@kline_1m collapse to one stream
    assert hub._wanted() == ["btcusdt@bookTicker", "btcusdt@kline_1m"]


async def test_unknown_symbol_is_ignored():
    hub = MarketHub("wss://x")
    hub.register(_md("BTCUSDT"), BarFeed("BTCUSDT", "1m"))
    assert hub._dispatch("dogeusdt@bookTicker", {"b": "1", "a": "2"}) is None

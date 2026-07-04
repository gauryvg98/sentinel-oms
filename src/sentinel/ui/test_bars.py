"""BarFeed candle ingest — the strategy's fixed-interval clock."""

from __future__ import annotations

from sentinel.ui.bars import BarFeed


def _k(t_ms, o, h, l, c):
    return {"t": t_ms, "o": str(o), "h": str(h), "l": str(l), "c": str(c)}


def test_ingest_appends_new_bars_and_updates_the_forming_one():
    bf = BarFeed("BTCUSDT", "5m")
    bf._ingest(_k(300_000, 1, 2, 0.5, 1.5))          # noqa: SLF001
    bf._ingest(_k(600_000, 1.5, 3, 1.4, 2.9))        # a new bar
    assert [c["t"] for c in bf.candles] == [300, 600]  # seconds, appended

    bf._ingest(_k(600_000, 1.5, 3.2, 1.4, 3.1))      # same t -> update in place
    assert len(bf.candles) == 2 and bf.candles[-1]["c"] == 3.1

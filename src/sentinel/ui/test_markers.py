"""Pure tests for chart-marker consolidation — one arrow per (candle, side),
carrying the aggregate + the individual fills for the hover tooltip."""

from __future__ import annotations

from sentinel.ui.server import consolidate_markers, interval_seconds


def test_interval_seconds():
    assert interval_seconds("1m") == 60
    assert interval_seconds("4h") == 4 * 3600
    assert interval_seconds("1d") == 86400
    assert interval_seconds("garbage") == 60      # safe default


def test_fills_in_one_candle_collapse_to_one_marker():
    # Three buys inside the same 4h candle (which opens at 0).
    fills = [
        {"t": 100, "side": "BUY", "qty": "0.001", "price": "100"},
        {"t": 200, "side": "BUY", "qty": "0.001", "price": "200"},
        {"t": 300, "side": "BUY", "qty": "0.002", "price": "150"},
    ]
    out = consolidate_markers(fills, interval_seconds("4h"))
    assert len(out) == 1
    m = out[0]
    assert m["t"] == 0 and m["side"] == "BUY" and m["n"] == 3
    assert m["qty"] == "0.004"
    # VWAP = (0.001*100 + 0.001*200 + 0.002*150) / 0.004 = 150
    assert m["price"] == "150.00"
    assert [d["t"] for d in m["detail"]] == [100, 200, 300]   # sorted


def test_buys_and_sells_split_same_candle():
    fills = [
        {"t": 10, "side": "BUY", "qty": "0.001", "price": "100"},
        {"t": 20, "side": "SELL", "qty": "0.001", "price": "110"},
    ]
    out = consolidate_markers(fills, 60)
    sides = {m["side"] for m in out}
    assert sides == {"BUY", "SELL"} and len(out) == 2


def test_across_candles_stay_separate_and_sorted():
    fills = [
        {"t": 130, "side": "BUY", "qty": "1", "price": "1"},   # candle 120
        {"t": 10, "side": "BUY", "qty": "1", "price": "1"},    # candle 0
    ]
    out = consolidate_markers(fills, 60)
    assert [m["t"] for m in out] == [0, 120]                   # ascending


def test_detail_capped_but_count_is_full():
    fills = [{"t": i, "side": "BUY", "qty": "1", "price": "1"} for i in range(40)]
    out = consolidate_markers(fills, 3600)
    assert out[0]["n"] == 40 and len(out[0]["detail"]) == 25   # full n, capped list

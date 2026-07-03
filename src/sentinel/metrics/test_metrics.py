"""Metrics registry tests."""

from sentinel.metrics import MetricsRegistry


def test_counters_and_rates():
    m = MetricsRegistry()
    for _ in range(5):
        m.inc("orders")
    snap = m.snapshot()
    assert snap["counters"]["orders"] == 5
    assert snap["rates"]["orders"] > 0


def test_timer_records_latency_percentiles():
    m = MetricsRegistry()
    for _ in range(20):
        with m.timer("apply_ms"):
            pass
    pcts = m.percentiles("apply_ms")
    assert set(pcts) == {"p50", "p95", "p99"}
    assert pcts["p50"] <= pcts["p99"]


def test_gauge_reflects_latest_value():
    m = MetricsRegistry()
    m.gauge("queue_depth", 3)
    m.gauge("queue_depth", 7)
    assert m.snapshot()["gauges"]["queue_depth"] == 7


def test_empty_registry_snapshots_cleanly():
    assert MetricsRegistry().snapshot() == {
        "counters": {}, "gauges": {}, "rates": {}, "latency": {}
    }

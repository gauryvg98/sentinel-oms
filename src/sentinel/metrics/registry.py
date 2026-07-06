"""Tiny in-process metrics: no external dependency, one JSON-able snapshot.

Counters (monotonic), gauges (set), and ring-buffer histograms with
percentiles + a rolling per-second rate. Enough for the terminal; a
Prometheus exporter can wrap the same registry later.
"""

from __future__ import annotations

import time
from collections import deque


class MetricsRegistry:
    def __init__(self, histogram_size: int = 2048) -> None:
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, deque[float]] = {}
        self._hist_total: dict[str, int] = {}       # monotonic sample count
        self._rates: dict[str, deque[float]] = {}   # event timestamps
        self._size = histogram_size

    # ------------------------------------------------------------- writers

    def inc(self, name: str, by: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + by
        self._rates.setdefault(name, deque(maxlen=4096)).append(time.monotonic())

    def gauge(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        self._histograms.setdefault(name, deque(maxlen=self._size)).append(value)
        self._hist_total[name] = self._hist_total.get(name, 0) + 1

    class _Timer:
        def __init__(self, registry: "MetricsRegistry", name: str) -> None:
            self._registry, self._name = registry, name

        def __enter__(self):
            self._t0 = time.perf_counter()
            return self

        def __exit__(self, *exc):
            self._registry.observe(
                self._name, (time.perf_counter() - self._t0) * 1000.0  # ms
            )

    def timer(self, name: str) -> "MetricsRegistry._Timer":
        return self._Timer(self, name)

    # ------------------------------------------------------------- readers

    def rate_per_sec(self, name: str, window: float = 10.0) -> float:
        stamps = self._rates.get(name)
        if not stamps:
            return 0.0
        cutoff = time.monotonic() - window
        recent = sum(1 for t in stamps if t >= cutoff)
        return round(recent / window, 2)

    def percentiles(self, name: str) -> dict[str, float]:
        values = sorted(self._histograms.get(name, ()))
        if not values:
            return {}

        def pct(p: float) -> float:
            idx = min(len(values) - 1, int(p * len(values)))
            return round(values[idx], 2)

        return {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)}

    def latency_detail(self, name: str) -> dict | None:
        """Everything a status card needs for one timed op: percentiles, avg,
        max, the monotonic sample count, and a recent-values sparkline. None
        until the op has been observed at least once."""
        vals = self._histograms.get(name)
        if not vals:
            return None
        d = dict(self.percentiles(name))
        d["avg"] = round(sum(vals) / len(vals), 2)
        d["max"] = round(max(vals), 2)
        d["count"] = self._hist_total.get(name, len(vals))
        d["spark"] = [round(v, 1) for v in list(vals)[-48:]]   # newest last
        return d

    def snapshot(self) -> dict:
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "rates": {name: self.rate_per_sec(name) for name in self._rates},
            "latency": {
                name: self.latency_detail(name) for name in self._histograms
            },
        }

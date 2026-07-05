"""SMA-crossover reference strategy — target-position form.

Fast SMA above slow -> desired stance LONG. Fast below -> FLAT. That's the
whole rule: the strategy states what it wants to hold given the data, every
bar. It is NOT edge-based — it reports the current desired stance, and the
runner decides whether any order is needed. Warming up -> no opinion (None),
so the runner leaves any existing position untouched until the strategy has
enough data.

Deliberately simple: the point of Sentinel is execution integrity, not alpha.
"""

from __future__ import annotations

from collections import deque
from decimal import Decimal

from .base import Decision, Stance


class SmaCross:
    def __init__(self, fast: int = 5, slow: int = 20, short: bool = False,
                 stop_floor_pct: Decimal = Decimal("0.005")) -> None:
        if fast >= slow:
            raise ValueError("fast period must be shorter than slow")
        # short=True -> stop-and-reverse: SHORT below the cross instead of FLAT,
        # so the strategy is always in the market and can profit in a bear leg
        # (needs a venue that allows shorting; spot clamps SHORT to FLAT).
        self._short = short
        # The strategy's protective stop is the SLOW SMA (the trend line): a long
        # is wrong once price falls back to it. Floored so a near-cross entry
        # (price hugging the SMA) doesn't hand the risk layer a ~0 stop and size
        # itself into the leverage cap.
        self._stop_floor = stop_floor_pct
        self.name = f"sma-{'ls' if short else 'cross'}({fast}/{slow})"
        self.fast_period = fast     # public: the chart overlays these windows
        self.slow_period = slow
        self._fast_n = fast
        self._slow_n = slow
        self._prices: deque[Decimal] = deque(maxlen=slow)

    def view_spec(self) -> dict:
        """How the terminal should render this strategy: which detail values
        matter in the panel, and what to draw on the chart. The UI is generic;
        the strategy owns its own presentation."""
        return {
            "rows": [
                {"label": "Fast SMA", "key": "fast", "kind": "price"},
                {"label": "Slow SMA", "key": "slow", "kind": "price"},
            ],
            "overlays": [
                {"kind": "line", "key": "fast",
                 "label": f"SMA {self.fast_period}", "color": "#fbbf24"},
                {"kind": "line", "key": "slow",
                 "label": f"SMA {self.slow_period}", "color": "#a78bfa"},
            ],
        }

    def reset(self) -> None:
        """Drop all accumulated bars — the indicator must forget when the
        underlying timeframe changes, else the SMA blends two timeframes'
        closes into a meaningless average."""
        self._prices.clear()

    def on_bar(self, close: Decimal) -> Decision:
        self._prices.append(close)
        if len(self._prices) < self._slow_n:
            return Decision(None, {"warming_up": True,
                                   "have": len(self._prices),
                                   "need": self._slow_n})

        prices = list(self._prices)
        fast = sum(prices[-self._fast_n:]) / self._fast_n
        slow = sum(prices) / self._slow_n
        below = Stance.SHORT if self._short else Stance.FLAT
        stance = Stance.LONG if fast > slow else below
        detail = {"fast": str(round(fast, 2)), "slow": str(round(slow, 2))}
        if stance in (Stance.LONG, Stance.SHORT):
            # Stop geometry: distance from price back to the slow SMA (the trend
            # line), floored at stop_floor_pct of price. The risk layer sizes and
            # enforces the SL/TP off this.
            gap = abs(close - slow)
            detail["stop_dist"] = str(max(gap, close * self._stop_floor))
        return Decision(stance, detail)

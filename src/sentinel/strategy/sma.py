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
    def __init__(self, fast: int = 5, slow: int = 20) -> None:
        if fast >= slow:
            raise ValueError("fast period must be shorter than slow")
        self.name = f"sma-cross({fast}/{slow})"
        self._fast_n = fast
        self._slow_n = slow
        self._prices: deque[Decimal] = deque(maxlen=slow)

    def on_bar(self, close: Decimal) -> Decision:
        self._prices.append(close)
        if len(self._prices) < self._slow_n:
            return Decision(None, {"warming_up": True,
                                   "have": len(self._prices),
                                   "need": self._slow_n})

        prices = list(self._prices)
        fast = sum(prices[-self._fast_n:]) / self._fast_n
        slow = sum(prices) / self._slow_n
        stance = Stance.LONG if fast > slow else Stance.FLAT
        return Decision(stance, {"fast": str(round(fast, 2)),
                                 "slow": str(round(slow, 2))})

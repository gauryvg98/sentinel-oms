"""SMA-crossover reference strategy.

Fast SMA crossing ABOVE slow -> ENTER (golden cross).
Fast crossing BELOW slow    -> EXIT  (death cross).
Signals fire only on the crossover EDGE, not every bar the fast stays above
— so the runner enters once and exits once, never over-trades a trend.

Deliberately simple: the point of Sentinel is execution integrity, not alpha.
This strategy exists to exercise the machinery with a real, deterministic
decision rule.
"""

from __future__ import annotations

from collections import deque
from decimal import Decimal

from .base import Decision, Signal


class SmaCross:
    def __init__(self, fast: int = 5, slow: int = 20) -> None:
        if fast >= slow:
            raise ValueError("fast period must be shorter than slow")
        self.name = f"sma-cross({fast}/{slow})"
        self._fast_n = fast
        self._slow_n = slow
        self._prices: deque[Decimal] = deque(maxlen=slow)
        self._fast_above: bool | None = None  # last relationship, for edge detection

    def on_bar(self, close: Decimal) -> Decision:
        self._prices.append(close)
        if len(self._prices) < self._slow_n:
            return Decision(Signal.HOLD, {"warming_up": True,
                                          "have": len(self._prices),
                                          "need": self._slow_n})

        prices = list(self._prices)
        fast = sum(prices[-self._fast_n:]) / self._fast_n
        slow = sum(prices) / self._slow_n
        above = fast > slow
        prev, self._fast_above = self._fast_above, above

        detail = {"fast": str(round(fast, 2)), "slow": str(round(slow, 2))}
        if prev is None:                 # first full window: establish state only
            return Decision(Signal.HOLD, detail)
        if above and not prev:
            return Decision(Signal.ENTER, detail)   # golden cross
        if not above and prev:
            return Decision(Signal.EXIT, detail)    # death cross
        return Decision(Signal.HOLD, detail)

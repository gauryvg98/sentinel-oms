"""Mark-price feed interface.

The OMS core never needs marks — integrity is price-blind. Marks exist for
the terminal (unrealized P&L, freshness) and for strategies (quote-at-decision
stamping). The interface is deliberately tiny so a real exchange stream
(Binance bookTicker, Alpaca trades) drops in behind it exactly like the
broker adapter.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Mark:
    instrument: str
    price: Decimal
    ts: float  # unix seconds — feed-supplied, used for freshness display


class MarkFeed(Protocol):
    def latest(self, instrument: str) -> Mark | None: ...


class SimMarkFeed:
    """Deterministic-when-seeded random-walk marks for demo/sim mode.
    tick() advances every instrument one step; a demo driver calls it on its
    own cadence."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = random.Random(seed)
        self._marks: dict[str, Mark] = {}
        self._clock: float = 0.0

    def add_instrument(self, instrument: str, start_price: str) -> None:
        self._marks[instrument] = Mark(
            instrument=instrument, price=Decimal(start_price), ts=self._clock
        )

    def tick(self, dt: float = 1.0) -> None:
        self._clock += dt
        for key, mark in self._marks.items():
            drift = Decimal(self._rng.uniform(-0.005, 0.005)).quantize(
                Decimal("0.00001")
            )
            new_price = max(
                Decimal("0.01"), (mark.price * (1 + drift)).quantize(Decimal("0.01"))
            )
            self._marks[key] = Mark(instrument=key, price=new_price, ts=self._clock)

    def latest(self, instrument: str) -> Mark | None:
        return self._marks.get(instrument)

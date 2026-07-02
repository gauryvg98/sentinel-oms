"""Point-in-time replay data contract (R4).

The rule: every historical read is as-of a time T that the replay clock
controls, and data beyond T is unreachable BY CONSTRUCTION — the interface
offers no method that could return it. Look-ahead bias isn't caught by a
runtime check bolted on top; it's impossible to express through this API.

- ReplayClock is the only source of 'now'. No component on the replay path
  may read the wall clock.
- ReplayDataAccess.read()/latest() reject as_of beyond the clock.
- A ListedContract is queryable only within its listed lifetime.
- Replay is deterministic: same loaded data + same clock walk -> identical
  results, because there is nothing else (no wall time, no randomness) in
  the path.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Sequence

from sentinel.domain import ProductDefinition


class FutureDataDenied(Exception):
    """A read attempted to see beyond the replay clock's now."""


class ContractLifetimeViolation(Exception):
    """A read targeted a contract outside its listed lifetime."""


class ReplayClock:
    """The single authority for simulated time. Monotonic: it never goes back."""

    def __init__(self, start: int) -> None:
        self._now = start

    @property
    def now(self) -> int:
        return self._now

    def advance_to(self, t: int) -> None:
        if t < self._now:
            raise ValueError(f"clock cannot go backward: {t} < {self._now}")
        self._now = t


@dataclass(frozen=True, slots=True)
class ListedContract:
    """A tradable instrument with an explicit lifetime and a first-class
    product definition — SPX/XSP-style vs SPY-style behavior is data on the
    product, never a conditional in consuming code."""

    symbol: str
    product: ProductDefinition
    listed_at: int
    delisted_at: int

    def __post_init__(self) -> None:
        if self.delisted_at <= self.listed_at:
            raise ValueError("delisted_at must be after listed_at")


@dataclass(frozen=True, slots=True)
class Tick:
    ts: int
    price: Decimal


@dataclass(slots=True)
class _Series:
    contract: ListedContract
    timestamps: list[int] = field(default_factory=list)
    ticks: list[Tick] = field(default_factory=list)


class ReplayDataAccess:
    """The ONLY read path for historical data on the replay side."""

    def __init__(self, clock: ReplayClock) -> None:
        self._clock = clock
        self._series: dict[str, _Series] = {}

    # ------------------------------------------------------------- loading

    def load(self, contract: ListedContract, ticks: Sequence[Tick]) -> None:
        ordered = sorted(ticks, key=lambda t: t.ts)
        for t in ordered:
            if not (contract.listed_at <= t.ts <= contract.delisted_at):
                raise ContractLifetimeViolation(
                    f"tick at {t.ts} outside {contract.symbol} lifetime"
                )
        self._series[contract.symbol] = _Series(
            contract=contract,
            timestamps=[t.ts for t in ordered],
            ticks=list(ordered),
        )

    def contract(self, symbol: str) -> ListedContract:
        return self._require(symbol).contract

    # --------------------------------------------------------------- reads

    def read(self, symbol: str, as_of: int) -> list[Tick]:
        """All ticks up to and including as_of. as_of may never exceed the
        clock, and must fall inside the contract's listed lifetime."""
        series = self._require(symbol)
        self._gate(series.contract, as_of)
        idx = bisect.bisect_right(series.timestamps, as_of)
        return series.ticks[:idx]

    def latest(self, symbol: str, as_of: int) -> Tick | None:
        ticks = self.read(symbol, as_of)
        return ticks[-1] if ticks else None

    # ----------------------------------------------------------- internals

    def _require(self, symbol: str) -> _Series:
        if symbol not in self._series:
            raise KeyError(f"no data loaded for {symbol!r}")
        return self._series[symbol]

    def _gate(self, contract: ListedContract, as_of: int) -> None:
        if as_of > self._clock.now:
            raise FutureDataDenied(
                f"read as_of={as_of} beyond clock now={self._clock.now}"
            )
        if not (contract.listed_at <= as_of <= contract.delisted_at):
            raise ContractLifetimeViolation(
                f"{contract.symbol} not listed at {as_of} "
                f"(lifetime [{contract.listed_at}, {contract.delisted_at}])"
            )

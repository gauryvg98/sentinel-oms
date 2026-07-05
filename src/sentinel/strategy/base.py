"""Strategy layer — the part the OMS deliberately does NOT contain.

A strategy declares a DESIRED STANCE (be LONG, or be FLAT) each bar; the runner
reconciles actual position -> desired stance. This "target-position" model is
how real systematic bots work: reconcile to target on every evaluation rather
than chase signal edges. It's crash-safe (on restart, bring actual into line
with desired) and it accounts for pre-existing positions for free.

Strategies are PURE and DETERMINISTIC: on_bar takes a price, returns a stance,
no I/O / clock / randomness. That's what makes them unit-testable and, later,
replay-safe (feed historical bars, get identical stances). The runner turns
stances into orders through the normal CommandGateway, so a strategy faces the
exact same guards a human does — it can never bypass single-writer or
never-over-exit. A bad strategy trades badly; it cannot corrupt state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class Stance(str, Enum):
    LONG = "LONG"    # want positive exposure
    FLAT = "FLAT"    # want to be out
    SHORT = "SHORT"  # want negative exposure (perps/margin only; spot clamps to FLAT)


@dataclass(frozen=True, slots=True)
class Bar:
    """One CLOSED OHLC bar — the richer input a strategy may prefer over a bare
    close (true range for ATR/ADX, highs/lows for a Donchian channel). A
    close-only strategy just ignores it; the runner feeds a Bar when the
    strategy exposes on_bar_ohlcv, else the close."""
    high: Decimal
    low: Decimal
    close: Decimal


@dataclass(frozen=True, slots=True)
class Decision:
    # None = no opinion yet (e.g. warming up): the runner does NOTHING, so a
    # warm-up period never flattens an existing position.
    stance: Stance | None
    detail: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Strategy(Protocol):
    name: str

    def on_bar(self, close: Decimal) -> Decision:
        """Called once per CLOSED bar. Pure: same inputs -> same output.

        A strategy MAY also expose ``on_bar_ohlcv(bar: Bar) -> Decision`` for the
        full OHLC bar; the runner prefers it when present. Both must be pure."""
        ...

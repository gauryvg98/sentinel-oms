"""Strategy layer — the part the OMS deliberately does NOT contain.

A strategy decides WHAT (enter/exit/hold); the OMS owns everything about
HOW it happens safely. Strategies here are PURE and DETERMINISTIC: on_bar
takes a price and returns a decision, with no I/O, no clock, no randomness.
That is what makes them unit-testable, and what will later make them
replay-safe (feed the same bars, get the same decisions).

The runner (runtime layer) turns decisions into orders through the normal
CommandGateway, so a strategy faces the exact same guards a human does — it
can never bypass single-writer, never-over-exit, or duplicate-entry. A bad
strategy trades badly; it cannot corrupt state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class Signal(str, Enum):
    ENTER = "ENTER"   # open / add a long
    EXIT = "EXIT"     # close the long
    HOLD = "HOLD"     # do nothing


@dataclass(frozen=True, slots=True)
class Decision:
    signal: Signal
    detail: dict[str, Any] = field(default_factory=dict)  # for the UI panel


@runtime_checkable
class Strategy(Protocol):
    name: str

    def on_bar(self, close: Decimal) -> Decision:
        """Called once per CLOSED bar. Pure: same inputs -> same output."""
        ...

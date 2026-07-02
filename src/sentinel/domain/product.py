"""First-class product definitions (R4).

Settlement, exercise style, and expiration behavior are configuration the
domain reads — never `if symbol == ...` conditionals scattered through logic.
The difference between a cash-settled European index option and a physically
delivered American ETF option is data here, and behavior everywhere else.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Settlement(str, Enum):
    CASH = "CASH"            # settles to cash; no delivery leg
    PHYSICAL = "PHYSICAL"    # delivers the underlying; assignment creates positions


class Exercise(str, Enum):
    EUROPEAN = "EUROPEAN"    # exercisable only at expiration
    AMERICAN = "AMERICAN"    # exercisable any time — early assignment is possible


@dataclass(frozen=True, slots=True)
class ProductDefinition:
    symbol: str
    settlement: Settlement
    exercise: Exercise
    multiplier: int

    def __post_init__(self) -> None:
        if self.multiplier <= 0:
            raise ValueError(f"multiplier must be positive, got {self.multiplier}")

    @property
    def early_assignment_possible(self) -> bool:
        """American exercise means a short position can be assigned before
        expiry — a risk path that simply does not exist for European products
        and must therefore be modeled, monitored, and tested separately."""
        return self.exercise is Exercise.AMERICAN

    @property
    def has_delivery_risk(self) -> bool:
        return self.settlement is Settlement.PHYSICAL

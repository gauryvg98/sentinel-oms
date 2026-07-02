"""Economic order intent — the durable, idempotent unit of authorization.

An intent is what the system persists BEFORE any broker submission (R1.1).
It is immutable: a replace is a new intent linked by trace, never a mutation.
The idempotency_key is client-generated so that retries of the same decision
collapse onto the same intent instead of creating a second one.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from uuid import UUID


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Authority(str, Enum):
    """Which authority created this intent (R1.13: exits are independent)."""

    ENTRY = "ENTRY"
    PROTECTIVE_EXIT = "PROTECTIVE_EXIT"


@dataclass(frozen=True, slots=True)
class EconomicOrderIntent:
    intent_id: UUID
    idempotency_key: str
    instrument: str
    side: Side
    qty: Decimal
    limit_price: Decimal | None
    authority: Authority
    trace_id: UUID

    def __post_init__(self) -> None:
        if self.qty <= 0:
            raise ValueError(f"intent qty must be positive, got {self.qty}")
        if self.limit_price is not None and self.limit_price <= 0:
            raise ValueError(f"limit price must be positive, got {self.limit_price}")
        if not self.idempotency_key:
            raise ValueError("idempotency_key must be non-empty")

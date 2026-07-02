"""BrokerAdapter — the boundary a real broker integration must satisfy.

Everything the OMS knows about brokers is this interface. The simulator lives
strictly behind it; a Schwab/IBKR adapter is a drop-in replacement.

Semantics that shape the whole system:
- ``submit`` may raise BrokerTimeout, which carries NO broker order id. That
  is the UNKNOWN trigger (R1.3): the order may or may not exist at the broker.
- ``query_order`` looks up by CLIENT order id — the reconciliation primitive.
  Returning None means "conclusively absent": the broker never accepted it.
- The event stream delivers fills and cancel confirmations. Delivery is
  at-least-once and may be late or duplicated; the ledger's exec_id dedup is
  what makes that safe (R1.7).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import AsyncIterator, Protocol

from sentinel.domain import Side


class BrokerError(Exception):
    pass


class BrokerReject(BrokerError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class BrokerTimeout(BrokerError):
    """No response and no broker order id. NOT a rejection — the submission
    outcome is unprovable until reconciliation asks the broker."""


class BrokerOrderState(str, Enum):
    WORKING = "WORKING"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class BrokerFill:
    client_order_id: str
    exec_id: str
    qty: Decimal
    price: Decimal


@dataclass(frozen=True, slots=True)
class BrokerCancelConfirmed:
    client_order_id: str


BrokerEvent = BrokerFill | BrokerCancelConfirmed


@dataclass(frozen=True, slots=True)
class BrokerOrderView:
    """The broker's authoritative view, as returned by reconciliation queries.
    ``fills`` carries every execution so a recovering OMS can backfill missed
    ones — exec_id dedup makes re-applying them exactly-once."""

    client_order_id: str
    broker_order_id: str
    state: BrokerOrderState
    filled_qty: Decimal
    fills: tuple[BrokerFill, ...]


class BrokerAdapter(Protocol):
    async def submit(
        self,
        *,
        client_order_id: str,
        instrument: str,
        side: Side,
        qty: Decimal,
        limit_price: Decimal | None,
    ) -> str:
        """Returns broker_order_id. Raises BrokerReject or BrokerTimeout."""
        ...

    async def cancel(self, client_order_id: str) -> None:
        """Requests cancellation; confirmation arrives on the event stream.
        Raises BrokerTimeout if the request outcome is unprovable."""
        ...

    async def query_order(self, client_order_id: str) -> BrokerOrderView | None:
        """Authoritative lookup by client order id. None = conclusively absent."""
        ...

    async def query_positions(self) -> dict[str, Decimal]:
        ...

    def events(self) -> AsyncIterator[BrokerEvent]:
        ...

"""Lifecycle events — the only things that move an order between states.

Frozen dataclasses, no timestamps: the ledger stamps time at append; the
domain stays deterministic and clock-free.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .states import OrderState


@dataclass(frozen=True, slots=True)
class SubmissionStarted:
    """Broker call is about to be made (intent is already durable)."""


@dataclass(frozen=True, slots=True)
class BrokerAcked:
    broker_order_id: str


@dataclass(frozen=True, slots=True)
class BrokerRejected:
    reason: str


@dataclass(frozen=True, slots=True)
class SubmissionTimedOut:
    """No response and no broker order id. This is NOT a rejection (R1.3)."""


@dataclass(frozen=True, slots=True)
class FillReceived:
    exec_id: str          # broker execution id — the ledger's dedup key (R1.2/R1.7)
    qty: Decimal
    price: Decimal


@dataclass(frozen=True, slots=True)
class CancelRequested:
    """Cancel (or the cancel half of a replace) sent to the broker."""


@dataclass(frozen=True, slots=True)
class CancelConfirmed:
    """Broker confirmed the cancel of all conclusively remaining quantity."""


@dataclass(frozen=True, slots=True)
class ReconcileStarted:
    """The order's true state must be recovered from the broker before any
    further action (UNKNOWN exit, crash recovery, late event, disagreement)."""
    cause: str


@dataclass(frozen=True, slots=True)
class ReconcileResolved:
    """Authoritative broker truth. The broker wins: resolved state and filled
    quantity REPLACE local belief (R1.12); divergence is auditable by comparing
    against the pre-reconciliation snapshot in the ledger.

    resolved_state=CANCELED with broker_order_id=None encodes 'conclusively
    absent at broker' — the intent never became exposure. Any resubmission is
    a NEW order under policy, never a reuse (R1.3).
    """

    resolved_state: OrderState
    broker_order_id: str | None
    filled_qty: Decimal


OrderEvent = (
    SubmissionStarted
    | BrokerAcked
    | BrokerRejected
    | SubmissionTimedOut
    | FillReceived
    | CancelRequested
    | CancelConfirmed
    | ReconcileStarted
    | ReconcileResolved
)

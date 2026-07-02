"""Sentinel domain layer — pure, deterministic, I/O-free."""

from .errors import (
    DomainError,
    IllegalTransition,
    OverfillViolation,
    RequiresReconciliation,
)
from .events import (
    BrokerAcked,
    BrokerRejected,
    CancelConfirmed,
    CancelRequested,
    FillReceived,
    OrderEvent,
    ReconcileResolved,
    ReconcileStarted,
    SubmissionStarted,
    SubmissionTimedOut,
)
from .intent import Authority, EconomicOrderIntent, Side
from .product import Exercise, ProductDefinition, Settlement
from .states import ALLOWED, TERMINAL, OrderState, is_terminal
from .transition import OrderCore, transition

__all__ = [
    "ALLOWED",
    "Authority",
    "BrokerAcked",
    "BrokerRejected",
    "CancelConfirmed",
    "CancelRequested",
    "DomainError",
    "EconomicOrderIntent",
    "Exercise",
    "FillReceived",
    "IllegalTransition",
    "OrderCore",
    "OrderEvent",
    "OrderState",
    "OverfillViolation",
    "ProductDefinition",
    "ReconcileResolved",
    "ReconcileStarted",
    "RequiresReconciliation",
    "Settlement",
    "Side",
    "SubmissionStarted",
    "SubmissionTimedOut",
    "TERMINAL",
    "is_terminal",
    "transition",
]

"""The pure transition function: (order, event) -> new order.

No I/O, no clock, no randomness. Every rule the OMS enforces about order
lifecycle lives here, which is what makes the integrity scenarios unit-testable
before any database or broker exists.

Defense in depth: each handler computes its target state, and the result is
ALSO checked against the ALLOWED map. A handler bug cannot smuggle an illegal
transition through.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal

from .errors import IllegalTransition, OverfillViolation, RequiresReconciliation
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
from .states import ALLOWED, TERMINAL, OrderState


@dataclass(frozen=True, slots=True)
class OrderCore:
    """The minimal immutable order snapshot the domain reasons about.

    Prices/average-cost live in ledger projections (derived from fills);
    the domain tracks only what transition decisions depend on.
    """

    order_id: str
    client_order_id: str      # what the broker knows us by — the reconciliation key
    instrument: str
    qty: Decimal
    filled_qty: Decimal
    state: OrderState
    broker_order_id: str | None = None

    @property
    def remaining_qty(self) -> Decimal:
        return self.qty - self.filled_qty


def _guard(current: OrderState, target: OrderState) -> None:
    if target not in ALLOWED[current]:
        raise IllegalTransition(f"{current.value} -> {target.value} is not allowed")


def transition(order: OrderCore, event: OrderEvent) -> OrderCore:  # noqa: C901
    s = order.state

    match event:
        case SubmissionStarted():
            _guard(s, OrderState.SUBMITTING)
            if s is not OrderState.CREATED:
                raise IllegalTransition(f"cannot submit from {s.value}")
            return replace(order, state=OrderState.SUBMITTING)

        case BrokerAcked(broker_order_id=bid):
            if s is not OrderState.SUBMITTING:
                raise IllegalTransition(f"ack in {s.value}")
            _guard(s, OrderState.WORKING)
            return replace(order, state=OrderState.WORKING, broker_order_id=bid)

        case BrokerRejected():
            if s is not OrderState.SUBMITTING:
                raise IllegalTransition(f"reject in {s.value}")
            _guard(s, OrderState.REJECTED)
            return replace(order, state=OrderState.REJECTED)

        case SubmissionTimedOut():
            # Timeout is NOT rejection (R1.3). The outcome is unprovable; the
            # order parks in UNKNOWN and only reconciliation may move it.
            if s is not OrderState.SUBMITTING:
                raise IllegalTransition(f"timeout in {s.value}")
            _guard(s, OrderState.UNKNOWN)
            return replace(order, state=OrderState.UNKNOWN)

        case FillReceived(qty=fqty, price=_):
            if s in TERMINAL or s is OrderState.UNKNOWN:
                # Evidence we cannot apply directly: a fill for an order we
                # believed finished (late fill, R1.7) or whose submission is
                # unprovable. Route through reconciliation — never apply blind.
                raise RequiresReconciliation(
                    f"fill received in {s.value}; reconcile before applying"
                )
            if s not in (
                OrderState.SUBMITTING,  # exec report racing ahead of the ack
                OrderState.WORKING,
                OrderState.PARTIAL,
                OrderState.CANCEL_PENDING,  # fill during cancel-pending (R1.6)
            ):
                raise IllegalTransition(f"fill in {s.value}")
            if fqty <= 0:
                raise IllegalTransition(f"fill qty must be positive, got {fqty}")
            new_filled = order.filled_qty + fqty
            if new_filled > order.qty:
                raise OverfillViolation(
                    f"fill of {fqty} takes filled to {new_filled} > order qty {order.qty}"
                )
            if new_filled == order.qty:
                target = OrderState.FILLED
            elif s is OrderState.CANCEL_PENDING:
                # Cancel is still open at the broker; the partial fill does not
                # resolve it. Remaining qty keeps shrinking until CancelConfirmed.
                target = OrderState.CANCEL_PENDING
            else:
                target = OrderState.PARTIAL
            _guard(s, target)
            return replace(order, state=target, filled_qty=new_filled)

        case CancelRequested():
            if s not in (OrderState.WORKING, OrderState.PARTIAL):
                # Cancel of UNKNOWN is forbidden by construction: you cannot
                # cancel what you cannot prove exists — reconcile first (R1.4).
                raise IllegalTransition(f"cancel requested in {s.value}")
            _guard(s, OrderState.CANCEL_PENDING)
            return replace(order, state=OrderState.CANCEL_PENDING)

        case CancelConfirmed():
            if s is not OrderState.CANCEL_PENDING:
                raise IllegalTransition(f"cancel confirmed in {s.value}")
            _guard(s, OrderState.CANCELED)
            return replace(order, state=OrderState.CANCELED)

        case ReconcileStarted():
            if s is OrderState.RECONCILING:
                raise IllegalTransition("already reconciling")
            _guard(s, OrderState.RECONCILING)
            return replace(order, state=OrderState.RECONCILING)

        case ReconcileResolved(
            resolved_state=rs, broker_order_id=bid, filled_qty=bfilled
        ):
            if s is not OrderState.RECONCILING:
                raise IllegalTransition(f"resolve in {s.value}")
            _guard(s, rs)
            if bfilled < 0 or bfilled > order.qty:
                raise OverfillViolation(
                    f"broker filled_qty {bfilled} outside [0, {order.qty}]"
                )
            # Broker truth REPLACES local belief (R1.12). The pre-reconcile
            # snapshot remains in the ledger, so divergence is auditable.
            return replace(
                order, state=rs, broker_order_id=bid, filled_qty=bfilled
            )

    raise IllegalTransition(f"unhandled event {type(event).__name__} in {s.value}")

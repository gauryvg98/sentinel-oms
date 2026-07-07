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

# A resting maker can be matched by the venue for MORE than it asked — fill
# granularity, or a demo engine that over-matches a resting order. The exchange
# is the source of truth (R1.12): we clamp the ORDER's recorded fill to its qty
# (the order invariant holds — filled never exceeds qty) while the fill rows
# carry the real executed quantity into the POSITION, so our exposure tracks the
# exchange rather than our order book. Only an ABSURD overfill — beyond this
# multiple of the order qty — is treated as a genuine bug (double-submit,
# broker/parse error) and HALTS; that can't be a mere over-match.
_OVERFILL_GROSS = Decimal("2")   # > 2x the order qty = something is truly wrong


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
                OrderState.RECONCILING,     # backfill ingestion during recovery
            ):
                raise IllegalTransition(f"fill in {s.value}")
            if fqty <= 0:
                raise IllegalTransition(f"fill qty must be positive, got {fqty}")
            new_filled = order.filled_qty + fqty
            if new_filled > order.qty * _OVERFILL_GROSS:
                raise OverfillViolation(
                    f"fill of {fqty} takes filled to {new_filled} > order qty {order.qty}"
                )
            # Clamp the order's recorded fill to its qty: a tolerated overfill
            # completes the order (filled never exceeds qty) while the fill row
            # still carries the real quantity into the position projection.
            recorded = min(new_filled, order.qty)
            if s is OrderState.RECONCILING:
                # Reconciliation ingests evidence but never concludes from it:
                # even a completing fill stays RECONCILING until resolution.
                target = OrderState.RECONCILING
            elif recorded >= order.qty:
                target = OrderState.FILLED
            elif s is OrderState.CANCEL_PENDING:
                # Cancel is still open at the broker; the partial fill does not
                # resolve it. Remaining qty keeps shrinking until CancelConfirmed.
                target = OrderState.CANCEL_PENDING
            else:
                target = OrderState.PARTIAL
            _guard(s, target)
            return replace(order, state=target, filled_qty=recorded)

        case CancelRequested():
            if s not in (OrderState.WORKING, OrderState.PARTIAL):
                # Cancel of UNKNOWN is forbidden by construction: you cannot
                # cancel what you cannot prove exists — reconcile first (R1.4).
                raise IllegalTransition(f"cancel requested in {s.value}")
            _guard(s, OrderState.CANCEL_PENDING)
            return replace(order, state=OrderState.CANCEL_PENDING)

        case CancelConfirmed():
            # A cancel confirmation is authoritative: the order is dead at the
            # broker. Usually it follows OUR cancel (CANCEL_PENDING), but it can
            # also be UNSOLICITED — the venue killed a resting or partly-filled
            # order itself: self-trade prevention (STP -> EXPIRED_IN_MATCH) or an
            # exchange-side cancel. Accept it from WORKING/PARTIAL too, keeping
            # any fills already applied; the remainder is gone -> CANCELED.
            if s is OrderState.RECONCILING:
                # A cancel-confirm landing mid-reconcile is the SAME race as a
                # fill landing mid-reconcile (see FillReceived above): a live
                # exec report arriving inside the reconcile window, not a
                # divergence. Reconciliation ingests evidence but never concludes
                # from a live event — the order stays RECONCILING until
                # ReconcileResolved replaces belief with broker truth. Absorb it
                # (recorded as evidence in the ledger, from==to==RECONCILING);
                # do NOT halt. Halting here is over-escalation and inconsistent
                # with how fills are treated in this very state.
                return order
            if s not in (OrderState.CANCEL_PENDING, OrderState.WORKING,
                         OrderState.PARTIAL):
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
            if bfilled < 0 or bfilled > order.qty * _OVERFILL_GROSS:
                raise OverfillViolation(
                    f"broker filled_qty {bfilled} outside [0, {order.qty}]"
                )
            # Broker truth REPLACES local belief (R1.12). The pre-reconcile
            # snapshot remains in the ledger, so divergence is auditable. A
            # tolerated overfill is clamped to qty so order invariants hold.
            return replace(
                order, state=rs, broker_order_id=bid,
                filled_qty=min(bfilled, order.qty),
            )

    raise IllegalTransition(f"unhandled event {type(event).__name__} in {s.value}")

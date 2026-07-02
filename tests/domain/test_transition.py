"""Behavioral tests for the pure transition function.

Each test names the integrity requirement it enforces (R1.x from
docs/REQUIREMENTS.md).
"""

from decimal import Decimal

import pytest

from sentinel.domain import (
    BrokerAcked,
    BrokerRejected,
    CancelConfirmed,
    CancelRequested,
    FillReceived,
    IllegalTransition,
    OrderCore,
    OrderState,
    OverfillViolation,
    ReconcileResolved,
    ReconcileStarted,
    RequiresReconciliation,
    SubmissionStarted,
    SubmissionTimedOut,
    transition,
)


def order(state: OrderState, qty="4", filled="0", broker_id=None) -> OrderCore:
    return OrderCore(
        order_id="O1",
        client_order_id="K1",
        instrument="IDX-OPT",
        qty=Decimal(qty),
        filled_qty=Decimal(filled),
        state=state,
        broker_order_id=broker_id,
    )


def fill(qty: str, exec_id: str = "E1", price: str = "4.20") -> FillReceived:
    return FillReceived(exec_id=exec_id, qty=Decimal(qty), price=Decimal(price))


# ---------------------------------------------------------------- happy path


def test_happy_path_to_full_fill():
    o = order(OrderState.CREATED)
    o = transition(o, SubmissionStarted())
    assert o.state is OrderState.SUBMITTING
    o = transition(o, BrokerAcked(broker_order_id="B9"))
    assert o.state is OrderState.WORKING and o.broker_order_id == "B9"
    o = transition(o, fill("1"))
    assert o.state is OrderState.PARTIAL and o.filled_qty == 1
    o = transition(o, fill("3", exec_id="E2"))
    assert o.state is OrderState.FILLED and o.remaining_qty == 0


def test_immediate_full_fill_from_submitting():
    """Execution report can race ahead of the ack — a marketable order."""
    o = transition(order(OrderState.SUBMITTING), fill("4"))
    assert o.state is OrderState.FILLED


def test_reject_from_submitting():
    o = transition(order(OrderState.SUBMITTING), BrokerRejected(reason="no perm"))
    assert o.state is OrderState.REJECTED


# ------------------------------------------------- R1.3 timeout is not reject


def test_timeout_goes_to_unknown_not_rejected():
    o = transition(order(OrderState.SUBMITTING), SubmissionTimedOut())
    assert o.state is OrderState.UNKNOWN


def test_unknown_cannot_be_resubmitted():
    """R1.3: no blind retry — SubmissionStarted from UNKNOWN must be illegal."""
    with pytest.raises(IllegalTransition):
        transition(order(OrderState.UNKNOWN), SubmissionStarted())


def test_unknown_cannot_be_canceled():
    """You cannot cancel what you cannot prove exists — reconcile first."""
    with pytest.raises(IllegalTransition):
        transition(order(OrderState.UNKNOWN), CancelRequested())


def test_unknown_fill_demands_reconciliation():
    """A fill on UNKNOWN is evidence, not something to apply blindly."""
    with pytest.raises(RequiresReconciliation):
        transition(order(OrderState.UNKNOWN), fill("1"))


def test_unknown_only_exit_is_reconcile():
    o = transition(order(OrderState.UNKNOWN), ReconcileStarted(cause="timeout"))
    assert o.state is OrderState.RECONCILING


# ----------------------------------------------- R1.5 / R1.6 fills & cancels


def test_partial_fill_accounting_is_exact():
    o = order(OrderState.WORKING)
    o = transition(o, fill("1", "E1"))
    o = transition(o, fill("2", "E2"))
    assert o.filled_qty == 3 and o.remaining_qty == 1
    assert o.state is OrderState.PARTIAL


def test_fill_during_cancel_pending_stays_cancel_pending():
    """R1.6: a partial fill does not resolve an open cancel."""
    o = order(OrderState.CANCEL_PENDING, filled="2")
    o = transition(o, fill("1"))
    assert o.state is OrderState.CANCEL_PENDING and o.filled_qty == 3


def test_full_fill_during_cancel_pending_wins_over_cancel():
    o = order(OrderState.CANCEL_PENDING, filled="3")
    o = transition(o, fill("1"))
    assert o.state is OrderState.FILLED


def test_cancel_confirm_after_race_fill():
    """The 3.14 shape: fills 3 of 4, cancel confirms the remaining 1."""
    o = order(OrderState.CANCEL_PENDING, filled="3")
    o = transition(o, CancelConfirmed())
    assert o.state is OrderState.CANCELED and o.filled_qty == 3


def test_cancel_only_from_live_states():
    for s in (OrderState.CREATED, OrderState.SUBMITTING, OrderState.FILLED):
        with pytest.raises(IllegalTransition):
            transition(order(s), CancelRequested())


# --------------------------------------------------------- R1.10 never over


def test_overfill_raises_never_absorbed():
    o = order(OrderState.PARTIAL, filled="3")
    with pytest.raises(OverfillViolation):
        transition(o, fill("2"))


def test_zero_or_negative_fill_rejected():
    with pytest.raises(IllegalTransition):
        transition(order(OrderState.WORKING), fill("0"))
    with pytest.raises(IllegalTransition):
        transition(order(OrderState.WORKING), fill("-1"))


# ------------------------------------------------- R1.7 late events reopen


def test_late_fill_on_filled_demands_reconciliation():
    with pytest.raises(RequiresReconciliation):
        transition(order(OrderState.FILLED, filled="4"), fill("1", "E9"))


def test_late_fill_on_canceled_demands_reconciliation():
    with pytest.raises(RequiresReconciliation):
        transition(order(OrderState.CANCELED, filled="2"), fill("1", "E9"))


def test_terminal_states_reopen_via_reconcile():
    o = transition(order(OrderState.FILLED, filled="4"), ReconcileStarted(cause="late fill"))
    assert o.state is OrderState.RECONCILING


# ------------------------------------------------- R1.11/R1.12 reconciliation


def test_reconcile_reachable_from_every_nonterminal_crash_state():
    for s in (
        OrderState.CREATED,
        OrderState.SUBMITTING,
        OrderState.WORKING,
        OrderState.PARTIAL,
        OrderState.CANCEL_PENDING,
        OrderState.UNKNOWN,
    ):
        o = transition(order(s), ReconcileStarted(cause="crash recovery"))
        assert o.state is OrderState.RECONCILING, s


def test_resolution_adopts_broker_truth():
    """R1.12: broker wins — state AND filled quantity replace local belief."""
    o = order(OrderState.RECONCILING, filled="1")
    o = transition(
        o,
        ReconcileResolved(
            resolved_state=OrderState.PARTIAL,
            broker_order_id="B9",
            filled_qty=Decimal("3"),
        ),
    )
    assert o.state is OrderState.PARTIAL
    assert o.filled_qty == 3          # broker's number, not ours
    assert o.broker_order_id == "B9"  # discovered id adopted (the 3.14 recovery)


def test_resolution_conclusively_absent_becomes_canceled_unexposed():
    o = order(OrderState.RECONCILING)
    o = transition(
        o,
        ReconcileResolved(
            resolved_state=OrderState.CANCELED,
            broker_order_id=None,
            filled_qty=Decimal("0"),
        ),
    )
    assert o.state is OrderState.CANCELED and o.filled_qty == 0


def test_resolution_cannot_exceed_order_qty():
    o = order(OrderState.RECONCILING)
    with pytest.raises(OverfillViolation):
        transition(
            o,
            ReconcileResolved(
                resolved_state=OrderState.FILLED,
                broker_order_id="B9",
                filled_qty=Decimal("5"),
            ),
        )


def test_resolution_only_from_reconciling():
    with pytest.raises(IllegalTransition):
        transition(
            order(OrderState.WORKING),
            ReconcileResolved(
                resolved_state=OrderState.FILLED,
                broker_order_id="B9",
                filled_qty=Decimal("4"),
            ),
        )


def test_double_reconcile_is_illegal():
    with pytest.raises(IllegalTransition):
        transition(order(OrderState.RECONCILING), ReconcileStarted(cause="dup"))


# ------------------------------------------------------------- immutability


def test_transition_never_mutates_input():
    o = order(OrderState.WORKING)
    transition(o, fill("1"))
    assert o.filled_qty == 0 and o.state is OrderState.WORKING

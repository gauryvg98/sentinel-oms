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


def test_unsolicited_cancel_from_working_is_accepted():
    """STP / exchange-side cancel of a resting order we never asked to cancel:
    a CancelConfirmed arrives straight from WORKING (not CANCEL_PENDING). It's
    authoritative — accept it -> CANCELED, don't halt."""
    o = order(OrderState.WORKING)
    o = transition(o, CancelConfirmed())
    assert o.state is OrderState.CANCELED and o.filled_qty == 0


def test_unsolicited_cancel_from_partial_keeps_fills():
    """The exact halt we hit: EXPIRED_IN_MATCH expires the resting remainder of a
    partly-filled peg. CancelConfirmed from PARTIAL -> CANCELED, fills retained."""
    o = order(OrderState.PARTIAL, filled="2")
    o = transition(o, CancelConfirmed())
    assert o.state is OrderState.CANCELED and o.filled_qty == 2


def test_cancel_confirm_still_illegal_from_non_live_states():
    for s in (OrderState.CREATED, OrderState.SUBMITTING, OrderState.FILLED):
        with pytest.raises(IllegalTransition):
            transition(order(s), CancelConfirmed())


# --------------------------------------------------------- R1.10 never over


def test_venue_overmatch_clamps_to_qty_never_over_books():
    # A resting maker matched past its qty (fill granularity / demo over-match):
    # the order COMPLETES, clamped to qty — the order book never exceeds what we
    # asked. The fill row still carries the real qty into the position, so
    # exposure tracks the exchange without the order booking more than ordered.
    o = transition(order(OrderState.PARTIAL, qty="4", filled="3"), fill("2"))
    assert o.state is OrderState.FILLED and o.filled_qty == Decimal("4")


def test_gross_overfill_still_halts():
    # A broker report beyond a sane multiple of the order qty can't be a mere
    # over-match — it's a double-submit / parse bug. Never absorbed: halt.
    with pytest.raises(OverfillViolation):
        transition(order(OrderState.WORKING, qty="4", filled="0"), fill("9"))


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


# --------------------------------------------- backfill ingestion during recon


def test_reconciling_ingests_fills_without_concluding():
    """Backfilled fills accumulate during reconciliation, but even a
    completing fill stays RECONCILING — only resolution exits."""
    o = order(OrderState.RECONCILING)
    o = transition(o, fill("3", "E1"))
    assert o.state is OrderState.RECONCILING and o.filled_qty == 3
    o = transition(o, fill("1", "E2"))          # completes the quantity
    assert o.state is OrderState.RECONCILING    # still not concluded
    assert o.filled_qty == 4


def test_reconciling_ingests_cancel_confirm_without_concluding():
    """A cancel-confirm landing mid-reconcile is the same race as a fill landing
    mid-reconcile: live evidence, not a divergence. It must be absorbed (order
    stays RECONCILING, fills retained) — only ReconcileResolved concludes. This
    is the exact halt we hit: CancelConfirmed arrived while the order was
    RECONCILING and raised IllegalTransition, taking the whole OMS down."""
    o = order(OrderState.RECONCILING, filled="2")
    o = transition(o, CancelConfirmed())
    assert o.state is OrderState.RECONCILING and o.filled_qty == 2
    # resolution is still the only exit — broker truth concludes it
    o = transition(o, ReconcileResolved(
        resolved_state=OrderState.CANCELED, broker_order_id="B1",
        filled_qty=Decimal("2")))
    assert o.state is OrderState.CANCELED and o.filled_qty == 2


def test_reconciling_backfill_overmatch_clamps_to_qty():
    # Backfilling a venue over-match: the order's booked fill clamps to qty and
    # stays RECONCILING (only resolution concludes). Never over-books.
    o = order(OrderState.RECONCILING, qty="4", filled="3")
    o = transition(o, fill("2"))
    assert o.state is OrderState.RECONCILING and o.filled_qty == Decimal("4")


def test_reconciling_backfill_gross_overfill_halts():
    o = order(OrderState.RECONCILING, qty="4", filled="0")
    with pytest.raises(OverfillViolation):
        transition(o, fill("9"))


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


def test_resolution_overmatch_clamps_to_qty():
    # Broker reports a resting order matched past its qty: resolution books it
    # to qty (order invariant holds); the fills carry true position.
    o = order(OrderState.RECONCILING, qty="4")
    o = transition(o, ReconcileResolved(
        resolved_state=OrderState.FILLED, broker_order_id="B9",
        filled_qty=Decimal("4.1")))
    assert o.state is OrderState.FILLED and o.filled_qty == Decimal("4")


def test_resolution_gross_overfill_halts():
    o = order(OrderState.RECONCILING, qty="4")
    with pytest.raises(OverfillViolation):
        transition(o, ReconcileResolved(
            resolved_state=OrderState.FILLED, broker_order_id="B9",
            filled_qty=Decimal("9")))


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

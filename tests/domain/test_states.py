"""Structural properties of the state machine itself.

These don't test transitions one by one (test_transition does); they assert
the SHAPE of the allowed map — the properties that make the machine safe.
"""

from sentinel.domain import ALLOWED, TERMINAL, OrderState, is_terminal


def test_every_state_has_an_entry_in_the_map():
    assert set(ALLOWED.keys()) == set(OrderState)


def test_unknown_exits_only_to_reconciling():
    """R1.3/R1.4: an unprovable submission may never be acted on directly."""
    assert ALLOWED[OrderState.UNKNOWN] == frozenset({OrderState.RECONCILING})


def test_terminal_states_reopen_only_to_reconciling():
    """R1.7: late evidence reopens an order for reconciliation — nothing else."""
    for terminal in TERMINAL:
        assert ALLOWED[terminal] == frozenset({OrderState.RECONCILING}), terminal


def test_no_state_transitions_directly_to_unknown_except_submitting():
    """UNKNOWN means 'submission outcome unprovable' — it can only arise from
    an in-flight submission, never from a state where the broker id is known."""
    for state, targets in ALLOWED.items():
        if OrderState.UNKNOWN in targets:
            assert state is OrderState.SUBMITTING, state


def test_reconciling_resolves_only_to_broker_expressible_states():
    """Reconciliation adopts broker truth; the broker cannot report CREATED,
    SUBMITTING, UNKNOWN, or RECONCILING."""
    assert ALLOWED[OrderState.RECONCILING] == frozenset(
        {
            OrderState.WORKING,
            OrderState.PARTIAL,
            OrderState.FILLED,
            OrderState.CANCELED,
            OrderState.REJECTED,
        }
    )


def test_every_nonterminal_state_can_reach_reconciling():
    """Crash recovery (R1.11) must be able to reconcile an order frozen in ANY
    non-terminal state."""
    for state in OrderState:
        if state is OrderState.RECONCILING:
            continue
        assert OrderState.RECONCILING in ALLOWED[state], state


def test_is_terminal():
    assert is_terminal(OrderState.FILLED)
    assert is_terminal(OrderState.CANCELED)
    assert is_terminal(OrderState.REJECTED)
    assert not is_terminal(OrderState.UNKNOWN)  # UNKNOWN is parked, not finished
    assert not is_terminal(OrderState.CANCEL_PENDING)

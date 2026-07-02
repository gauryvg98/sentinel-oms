"""Order lifecycle states and the allowed-transition map.

The map is data, not code: it serves as documentation, as the runtime guard
inside ``transition()``, and as the oracle the tests check against. If a
transition isn't in this map, it cannot happen — anywhere.
"""

from __future__ import annotations

from enum import Enum


class OrderState(str, Enum):
    CREATED = "CREATED"                # intent persisted, nothing submitted
    SUBMITTING = "SUBMITTING"          # broker call in flight
    WORKING = "WORKING"                # acked, live at broker, no fills yet
    PARTIAL = "PARTIAL"                # some quantity filled, order still live
    CANCEL_PENDING = "CANCEL_PENDING"  # cancel/replace requested, not yet confirmed
    UNKNOWN = "UNKNOWN"                # submission outcome unprovable (timeout, no id)
    RECONCILING = "RECONCILING"        # querying broker for authoritative truth
    FILLED = "FILLED"                  # terminal (reopenable only via reconciliation)
    CANCELED = "CANCELED"              # terminal (reopenable only via reconciliation)
    REJECTED = "REJECTED"              # terminal (reopenable only via reconciliation)


TERMINAL: frozenset[OrderState] = frozenset(
    {OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED}
)

# UNKNOWN's only exit is RECONCILING: an unprovable submission may never be
# acted on directly — no retry, no cancel, no fill application — until the
# broker has been asked what actually happened.
#
# Terminal states transition only to RECONCILING: a late fill or disagreement
# after presumed-terminal reopens the order for reconciliation; it is never
# applied blindly.
ALLOWED: dict[OrderState, frozenset[OrderState]] = {
    OrderState.CREATED: frozenset({OrderState.SUBMITTING, OrderState.RECONCILING}),
    OrderState.SUBMITTING: frozenset(
        {
            OrderState.WORKING,
            OrderState.PARTIAL,   # execution report can race ahead of the ack
            OrderState.FILLED,    # marketable order can fill on arrival
            OrderState.REJECTED,
            OrderState.UNKNOWN,
            OrderState.RECONCILING,  # crash recovery of an in-flight submission
        }
    ),
    OrderState.WORKING: frozenset(
        {
            OrderState.PARTIAL,
            OrderState.FILLED,
            OrderState.CANCEL_PENDING,
            OrderState.RECONCILING,
        }
    ),
    OrderState.PARTIAL: frozenset(
        {
            OrderState.PARTIAL,   # further partial fills
            OrderState.FILLED,
            OrderState.CANCEL_PENDING,
            OrderState.RECONCILING,
        }
    ),
    OrderState.CANCEL_PENDING: frozenset(
        {
            OrderState.CANCEL_PENDING,  # fill during cancel-pending: cancel still open
            OrderState.CANCELED,
            OrderState.FILLED,          # fully filled before the cancel took effect
            OrderState.RECONCILING,
        }
    ),
    OrderState.UNKNOWN: frozenset({OrderState.RECONCILING}),
    OrderState.RECONCILING: frozenset(
        {
            OrderState.WORKING,
            OrderState.PARTIAL,
            OrderState.FILLED,
            OrderState.CANCELED,
            OrderState.REJECTED,
        }
    ),
    OrderState.FILLED: frozenset({OrderState.RECONCILING}),
    OrderState.CANCELED: frozenset({OrderState.RECONCILING}),
    OrderState.REJECTED: frozenset({OrderState.RECONCILING}),
}


def is_terminal(state: OrderState) -> bool:
    return state in TERMINAL

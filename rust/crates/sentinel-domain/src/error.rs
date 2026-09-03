//! What a transition can refuse, and why the distinction matters.
//!
//! [`TransitionError::Illegal`] means the caller has a bug: it asked for
//! something the lifecycle does not contain.
//!
//! [`TransitionError::RequiresReconciliation`] means the *outside world*
//! produced evidence that is legal-looking but not applicable here — a fill for
//! an order we believed finished, a cancel confirmation contradicting what we
//! booked. Nothing is wrong with the caller; the order has to go and ask the
//! venue before anything else touches it.
//!
//! [`TransitionError::Overfill`] means either the venue or our own accounting
//! is faulty. It is never absorbed.
//!
//! Every variant is `Copy`. An error on the hot path that allocates is an error
//! that behaves differently under the memory pressure it tends to arrive in.

use core::fmt;

use sentinel_types::Qty;

use crate::event::EventKind;
use crate::state::OrderState;

/// A transition that did not happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionError {
    /// The lifecycle does not contain this move. A programming error.
    Illegal {
        /// Where the order was.
        from: OrderState,
        /// What was applied to it.
        event: EventKind,
        /// Where that would have taken it, when a target was computed at all.
        to: Option<OrderState>,
    },
    /// Evidence arrived that may not be applied from this state. Move the order
    /// to [`OrderState::Reconciling`] and resolve it against the venue first.
    RequiresReconciliation {
        /// Where the order is.
        state: OrderState,
        /// The evidence that cannot be applied here.
        event: EventKind,
    },
    /// A fill would take cumulative filled quantity absurdly beyond the order.
    ///
    /// Not the same as a mild over-match, which is tolerated and clamped — see
    /// [`crate::transition`]. This is a double-submit, a parse fault, or a venue
    /// fault, and all three demand a halt.
    Overfill {
        /// What the order asked for.
        ordered: Qty,
        /// What it had filled before this event.
        filled: Qty,
        /// What this event would have added.
        attempted: Qty,
    },
    /// A fill claimed a quantity of zero or less.
    NonPositiveFill {
        /// What the fill claimed.
        qty: Qty,
    },
    /// Arithmetic left the representable range. Reported rather than wrapped,
    /// because a wrapped position is worse than a stopped system.
    Overflow,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Illegal {
                from,
                event,
                to: Some(to),
            } => {
                write!(f, "{event} in {from}: {from} -> {to} is not allowed")
            }
            Self::Illegal {
                from,
                event,
                to: None,
            } => {
                write!(f, "{event} in {from} is not allowed")
            }
            Self::RequiresReconciliation { state, event } => {
                write!(f, "{event} in {state}; reconcile before applying")
            }
            Self::Overfill {
                ordered,
                filled,
                attempted,
            } => write!(
                f,
                "fill of {attempted} takes filled to {} on an order of {ordered}",
                filled.raw().saturating_add(attempted.raw())
            ),
            Self::NonPositiveFill { qty } => {
                write!(f, "fill qty must be positive, got {qty}")
            }
            Self::Overflow => f.write_str("quantity arithmetic overflowed"),
        }
    }
}

impl core::error::Error for TransitionError {}

impl TransitionError {
    /// Whether the order should be routed through reconciliation rather than
    /// having the error propagate.
    ///
    /// The engine branches on this, and branching on a method rather than on a
    /// variant means adding a future "ask the venue" case cannot be forgotten
    /// at the call sites.
    #[must_use]
    pub const fn needs_reconciliation(self) -> bool {
        matches!(self, Self::RequiresReconciliation { .. })
    }

    /// Whether this error means the account's accounting cannot be trusted.
    ///
    /// These halt. Absorbing one means continuing to trade on numbers that have
    /// already been shown to be wrong.
    #[must_use]
    pub const fn is_integrity_violation(self) -> bool {
        matches!(self, Self::Overfill { .. } | Self::Overflow)
    }
}

/// An intent that could not be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentError {
    /// Quantity was zero or negative. Direction lives in `Side`.
    NonPositiveQty,
    /// A price was zero or negative.
    NonPositivePrice,
    /// Both a stop and a limit price were given.
    ///
    /// Stop-limit is deliberately unsupported rather than approximated: every
    /// venue resolves the two prices differently, and an adapter guessing which
    /// one this system meant is a guess about where money goes.
    StopLimitUnsupported,
    /// The idempotency key was empty. Without it there is nothing to collapse a
    /// retry onto, and duplicate exposure stops being structurally impossible.
    EmptyIdempotencyKey,
    /// The order kind was given without the price it requires.
    MissingPrice {
        /// The kind that needs it.
        kind: sentinel_types::OrderKind,
    },
}

impl fmt::Display for IntentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonPositiveQty => f.write_str("intent qty must be positive"),
            Self::NonPositivePrice => f.write_str("prices must be positive"),
            Self::StopLimitUnsupported => {
                f.write_str("stop-limit is not supported: set a stop price or a limit price")
            }
            Self::EmptyIdempotencyKey => f.write_str("idempotency key must be non-empty"),
            Self::MissingPrice { kind } => write!(f, "{kind} requires a price"),
        }
    }
}

impl core::error::Error for IntentError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_reconciliation_errors_ask_for_reconciliation() {
        assert!(
            TransitionError::RequiresReconciliation {
                state: OrderState::Filled,
                event: EventKind::FillReceived,
            }
            .needs_reconciliation()
        );
        assert!(
            !TransitionError::Illegal {
                from: OrderState::Filled,
                event: EventKind::CancelRequested,
                to: None,
            }
            .needs_reconciliation()
        );
    }

    #[test]
    fn overfill_and_overflow_are_integrity_violations() {
        assert!(
            TransitionError::Overfill {
                ordered: Qty::whole(1),
                filled: Qty::whole(1),
                attempted: Qty::whole(5),
            }
            .is_integrity_violation()
        );
        assert!(TransitionError::Overflow.is_integrity_violation());
        // A late fill is the world being messy, not the books being wrong.
        assert!(
            !TransitionError::RequiresReconciliation {
                state: OrderState::Filled,
                event: EventKind::FillReceived,
            }
            .is_integrity_violation()
        );
    }

    #[test]
    fn errors_read_as_sentences() {
        assert_eq!(
            TransitionError::Illegal {
                from: OrderState::Unknown,
                event: EventKind::CancelRequested,
                to: Some(OrderState::CancelPending),
            }
            .to_string(),
            "CANCEL_REQUESTED in UNKNOWN: UNKNOWN -> CANCEL_PENDING is not allowed"
        );
        assert_eq!(
            TransitionError::RequiresReconciliation {
                state: OrderState::Filled,
                event: EventKind::FillReceived,
            }
            .to_string(),
            "FILL_APPLIED in FILLED; reconcile before applying"
        );
    }

    #[test]
    fn errors_are_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<TransitionError>();
        assert_copy::<IntentError>();
    }
}

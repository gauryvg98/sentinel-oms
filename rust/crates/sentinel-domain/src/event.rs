//! Lifecycle events — the only things that move an order between states.
//!
//! No timestamps. The journal stamps time at append; the domain stays
//! clock-free, which is what makes a replay reproduce.
//!
//! Every variant is `Copy` and heap-free, so an event can be built, matched on,
//! and written without an allocation or a borrow that outlives the batch.

use sentinel_types::{ExecId, Price, Qty, VenueOrderId, wire_enum};

use crate::state::OrderState;

wire_enum! {
    /// The discriminant of an [`OrderEvent`], as it appears on disk.
    ///
    /// Separate from the event itself so a decoder can read one byte, decide
    /// what it is holding, and reject an unknown kind before parsing a payload
    /// it would have to guess the shape of.
    EventKind {
        /// Intent is durable and the venue call is about to be made.
        SubmissionStarted = 1 => "SUBMISSION_STARTED",
        /// The venue acknowledged and named the order.
        VenueAcked = 2 => "VENUE_ACKED",
        /// The venue refused the order.
        VenueRejected = 3 => "VENUE_REJECTED",
        /// No response and no venue order id. Not a rejection (R1.3).
        SubmissionTimedOut = 4 => "SUBMISSION_TIMED_OUT",
        /// An execution.
        FillReceived = 5 => "FILL_APPLIED",
        /// A cancel was sent to the venue.
        CancelRequested = 6 => "CANCEL_REQUESTED",
        /// The venue confirmed the cancel of everything conclusively remaining.
        CancelConfirmed = 7 => "CANCEL_CONFIRMED",
        /// The order's true state must be recovered before anything else acts.
        ReconcileStarted = 8 => "RECONCILE_STARTED",
        /// Authoritative venue truth, replacing local belief.
        ReconcileResolved = 9 => "RECONCILE_RESOLVED",
    }
}

/// Why an order entered reconciliation.
///
/// An enum rather than the free text the Python carried: the cause is read by
/// the reconciler to decide how hard to try and by the operator to understand
/// what happened, and a string field that is only ever compared against a
/// handful of literals is a closed set wearing an open type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReconcileCause {
    /// Submission timed out; the outcome is unprovable.
    SubmissionTimeout,
    /// A cancel request timed out; the order may fill or cancel.
    CancelTimeout,
    /// Found mid-submission at boot — the crash window (R1.11).
    ReplayedMidSubmission,
    /// A fill arrived for an order we believed terminal (R1.7).
    LateFill,
    /// A cancel confirmation contradicted what we booked.
    LateCancelConfirm,
    /// No venue event touched this order for longer than any honest in-flight
    /// window, so one was probably lost in a stream gap.
    StaleSweep,
    /// Local and venue position disagree (R1.12).
    PositionDisagreement,
}

impl ReconcileCause {
    /// The canonical text form, for the ledger and the operator's screen.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubmissionTimeout => "submission timeout",
            Self::CancelTimeout => "cancel timeout",
            Self::ReplayedMidSubmission => "replayed mid-submission",
            Self::LateFill => "late fill",
            Self::LateCancelConfirm => "late cancel-confirm",
            Self::StaleSweep => "stale sweep",
            Self::PositionDisagreement => "position disagreement",
        }
    }
}

impl core::fmt::Display for ReconcileCause {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why the venue refused an order.
///
/// The code is the venue's own; the text is kept short and inline because it
/// ends up in a ledger row an operator reads, and a heap string in an event
/// payload is the thing this crate does not do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RejectReason {
    /// The venue's numeric code, when it gave one.
    pub code: i32,
    /// The venue's message, truncated to what fits.
    pub text: sentinel_types::InlineStr<64>,
}

impl RejectReason {
    /// Build a reason, truncating the venue's prose to the inline capacity.
    ///
    /// Truncation is safe *here* and not for ids: a shortened message is still
    /// a message, whereas a shortened id is a different order.
    #[must_use]
    pub fn new(code: i32, text: &str) -> Self {
        let cap = sentinel_types::InlineStr::<64>::CAPACITY;
        let mut end = text.len().min(cap);
        // Do not split a multi-byte character.
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            code,
            text: sentinel_types::InlineStr::new(&text[..end]).unwrap_or_default(),
        }
    }
}

impl core::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "[{}] {}", self.code, self.text)
    }
}

/// Something that happened to an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderEvent {
    /// The venue call is about to be made. The intent is already durable.
    SubmissionStarted,
    /// The venue acknowledged, and named the order.
    VenueAcked {
        /// The venue's identifier for this order.
        venue_order_id: VenueOrderId,
    },
    /// The venue refused the order.
    VenueRejected {
        /// What the venue said.
        reason: RejectReason,
    },
    /// The submission produced no response and no venue order id.
    ///
    /// Deliberately carries nothing. There is no field here that could tempt a
    /// caller into deciding what "probably" happened.
    SubmissionTimedOut,
    /// An execution.
    FillReceived {
        /// The venue's execution id — the dedup key (R1.2 / R1.7).
        exec_id: ExecId,
        /// Quantity executed. Always positive.
        qty: Qty,
        /// Execution price.
        price: Price,
    },
    /// A cancel — or the cancel half of a replace — was sent to the venue.
    CancelRequested,
    /// The venue confirmed the cancel of all conclusively remaining quantity.
    CancelConfirmed,
    /// The order's true state must be recovered from the venue before any
    /// further action.
    ReconcileStarted {
        /// What put it here.
        cause: ReconcileCause,
    },
    /// Authoritative venue truth. The venue wins: the resolved state and filled
    /// quantity replace local belief (R1.12), and the pre-reconciliation
    /// snapshot stays in the journal so the divergence is auditable.
    ///
    /// `state: Canceled` with `venue_order_id: None` encodes "conclusively
    /// absent at the venue" — the intent never became exposure. Any
    /// resubmission is a new order under policy, never a reuse (R1.3).
    ReconcileResolved {
        /// What the venue says the order's state is.
        state: OrderState,
        /// What the venue calls it, if it exists there at all.
        venue_order_id: Option<VenueOrderId>,
        /// What the venue says has filled.
        filled_qty: Qty,
    },
}

impl OrderEvent {
    /// This event's discriminant.
    #[must_use]
    pub const fn kind(&self) -> EventKind {
        match self {
            Self::SubmissionStarted => EventKind::SubmissionStarted,
            Self::VenueAcked { .. } => EventKind::VenueAcked,
            Self::VenueRejected { .. } => EventKind::VenueRejected,
            Self::SubmissionTimedOut => EventKind::SubmissionTimedOut,
            Self::FillReceived { .. } => EventKind::FillReceived,
            Self::CancelRequested => EventKind::CancelRequested,
            Self::CancelConfirmed => EventKind::CancelConfirmed,
            Self::ReconcileStarted { .. } => EventKind::ReconcileStarted,
            Self::ReconcileResolved { .. } => EventKind::ReconcileResolved,
        }
    }

    /// Whether this event came from the venue rather than from us.
    ///
    /// The distinction decides what a stale-order sweep counts as activity: our
    /// own events prove we are still trying, not that the venue has answered.
    #[must_use]
    pub const fn is_venue_evidence(&self) -> bool {
        matches!(
            self,
            Self::VenueAcked { .. }
                | Self::VenueRejected { .. }
                | Self::FillReceived { .. }
                | Self::CancelConfirmed
                | Self::ReconcileResolved { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_has_exactly_one_event_shape() {
        // Adding a variant without extending `kind()` fails to compile; adding
        // a `Kind` without a variant is caught here.
        let samples = [
            OrderEvent::SubmissionStarted,
            OrderEvent::VenueAcked {
                venue_order_id: VenueOrderId::empty(),
            },
            OrderEvent::VenueRejected {
                reason: RejectReason::new(-2019, "insufficient margin"),
            },
            OrderEvent::SubmissionTimedOut,
            OrderEvent::FillReceived {
                exec_id: ExecId::empty(),
                qty: Qty::whole(1),
                price: Price::whole(1),
            },
            OrderEvent::CancelRequested,
            OrderEvent::CancelConfirmed,
            OrderEvent::ReconcileStarted {
                cause: ReconcileCause::LateFill,
            },
            OrderEvent::ReconcileResolved {
                state: OrderState::Filled,
                venue_order_id: None,
                filled_qty: Qty::whole(1),
            },
        ];
        assert_eq!(samples.len(), EventKind::ALL.len());
        for (sample, &kind) in samples.iter().zip(EventKind::ALL) {
            assert_eq!(sample.kind(), kind);
        }
    }

    #[test]
    fn only_the_venue_counts_as_venue_evidence() {
        assert!(OrderEvent::CancelConfirmed.is_venue_evidence());
        assert!(
            OrderEvent::FillReceived {
                exec_id: ExecId::empty(),
                qty: Qty::whole(1),
                price: Price::whole(1),
            }
            .is_venue_evidence()
        );
        // Our own doing. A sweep that counted these would never fire.
        assert!(!OrderEvent::SubmissionStarted.is_venue_evidence());
        assert!(!OrderEvent::CancelRequested.is_venue_evidence());
        assert!(!OrderEvent::SubmissionTimedOut.is_venue_evidence());
        assert!(
            !OrderEvent::ReconcileStarted {
                cause: ReconcileCause::StaleSweep
            }
            .is_venue_evidence()
        );
    }

    #[test]
    fn reject_reason_truncates_prose_without_splitting_a_character() {
        let long = "margin is insufficient — ".repeat(10);
        let r = RejectReason::new(-2019, &long);
        assert!(r.text.len() <= 64);
        // Still valid text, not a broken code point.
        assert!(long.starts_with(r.text.as_str()));
    }

    #[test]
    fn reject_reason_keeps_short_messages_whole() {
        let r = RejectReason::new(-2027, "exceeds max notional");
        assert_eq!(r.text.as_str(), "exceeds max notional");
        assert_eq!(r.to_string(), "[-2027] exceeds max notional");
    }

    #[test]
    fn events_are_copy_and_heap_free() {
        // The property the whole module is shaped around: an event can be
        // built, matched on and written without an allocation, and cannot
        // change between being appended and being flushed.
        fn assert_copy<T: Copy>() {}
        assert_copy::<OrderEvent>();
        assert_copy::<RejectReason>();
        assert_copy::<ReconcileCause>();
    }
}

//! Order lifecycle states and the allowed-transition map.
//!
//! The map is data, not code. It serves three jobs at once: it documents the
//! lifecycle, it is the runtime guard inside [`crate::transition`], and it is
//! the oracle the tests check against. A transition absent from this map cannot
//! happen anywhere.

use sentinel_types::wire_enum;

wire_enum! {
    /// Where an order is in its life.
    ///
    /// Two of these carry the whole design.
    ///
    /// `Unknown` exists because a submission that timed out with no venue order
    /// id has an *unprovable* outcome. Calling it rejected would be a guess, and
    /// the retry that follows from that guess is how a position gets doubled.
    ///
    /// `Reconciling` exists because the venue is the source of truth, and asking
    /// it is a state, not a function call — evidence arriving mid-question must
    /// be recorded without being acted on.
    OrderState {
        /// Intent is durable. Nothing has been submitted.
        Created = 1 => "CREATED",
        /// The venue call is in flight.
        Submitting = 2 => "SUBMITTING",
        /// Acknowledged, live at the venue, nothing filled.
        Working = 3 => "WORKING",
        /// Some quantity filled; the order is still live.
        Partial = 4 => "PARTIAL",
        /// A cancel has been requested and not yet confirmed.
        CancelPending = 5 => "CANCEL_PENDING",
        /// The submission outcome is unprovable. No retry, no cancel, no fill
        /// application — only reconciliation may move it.
        Unknown = 6 => "UNKNOWN",
        /// The venue is being asked what actually happened.
        Reconciling = 7 => "RECONCILING",
        /// Terminal. Reopenable only through reconciliation.
        Filled = 8 => "FILLED",
        /// Terminal. Reopenable only through reconciliation.
        Canceled = 9 => "CANCELED",
        /// Terminal. Reopenable only through reconciliation.
        Rejected = 10 => "REJECTED",
    }
}

impl OrderState {
    /// Whether the order is finished, absent new evidence from the venue.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected)
    }

    /// Whether the order is live at the venue and may still fill.
    #[must_use]
    pub const fn is_live(self) -> bool {
        matches!(
            self,
            Self::Created
                | Self::Submitting
                | Self::Working
                | Self::Partial
                | Self::CancelPending
                | Self::Unknown
                | Self::Reconciling
        )
    }

    /// Whether the order blocks new entries on its instrument.
    ///
    /// An order whose true state we cannot prove holds the instrument (R1.4):
    /// submitting a second order against exposure that may or may not exist is
    /// exactly the duplicate this system refuses to be capable of.
    #[must_use]
    pub const fn locks_instrument(self) -> bool {
        matches!(self, Self::Unknown | Self::Reconciling)
    }

    /// States reachable from this one.
    ///
    /// `Unknown`'s only exit is `Reconciling`: an unprovable submission may
    /// never be acted on directly until the venue has been asked.
    ///
    /// Terminal states lead only to `Reconciling`: a late fill or a
    /// disagreement after presumed-terminal reopens the order for
    /// reconciliation, and is never applied blind.
    #[must_use]
    pub const fn allowed(self) -> &'static [Self] {
        match self {
            Self::Created => &[Self::Submitting, Self::Reconciling],
            Self::Submitting => &[
                Self::Working,
                // An execution report can race ahead of the acknowledgement.
                Self::Partial,
                // A marketable order can fill on arrival.
                Self::Filled,
                Self::Rejected,
                Self::Unknown,
                // Crash recovery of a submission that was in flight.
                Self::Reconciling,
            ],
            Self::Working => &[
                Self::Partial,
                Self::Filled,
                Self::CancelPending,
                // Unsolicited: self-trade prevention, or a venue-side cancel.
                Self::Canceled,
                Self::Reconciling,
            ],
            Self::Partial => &[
                // Further partial fills.
                Self::Partial,
                Self::Filled,
                Self::CancelPending,
                // Unsolicited cancel of the resting remainder.
                Self::Canceled,
                Self::Reconciling,
            ],
            Self::CancelPending => &[
                // A fill while cancel-pending: the cancel is still open.
                Self::CancelPending,
                Self::Canceled,
                // Fully filled before the cancel took effect.
                Self::Filled,
                Self::Reconciling,
            ],
            Self::Unknown => &[Self::Reconciling],
            Self::Reconciling => &[
                // Ingests backfilled fills as evidence without acting on them;
                // only resolution exits this state.
                Self::Reconciling,
                Self::Working,
                Self::Partial,
                Self::Filled,
                Self::Canceled,
                Self::Rejected,
            ],
            Self::Filled | Self::Canceled | Self::Rejected => &[Self::Reconciling],
        }
    }

    /// Whether `target` is reachable from this state.
    #[must_use]
    pub fn permits(self, target: Self) -> bool {
        self.allowed().contains(&target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_leads_only_to_reconciling() {
        // The load-bearing rule of R1.3 and R1.4, asserted against the map
        // itself rather than against the code that consults it.
        assert_eq!(OrderState::Unknown.allowed(), &[OrderState::Reconciling]);
    }

    #[test]
    fn terminal_states_lead_only_to_reconciling() {
        for &s in OrderState::ALL {
            if s.is_terminal() {
                assert_eq!(s.allowed(), &[OrderState::Reconciling], "{s}");
            }
        }
    }

    #[test]
    fn nothing_returns_to_created_or_submitting() {
        // Both are pre-venue states. Reaching one again would mean an order
        // that has been seen by the venue is about to be submitted afresh.
        for &from in OrderState::ALL {
            assert!(!from.permits(OrderState::Created), "{from} -> CREATED");
            if from != OrderState::Created {
                assert!(
                    !from.permits(OrderState::Submitting),
                    "{from} -> SUBMITTING"
                );
            }
        }
    }

    #[test]
    fn every_state_is_reachable_from_somewhere() {
        // A state nothing leads to is either dead code or a missing edge, and
        // the map is too easy to extend without noticing which.
        for &target in OrderState::ALL {
            if target == OrderState::Created {
                continue; // the entry point: an intent starts here.
            }
            assert!(
                OrderState::ALL.iter().any(|&from| from.permits(target)),
                "{target} is unreachable"
            );
        }
    }

    #[test]
    fn only_unprovable_states_lock_the_instrument() {
        for &s in OrderState::ALL {
            assert_eq!(
                s.locks_instrument(),
                matches!(s, OrderState::Unknown | OrderState::Reconciling),
                "{s}"
            );
        }
    }

    #[test]
    fn live_and_terminal_partition_the_states() {
        for &s in OrderState::ALL {
            assert_ne!(s.is_live(), s.is_terminal(), "{s} is both or neither");
        }
    }

    #[test]
    fn the_map_holds_no_duplicate_edges() {
        for &from in OrderState::ALL {
            let edges = from.allowed();
            for (i, &a) in edges.iter().enumerate() {
                assert!(
                    !edges[i + 1..].contains(&a),
                    "{from} lists {a} more than once"
                );
            }
        }
    }
}

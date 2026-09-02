//! The pure transition function: `(order, event) -> order`.
//!
//! No I/O, no clock, no randomness, no allocation. Every rule this system
//! enforces about order lifecycle lives here, which is what lets the integrity
//! scenarios be tested before a journal or a venue exists.
//!
//! Defence in depth: each arm computes its target state, and the result is
//! *also* checked against [`OrderState::allowed`]. An arm with a bug cannot
//! smuggle an illegal transition past the map.

use sentinel_types::Qty;

use crate::error::TransitionError;
use crate::event::OrderEvent;
use crate::order::OrderCore;
use crate::state::OrderState;

/// A resting maker can be matched by the venue for more than it asked — fill
/// granularity, or a demo engine that over-matches. The venue is the source of
/// truth (R1.12), so the *order's* recorded fill is clamped to its quantity
/// (keeping the order invariant `filled <= qty`) while the fill record carries
/// the real executed quantity into the position, so exposure tracks the venue
/// rather than our order book.
///
/// Only an absurd overfill — beyond this multiple — is treated as a genuine
/// fault. That cannot be a mere over-match; it is a double-submit or a parse
/// error, and both must halt.
const OVERFILL_GROSS_MULTIPLE: i64 = 2;

/// Apply an event to an order.
///
/// # Errors
/// - [`TransitionError::Illegal`] when the lifecycle does not contain the move.
/// - [`TransitionError::RequiresReconciliation`] when the event is legal-looking
///   evidence that may not be applied from this state — route the order through
///   reconciliation rather than treating this as a failure.
/// - [`TransitionError::Overfill`] / [`TransitionError::Overflow`] when the
///   arithmetic says the books are already wrong.
pub fn transition(order: OrderCore, event: OrderEvent) -> Result<OrderCore, TransitionError> {
    let s = order.state;
    let kind = event.kind();
    let illegal = |to: Option<OrderState>| TransitionError::Illegal {
        from: s,
        event: kind,
        to,
    };

    match event {
        OrderEvent::SubmissionStarted => {
            if s != OrderState::Created {
                return Err(illegal(Some(OrderState::Submitting)));
            }
            guard(s, OrderState::Submitting, kind)?;
            Ok(OrderCore {
                state: OrderState::Submitting,
                ..order
            })
        }

        OrderEvent::VenueAcked { venue_order_id } => {
            if s != OrderState::Submitting {
                return Err(illegal(Some(OrderState::Working)));
            }
            guard(s, OrderState::Working, kind)?;
            Ok(OrderCore {
                state: OrderState::Working,
                venue_order_id: Some(venue_order_id),
                ..order
            })
        }

        OrderEvent::VenueRejected { .. } => {
            if s != OrderState::Submitting {
                return Err(illegal(Some(OrderState::Rejected)));
            }
            guard(s, OrderState::Rejected, kind)?;
            Ok(OrderCore {
                state: OrderState::Rejected,
                ..order
            })
        }

        OrderEvent::SubmissionTimedOut => {
            // A timeout is not a rejection (R1.3). The outcome is unprovable,
            // so the order parks in UNKNOWN and only reconciliation moves it.
            // There is no branch below this that could resubmit; the code that
            // would blind-retry was never written.
            if s != OrderState::Submitting {
                return Err(illegal(Some(OrderState::Unknown)));
            }
            guard(s, OrderState::Unknown, kind)?;
            Ok(OrderCore {
                state: OrderState::Unknown,
                ..order
            })
        }

        OrderEvent::FillReceived { qty, .. } => apply_fill(order, qty, kind),

        OrderEvent::CancelRequested => {
            // Cancelling an UNKNOWN order is forbidden by construction: you
            // cannot cancel what you cannot prove exists (R1.4). Reconcile first.
            if !matches!(s, OrderState::Working | OrderState::Partial) {
                return Err(illegal(Some(OrderState::CancelPending)));
            }
            guard(s, OrderState::CancelPending, kind)?;
            Ok(OrderCore {
                state: OrderState::CancelPending,
                ..order
            })
        }

        OrderEvent::CancelConfirmed => apply_cancel_confirmed(order, kind),

        OrderEvent::ReconcileStarted { .. } => {
            if s == OrderState::Reconciling {
                return Err(illegal(Some(OrderState::Reconciling)));
            }
            guard(s, OrderState::Reconciling, kind)?;
            Ok(OrderCore {
                state: OrderState::Reconciling,
                ..order
            })
        }

        OrderEvent::ReconcileResolved {
            state,
            venue_order_id,
            filled_qty,
        } => {
            if s != OrderState::Reconciling {
                return Err(illegal(Some(state)));
            }
            guard(s, state, kind)?;
            let ceiling = gross_ceiling(order.qty)?;
            if filled_qty.is_negative() || filled_qty > ceiling {
                return Err(TransitionError::Overfill {
                    ordered: order.qty,
                    filled: Qty::ZERO,
                    attempted: filled_qty,
                });
            }
            // Venue truth replaces local belief (R1.12). The pre-reconcile
            // snapshot stays in the journal, so the divergence is auditable.
            Ok(OrderCore {
                state,
                venue_order_id: venue_order_id.or(order.venue_order_id),
                filled_qty: filled_qty.min(order.qty),
                ..order
            })
        }
    }
}

/// The map check. Every arm goes through it, including the ones that already
/// pinned the source state, because the map is the specification and the arm is
/// only an implementation of it.
fn guard(
    from: OrderState,
    to: OrderState,
    event: crate::event::EventKind,
) -> Result<(), TransitionError> {
    if from.permits(to) {
        Ok(())
    } else {
        Err(TransitionError::Illegal {
            from,
            event,
            to: Some(to),
        })
    }
}

/// `qty * OVERFILL_GROSS_MULTIPLE`, as an addition so no multiplication can wrap.
fn gross_ceiling(qty: Qty) -> Result<Qty, TransitionError> {
    let mut ceiling = qty;
    for _ in 1..OVERFILL_GROSS_MULTIPLE {
        ceiling = ceiling
            .checked_add(qty)
            .map_err(|_| TransitionError::Overflow)?;
    }
    Ok(ceiling)
}

fn apply_fill(
    order: OrderCore,
    fill_qty: Qty,
    kind: crate::event::EventKind,
) -> Result<OrderCore, TransitionError> {
    let s = order.state;

    if s.is_terminal() || s == OrderState::Unknown {
        // Evidence we cannot apply directly: a fill for an order we believed
        // finished (R1.7), or one whose submission is unprovable. Route through
        // reconciliation — never apply blind.
        return Err(TransitionError::RequiresReconciliation {
            state: s,
            event: kind,
        });
    }

    if !matches!(
        s,
        // An execution report racing ahead of the acknowledgement.
        OrderState::Submitting
            | OrderState::Working
            | OrderState::Partial
            // A fill during cancel-pending (R1.6).
            | OrderState::CancelPending
            // Backfill ingestion during recovery.
            | OrderState::Reconciling
    ) {
        return Err(TransitionError::Illegal {
            from: s,
            event: kind,
            to: None,
        });
    }

    if !fill_qty.is_positive() {
        return Err(TransitionError::NonPositiveFill { qty: fill_qty });
    }

    let new_filled = order
        .filled_qty
        .checked_add(fill_qty)
        .map_err(|_| TransitionError::Overflow)?;
    if new_filled > gross_ceiling(order.qty)? {
        return Err(TransitionError::Overfill {
            ordered: order.qty,
            filled: order.filled_qty,
            attempted: fill_qty,
        });
    }

    // Clamp the order's recorded fill: a tolerated over-match completes the
    // order while the fill record still carries the real quantity into the
    // position projection.
    let recorded = new_filled.min(order.qty);

    let target = if s == OrderState::Reconciling {
        // Reconciliation ingests evidence and never concludes from it. Even a
        // completing fill stays RECONCILING until resolution.
        OrderState::Reconciling
    } else if recorded >= order.qty {
        OrderState::Filled
    } else if s == OrderState::CancelPending {
        // The cancel is still open at the venue; a partial fill does not
        // resolve it. The remainder keeps shrinking until CancelConfirmed.
        OrderState::CancelPending
    } else {
        OrderState::Partial
    };

    guard(s, target, kind)?;
    Ok(OrderCore {
        state: target,
        filled_qty: recorded,
        ..order
    })
}

fn apply_cancel_confirmed(
    order: OrderCore,
    kind: crate::event::EventKind,
) -> Result<OrderCore, TransitionError> {
    let s = order.state;

    // A cancel confirmation is authoritative: the order is dead at the venue.
    // Usually it follows our own cancel, but it can also be unsolicited — the
    // venue killing a resting or partly-filled order itself through self-trade
    // prevention or an exchange-side cancel.
    match s {
        OrderState::Reconciling => {
            // The same race as a fill landing mid-reconcile: a live execution
            // report arriving inside the reconcile window, not a divergence.
            // Reconciliation ingests evidence but never concludes from it, so
            // absorb it — recorded in the journal as from == to == RECONCILING —
            // and let ReconcileResolved replace belief with venue truth.
            // Halting here would be over-escalation, and inconsistent with how
            // a fill is treated in this very state.
            Ok(order)
        }
        OrderState::Canceled => {
            // Duplicate delivery. Venue streams are at-least-once, and unlike a
            // fill a cancel confirmation carries no dedup key of its own.
            // Re-confirming a cancel we already booked changes nothing.
            Ok(order)
        }
        OrderState::Filled | OrderState::Rejected | OrderState::Unknown => {
            // This contradicts what we booked. Evidence, not application — the
            // same policy as a late fill (R1.7): resolve against venue truth
            // rather than either absorbing blind or halting.
            Err(TransitionError::RequiresReconciliation {
                state: s,
                event: kind,
            })
        }
        OrderState::CancelPending | OrderState::Working | OrderState::Partial => {
            guard(s, OrderState::Canceled, kind)?;
            Ok(OrderCore {
                state: OrderState::Canceled,
                ..order
            })
        }
        OrderState::Created | OrderState::Submitting => Err(TransitionError::Illegal {
            from: s,
            event: kind,
            to: Some(OrderState::Canceled),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventKind, ReconcileCause, RejectReason};
    use sentinel_types::{ClientOrderId, ExecId, Instrument, Price, Side, VenueOrderId};

    fn new_order(qty: i64) -> OrderCore {
        OrderCore::created(
            ClientOrderId::new("UI-4bb5ba826e2e1").unwrap(),
            Instrument::new("BTCUSD").unwrap(),
            Side::Buy,
            Qty::whole(qty),
        )
    }

    fn venue_id(s: &str) -> VenueOrderId {
        VenueOrderId::new(s).unwrap()
    }

    fn fill(exec: &str, qty: i64) -> OrderEvent {
        OrderEvent::FillReceived {
            exec_id: ExecId::new(exec).unwrap(),
            qty: Qty::whole(qty),
            price: Price::whole(100),
        }
    }

    /// Drive an order through a sequence, asserting each step lands.
    fn drive(mut o: OrderCore, events: &[OrderEvent]) -> OrderCore {
        for &e in events {
            o = transition(o, e).unwrap_or_else(|err| panic!("{:?} failed: {err}", e.kind()));
        }
        o
    }

    fn working(qty: i64) -> OrderCore {
        drive(
            new_order(qty),
            &[
                OrderEvent::SubmissionStarted,
                OrderEvent::VenueAcked {
                    venue_order_id: venue_id("V-1"),
                },
            ],
        )
    }

    // ------------------------------------------------------------ happy paths

    #[test]
    fn submission_acknowledgement_and_a_full_fill() {
        let o = drive(
            new_order(10),
            &[
                OrderEvent::SubmissionStarted,
                OrderEvent::VenueAcked {
                    venue_order_id: venue_id("V-1"),
                },
                fill("E-1", 10),
            ],
        );
        assert_eq!(o.state, OrderState::Filled);
        assert_eq!(o.filled_qty, Qty::whole(10));
        assert_eq!(o.venue_order_id, Some(venue_id("V-1")));
        assert_eq!(o.remaining_qty(), Qty::ZERO);
    }

    /// R1.5 — filled and remaining are exact across any sequence of partials.
    #[test]
    fn partial_fills_account_exactly() {
        let mut o = working(10);
        for (i, q) in [3i64, 3, 1, 2].into_iter().enumerate() {
            o = transition(o, fill(&format!("E-{i}"), q)).unwrap();
            assert_eq!(o.state, OrderState::Partial, "step {i}");
        }
        assert_eq!(o.filled_qty, Qty::whole(9));
        assert_eq!(o.remaining_qty(), Qty::whole(1));
        o = transition(o, fill("E-last", 1)).unwrap();
        assert_eq!(o.state, OrderState::Filled);
        assert_eq!(o.remaining_qty(), Qty::ZERO);
    }

    #[test]
    fn an_execution_report_may_race_ahead_of_the_acknowledgement() {
        let o = drive(
            new_order(10),
            &[OrderEvent::SubmissionStarted, fill("E-1", 4)],
        );
        assert_eq!(o.state, OrderState::Partial);
        assert_eq!(o.filled_qty, Qty::whole(4));
    }

    // ---------------------------------------------------- R1.3 timeout is not a rejection

    #[test]
    fn a_timeout_parks_in_unknown_and_nothing_may_act_on_it() {
        let o = drive(
            new_order(10),
            &[
                OrderEvent::SubmissionStarted,
                OrderEvent::SubmissionTimedOut,
            ],
        );
        assert_eq!(o.state, OrderState::Unknown);
        assert!(o.locks_instrument());

        // Not a rejection: it is not terminal, and nothing resubmits from here.
        assert!(!o.is_terminal());

        // Every door out of UNKNOWN except reconciliation is shut.
        assert!(transition(o, OrderEvent::CancelRequested).is_err());
        assert!(transition(o, OrderEvent::SubmissionStarted).is_err());
        assert!(
            transition(
                o,
                OrderEvent::VenueAcked {
                    venue_order_id: venue_id("V-1")
                }
            )
            .is_err()
        );
        // And a fill is evidence to reconcile, not something to apply.
        assert_eq!(
            transition(o, fill("E-1", 4)),
            Err(TransitionError::RequiresReconciliation {
                state: OrderState::Unknown,
                event: EventKind::FillReceived,
            })
        );

        // The one legal move.
        let o = transition(
            o,
            OrderEvent::ReconcileStarted {
                cause: ReconcileCause::SubmissionTimeout,
            },
        )
        .unwrap();
        assert_eq!(o.state, OrderState::Reconciling);
    }

    #[test]
    fn a_timeout_outside_submitting_is_a_caller_bug() {
        let o = working(10);
        assert!(matches!(
            transition(o, OrderEvent::SubmissionTimedOut),
            Err(TransitionError::Illegal {
                from: OrderState::Working,
                ..
            })
        ));
    }

    // ---------------------------------------------------- R1.6 fill during cancel-pending

    #[test]
    fn a_fill_during_cancel_pending_is_accepted_and_the_cancel_stays_open() {
        let o = drive(working(10), &[fill("E-1", 4), OrderEvent::CancelRequested]);
        assert_eq!(o.state, OrderState::CancelPending);

        let o = transition(o, fill("E-2", 3)).unwrap();
        // Still cancel-pending: the venue has not answered the cancel yet.
        assert_eq!(o.state, OrderState::CancelPending);
        assert_eq!(o.filled_qty, Qty::whole(7));

        // The cancel then applies only to what conclusively remains.
        let o = transition(o, OrderEvent::CancelConfirmed).unwrap();
        assert_eq!(o.state, OrderState::Canceled);
        assert_eq!(o.filled_qty, Qty::whole(7));
        assert_eq!(o.remaining_qty(), Qty::whole(3));
    }

    #[test]
    fn a_completing_fill_during_cancel_pending_fills_the_order() {
        let o = drive(working(10), &[OrderEvent::CancelRequested, fill("E-1", 10)]);
        assert_eq!(o.state, OrderState::Filled);
    }

    // ---------------------------------------------------- R1.7 late fill

    #[test]
    fn a_fill_after_terminal_is_evidence_not_an_application() {
        for terminal in [
            OrderState::Filled,
            OrderState::Canceled,
            OrderState::Rejected,
        ] {
            let o = OrderCore {
                state: terminal,
                ..working(10)
            };
            assert_eq!(
                transition(o, fill("E-late", 1)),
                Err(TransitionError::RequiresReconciliation {
                    state: terminal,
                    event: EventKind::FillReceived,
                }),
                "{terminal}"
            );
        }
    }

    #[test]
    fn a_cancel_confirm_contradicting_a_terminal_order_reconciles() {
        for state in [
            OrderState::Filled,
            OrderState::Rejected,
            OrderState::Unknown,
        ] {
            let o = OrderCore {
                state,
                ..working(10)
            };
            assert_eq!(
                transition(o, OrderEvent::CancelConfirmed),
                Err(TransitionError::RequiresReconciliation {
                    state,
                    event: EventKind::CancelConfirmed,
                }),
                "{state}"
            );
        }
    }

    #[test]
    fn a_duplicate_cancel_confirm_is_absorbed_not_escalated() {
        // At-least-once delivery, and no dedup key of its own.
        let o = drive(
            working(10),
            &[OrderEvent::CancelRequested, OrderEvent::CancelConfirmed],
        );
        assert_eq!(o.state, OrderState::Canceled);
        let again = transition(o, OrderEvent::CancelConfirmed).unwrap();
        assert_eq!(again, o);
    }

    #[test]
    fn an_unsolicited_cancel_is_accepted_from_working_and_partial() {
        // Self-trade prevention, or a venue-side cancel. We never asked.
        let o = transition(working(10), OrderEvent::CancelConfirmed).unwrap();
        assert_eq!(o.state, OrderState::Canceled);

        let o = drive(working(10), &[fill("E-1", 4)]);
        let o = transition(o, OrderEvent::CancelConfirmed).unwrap();
        assert_eq!(o.state, OrderState::Canceled);
        assert_eq!(o.filled_qty, Qty::whole(4), "fills already applied survive");
    }

    // ---------------------------------------------------- reconciliation

    #[test]
    fn reconciliation_ingests_evidence_without_concluding_from_it() {
        let o = drive(
            working(10),
            &[OrderEvent::ReconcileStarted {
                cause: ReconcileCause::StaleSweep,
            }],
        );
        // A completing fill arrives mid-question. It is recorded, and the
        // order still does not leave RECONCILING.
        let o = transition(o, fill("E-1", 10)).unwrap();
        assert_eq!(o.state, OrderState::Reconciling);
        assert_eq!(o.filled_qty, Qty::whole(10));

        // A cancel confirmation lands in the same window: absorbed, not fatal.
        let same = transition(o, OrderEvent::CancelConfirmed).unwrap();
        assert_eq!(same, o);

        // Only resolution ends it.
        let o = transition(
            o,
            OrderEvent::ReconcileResolved {
                state: OrderState::Filled,
                venue_order_id: Some(venue_id("V-1")),
                filled_qty: Qty::whole(10),
            },
        )
        .unwrap();
        assert_eq!(o.state, OrderState::Filled);
    }

    /// R1.12 — the venue wins, and the correction is an explicit event.
    #[test]
    fn resolution_replaces_local_belief_with_venue_truth() {
        let o = drive(
            working(10),
            &[
                fill("E-1", 7),
                OrderEvent::ReconcileStarted {
                    cause: ReconcileCause::PositionDisagreement,
                },
            ],
        );
        assert_eq!(o.filled_qty, Qty::whole(7), "what we believed");

        let resolved = transition(
            o,
            OrderEvent::ReconcileResolved {
                state: OrderState::Filled,
                venue_order_id: Some(venue_id("V-1")),
                filled_qty: Qty::whole(10),
            },
        )
        .unwrap();
        assert_eq!(resolved.filled_qty, Qty::whole(10), "what the venue says");
    }

    #[test]
    fn resolution_can_report_the_order_never_existed() {
        // CANCELED with no venue id: conclusively absent. The intent never
        // became exposure, and any resubmission is a new order (R1.3).
        let o = drive(
            new_order(10),
            &[
                OrderEvent::SubmissionStarted,
                OrderEvent::SubmissionTimedOut,
                OrderEvent::ReconcileStarted {
                    cause: ReconcileCause::SubmissionTimeout,
                },
            ],
        );
        let o = transition(
            o,
            OrderEvent::ReconcileResolved {
                state: OrderState::Canceled,
                venue_order_id: None,
                filled_qty: Qty::ZERO,
            },
        )
        .unwrap();
        assert_eq!(o.state, OrderState::Canceled);
        assert_eq!(o.filled_qty, Qty::ZERO);
        assert_eq!(o.venue_order_id, None);
    }

    #[test]
    fn resolution_keeps_a_venue_id_we_already_knew() {
        let o = drive(
            working(10),
            &[OrderEvent::ReconcileStarted {
                cause: ReconcileCause::StaleSweep,
            }],
        );
        let o = transition(
            o,
            OrderEvent::ReconcileResolved {
                state: OrderState::Working,
                venue_order_id: None,
                filled_qty: Qty::ZERO,
            },
        )
        .unwrap();
        assert_eq!(o.venue_order_id, Some(venue_id("V-1")));
    }

    #[test]
    fn reconciling_twice_is_a_caller_bug() {
        let o = drive(
            working(10),
            &[OrderEvent::ReconcileStarted {
                cause: ReconcileCause::StaleSweep,
            }],
        );
        assert!(
            transition(
                o,
                OrderEvent::ReconcileStarted {
                    cause: ReconcileCause::LateFill
                }
            )
            .is_err()
        );
    }

    #[test]
    fn resolving_an_order_that_is_not_reconciling_is_a_caller_bug() {
        let o = working(10);
        assert!(matches!(
            transition(
                o,
                OrderEvent::ReconcileResolved {
                    state: OrderState::Filled,
                    venue_order_id: None,
                    filled_qty: Qty::whole(10),
                },
            ),
            Err(TransitionError::Illegal {
                from: OrderState::Working,
                ..
            })
        ));
    }

    // ---------------------------------------------------- overfill

    #[test]
    fn a_mild_over_match_is_tolerated_and_clamped() {
        // The venue matched 11 against an order of 10. The venue is the truth,
        // so the order completes; the fill record carries the real 11 into the
        // position projection, which is the ledger's job and not this one's.
        let o = transition(working(10), fill("E-1", 11)).unwrap();
        assert_eq!(o.state, OrderState::Filled);
        assert_eq!(
            o.filled_qty,
            Qty::whole(10),
            "clamped, so filled <= qty holds"
        );
    }

    #[test]
    fn an_absurd_overfill_is_a_fault_and_says_so() {
        let err = transition(working(10), fill("E-1", 21)).unwrap_err();
        assert!(matches!(err, TransitionError::Overfill { .. }));
        assert!(err.is_integrity_violation());
    }

    #[test]
    fn overfill_is_measured_cumulatively_not_per_fill() {
        let o = drive(working(10), &[fill("E-1", 9)]);
        // 9 + 9 = 18, under the 20 ceiling: tolerated and clamped.
        let o2 = transition(o, fill("E-2", 9)).unwrap();
        assert_eq!(o2.filled_qty, Qty::whole(10));
        // 9 + 12 = 21, over it.
        assert!(matches!(
            transition(o, fill("E-3", 12)),
            Err(TransitionError::Overfill { .. })
        ));
    }

    #[test]
    fn a_zero_or_negative_fill_is_refused() {
        for q in [0i64, -1] {
            let e = OrderEvent::FillReceived {
                exec_id: ExecId::new("E-1").unwrap(),
                qty: Qty::whole(q),
                price: Price::whole(100),
            };
            assert!(matches!(
                transition(working(10), e),
                Err(TransitionError::NonPositiveFill { .. })
            ));
        }
    }

    #[test]
    fn resolution_refuses_a_filled_quantity_outside_the_tolerance() {
        let o = drive(
            working(10),
            &[OrderEvent::ReconcileStarted {
                cause: ReconcileCause::StaleSweep,
            }],
        );
        for bad in [Qty::whole(-1), Qty::whole(21)] {
            assert!(matches!(
                transition(
                    o,
                    OrderEvent::ReconcileResolved {
                        state: OrderState::Filled,
                        venue_order_id: None,
                        filled_qty: bad,
                    },
                ),
                Err(TransitionError::Overfill { .. })
            ));
        }
    }

    // ---------------------------------------------------- structural

    #[test]
    fn cancelling_an_unknown_order_is_impossible() {
        // R1.4 stated as a property: you cannot cancel what you cannot prove
        // exists, and there is no state outside WORKING/PARTIAL where you can.
        for &s in OrderState::ALL {
            let o = OrderCore {
                state: s,
                ..working(10)
            };
            let allowed = matches!(s, OrderState::Working | OrderState::Partial);
            assert_eq!(
                transition(o, OrderEvent::CancelRequested).is_ok(),
                allowed,
                "{s}"
            );
        }
    }

    #[test]
    fn no_event_can_reach_a_state_the_map_forbids() {
        // The defence-in-depth claim, checked exhaustively rather than argued:
        // whatever the arms compute, every successful transition is an edge the
        // map contains.
        let samples = [
            OrderEvent::SubmissionStarted,
            OrderEvent::VenueAcked {
                venue_order_id: venue_id("V-1"),
            },
            OrderEvent::VenueRejected {
                reason: RejectReason::new(-2019, "no margin"),
            },
            OrderEvent::SubmissionTimedOut,
            fill("E-1", 1),
            fill("E-2", 10),
            OrderEvent::CancelRequested,
            OrderEvent::CancelConfirmed,
            OrderEvent::ReconcileStarted {
                cause: ReconcileCause::StaleSweep,
            },
            OrderEvent::ReconcileResolved {
                state: OrderState::Filled,
                venue_order_id: None,
                filled_qty: Qty::whole(10),
            },
        ];
        for &from in OrderState::ALL {
            for &event in &samples {
                let before = OrderCore {
                    state: from,
                    ..working(10)
                };
                if let Ok(after) = transition(before, event) {
                    assert!(
                        after.state == from || from.permits(after.state),
                        "{from} -{:?}-> {} is not in the map",
                        event.kind(),
                        after.state
                    );
                }
            }
        }
    }

    #[test]
    fn the_authorised_quantity_is_never_rewritten() {
        // A replace is a new order. Nothing in the lifecycle edits `qty`, and
        // an arm that did would let a fill change what was authorised.
        let samples = [
            OrderEvent::SubmissionStarted,
            OrderEvent::VenueAcked {
                venue_order_id: venue_id("V-1"),
            },
            fill("E-1", 3),
            OrderEvent::CancelRequested,
            OrderEvent::CancelConfirmed,
            OrderEvent::ReconcileStarted {
                cause: ReconcileCause::StaleSweep,
            },
        ];
        for &from in OrderState::ALL {
            for &event in &samples {
                let before = OrderCore {
                    state: from,
                    ..working(10)
                };
                if let Ok(after) = transition(before, event) {
                    assert_eq!(after.qty, before.qty, "{from} {:?}", event.kind());
                    assert_eq!(after.client_order_id, before.client_order_id);
                    assert_eq!(after.instrument, before.instrument);
                    assert_eq!(after.side, before.side);
                }
            }
        }
    }

    #[test]
    fn filled_never_exceeds_the_authorised_quantity() {
        // The order invariant, over every state and a range of fills.
        for &from in OrderState::ALL {
            for q in [1i64, 5, 10, 11, 19] {
                let before = OrderCore {
                    state: from,
                    ..working(10)
                };
                if let Ok(after) = transition(before, fill("E-x", q)) {
                    assert!(after.filled_qty <= after.qty, "{from} filled {q}");
                }
            }
        }
    }
}

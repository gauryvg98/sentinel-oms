//! The R1 scenarios, at the level the domain can demonstrate them.
//!
//! These are not unit tests of a function; they are the specification, driven
//! end to end through the lifecycle. What the domain cannot show alone — that
//! intent was durable before submission, that a crash and restart rebuild the
//! same state, that exits are bounded by the reconciled position — belongs to
//! the journal, the engine and the risk crate, and is tested there against the
//! same scenario numbers.
//!
//! The compound scenario (R1.14) is one test, run as one sequence, because a
//! chain of failures that each pass alone is exactly the thing that broke.

use sentinel_types::{ClientOrderId, ExecId, Instrument, Price, Qty, Side, VenueOrderId};

use crate::{OrderCore, OrderEvent, OrderState, ReconcileCause, TransitionError, transition};

fn order(qty: i64) -> OrderCore {
    OrderCore::created(
        ClientOrderId::new("SENT-R1-14").unwrap(),
        Instrument::new("BTCUSD").unwrap(),
        Side::Buy,
        Qty::whole(qty),
    )
}

fn fill(exec: &str, qty: i64) -> OrderEvent {
    OrderEvent::FillReceived {
        exec_id: ExecId::new(exec).unwrap(),
        qty: Qty::whole(qty),
        price: Price::parse("109432.5").unwrap(),
    }
}

fn step(o: OrderCore, e: OrderEvent) -> OrderCore {
    transition(o, e).unwrap_or_else(|err| panic!("{:?} rejected: {err}", e.kind()))
}

/// R1.14 — the compound scenario, as one deterministic sequence.
///
/// Multi-contract intent, a submission timeout with no venue id, a venue that
/// accepted anyway, two partial fills, a cancel request, a third fill during
/// cancel-pending, a crash, a restart that disagrees with the venue, and
/// reconciliation to the true filled quantity.
///
/// The assertions at the end are the point: no duplicate entry, correct
/// remaining quantity, and an order whose recorded fill is the venue's number
/// rather than ours.
#[test]
fn r1_14_compound_failure_chain() {
    // Ten contracts authorised. The intent is durable before this line; that
    // half of R1.1 is the journal's to prove.
    let o = order(10);
    assert_eq!(o.state, OrderState::Created);

    // --- submission times out with no venue order id ------------------------
    let o = step(o, OrderEvent::SubmissionStarted);
    let o = step(o, OrderEvent::SubmissionTimedOut);
    assert_eq!(
        o.state,
        OrderState::Unknown,
        "R1.3: a timeout is not a rejection"
    );
    assert!(o.locks_instrument(), "R1.4: the instrument is held");

    // Nothing may act on it. In particular there is no resubmission path — a
    // second SubmissionStarted is refused by the lifecycle, not by a policy
    // check someone could relax later.
    assert!(transition(o, OrderEvent::SubmissionStarted).is_err());
    assert!(transition(o, OrderEvent::CancelRequested).is_err());

    // --- the venue had accepted it after all --------------------------------
    let o = step(
        o,
        OrderEvent::ReconcileStarted {
            cause: ReconcileCause::SubmissionTimeout,
        },
    );
    let o = step(
        o,
        OrderEvent::ReconcileResolved {
            state: OrderState::Working,
            venue_order_id: Some(VenueOrderId::new("V-77").unwrap()),
            filled_qty: Qty::ZERO,
        },
    );
    assert_eq!(o.state, OrderState::Working);
    assert_eq!(o.venue_order_id, Some(VenueOrderId::new("V-77").unwrap()));
    assert!(
        !o.locks_instrument(),
        "resolved: the instrument is released"
    );

    // --- two partial fills --------------------------------------------------
    let o = step(o, fill("E-1", 3));
    let o = step(o, fill("E-2", 2));
    assert_eq!(o.state, OrderState::Partial);
    assert_eq!(
        o.filled_qty,
        Qty::whole(5),
        "R1.5: partials account exactly"
    );

    // --- cancel requested, then a third fill lands mid-cancel ---------------
    let o = step(o, OrderEvent::CancelRequested);
    assert_eq!(o.state, OrderState::CancelPending);

    let o = step(o, fill("E-3", 2));
    assert_eq!(
        o.state,
        OrderState::CancelPending,
        "R1.6: the fill is accepted and the cancel stays open"
    );
    assert_eq!(o.filled_qty, Qty::whole(7));

    // --- crash. restart rebuilds this exact state from the log --------------
    // Rebuilding is the journal's test; what the domain guarantees is that the
    // fold is a pure function of the events, so the reconstruction below is
    // the same value by construction.
    let rebuilt = replay(&[
        OrderEvent::SubmissionStarted,
        OrderEvent::SubmissionTimedOut,
        OrderEvent::ReconcileStarted {
            cause: ReconcileCause::SubmissionTimeout,
        },
        OrderEvent::ReconcileResolved {
            state: OrderState::Working,
            venue_order_id: Some(VenueOrderId::new("V-77").unwrap()),
            filled_qty: Qty::ZERO,
        },
        fill("E-1", 3),
        fill("E-2", 2),
        OrderEvent::CancelRequested,
        fill("E-3", 2),
    ]);
    assert_eq!(rebuilt, o, "R1.11: replay reproduces the pre-crash state");

    // --- restart finds the venue disagrees ----------------------------------
    // We believe 7 filled. The venue says 8: a fill landed in the gap while we
    // were down.
    let o = step(
        o,
        OrderEvent::ReconcileStarted {
            cause: ReconcileCause::PositionDisagreement,
        },
    );

    // Evidence arriving mid-question is recorded and not acted on.
    let o = step(o, fill("E-4", 1));
    assert_eq!(o.state, OrderState::Reconciling, "still asking");

    let o = step(
        o,
        OrderEvent::ReconcileResolved {
            state: OrderState::Canceled,
            venue_order_id: Some(VenueOrderId::new("V-77").unwrap()),
            filled_qty: Qty::whole(8),
        },
    );

    // --- what must be true at the end ---------------------------------------
    assert_eq!(o.state, OrderState::Canceled);
    assert_eq!(
        o.filled_qty,
        Qty::whole(8),
        "R1.12: the venue's number wins"
    );
    assert_eq!(o.remaining_qty(), Qty::whole(2), "the cancel took the rest");
    assert_eq!(
        o.qty,
        Qty::whole(10),
        "R1.9: still one order, never re-authorised"
    );
    assert!(!o.locks_instrument(), "converged: trading may resume");
}

/// Fold events over a fresh order, the way recovery does.
fn replay(events: &[OrderEvent]) -> OrderCore {
    events.iter().fold(order(10), |o, &e| step(o, e))
}

/// R1.11 — replay is a pure fold, so it is deterministic by construction.
#[test]
fn replaying_the_same_events_twice_yields_the_same_order() {
    let events = [
        OrderEvent::SubmissionStarted,
        OrderEvent::VenueAcked {
            venue_order_id: VenueOrderId::new("V-1").unwrap(),
        },
        fill("E-1", 4),
        fill("E-2", 6),
    ];
    assert_eq!(replay(&events), replay(&events));
    assert_eq!(replay(&events).state, OrderState::Filled);
}

/// R1.7 — a fill after the order was presumed terminal is never dropped and
/// never double-applied. It becomes evidence, and reconciliation decides.
#[test]
fn r1_7_a_late_fill_reopens_rather_than_being_dropped() {
    let o = replay(&[
        OrderEvent::SubmissionStarted,
        OrderEvent::VenueAcked {
            venue_order_id: VenueOrderId::new("V-1").unwrap(),
        },
        fill("E-1", 10),
    ]);
    assert_eq!(o.state, OrderState::Filled);

    // The venue sends one more. It is not applied, and it is not discarded.
    let err = transition(o, fill("E-2", 1)).unwrap_err();
    assert!(err.needs_reconciliation());
    assert!(
        !err.is_integrity_violation(),
        "the world is messy, the books are not wrong"
    );

    // The engine's response to that error is to reconcile, and the truth comes
    // back: the venue over-matched, and the order really is complete at 10.
    let o = step(
        o,
        OrderEvent::ReconcileStarted {
            cause: ReconcileCause::LateFill,
        },
    );
    let o = step(
        o,
        OrderEvent::ReconcileResolved {
            state: OrderState::Filled,
            venue_order_id: Some(VenueOrderId::new("V-1").unwrap()),
            filled_qty: Qty::whole(10),
        },
    );
    assert_eq!(o.filled_qty, Qty::whole(10), "applied once, not twice");
}

/// R1.3 — stated as a structural property rather than a behaviour.
///
/// There is no sequence of events that takes an order from a timeout to a
/// second submission. Not "no code path does it" — no path exists.
#[test]
fn r1_3_nothing_resubmits_after_a_timeout() {
    let timed_out = replay(&[
        OrderEvent::SubmissionStarted,
        OrderEvent::SubmissionTimedOut,
    ]);
    assert_eq!(timed_out.state, OrderState::Unknown);

    // Breadth-first over the whole reachable graph from UNKNOWN, applying every
    // event shape at each step. SUBMITTING must never appear.
    let samples = [
        OrderEvent::SubmissionStarted,
        OrderEvent::VenueAcked {
            venue_order_id: VenueOrderId::new("V-1").unwrap(),
        },
        OrderEvent::SubmissionTimedOut,
        fill("E-x", 1),
        OrderEvent::CancelRequested,
        OrderEvent::CancelConfirmed,
        OrderEvent::ReconcileStarted {
            cause: ReconcileCause::LateFill,
        },
        OrderEvent::ReconcileResolved {
            state: OrderState::Working,
            venue_order_id: None,
            filled_qty: Qty::ZERO,
        },
    ];

    let mut seen = vec![timed_out.state];
    let mut frontier = vec![timed_out];
    while let Some(o) = frontier.pop() {
        for &e in &samples {
            if let Ok(next) = transition(o, e) {
                assert_ne!(
                    next.state,
                    OrderState::Submitting,
                    "reached SUBMITTING from {} via {:?}",
                    o.state,
                    e.kind()
                );
                if !seen.contains(&next.state) {
                    seen.push(next.state);
                    frontier.push(next);
                }
            }
        }
    }
    assert!(seen.len() > 1, "the search actually explored something");
}

/// R1.9 — two commands aiming at the same exposure resolve to one order.
///
/// The domain's half of this is that an order's authorised quantity is written
/// once and never changed by anything that happens to it afterwards. The other
/// half — that a replayed intent finds the existing order instead of creating a
/// second one — is the ledger's, and is keyed on the same client order id.
#[test]
fn r1_9_authorised_quantity_is_written_once() {
    let o = replay(&[
        OrderEvent::SubmissionStarted,
        OrderEvent::VenueAcked {
            venue_order_id: VenueOrderId::new("V-1").unwrap(),
        },
        fill("E-1", 3),
        OrderEvent::CancelRequested,
        fill("E-2", 2),
        OrderEvent::CancelConfirmed,
    ]);
    assert_eq!(o.qty, Qty::whole(10));
    assert_eq!(o.client_order_id, ClientOrderId::new("SENT-R1-14").unwrap());
}

/// An overfill beyond tolerance is an integrity violation, not a reconcile.
///
/// The distinction decides what the engine does with it: reconcile means ask
/// the venue, integrity violation means stop trading. Conflating them would
/// turn a doubled position into a retry.
#[test]
fn an_absurd_overfill_is_distinguishable_from_a_late_fill() {
    let o = replay(&[
        OrderEvent::SubmissionStarted,
        OrderEvent::VenueAcked {
            venue_order_id: VenueOrderId::new("V-1").unwrap(),
        },
    ]);
    let err = transition(o, fill("E-1", 25)).unwrap_err();
    assert!(matches!(err, TransitionError::Overfill { .. }));
    assert!(err.is_integrity_violation());
    assert!(!err.needs_reconciliation());
}

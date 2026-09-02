//! The book, driven through the journal rather than poked at directly.
//!
//! Every test here goes through `append -> flush -> fold`, because that is the
//! only path the live system uses. A test that constructed a [`Book`] by hand
//! would be testing a structure the engine never produces.

use std::path::PathBuf;

use sentinel_domain::{EconomicOrderIntent, OrderEvent, OrderState, ReconcileCause, RejectReason};
use sentinel_journal::{Journal, Seq, Tailer};
use sentinel_record::{Actor, Asset, DecisionKind, Note, Record};
use sentinel_types::{
    Authority, ClientOrderId, ExecId, Instrument, Money, Nanos, OrderKind, Price, Qty, Side,
    TraceId, VenueOrderId,
};

use crate::{Applied, Book, Violation, check};

/// A journal and a book kept in step, the way the engine keeps them.
struct Fixture {
    dir: PathBuf,
    journal: Journal,
    book: Book,
    clock: u64,
    /// Set by a test that goes on to reopen the journal itself.
    keep_dir: bool,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("sentinel-ledger-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let journal = Journal::claim(&dir, Nanos::from_u64(1)).unwrap();
        let mut fixture = Self {
            dir,
            journal,
            book: Book::new(),
            clock: 1_000,
            keep_dir: false,
        };
        // The book folds the claim record too, so the live book and a rebuild
        // see exactly the same log.
        fixture.book = fixture.rebuilt();
        fixture
    }

    /// Fill in a lifecycle record's from/to states from the book.
    fn stamp(&self, record: Record) -> Record {
        let Record::OrderEvent {
            client_order_id,
            trace_id,
            event,
            ..
        } = record
        else {
            return record;
        };
        let from_state = self
            .book
            .order(&client_order_id)
            .map_or(OrderState::Created, |o| o.core.state);
        let existing = self.book.order(&client_order_id).map(|o| o.core);
        let next = existing.and_then(|core| sentinel_domain::transition(core, event).ok());
        let to_state = next.map_or(from_state, |next| next.state);
        Record::OrderEvent {
            client_order_id,
            trace_id,
            from_state,
            to_state,
            // Same rule as the engine: a new order is created by this record,
            // and otherwise the transition succeeding is what applied means.
            applied: existing.is_none() || next.is_some(),
            event,
        }
    }

    /// Append a record, make it durable, and apply it to the live book.
    fn feed(&mut self, record: Record) -> Applied {
        let record = self.stamp(record);
        self.clock += 1_000_000;
        let now = Nanos::from_u64(self.clock);
        let payload = record.to_bytes();
        let seq = self
            .journal
            .append_and_flush(record.kind(), now, &payload)
            .unwrap();
        let header = sentinel_journal::FrameHeader {
            seq,
            epoch: self.journal.epoch(),
            nanos: now,
            kind: record.kind(),
            len: u32::try_from(payload.len()).unwrap(),
        };
        self.book.apply(&header, record).unwrap()
    }

    /// Feed a record expecting it to be refused.
    fn feed_err(&mut self, record: Record) -> crate::LedgerError {
        let record = self.stamp(record);
        self.clock += 1_000_000;
        let now = Nanos::from_u64(self.clock);
        let payload = record.to_bytes();
        let seq = self
            .journal
            .append_and_flush(record.kind(), now, &payload)
            .unwrap();
        let header = sentinel_journal::FrameHeader {
            seq,
            epoch: self.journal.epoch(),
            nanos: now,
            kind: record.kind(),
            len: u32::try_from(payload.len()).unwrap(),
        };
        self.book.apply(&header, record).unwrap_err()
    }

    /// Advance journal time without any other effect.
    fn tick(&mut self, by_nanos: u64) {
        self.clock += by_nanos;
        let now = Nanos::from_u64(self.clock);
        let seq = self
            .journal
            .append_and_flush(sentinel_journal::RecordKind::Ticked, now, b"")
            .unwrap();
        let header = sentinel_journal::FrameHeader {
            seq,
            epoch: self.journal.epoch(),
            nanos: now,
            kind: sentinel_journal::RecordKind::Ticked,
            len: 0,
        };
        self.book.apply(&header, Record::Ticked).unwrap();
    }

    /// Fold the log from the beginning, as recovery does.
    fn rebuilt(&self) -> Book {
        let mut tailer = Tailer::open(&self.dir, Seq::ZERO).unwrap();
        Book::rebuild(&mut tailer).unwrap()
    }

    /// Hand the directory to the caller and stop cleaning it up, so a test can
    /// close this writer and open another against the same journal.
    fn keep(&mut self) -> PathBuf {
        self.keep_dir = true;
        self.dir.clone()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if !self.keep_dir {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn coid(s: &str) -> ClientOrderId {
    ClientOrderId::new(s).unwrap()
}

fn btc() -> Instrument {
    Instrument::new("BTCUSD").unwrap()
}

fn px(s: &str) -> Price {
    Price::parse(s).unwrap()
}

fn entry(id: &str, side: Side, qty: i64) -> Record {
    Record::IntentPersisted(
        EconomicOrderIntent::market(
            coid(id),
            btc(),
            side,
            Qty::whole(qty),
            Authority::Entry,
            TraceId::from_u128(0x6440_ba96),
        )
        .unwrap(),
    )
}

fn exit(id: &str, side: Side, qty: i64) -> Record {
    Record::IntentPersisted(
        EconomicOrderIntent::market(
            coid(id),
            btc(),
            side,
            Qty::whole(qty),
            Authority::ProtectiveExit,
            TraceId::from_u128(0x8f82_b303),
        )
        .unwrap(),
    )
}

/// A lifecycle record with its states left blank.
///
/// [`Fixture::feed`] fills them in from the book, the same way the engine does.
/// Writing them out at every call site would make these tests assert the state
/// machine's answers rather than exercise them.
fn event(id: &str, event: OrderEvent) -> Record {
    Record::OrderEvent {
        client_order_id: coid(id),
        trace_id: TraceId::from_u128(0x6440_ba96),
        from_state: OrderState::Created,
        to_state: OrderState::Created,
        // Filled in by `Fixture::feed`, like the states.
        applied: true,
        event,
    }
}

fn ack(id: &str, venue: &str) -> Record {
    event(
        id,
        OrderEvent::VenueAcked {
            venue_order_id: VenueOrderId::new(venue).unwrap(),
        },
    )
}

fn fill(id: &str, exec: &str, qty: i64, price: &str) -> Record {
    event(
        id,
        OrderEvent::FillReceived {
            exec_id: ExecId::new(exec).unwrap(),
            qty: Qty::whole(qty),
            price: px(price),
        },
    )
}

/// Drive one entry to WORKING.
fn working(f: &mut Fixture, id: &str, side: Side, qty: i64) {
    f.feed(entry(id, side, qty));
    f.feed(event(id, OrderEvent::SubmissionStarted));
    f.feed(ack(id, "V-1"));
}

/// Drive one protective exit to WORKING.
fn working_exit(f: &mut Fixture, id: &str, side: Side, qty: i64) {
    f.feed(exit(id, side, qty));
    f.feed(event(id, OrderEvent::SubmissionStarted));
    f.feed(ack(id, "V-2"));
}

// ------------------------------------------------------------------ basics

#[test]
fn an_intent_creates_an_order_in_created() {
    let mut f = Fixture::new("intent");
    assert_eq!(f.feed(entry("UI-1", Side::Buy, 3)), Applied::Changed);

    let order = f.book.order(&coid("UI-1")).unwrap();
    assert_eq!(order.core.state, OrderState::Created);
    assert_eq!(order.core.qty, Qty::whole(3));
    assert_eq!(order.authority, Authority::Entry);
    assert_eq!(order.kind, OrderKind::Market);
    assert!(check(&f.book).holds());
}

#[test]
fn fills_move_the_order_and_the_position_together() {
    let mut f = Fixture::new("fills");
    working(&mut f, "UI-1", Side::Buy, 4);
    f.feed(fill("UI-1", "E-1", 1, "100"));
    f.feed(fill("UI-1", "E-2", 3, "104"));

    let order = f.book.order(&coid("UI-1")).unwrap();
    assert_eq!(order.core.state, OrderState::Filled);
    assert_eq!(order.core.filled_qty, Qty::whole(4));

    let position = f.book.position(&btc());
    assert_eq!(position.qty, Qty::whole(4));
    assert_eq!(position.cost_basis, Money::parse("412").unwrap());
    assert!(check(&f.book).holds());
}

// ------------------------------------------------- R1.2 durable idempotency

/// R1.2 — a duplicate execution is a complete no-op.
#[test]
fn r1_2_a_duplicate_fill_moves_nothing() {
    let mut f = Fixture::new("dup-fill");
    working(&mut f, "UI-1", Side::Buy, 4);
    f.feed(fill("UI-1", "E-1", 2, "100"));

    let before_order = *f.book.order(&coid("UI-1")).unwrap();
    let before_position = f.book.position(&btc());

    // The venue redelivers. At-least-once is the contract, not an anomaly.
    assert_eq!(f.feed(fill("UI-1", "E-1", 2, "100")), Applied::Duplicate);

    assert_eq!(*f.book.order(&coid("UI-1")).unwrap(), before_order);
    assert_eq!(f.book.position(&btc()), before_position);
    assert_eq!(f.book.fill_count(), 1);
    assert!(check(&f.book).holds());
}

/// R1.2 — a replayed intent finds the order that exists and creates nothing.
#[test]
fn r1_2_a_replayed_intent_creates_no_second_order() {
    let mut f = Fixture::new("dup-intent");
    f.feed(entry("UI-1", Side::Buy, 3));
    assert_eq!(f.feed(entry("UI-1", Side::Buy, 3)), Applied::Duplicate);
    assert_eq!(f.book.orders().count(), 1);
}

/// R1.2 — and that idempotency is durable, because it lives in the log.
#[test]
fn r1_2_idempotency_survives_a_restart() {
    let mut f = Fixture::new("dup-restart");
    working(&mut f, "UI-1", Side::Buy, 4);
    f.feed(fill("UI-1", "E-1", 2, "100"));

    let rebuilt = f.rebuilt();
    assert_eq!(rebuilt.fill_count(), 1);
    assert_eq!(rebuilt.position(&btc()).qty, Qty::whole(2));

    // Redelivered after the restart: still nothing.
    assert_eq!(f.feed(fill("UI-1", "E-1", 2, "100")), Applied::Duplicate);
    assert_eq!(f.rebuilt().position(&btc()).qty, Qty::whole(2));
}

// ------------------------------------------------------ R1.4 UNKNOWN locking

/// R1.4 — an order whose truth we cannot prove holds its instrument.
#[test]
fn r1_4_an_unprovable_submission_locks_the_instrument() {
    let mut f = Fixture::new("lock");
    f.feed(entry("UI-1", Side::Buy, 3));
    f.feed(event("UI-1", OrderEvent::SubmissionStarted));
    assert!(!f.book.instrument_locked(&btc()));

    f.feed(event("UI-1", OrderEvent::SubmissionTimedOut));
    assert!(f.book.instrument_locked(&btc()), "UNKNOWN holds it");
    assert_eq!(
        f.book.locking_order(&btc()).unwrap().core.client_order_id,
        coid("UI-1"),
        "and says which order to look at"
    );

    // Reconciling still holds it: the question has not been answered.
    f.feed(event(
        "UI-1",
        OrderEvent::ReconcileStarted {
            cause: ReconcileCause::SubmissionTimeout,
        },
    ));
    assert!(f.book.instrument_locked(&btc()));

    f.feed(event(
        "UI-1",
        OrderEvent::ReconcileResolved {
            state: OrderState::Canceled,
            venue_order_id: None,
            filled_qty: Qty::ZERO,
        },
    ));
    assert!(!f.book.instrument_locked(&btc()), "resolved: released");
}

// ------------------------------------------------------------ R1.7 late fill

/// R1.7 — a fill for an order we believed terminal is neither applied nor lost.
#[test]
fn r1_7_a_late_fill_is_reported_rather_than_applied() {
    let mut f = Fixture::new("late-fill");
    working(&mut f, "UI-1", Side::Buy, 2);
    f.feed(fill("UI-1", "E-1", 2, "100"));
    assert_eq!(
        f.book.order(&coid("UI-1")).unwrap().core.state,
        OrderState::Filled
    );

    let applied = f.feed(fill("UI-1", "E-2", 1, "101"));
    assert_eq!(
        applied,
        Applied::NeedsReconciliation {
            client_order_id: coid("UI-1")
        }
    );
    // Not applied: the position has not moved.
    assert_eq!(f.book.position(&btc()).qty, Qty::whole(2));
    // Not lost either: the record is in the log, and a rebuild reports the
    // same thing rather than silently skipping it.
    assert_eq!(f.rebuilt().position(&btc()).qty, Qty::whole(2));
    assert!(check(&f.book).holds());
}

// ------------------------------------------------------ R1.10 never over-exit

/// R1.10 — outstanding exits are visible, so a guard can bound the next one.
#[test]
fn r1_10_committed_exit_quantity_is_visible() {
    let mut f = Fixture::new("exits");
    working(&mut f, "UI-1", Side::Buy, 10);
    f.feed(fill("UI-1", "E-1", 10, "100"));
    assert_eq!(f.book.position(&btc()).qty, Qty::whole(10));
    assert_eq!(f.book.committed_to_exits(&btc()), Qty::ZERO);

    working_exit(&mut f, "SL-1", Side::Sell, 4);
    assert_eq!(f.book.committed_to_exits(&btc()), Qty::whole(4));

    // A partial fill on the exit shrinks what is still committed.
    f.feed(fill("SL-1", "E-2", 1, "101"));
    assert_eq!(f.book.committed_to_exits(&btc()), Qty::whole(3));
    assert_eq!(f.book.position(&btc()).qty, Qty::whole(9));
    assert!(check(&f.book).holds());
}

/// R1.10 — over-exiting is caught by the invariant, not merely by the guard.
#[test]
fn r1_10_exits_beyond_the_position_are_a_violation() {
    let mut f = Fixture::new("over-exit");
    working(&mut f, "UI-1", Side::Buy, 2);
    f.feed(fill("UI-1", "E-1", 2, "100"));

    // Two exits of 2 against a position of 2. Whatever produced this, the
    // book itself says it is wrong.
    f.feed(exit("SL-1", Side::Sell, 2));
    f.feed(event("SL-1", OrderEvent::SubmissionStarted));
    f.feed(ack("SL-1", "V-2"));
    f.feed(exit("SL-2", Side::Sell, 2));
    f.feed(event("SL-2", OrderEvent::SubmissionStarted));
    f.feed(ack("SL-2", "V-3"));

    let report = check(&f.book);
    assert!(!report.holds());
    assert!(report.violations.iter().any(|v| matches!(
        v,
        Violation::ExitsExceedPosition { exits, position, .. }
            if *exits == Qty::whole(4) && *position == Qty::whole(2)
    )));
}

// ------------------------------------------------------------ R1.11 recovery

/// R1.11 — a rebuild from the log reproduces the live book exactly.
#[test]
fn r1_11_a_rebuild_equals_the_live_book() {
    let mut f = Fixture::new("rebuild");
    working(&mut f, "UI-1", Side::Buy, 10);
    f.feed(fill("UI-1", "E-1", 3, "100"));
    f.feed(fill("UI-1", "E-2", 2, "102"));
    f.feed(event("UI-1", OrderEvent::CancelRequested));
    f.feed(fill("UI-1", "E-3", 2, "103"));
    f.feed(event("UI-1", OrderEvent::CancelConfirmed));
    working_exit(&mut f, "SL-1", Side::Sell, 7);
    f.feed(Record::Mark {
        instrument: btc(),
        price: px("109432.5"),
    });
    f.feed(Record::Balance {
        asset: Asset::new("USD").unwrap(),
        amount: Money::parse("5942.65").unwrap(),
    });

    let rebuilt = f.rebuilt();
    assert_eq!(rebuilt, f.book);
    assert!(check(&rebuilt).holds());
}

/// R1.11 — and rebuilding twice gives the same answer, which is what makes a
/// replay worth running at all.
#[test]
fn r1_11_rebuilding_is_deterministic() {
    let mut f = Fixture::new("determinism");
    working(&mut f, "UI-1", Side::Buy, 5);
    for (i, q) in [1i64, 2, 2].into_iter().enumerate() {
        f.feed(fill("UI-1", &format!("E-{i}"), q, "100"));
    }
    assert_eq!(f.rebuilt(), f.rebuilt());
}

/// R1.11 — a second writer resumes from the log and keeps its numbering.
#[test]
fn r1_11_a_restart_resumes_the_book_and_the_sequence() {
    let mut f = Fixture::new("restart");
    working(&mut f, "UI-1", Side::Buy, 5);
    f.feed(fill("UI-1", "E-1", 2, "100"));
    let dir = f.keep();
    let before = f.rebuilt();
    let last_seq = before.last_seq();
    drop(f);

    // A fresh writer takes the journal. The epoch advances; nothing else moves.
    let journal = Journal::claim(&dir, Nanos::from_u64(9_000)).unwrap();
    assert_eq!(journal.epoch().as_u64(), 2);
    drop(journal);

    let mut tailer = Tailer::open(&dir, Seq::ZERO).unwrap();
    let after = Book::rebuild(&mut tailer).unwrap();
    assert_eq!(after.position(&btc()).qty, Qty::whole(2));
    assert_eq!(
        after.order(&coid("UI-1")).unwrap().core.filled_qty,
        Qty::whole(2)
    );
    assert!(after.last_seq() > last_seq, "the claim record was appended");
    assert_eq!(after.epoch().as_u64(), 2);
    let _ = std::fs::remove_dir_all(&dir);
}

// --------------------------------------------------------------- stale sweep

#[test]
fn only_venue_evidence_counts_as_the_venue_having_spoken() {
    let mut f = Fixture::new("stale");
    working(&mut f, "UI-1", Side::Buy, 5);
    let one_minute = 60_000_000_000u64;

    f.tick(3 * one_minute);
    assert_eq!(
        f.book.stale_orders(2 * one_minute),
        vec![coid("UI-1")],
        "nothing from the venue for three minutes"
    );

    // Our own event. We are still trying; nobody has answered.
    f.feed(event("UI-1", OrderEvent::CancelRequested));
    assert_eq!(
        f.book.stale_orders(2 * one_minute),
        vec![coid("UI-1")],
        "our own event is not the venue speaking"
    );

    // The venue speaks.
    f.feed(fill("UI-1", "E-1", 1, "100"));
    assert!(f.book.stale_orders(2 * one_minute).is_empty());
}

#[test]
fn an_order_already_reconciling_is_not_swept_again() {
    // The Python system swept these and measured 11,409 enqueues against 2,159
    // completed reconciliations, each redundant attempt hammering a venue that
    // was already throttling it.
    let mut f = Fixture::new("no-resweep");
    working(&mut f, "UI-1", Side::Buy, 5);
    f.feed(event(
        "UI-1",
        OrderEvent::ReconcileStarted {
            cause: ReconcileCause::StaleSweep,
        },
    ));
    f.tick(600_000_000_000);
    assert!(f.book.stale_orders(60_000_000_000).is_empty());
}

#[test]
fn a_terminal_order_is_never_swept() {
    let mut f = Fixture::new("terminal-sweep");
    working(&mut f, "UI-1", Side::Buy, 1);
    f.feed(fill("UI-1", "E-1", 1, "100"));
    f.tick(600_000_000_000);
    assert!(f.book.stale_orders(60_000_000_000).is_empty());
}

// ------------------------------------------------------------------- errors

#[test]
fn an_event_for_an_order_with_no_intent_is_refused() {
    // Inside the fold this is fatal: the intent is the authority for everything
    // that follows it, so an event without one means the log is incomplete.
    let mut f = Fixture::new("orphan");
    let err = f.feed_err(event("UI-ghost", OrderEvent::SubmissionStarted));
    assert!(matches!(err, crate::LedgerError::UnknownOrder { .. }));
    assert!(err.is_integrity_violation());
}

#[test]
fn an_illegal_transition_is_refused_and_named() {
    let mut f = Fixture::new("illegal");
    f.feed(entry("UI-1", Side::Buy, 3));
    // An acknowledgement before anything was submitted.
    let err = f.feed_err(ack("UI-1", "V-1"));
    assert!(matches!(err, crate::LedgerError::Transition { .. }));
    assert!(
        !err.is_integrity_violation(),
        "a caller bug, not a wrong book"
    );
}

#[test]
fn a_rejection_is_terminal_and_moves_no_position() {
    let mut f = Fixture::new("reject");
    f.feed(entry("UI-1", Side::Buy, 3));
    f.feed(event("UI-1", OrderEvent::SubmissionStarted));
    f.feed(event(
        "UI-1",
        OrderEvent::VenueRejected {
            reason: RejectReason::new(-2019, "insufficient margin"),
        },
    ));
    assert_eq!(
        f.book.order(&coid("UI-1")).unwrap().core.state,
        OrderState::Rejected
    );
    assert!(f.book.position(&btc()).is_flat());
    assert!(check(&f.book).holds());
}

// --------------------------------------------------------------- accounting

#[test]
fn a_round_trip_books_realised_pnl_and_leaves_nothing_behind() {
    let mut f = Fixture::new("round-trip");
    working(&mut f, "UI-1", Side::Buy, 3);
    f.feed(fill("UI-1", "E-1", 3, "100"));
    working_exit(&mut f, "SL-1", Side::Sell, 3);
    f.feed(fill("SL-1", "E-2", 3, "110"));

    assert!(f.book.position(&btc()).is_flat());
    assert_eq!(f.book.realized(), Money::parse("30").unwrap());
    assert_eq!(f.book.unrealized(), Money::ZERO);
    assert!(check(&f.book).holds());
}

#[test]
fn unrealised_pnl_follows_the_last_mark() {
    let mut f = Fixture::new("marks");
    working(&mut f, "UI-1", Side::Buy, 2);
    f.feed(fill("UI-1", "E-1", 2, "100"));

    assert_eq!(f.book.unrealized(), Money::ZERO, "no mark yet");
    f.feed(Record::Mark {
        instrument: btc(),
        price: px("110"),
    });
    assert_eq!(f.book.unrealized(), Money::parse("20").unwrap());
    f.feed(Record::Mark {
        instrument: btc(),
        price: px("95"),
    });
    assert_eq!(f.book.unrealized(), Money::parse("-10").unwrap());
}

#[test]
fn a_tolerated_over_match_lands_in_full_on_the_position() {
    // The order's record is clamped to what was authorised; the position
    // follows the venue, because the venue is what actually happened.
    let mut f = Fixture::new("over-match");
    working(&mut f, "UI-1", Side::Buy, 10);
    f.feed(fill("UI-1", "E-1", 11, "100"));

    let order = f.book.order(&coid("UI-1")).unwrap();
    assert_eq!(order.core.filled_qty, Qty::whole(10), "clamped");
    assert_eq!(f.book.position(&btc()).qty, Qty::whole(11), "the truth");

    // And the invariants still hold: position equals the sum of fills, and the
    // order's recorded fill is inside its authorised size.
    assert!(check(&f.book).holds(), "{}", check(&f.book));
}

// -------------------------------------------------------------- halt / state

#[test]
fn a_halt_is_durable_and_survives_a_rebuild() {
    // The evidence that outlives the log line. The Python system's only record
    // of why it halted was one decision row, and it was the thing that made
    // the five-day outage diagnosable at all.
    let mut f = Fixture::new("halt");
    assert!(!f.book.is_halted());
    f.feed(Record::Decision {
        instrument: Instrument::empty(),
        actor: Actor::Supervisor,
        kind: DecisionKind::Halted,
        trace_id: TraceId::NONE,
        value: Money::ZERO,
        note: Note::new("writer-lock lost").unwrap(),
    });
    assert!(f.book.is_halted());
    assert!(f.rebuilt().is_halted());

    f.feed(Record::Decision {
        instrument: Instrument::empty(),
        actor: Actor::Supervisor,
        kind: DecisionKind::Resumed,
        trace_id: TraceId::NONE,
        value: Money::ZERO,
        note: Note::empty(),
    });
    assert!(!f.rebuilt().is_halted());
}

#[test]
fn journal_time_advances_with_the_log_and_never_backwards() {
    let mut f = Fixture::new("time");
    let t0 = f.book.now();
    f.tick(5_000_000_000);
    assert!(f.book.now() > t0);

    // A record stamped in the past does not rewind the book's clock; ages
    // measured against it would jump around, and a replay would measure
    // different ages than the live run did.
    let before = f.book.now();
    let header = sentinel_journal::FrameHeader {
        seq: f.book.last_seq().next(),
        epoch: f.journal.epoch(),
        nanos: Nanos::from_u64(1),
        kind: sentinel_journal::RecordKind::Ticked,
        len: 0,
    };
    f.book.apply(&header, Record::Ticked).unwrap();
    assert_eq!(f.book.now(), before);
}

#[test]
fn listings_are_stable_across_reads() {
    // Hash order varies between runs. A listing that reshuffles is unreadable
    // on a screen, and iteration order that varies must not reach an output.
    let mut f = Fixture::new("stable");
    for i in 0..8 {
        f.feed(entry(&format!("UI-{i}"), Side::Buy, 1));
    }
    let first: Vec<_> = f.book.orders().map(|o| o.core.client_order_id).collect();
    for _ in 0..5 {
        let again: Vec<_> = f.book.orders().map(|o| o.core.client_order_id).collect();
        assert_eq!(again, first);
    }
    assert_eq!(
        f.rebuilt()
            .orders()
            .map(|o| o.core.client_order_id)
            .collect::<Vec<_>>(),
        first
    );
}

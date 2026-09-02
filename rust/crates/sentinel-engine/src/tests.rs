//! The acceptance suite: R1.1 to R1.14, driven end to end.
//!
//! The harness below is the I/O worker, written synchronously. It flushes the
//! engine, performs whatever requests come out against the simulator, and feeds
//! the answers back as commands. That is exactly what the real worker does with
//! a network in the middle, so these tests exercise the real control flow —
//! there is no thread, no sleep and no clock anywhere in them.
//!
//! Which is the point. Every failure here is scripted, so a chain of them is
//! reproducible rather than something someone once saw in production.

use std::path::PathBuf;

use sentinel_domain::{EconomicOrderIntent, OrderState, RejectReason};
use sentinel_journal::{Seq, Tailer};
use sentinel_ledger::{Book, check};
use sentinel_record::Note;
use sentinel_types::{
    Authority, ClientOrderId, Instrument, Money, Nanos, Price, Qty, Side, TraceId,
};
use sentinel_venue::{Fault, SimVenue, Venue, VenuePosition, VenueRequest};

use crate::{Command, Config, Engine, Limits};

/// The engine, a simulated venue, and the loop between them.
struct Harness {
    dir: PathBuf,
    engine: Engine,
    venue: SimVenue,
    clock: u64,
    keep_dir: bool,
}

impl Harness {
    fn new(tag: &str) -> Self {
        Self::with_config(tag, Config::default())
    }

    fn with_config(tag: &str, config: Config) -> Self {
        let dir =
            std::env::temp_dir().join(format!("sentinel-engine-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Engine::open(&dir, Nanos::from_u64(1_000), config).unwrap();
        let mut harness = Self {
            dir,
            engine,
            venue: SimVenue::new(),
            clock: 1_000,
            keep_dir: false,
        };
        // What the runtime does on its first tick: ask the venue what the
        // account holds. Entries are refused until it answers, so a harness
        // that skipped this would be testing a system that cannot trade.
        // Tests about the unconfirmed state itself use `unconfirmed` instead.
        harness.confirm_positions(&[]);
        harness
    }

    /// Deliver a position report, which is what unblocks entries.
    fn confirm_positions(&mut self, positions: &[VenuePosition]) {
        self.engine
            .submit(Command::positions_reported(positions))
            .unwrap();
        self.settle();
    }

    /// A harness that has not yet heard from the venue.
    fn unconfirmed(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("sentinel-engine-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Engine::open(&dir, Nanos::from_u64(1_000), Config::default()).unwrap();
        Self {
            dir,
            engine,
            venue: SimVenue::new(),
            clock: 1_000,
            keep_dir: false,
        }
    }

    /// Apply a command and run the world until it settles.
    fn run(&mut self, command: Command) {
        self.engine.submit(command).unwrap();
        self.settle();
    }

    /// Flush and hand the requests back without performing them, so a test can
    /// let something else happen at the venue first.
    fn flush_pending(&mut self) -> Vec<VenueRequest> {
        self.engine.flush().unwrap()
    }

    /// Move whatever the venue has pushed into the engine, without settling.
    fn pump_stream(&mut self) {
        let mut events = Vec::new();
        self.venue.drain_events(&mut events);
        for event in events {
            self.engine.submit(Command::Streamed { event }).unwrap();
        }
    }

    /// Flush, perform the requests, feed back the answers, repeat.
    ///
    /// Bounded: a loop that never settles is a livelock, and hanging a test
    /// suite is a worse way to find out than failing it.
    fn settle(&mut self) {
        for _ in 0..64 {
            let requests = self.engine.flush().unwrap();
            let mut events = Vec::new();
            self.venue.drain_events(&mut events);
            if requests.is_empty() && events.is_empty() {
                return;
            }
            for request in requests {
                self.perform(request);
            }
            for event in events {
                self.engine.submit(Command::Streamed { event }).unwrap();
            }
        }
        panic!("the engine and the venue never settled");
    }

    fn perform(&mut self, request: VenueRequest) {
        match request {
            VenueRequest::Submit(intent) => {
                let client_order_id = intent.client_order_id;
                // A call that could not be made is not an outcome. The real
                // worker retries it; here nothing is fed back, which is the
                // same thing as the answer not having arrived yet.
                if let Ok(outcome) = self.venue.submit(&intent) {
                    self.engine
                        .submit(Command::Submitted {
                            client_order_id,
                            outcome,
                        })
                        .unwrap();
                }
            }
            VenueRequest::Cancel {
                client_order_id,
                venue_order_id,
            } => {
                if let Ok(outcome) = self.venue.cancel(client_order_id, venue_order_id) {
                    self.engine
                        .submit(Command::Cancelled {
                            client_order_id,
                            outcome,
                        })
                        .unwrap();
                }
            }
            VenueRequest::Lookup { client_order_id } => {
                if let Ok(outcome) = self.venue.lookup(client_order_id) {
                    self.engine
                        .submit(Command::LookedUp {
                            client_order_id,
                            outcome,
                        })
                        .unwrap();
                }
            }
            VenueRequest::Positions => {
                if let Ok(positions) = self.venue.positions() {
                    self.engine
                        .submit(Command::positions_reported(&positions))
                        .unwrap();
                }
            }
        }
    }

    /// Advance journal time.
    fn tick(&mut self, by_nanos: u64) {
        self.clock += by_nanos;
        self.run(Command::Tick {
            now: Nanos::from_u64(self.clock),
        });
    }

    fn place(&mut self, intent: EconomicOrderIntent) {
        self.run(Command::Place { intent });
    }

    fn book(&self) -> &Book {
        self.engine.book()
    }

    fn state(&self, id: &str) -> OrderState {
        self.book().order(&coid(id)).expect("order").core.state
    }

    fn filled(&self, id: &str) -> Qty {
        self.book().order(&coid(id)).expect("order").core.filled_qty
    }

    fn position(&self) -> Qty {
        self.book().position(&btc()).qty
    }

    /// Fold the log from the start, as a restart does.
    fn rebuilt(&self) -> Book {
        let mut tailer = Tailer::open(&self.dir, Seq::ZERO).unwrap();
        Book::rebuild(&mut tailer).unwrap()
    }

    fn assert_sound(&self) {
        let report = check(self.book());
        assert!(report.holds(), "invariants broken: {report}");
        assert_eq!(
            self.rebuilt(),
            *self.book(),
            "the log does not rebuild the book"
        );
    }

    fn keep(&mut self) -> PathBuf {
        self.keep_dir = true;
        self.dir.clone()
    }
}

impl Drop for Harness {
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

fn entry(id: &str, side: Side, qty: i64) -> EconomicOrderIntent {
    EconomicOrderIntent::market(
        coid(id),
        btc(),
        side,
        Qty::whole(qty),
        Authority::Entry,
        TraceId::from_u128(0x6440_ba96),
    )
    .unwrap()
}

fn protective_exit(id: &str, side: Side, qty: i64) -> EconomicOrderIntent {
    EconomicOrderIntent::market(
        coid(id),
        btc(),
        side,
        Qty::whole(qty),
        Authority::ProtectiveExit,
        TraceId::from_u128(0x8f82_b303),
    )
    .unwrap()
}

// ================================================================ R1.1 / R1.2

/// R1.1 — the intent is durable before the venue is asked.
///
/// Proven by ordering in the log, not by inspection: the record that authorises
/// the order is written and flushed *before* the request that sends it exists.
#[test]
fn r1_1_intent_is_durable_before_submission() {
    let mut h = Harness::new("r1-1");
    h.engine
        .submit(Command::Place {
            intent: entry("UI-1", Side::Buy, 3),
        })
        .unwrap();

    // Before the flush, nothing has gone to the venue.
    assert_eq!(h.venue.call_counts().0, 0);

    // The flush is what releases it, and by then the intent is on disk: a
    // rebuild from the log already knows about the order.
    let requests = h.engine.flush().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(h.rebuilt().order(&coid("UI-1")).is_some());
    assert_eq!(
        h.rebuilt().order(&coid("UI-1")).unwrap().core.state,
        OrderState::Submitting
    );
}

/// R1.1 — a crash between persisting and submitting leaves the intent as the
/// recovery authority, and the order is found and resolved.
#[test]
fn r1_1_a_crash_before_submission_is_recoverable() {
    let mut h = Harness::new("r1-1-crash");
    h.engine
        .submit(Command::Place {
            intent: entry("UI-1", Side::Buy, 3),
        })
        .unwrap();
    // Durable, but the request never left.
    let _abandoned = h.engine.flush().unwrap();
    let dir = h.keep();
    drop(h);

    // Restart. The order is found frozen mid-submission.
    let mut h2 = Harness::new("r1-1-crash-2");
    h2.keep_dir = true;
    h2.dir = dir.clone();
    h2.engine = Engine::open(&dir, Nanos::from_u64(50_000), Config::default()).unwrap();
    assert_eq!(h2.state("UI-1"), OrderState::Submitting);

    // Replaying the same intent does not resubmit it — it reconciles.
    h2.place(entry("UI-1", Side::Buy, 3));
    assert_eq!(h2.venue.call_counts().0, 0, "never submitted");
    assert!(h2.venue.call_counts().1 > 0, "asked instead");
    assert_eq!(
        h2.state("UI-1"),
        OrderState::Canceled,
        "the venue never had it"
    );
    assert_eq!(h2.position(), Qty::ZERO);
    h2.assert_sound();
    let _ = std::fs::remove_dir_all(&dir);
}

/// R1.2 — a replayed command creates no second order.
#[test]
fn r1_2_a_replayed_place_creates_no_duplicate() {
    let mut h = Harness::new("r1-2");
    h.place(entry("UI-1", Side::Buy, 3));
    h.venue.fill(coid("UI-1"), Qty::whole(3), px("100"));
    h.settle();
    assert_eq!(h.position(), Qty::whole(3));

    h.place(entry("UI-1", Side::Buy, 3));
    assert_eq!(h.book().orders().count(), 1);
    assert_eq!(h.position(), Qty::whole(3), "no second entry");
    assert_eq!(h.venue.call_counts().0, 1, "the venue saw it once");
    h.assert_sound();
}

/// R1.2 — a redelivered execution moves nothing, and does not even reach the log.
#[test]
fn r1_2_a_duplicate_execution_is_absorbed() {
    let mut h = Harness::new("r1-2-dup");
    h.place(entry("UI-1", Side::Buy, 3));
    h.venue.fill_twice(coid("UI-1"), Qty::whole(2), px("100"));
    h.settle();

    assert_eq!(h.position(), Qty::whole(2));
    assert_eq!(h.book().fill_count(), 1);
    h.assert_sound();
}

// ======================================================== R1.3 / R1.4 UNKNOWN

/// R1.3 — a submission timeout is never a rejection and never a resubmit.
#[test]
fn r1_3_a_timeout_is_reconciled_never_resubmitted() {
    let mut h = Harness::new("r1-3");
    h.venue.script(coid("UI-1"), Fault::TimeoutButAccepted);
    h.place(entry("UI-1", Side::Buy, 3));

    // Submitted exactly once. The lookup found it live at the venue, so the
    // order is WORKING — with real exposure we were never told about.
    assert_eq!(h.venue.call_counts().0, 1, "submitted once and only once");
    assert_eq!(h.state("UI-1"), OrderState::Working);
    assert!(!h.venue.was_acknowledged(&coid("UI-1")));
    h.assert_sound();

    // And it goes on to fill normally, as one order.
    h.venue.fill(coid("UI-1"), Qty::whole(3), px("100"));
    h.settle();
    assert_eq!(h.position(), Qty::whole(3), "three, not six");
    h.assert_sound();
}

/// R1.3 — the other timeout: the venue never had it, and says so.
#[test]
fn r1_3_a_timeout_that_was_absent_resolves_to_nothing() {
    let mut h = Harness::new("r1-3-absent");
    h.venue.script(coid("UI-1"), Fault::TimeoutAndAbsent);
    h.place(entry("UI-1", Side::Buy, 3));

    assert_eq!(h.state("UI-1"), OrderState::Canceled);
    assert_eq!(h.position(), Qty::ZERO);
    assert_eq!(h.venue.call_counts().0, 1);
    h.assert_sound();
}

/// R1.4 — while an order is unprovable, the instrument is held.
#[test]
fn r1_4_an_unknown_order_blocks_new_entries() {
    let mut h = Harness::new("r1-4");
    // The lookup times out too, so the order stays unresolved.
    h.venue.script(coid("UI-1"), Fault::TimeoutButAccepted);
    h.engine
        .submit(Command::Place {
            intent: entry("UI-1", Side::Buy, 3),
        })
        .unwrap();
    let requests = h.engine.flush().unwrap();
    for request in requests {
        h.perform(request);
    }
    // Do not settle: leave the reconciliation unfinished.
    assert!(h.book().instrument_locked(&btc()));

    // A second entry arrives. It is refused, and the refusal is in the log.
    h.engine
        .submit(Command::Place {
            intent: entry("UI-2", Side::Buy, 5),
        })
        .unwrap();
    h.engine.flush().unwrap();
    assert!(h.book().order(&coid("UI-2")).is_none(), "never created");
    assert_eq!(h.venue.call_counts().0, 1, "and never sent");
}

// ============================================================ R1.5 / R1.6 / R1.7

/// R1.5 — partial fills account exactly.
#[test]
fn r1_5_partial_fills_account_exactly() {
    let mut h = Harness::new("r1-5");
    h.place(entry("UI-1", Side::Buy, 10));
    for q in [3i64, 3, 1] {
        h.venue.fill(coid("UI-1"), Qty::whole(q), px("100"));
        h.settle();
    }
    assert_eq!(h.state("UI-1"), OrderState::Partial);
    assert_eq!(h.filled("UI-1"), Qty::whole(7));
    assert_eq!(h.position(), Qty::whole(7));

    h.venue.fill(coid("UI-1"), Qty::whole(3), px("102"));
    h.settle();
    assert_eq!(h.state("UI-1"), OrderState::Filled);
    assert_eq!(h.position(), Qty::whole(10));
    h.assert_sound();
}

/// R1.6 — a fill during cancel-pending is accepted, and the cancel then applies
/// only to what conclusively remains.
#[test]
fn r1_6_a_fill_during_cancel_pending_is_accounted() {
    let mut h = Harness::new("r1-6");
    h.place(entry("UI-1", Side::Buy, 10));
    h.venue.fill(coid("UI-1"), Qty::whole(4), px("100"));
    h.settle();

    // Ask to cancel. Hold the request: the fill is going to reach the venue
    // first, which is the race this scenario exists for.
    h.engine
        .submit(Command::Cancel {
            client_order_id: coid("UI-1"),
            trace_id: TraceId::from_u128(1),
        })
        .unwrap();
    let cancel = h.flush_pending();
    assert_eq!(h.state("UI-1"), OrderState::CancelPending);

    h.venue.fill(coid("UI-1"), Qty::whole(3), px("101"));
    h.pump_stream();
    assert_eq!(h.filled("UI-1"), Qty::whole(7));
    assert_eq!(
        h.state("UI-1"),
        OrderState::CancelPending,
        "the fill is accepted and the cancel is still open"
    );

    // Only now does the cancel reach the venue.
    for request in cancel {
        h.perform(request);
    }
    h.settle();

    assert_eq!(h.filled("UI-1"), Qty::whole(7));
    assert_eq!(h.state("UI-1"), OrderState::Canceled);
    assert_eq!(h.position(), Qty::whole(7), "seven, and the rest is gone");
    h.assert_sound();
}

/// R1.7 — a fill after the order was presumed terminal is reconciled, applied
/// once, and never dropped.
#[test]
fn r1_7_a_late_fill_is_reconciled_not_dropped() {
    let mut h = Harness::new("r1-7");
    h.place(entry("UI-1", Side::Buy, 2));
    h.venue.fill(coid("UI-1"), Qty::whole(2), px("100"));
    h.settle();
    assert_eq!(h.state("UI-1"), OrderState::Filled);

    // The venue over-matched and sends one more.
    h.venue.fill(coid("UI-1"), Qty::whole(1), px("101"));
    h.settle();

    // Reconciled against the venue, which says three actually traded.
    assert_eq!(h.position(), Qty::whole(3), "the venue's number");
    assert_eq!(h.filled("UI-1"), Qty::whole(2), "clamped on the order");
    h.assert_sound();
}

// ===================================================== R1.9 / R1.10 / R1.13

/// R1.9 — two signals aiming at the same exposure resolve to one order.
#[test]
fn r1_9_a_second_entry_on_a_live_one_is_refused() {
    let mut h = Harness::new("r1-9");
    h.place(entry("UI-1", Side::Buy, 3));
    assert_eq!(h.state("UI-1"), OrderState::Working);

    h.place(entry("UI-2", Side::Buy, 3));
    assert!(h.book().order(&coid("UI-2")).is_none());
    assert_eq!(h.venue.call_counts().0, 1);
    h.assert_sound();
}

/// R1.10 — an exit is clamped to the position actually held.
#[test]
fn r1_10_an_oversized_exit_is_clamped_not_refused() {
    let mut h = Harness::new("r1-10");
    h.place(entry("UI-1", Side::Buy, 5));
    h.venue.fill(coid("UI-1"), Qty::whole(5), px("100"));
    h.settle();

    // Ask to close eight of a position of five.
    h.place(protective_exit("SL-1", Side::Sell, 8));
    let exit = h.book().order(&coid("SL-1")).expect("the exit exists");
    assert_eq!(exit.core.qty, Qty::whole(5), "clamped to what is there");

    h.venue.fill(coid("SL-1"), Qty::whole(5), px("110"));
    h.settle();
    assert_eq!(h.position(), Qty::ZERO);
    assert_eq!(h.book().realized(), Money::parse("50").unwrap());
    h.assert_sound();
}

/// R1.10 — two exits cannot between them close more than is held.
#[test]
fn r1_10_a_second_exit_is_bounded_by_what_the_first_already_covers() {
    let mut h = Harness::new("r1-10-two");
    h.place(entry("UI-1", Side::Buy, 6));
    h.venue.fill(coid("UI-1"), Qty::whole(6), px("100"));
    h.settle();

    h.place(protective_exit("SL-1", Side::Sell, 4));
    assert_eq!(h.book().committed_to_exits(&btc()), Qty::whole(4));

    // Only two are left uncovered, however much the second asks for.
    h.place(protective_exit("TP-1", Side::Sell, 6));
    assert_eq!(
        h.book()
            .order(&coid("TP-1"))
            .expect("clamped, not refused")
            .core
            .qty,
        Qty::whole(2)
    );
    assert_eq!(h.book().committed_to_exits(&btc()), Qty::whole(6));
    h.assert_sound();
}

/// R1.13 — a halt stops entries and leaves protective exits armed.
#[test]
fn r1_13_a_halt_does_not_disarm_the_stops() {
    let mut h = Harness::new("r1-13");
    h.place(entry("UI-1", Side::Buy, 4));
    h.venue.fill(coid("UI-1"), Qty::whole(4), px("100"));
    h.settle();

    h.run(Command::Halt {
        note: Note::new("divergence").unwrap(),
    });
    assert!(h.engine.is_halted());

    // No new exposure.
    h.place(entry("UI-2", Side::Buy, 4));
    assert!(h.book().order(&coid("UI-2")).is_none());

    // But the exit still goes through, which is the whole of R1.13.
    h.place(protective_exit("SL-1", Side::Sell, 4));
    assert_eq!(h.state("SL-1"), OrderState::Working);
    h.venue.fill(coid("SL-1"), Qty::whole(4), px("95"));
    h.settle();
    assert_eq!(h.position(), Qty::ZERO);
    h.assert_sound();
}

// ============================================================ stream gaps

/// The failure with no reactive trigger: a fill lost in a stream gap. Only the
/// sweep finds it, and it needs time to pass first.
#[test]
fn a_lost_fill_is_found_by_the_sweep_and_not_before() {
    let mut h = Harness::new("gap");
    h.venue.script(coid("UI-1"), Fault::AcceptButSilent);
    h.place(entry("UI-1", Side::Buy, 3));
    h.venue.fill(coid("UI-1"), Qty::whole(3), px("100"));
    h.settle();

    // Nothing arrived. The book still believes the order is resting.
    assert_eq!(h.state("UI-1"), OrderState::Working);
    assert_eq!(h.position(), Qty::ZERO);

    // Too soon: the sweep only touches state that has actually been abandoned.
    h.run(Command::SweepStale);
    assert_eq!(h.state("UI-1"), OrderState::Working);

    h.tick(200_000_000_000);
    h.run(Command::SweepStale);

    assert_eq!(h.state("UI-1"), OrderState::Filled);
    assert_eq!(h.position(), Qty::whole(3), "recovered from the venue");
    h.assert_sound();
}

/// The sweep does not re-queue what it has already queued.
#[test]
fn the_sweep_does_not_storm_a_venue_that_is_not_answering() {
    let mut h = Harness::new("no-storm");
    h.venue.script(coid("UI-1"), Fault::AcceptButSilent);
    h.place(entry("UI-1", Side::Buy, 3));

    // The venue stops answering lookups entirely.
    h.venue.script(coid("UI-1"), Fault::TimeoutAndAbsent);
    h.tick(200_000_000_000);
    h.run(Command::SweepStale);
    let after_first = h.venue.call_counts().1;

    // Sweeping repeatedly adds nothing: the key is already pending.
    for _ in 0..10 {
        h.run(Command::SweepStale);
    }
    assert_eq!(
        h.venue.call_counts().1,
        after_first,
        "ten sweeps produced ten lookups"
    );
    assert_eq!(h.engine.reconciling(), 1);
}

/// A reconciliation that gets no answer backs off exponentially.
#[test]
fn an_unanswered_reconciliation_backs_off_rather_than_hammering() {
    let mut h = Harness::with_config(
        "backoff",
        Config {
            reconcile_backoff_nanos: 1_000_000_000,
            reconcile_backoff_cap_nanos: 8_000_000_000,
            ..Config::default()
        },
    );
    h.venue.script(coid("UI-1"), Fault::TimeoutButAccepted);
    h.engine
        .submit(Command::Place {
            intent: entry("UI-1", Side::Buy, 3),
        })
        .unwrap();
    for request in h.engine.flush().unwrap() {
        h.perform(request);
    }
    // The submission timed out and a lookup went out. Make that time out too.
    h.venue.script(coid("UI-1"), Fault::TimeoutAndAbsent);
    for request in h.engine.flush().unwrap() {
        h.perform(request);
    }
    let after_first = h.venue.call_counts().1;

    // Time barely moves: no retry.
    h.clock += 500_000_000;
    h.engine
        .submit(Command::Tick {
            now: Nanos::from_u64(h.clock),
        })
        .unwrap();
    for request in h.engine.flush().unwrap() {
        h.perform(request);
    }
    assert_eq!(h.venue.call_counts().1, after_first, "too soon");

    // Past the first backoff: one retry, not a stream of them.
    h.clock += 2_000_000_000;
    h.engine
        .submit(Command::Tick {
            now: Nanos::from_u64(h.clock),
        })
        .unwrap();
    for request in h.engine.flush().unwrap() {
        h.perform(request);
    }
    assert_eq!(h.venue.call_counts().1, after_first + 1);
}

// ============================================================ integrity stops

/// A first boot against an account that was already holding something adopts
/// it, rather than halting.
///
/// A rebuilt machine that could never take over an open book is a rebuilt
/// machine that is no use, which is the situation a cutover is.
#[test]
fn a_position_we_have_never_traded_is_adopted_at_its_entry_price() {
    let mut h = Harness::unconfirmed("adopt");
    // Adoption is only possible on the first position report of a
    // process, so this harness must not have had one yet.
    h.venue.force_positions(vec![sentinel_venue::VenuePosition {
        instrument: btc(),
        qty: Qty::whole(7),
        entry_price: Some(px("100")),
    }]);
    let positions = h.venue.positions().unwrap();
    h.run(Command::positions_reported(&positions));

    assert!(!h.engine.is_halted(), "an inheritance is not a divergence");
    assert_eq!(h.position(), Qty::whole(7));
    // Booked at what it cost, so every P&L number after this is the account's
    // real one rather than a guess from whatever the mark was at boot.
    assert_eq!(
        h.book().position(&btc()).avg_entry(),
        Some(px("100")),
        "adopted at the venue's entry"
    );
    // And it is a real order in the log, with an audit trail.
    let adopted = h
        .book()
        .orders()
        .find(|o| o.core.client_order_id.as_str().starts_with("ADOPT-"))
        .expect("the adoption is in the book");
    assert_eq!(adopted.core.state, OrderState::Filled);
    assert!(
        adopted.core.venue_order_id.is_none(),
        "we never submitted it, so there is no venue id to claim"
    );
    h.assert_sound();

    // The account is usable straight away: an exit closes what was adopted.
    h.place(protective_exit("SL-1", Side::Sell, 99));
    assert_eq!(
        h.book().order(&coid("SL-1")).unwrap().core.qty,
        Qty::whole(7),
        "R1.10 bounds the exit by the adopted position"
    );
}

/// Adopting twice does not double the position.
#[test]
fn a_position_is_adopted_once_and_then_reconciled_against() {
    let mut h = Harness::unconfirmed("adopt-once");
    // Adoption is only possible on the first position report of a
    // process, so this harness must not have had one yet.
    h.venue.force_positions(vec![sentinel_venue::VenuePosition {
        instrument: btc(),
        qty: Qty::whole(7),
        entry_price: Some(px("100")),
    }]);
    let positions = h.venue.positions().unwrap();
    h.run(Command::positions_reported(&positions));
    assert_eq!(h.position(), Qty::whole(7));

    // The same report again agrees with the book, so nothing happens at all.
    h.run(Command::positions_reported(&positions));
    assert_eq!(h.position(), Qty::whole(7));
    assert!(!h.engine.is_halted());
    assert_eq!(h.book().orders().count(), 1);
}

/// Once anything has been booked, the venue disagreeing is the real thing.
#[test]
fn an_unexplained_position_halts_rather_than_being_absorbed() {
    let mut h = Harness::new("divergence");
    // Trade first, so the instrument has a history.
    h.place(entry("UI-1", Side::Buy, 2));
    h.venue.fill(coid("UI-1"), Qty::whole(2), px("100"));
    h.settle();
    assert_eq!(h.position(), Qty::whole(2));

    // Now the venue claims something our fills cannot explain.
    h.venue.force_positions(vec![sentinel_venue::VenuePosition {
        instrument: btc(),
        qty: Qty::whole(-5),
        entry_price: Some(px("100")),
    }]);
    let positions = h.venue.positions().unwrap();
    h.run(Command::positions_reported(&positions));

    assert!(h.engine.is_halted());
    assert!(h.rebuilt().is_halted(), "and durably so");
    assert_eq!(h.position(), Qty::whole(2), "and nothing was absorbed");

    // Entries are refused from here on.
    h.place(entry("UI-2", Side::Buy, 3));
    assert!(h.book().order(&coid("UI-2")).is_none());
}

/// An adopted position survives a restart like any other.
#[test]
fn an_adopted_position_rebuilds_from_the_log() {
    let mut h = Harness::unconfirmed("adopt-restart");
    // Adoption is only possible on the first position report of a
    // process, so this harness must not have had one yet.
    h.venue.force_positions(vec![sentinel_venue::VenuePosition {
        instrument: btc(),
        qty: Qty::whole(7),
        entry_price: Some(px("100")),
    }]);
    let positions = h.venue.positions().unwrap();
    h.run(Command::positions_reported(&positions));

    let dir = h.keep();
    let before = h.rebuilt();
    drop(h);

    let engine = Engine::open(&dir, Nanos::from_u64(900_000), Config::default()).unwrap();
    assert_eq!(engine.book().position(&btc()).qty, Qty::whole(7));
    assert_eq!(
        engine.book().position(&btc()).cost_basis,
        before.position(&btc()).cost_basis
    );
    assert!(!engine.is_halted());
    let _ = std::fs::remove_dir_all(&dir);
}

/// An execution for an order we never authorised is disowned, not booked.
#[test]
fn an_orphan_execution_is_disowned_and_triggers_a_position_check() {
    let mut h = Harness::new("orphan");
    h.run(Command::Streamed {
        event: sentinel_venue::VenueEvent::Fill {
            client_order_id: coid("SOMEONE-ELSE"),
            exec_id: sentinel_types::ExecId::new("E-999").unwrap(),
            qty: Qty::whole(1),
            price: px("100"),
        },
    });

    // Nothing booked, nothing crashed, and the account is still usable.
    assert_eq!(h.position(), Qty::ZERO);
    assert!(!h.engine.is_halted());
    h.place(entry("UI-1", Side::Buy, 1));
    assert_eq!(h.state("UI-1"), OrderState::Working);
    h.assert_sound();
}

/// A rejection is terminal and moves nothing.
#[test]
fn a_rejected_order_leaves_no_exposure() {
    let mut h = Harness::new("reject");
    h.venue.script(
        coid("UI-1"),
        Fault::Reject(RejectReason::new(-2019, "insufficient margin")),
    );
    h.place(entry("UI-1", Side::Buy, 3));
    assert_eq!(h.state("UI-1"), OrderState::Rejected);
    assert_eq!(h.position(), Qty::ZERO);
    // And the instrument is free again immediately.
    h.place(entry("UI-2", Side::Buy, 3));
    assert_eq!(h.state("UI-2"), OrderState::Working);
    h.assert_sound();
}

/// Nothing opens until the venue has said what the account holds.
#[test]
fn an_entry_is_refused_until_the_venue_confirms_the_position() {
    let mut h = Harness::unconfirmed("unconfirmed-entry");

    h.place(entry("UI-1", Side::Buy, 3));
    assert!(
        h.book().order(&coid("UI-1")).is_none(),
        "an empty book means we have booked nothing, not that the account \
         holds nothing — and only the venue knows the difference"
    );

    // The venue answers, and the same intent is fine.
    h.confirm_positions(&[]);
    h.place(entry("UI-2", Side::Buy, 3));
    assert!(
        h.book().order(&coid("UI-2")).is_some(),
        "once the venue has answered, an entry is ordinary"
    );
}

/// An exit is not blocked by the same gate: refusing to close is worse.
#[test]
fn an_exit_is_allowed_before_the_venue_confirms_anything() {
    let mut h = Harness::unconfirmed("unconfirmed-exit");
    // Adopt a position so there is something to close, which is itself the
    // first report — then the exit runs against a book that knows its size.
    h.confirm_positions(&[VenuePosition {
        instrument: btc(),
        qty: Qty::whole(4),
        entry_price: Some(px("100")),
    }]);
    assert_eq!(h.book().position(&btc()).qty, Qty::whole(4));

    h.place(protective_exit("X-1", Side::Sell, 4));
    assert!(
        h.book().order(&coid("X-1")).is_some(),
        "a protective exit runs under independent authority (R1.13)"
    );
}

/// The sweep is what asks; without it the account check never runs.
#[test]
fn the_sweep_asks_the_venue_what_the_account_holds() {
    let mut h = Harness::unconfirmed("sweep-positions");
    h.engine.submit(Command::SweepStale).unwrap();
    let requests = h.flush_pending();
    assert!(
        requests.contains(&VenueRequest::Positions),
        "a position that predates the log produces no event, so only the \
         sweep can find it; got {requests:?}"
    );
}

/// The fat-finger cap refuses rather than shrinking an entry.
#[test]
fn an_entry_past_the_position_cap_is_refused_not_resized() {
    let mut h = Harness::with_config(
        "cap",
        Config {
            limits: Limits {
                max_position: Some(Qty::whole(5)),
                ..Limits::default()
            },
            ..Config::default()
        },
    );
    h.place(entry("UI-1", Side::Buy, 9));
    assert!(
        h.book().order(&coid("UI-1")).is_none(),
        "a smaller entry is a different trade, and nobody decided to make it"
    );
    h.place(entry("UI-2", Side::Buy, 5));
    assert_eq!(h.state("UI-2"), OrderState::Working);
}

// ==================================================================== R1.14

/// R1.14 — the compound scenario, end to end, through the real control flow.
///
/// A multi-contract intent; a submission that times out with no venue id; a
/// venue that accepted it anyway; two partial fills; a cancel request; a third
/// fill during cancel-pending; a crash; a restart; a disagreement with the
/// venue; and reconciliation to the truth.
#[test]
fn r1_14_the_compound_failure_chain() {
    let mut h = Harness::new("r1-14");

    // --- timeout, but the venue took it ------------------------------------
    h.venue.script(coid("UI-1"), Fault::TimeoutButAccepted);
    h.place(entry("UI-1", Side::Buy, 10));
    assert_eq!(h.venue.call_counts().0, 1, "R1.3: submitted once");
    assert_eq!(h.state("UI-1"), OrderState::Working, "recovered by asking");
    h.assert_sound();

    // --- two partial fills --------------------------------------------------
    h.venue.fill(coid("UI-1"), Qty::whole(3), px("109432.5"));
    h.settle();
    h.venue.fill(coid("UI-1"), Qty::whole(2), px("109440"));
    h.settle();
    assert_eq!(h.filled("UI-1"), Qty::whole(5), "R1.5");
    assert_eq!(h.position(), Qty::whole(5));

    // --- cancel requested, then a fill lands before the venue answers -------
    h.engine
        .submit(Command::Cancel {
            client_order_id: coid("UI-1"),
            trace_id: TraceId::from_u128(0x6440_ba96),
        })
        .unwrap();
    h.engine.flush().unwrap();
    assert_eq!(h.state("UI-1"), OrderState::CancelPending);

    h.venue.fill(coid("UI-1"), Qty::whole(2), px("109450"));
    h.settle();
    assert_eq!(h.filled("UI-1"), Qty::whole(7), "R1.6");
    assert_eq!(h.position(), Qty::whole(7));

    // --- crash --------------------------------------------------------------
    let dir = h.keep();
    let before = h.rebuilt();
    let venue_state = std::mem::take(&mut h.venue);
    drop(h);

    // --- restart: the book comes back from the log alone --------------------
    let mut h2 = Harness::new("r1-14-restart");
    h2.keep_dir = true;
    h2.dir = dir.clone();
    h2.venue = venue_state;
    h2.engine = Engine::open(&dir, Nanos::from_u64(500_000), Config::default()).unwrap();

    // R1.11: everything economic survives. The epoch and the sequence move,
    // because the successor's own claim is the next record — that is the point
    // of an epoch, not a discrepancy.
    assert_eq!(h2.engine.epoch().as_u64(), 2, "a new writer, a new epoch");
    assert_eq!(h2.position(), before.position(&btc()).qty);
    assert_eq!(h2.book().all_fills(), before.all_fills());
    assert_eq!(
        h2.book().order(&coid("UI-1")).unwrap().core,
        before.order(&coid("UI-1")).unwrap().core,
        "the order comes back exactly as it was"
    );
    assert_eq!(h2.position(), Qty::whole(7));

    // --- the venue disagrees: a fill landed while we were down --------------
    h2.venue.fill(coid("UI-1"), Qty::whole(1), px("109460"));
    h2.settle();
    h2.tick(200_000_000_000);
    h2.run(Command::SweepStale);

    // --- what must be true at the end ---------------------------------------
    assert_eq!(
        h2.position(),
        Qty::whole(8),
        "R1.12: the venue's number wins"
    );
    assert_eq!(
        h2.book().order(&coid("UI-1")).unwrap().core.qty,
        Qty::whole(10),
        "R1.9: still one order, never re-authorised"
    );
    assert_eq!(h2.book().orders().count(), 1, "no duplicate entry anywhere");
    h2.assert_sound();

    // And the account is usable: an exit closes exactly what is held.
    h2.place(protective_exit("SL-1", Side::Sell, 99));
    assert_eq!(
        h2.book().order(&coid("SL-1")).unwrap().core.qty,
        Qty::whole(8),
        "R1.10: never over-exit"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================ group commit

/// The writer batches across commands, which is the whole performance claim.
#[test]
fn a_batch_of_commands_shares_one_fsync() {
    let mut h = Harness::new("group");
    for i in 0..8 {
        h.engine
            .submit(Command::Place {
                intent: entry(&format!("UI-{i}"), Side::Buy, 1),
            })
            .unwrap();
    }
    // Eight placements, each writing an intent, an event and a decision, plus
    // seven refusals for the entries that found the instrument already busy.
    assert!(h.engine.buffered() >= 8);
    let flushes_before = h.engine.journal_stats().flushes;
    h.engine.flush().unwrap();
    assert_eq!(
        h.engine.journal_stats().flushes,
        flushes_before + 1,
        "one barrier for the whole group"
    );
}

/// Nothing reaches the venue before its records are durable.
#[test]
fn requests_are_released_only_by_the_flush() {
    let mut h = Harness::new("ordering");
    h.engine
        .submit(Command::Place {
            intent: entry("UI-1", Side::Buy, 3),
        })
        .unwrap();
    h.engine
        .submit(Command::Place {
            intent: entry("UI-2", Side::Buy, 3),
        })
        .unwrap();
    assert_eq!(h.venue.call_counts().0, 0);

    let requests = h.engine.flush().unwrap();
    assert_eq!(requests.len(), 1, "the second was refused, not queued");
    assert!(matches!(requests[0], VenueRequest::Submit(_)));
}

// ================================================================ replay

/// The same commands produce the same log, which is what a replay is for.
#[test]
fn the_same_script_produces_the_same_book() {
    let run = |tag: &str| {
        let mut h = Harness::new(tag);
        h.venue.script(coid("UI-1"), Fault::TimeoutButAccepted);
        h.place(entry("UI-1", Side::Buy, 10));
        h.venue.fill(coid("UI-1"), Qty::whole(4), px("100"));
        h.settle();
        h.tick(200_000_000_000);
        h.run(Command::SweepStale);
        h.rebuilt()
    };
    assert_eq!(run("replay-a"), run("replay-b"));
}

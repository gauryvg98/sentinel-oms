//! The single writer.
//!
//! One structure owns the journal and the book, and the only way into it is
//! [`Engine::submit`]. There is no lock, because there is nothing to lock
//! against: one writer, one journal, one `flock`, and a fencing epoch on every
//! record so an interleave would be visible in the log rather than merely
//! improbable.
//!
//! **The venue is never called from here.** A command produces records and, if
//! it needs the venue, a [`VenueRequest`] that is handed out *after* the
//! records are durable. In the system this replaces, the instrument lock was
//! held across the venue's HTTP round trip, so every inbound fill queued behind
//! a network call — ten seconds to place an order on an account trading one
//! symbol. Here the writer's slowest step is one `fsync` shared with everything
//! else queued in that instant.
//!
//! Apply, then commit, then acknowledge. [`Engine::submit`] applies to memory
//! and buffers; [`Engine::flush`] makes the group durable and releases the
//! requests. A crash between them loses records that were never acted on.

use std::collections::HashMap;

use sentinel_domain::{EconomicOrderIntent, OrderEvent, OrderState, ReconcileCause};
use sentinel_journal::{FrameHeader, Journal, Seq, Tailer};
use sentinel_ledger::{Applied, Book};
use sentinel_record::{Actor, DecisionKind, Note, Record};
use sentinel_types::{ClientOrderId, Epoch, Instrument, Money, Nanos, Qty, TraceId};
use sentinel_venue::{
    CancelOutcome, LookupOutcome, SubmitOutcome, VenueEvent, VenuePosition, VenueRequest,
};

use crate::command::Command;
use crate::error::{EngineError, Result};
use crate::guards::{Limits, Refusal, Verdict, evaluate};

/// How the engine paces itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Guard limits.
    pub limits: Limits,
    /// How long the venue may say nothing about a live order before the sweep
    /// goes looking. Comfortably longer than any honest in-flight window, so
    /// the sweep only ever touches state that has actually been abandoned.
    pub stale_after_nanos: u64,
    /// First delay before retrying a reconciliation that got no answer.
    pub reconcile_backoff_nanos: u64,
    /// Cap on that delay.
    ///
    /// The Python system retried on a fixed two-second cadence, so a batch of
    /// unreconcilable orders hit a throttling venue every two seconds each,
    /// worsening the throttling that caused the timeouts. Exponential with a
    /// ceiling is the fix.
    pub reconcile_backoff_cap_nanos: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            stale_after_nanos: 120_000_000_000,
            reconcile_backoff_nanos: 1_000_000_000,
            reconcile_backoff_cap_nanos: 60_000_000_000,
        }
    }
}

/// The high bits of an adopted position's trace id.
///
/// Renders as `00000000000000000000adb7…` in a log, which is recognisable at a
/// glance and cannot collide with a trace minted at the gateway — those are
/// generated in the low 64 bits.
const ADOPTION_TRACE_MARKER: u128 = 0x0000_adb7;

/// An order waiting for the venue to say what it really is.
#[derive(Debug, Clone, Copy)]
struct Pending {
    /// When to ask again, if the last attempt produced no answer.
    retry_at: Nanos,
    /// How long to wait after the next failure.
    backoff: u64,
    /// Whether a request is currently out.
    in_flight: bool,
}

/// The writer.
#[derive(Debug)]
pub struct Engine {
    journal: Journal,
    book: Book,
    config: Config,
    /// Requests buffered until their records are durable.
    uncommitted: Vec<VenueRequest>,
    /// Keys queued or in flight for reconciliation.
    ///
    /// The dedup that stops a queue storm. Without it the stale sweep re-adds
    /// every stuck order on every pass while the timeout path re-adds the same
    /// keys — measured in the Python system as 11,409 sweep enqueues against
    /// 2,159 completed reconciliations.
    reconciling: HashMap<ClientOrderId, Pending>,
    /// Reused across appends so a record costs no allocation.
    scratch: Vec<u8>,
    halted: bool,
    /// Whether the venue has reported the account's positions since this
    /// process started.
    ///
    /// Deliberately *not* replayed from the journal. It is a statement about
    /// this process having spoken to the venue, and a log can only say that
    /// some earlier process did. Rebuilding it from history would restore the
    /// belief without the conversation that justified it.
    position_confirmed: bool,
}

impl Engine {
    /// Claim the journal, rebuild the book, and take over.
    ///
    /// # Errors
    /// [`EngineError`] when the journal is held by another process, the log
    /// does not fold, or the directory cannot be read.
    pub fn open(dir: impl AsRef<std::path::Path>, now: Nanos, config: Config) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let journal = Journal::claim(&dir, now).map_err(EngineError::Journal)?;
        let mut tailer = Tailer::open(&dir, Seq::ZERO).map_err(EngineError::Journal)?;
        let book = Book::rebuild(&mut tailer).map_err(EngineError::Ledger)?;
        let halted = book.is_halted();
        Ok(Self {
            journal,
            book,
            config,
            uncommitted: Vec::new(),
            reconciling: HashMap::new(),
            scratch: Vec::with_capacity(512),
            // False on every boot, including a resumed one. See the field.
            position_confirmed: false,
            halted,
        })
    }

    /// What the engine believes.
    #[must_use]
    pub const fn book(&self) -> &Book {
        &self.book
    }

    /// This writer's epoch.
    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.journal.epoch()
    }

    /// Whether the account has stopped trading.
    #[must_use]
    pub const fn is_halted(&self) -> bool {
        self.halted
    }

    /// Journal time.
    #[must_use]
    pub const fn now(&self) -> Nanos {
        self.book.now()
    }

    /// Orders currently queued or in flight for reconciliation.
    #[must_use]
    pub fn reconciling(&self) -> usize {
        self.reconciling.len()
    }

    /// Apply a command.
    ///
    /// Records are buffered, not durable. Nothing acts on them until
    /// [`Self::flush`].
    ///
    /// # Errors
    /// [`EngineError`] when the journal refuses the append or the book refuses
    /// the record. Both are conditions the process cannot trade through.
    pub fn submit(&mut self, command: Command) -> Result<()> {
        match command {
            Command::Tick { now } => self.on_tick(now),
            Command::Place { intent } => self.on_place(intent),
            Command::Cancel {
                client_order_id,
                trace_id,
            } => self.on_cancel(client_order_id, trace_id),
            Command::Submitted {
                client_order_id,
                outcome,
            } => self.on_submitted(client_order_id, outcome),
            Command::Cancelled {
                client_order_id,
                outcome,
            } => self.on_cancelled(client_order_id, outcome),
            Command::LookedUp {
                client_order_id,
                outcome,
            } => self.on_looked_up(client_order_id, outcome),
            Command::Streamed { event } => self.on_streamed(event),
            Command::PositionsReported { positions } => self.on_positions(&positions),
            Command::SweepStale => self.on_sweep(),
            Command::Halt { note } => self.on_halt(note),
            Command::Resume { note } => self.on_resume(note),
            Command::Decide {
                actor,
                instrument,
                kind,
                trace_id,
                value,
                note,
            } => self.decide(instrument, actor, kind, trace_id, value, note),
        }
    }

    /// Make everything buffered durable, and release the requests it produced.
    ///
    /// The requests come out *here* rather than from `submit`, and that
    /// ordering is the guarantee: no order reaches the venue before the intent
    /// authorising it reaches the disk.
    ///
    /// # Errors
    /// [`EngineError::Journal`] when the flush fails. That poisons the journal
    /// permanently — memory is ahead of a log still being served from, and the
    /// only safe move is for the process to restart.
    pub fn flush(&mut self) -> Result<Vec<VenueRequest>> {
        self.journal.flush().map_err(EngineError::Journal)?;
        Ok(core::mem::take(&mut self.uncommitted))
    }

    /// Records appended and not yet durable.
    #[must_use]
    pub const fn buffered(&self) -> u32 {
        self.journal.buffered()
    }

    /// Journal counters — including records per `fsync`, which is the one
    /// number that says whether group commit is engaging.
    #[must_use]
    pub const fn journal_stats(&self) -> sentinel_journal::Stats {
        self.journal.stats()
    }

    // ------------------------------------------------------------- writing

    /// Append a record and apply it to the book, stamped with journal time.
    fn write(&mut self, record: Record) -> Result<Applied> {
        self.write_at(self.book.now(), record)
    }

    /// Append a record stamped at `now`.
    ///
    /// Only the tick uses this. Every other record is stamped with the time the
    /// book already holds, because a record is caused by the tick that preceded
    /// it — stamping each one with a fresh reading would be a clock read below
    /// the gateway, and replay would stop reproducing.
    fn write_at(&mut self, now: Nanos, record: Record) -> Result<Applied> {
        self.scratch.clear();
        record.encode(&mut self.scratch);
        let kind = record.kind();
        let seq = self
            .journal
            .append(kind, now, &self.scratch)
            .map_err(EngineError::Journal)?;
        let header = FrameHeader {
            seq,
            epoch: self.journal.epoch(),
            nanos: now,
            kind,
            len: u32::try_from(self.scratch.len()).unwrap_or(u32::MAX),
        };
        self.book
            .apply(&header, record)
            .map_err(EngineError::Ledger)
    }

    /// Append a lifecycle event, with where the order was and where it goes.
    ///
    /// The destination is computed here, by the same pure function the book
    /// will run, and written into the record. That is what lets a consumer of
    /// this log — the gateway, the projector — know what happened without
    /// owning a second copy of the state machine. The book re-runs the
    /// transition and refuses the record if the two disagree, so the field is
    /// a checked claim rather than a trusted one.
    fn write_event(
        &mut self,
        client_order_id: ClientOrderId,
        trace_id: TraceId,
        event: OrderEvent,
    ) -> Result<Applied> {
        let existing = self.book.order(&client_order_id).map(|o| o.core);
        let from_state = existing.map_or(OrderState::Created, |core| core.state);
        let next = existing.and_then(|core| sentinel_domain::transition(core, event).ok());
        // A transition that refuses is still recorded, as evidence: the state
        // does not move, so `to` is `from`. That is the case the Python ledger
        // wrote as `from == to == RECONCILING`.
        let to_state = next.map_or(from_state, |next| next.state);
        // An order the book has not seen yet is about to be created by this
        // very record, so there is no transition to run and nothing refused it.
        // Otherwise the transition having succeeded is what "applied" means —
        // and note it is independent of whether the state moved, because a fill
        // booked mid-reconcile applies without moving anything.
        let applied = existing.is_none() || next.is_some();
        self.write(Record::OrderEvent {
            client_order_id,
            trace_id,
            from_state,
            to_state,
            applied,
            event,
        })
    }

    fn decide(
        &mut self,
        instrument: Instrument,
        actor: Actor,
        kind: DecisionKind,
        trace_id: TraceId,
        value: Money,
        note: Note,
    ) -> Result<()> {
        self.write(Record::Decision {
            instrument,
            actor,
            kind,
            trace_id,
            value,
            note,
        })?;
        Ok(())
    }

    fn request(&mut self, request: VenueRequest) {
        self.uncommitted.push(request);
    }

    // ------------------------------------------------------------ commands

    fn on_tick(&mut self, now: Nanos) -> Result<()> {
        if now <= self.book.now() {
            // Time below the gateway is monotonic by construction. A tick that
            // goes backwards is a bug upstream, and applying it would make
            // every age the engine measures jump around.
            return Ok(());
        }
        // Stamped with the *new* time: this record is what advances the book's
        // clock, so stamping it with the old one would leave time standing
        // still and every age the engine measures pinned at zero.
        self.write_at(now, Record::Ticked)?;
        self.retry_due_reconciliations(now);
        Ok(())
    }

    fn on_place(&mut self, intent: EconomicOrderIntent) -> Result<()> {
        // Idempotency outranks the guards. A replay of an existing intent is
        // not new exposure, so it must short-circuit *before* a guard can
        // refuse it — guards exist to block new exposure only (R1.2).
        if let Some(existing) = self.book.order(&intent.client_order_id) {
            if existing.core.state == OrderState::Submitting {
                // Frozen mid-submission: the crash window. Reconciliation, not
                // a resubmit, is the only safe wake-up.
                self.begin_reconcile(
                    intent.client_order_id,
                    intent.trace_id,
                    ReconcileCause::ReplayedMidSubmission,
                )?;
            }
            return Ok(());
        }

        let limits = Limits {
            position_confirmed: self.position_confirmed,
            ..self.config.limits
        };
        match evaluate(&self.book, &intent, limits, self.halted) {
            Verdict::Refuse(refusal) => self.refuse(&intent, refusal),
            Verdict::Clamp { to } => {
                let clamped = EconomicOrderIntent { qty: to, ..intent };
                self.decide(
                    intent.instrument,
                    Actor::Guards,
                    DecisionKind::ExitClamped,
                    intent.trace_id,
                    Money::from_raw(to.raw()),
                    Note::new(&format!("exit {} clamped to {to}", intent.qty)).unwrap_or_default(),
                )?;
                self.authorise(clamped)
            }
            Verdict::Allow => self.authorise(intent),
        }
    }

    fn refuse(&mut self, intent: &EconomicOrderIntent, refusal: Refusal) -> Result<()> {
        // A blocked entry produces no order and therefore no order event, which
        // is exactly why it needs a record of its own: without one, "the bot
        // did nothing for six hours" has no explanation anywhere in the log.
        self.decide(
            intent.instrument,
            Actor::Guards,
            DecisionKind::EntryBlocked,
            intent.trace_id,
            Money::from_raw(refusal.value().raw()),
            refusal.note(),
        )
    }

    /// Make the intent durable, then ask the venue. In that order (R1.1).
    fn authorise(&mut self, intent: EconomicOrderIntent) -> Result<()> {
        self.write(Record::IntentPersisted(intent))?;
        self.write_event(
            intent.client_order_id,
            intent.trace_id,
            OrderEvent::SubmissionStarted,
        )?;
        self.decide(
            intent.instrument,
            if intent.opens_exposure() {
                Actor::Gateway
            } else {
                Actor::Protection
            },
            if intent.opens_exposure() {
                DecisionKind::EntryPlaced
            } else {
                DecisionKind::ExitPlaced
            },
            intent.trace_id,
            Money::from_raw(intent.qty.raw()),
            Note::new(&format!(
                "{} {} {}",
                intent.side, intent.qty, intent.instrument
            ))
            .unwrap_or_default(),
        )?;
        self.request(VenueRequest::Submit(intent));
        Ok(())
    }

    fn on_cancel(&mut self, client_order_id: ClientOrderId, trace_id: TraceId) -> Result<()> {
        let Some(order) = self.book.order(&client_order_id) else {
            return Ok(());
        };
        let venue_order_id = order.core.venue_order_id;
        // Cancelling an UNKNOWN order is refused by the lifecycle itself, not
        // by a check here: you cannot cancel what you cannot prove exists
        // (R1.4). `write_event` returns the transition error.
        match self.write_event(client_order_id, trace_id, OrderEvent::CancelRequested) {
            Ok(_) => {
                self.request(VenueRequest::Cancel {
                    client_order_id,
                    venue_order_id,
                });
                Ok(())
            }
            Err(EngineError::Ledger(e)) if !e.is_integrity_violation() => {
                // Not cancellable from where it is. Recorded and dropped, not
                // escalated: an operator asking twice is not a fault.
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn on_submitted(
        &mut self,
        client_order_id: ClientOrderId,
        outcome: SubmitOutcome,
    ) -> Result<()> {
        let trace_id = self.trace_of(client_order_id);
        match outcome {
            SubmitOutcome::Acked(venue_order_id) => {
                self.write_event(
                    client_order_id,
                    trace_id,
                    OrderEvent::VenueAcked { venue_order_id },
                )?;
            }
            SubmitOutcome::Rejected(reason) => {
                self.write_event(
                    client_order_id,
                    trace_id,
                    OrderEvent::VenueRejected { reason },
                )?;
            }
            SubmitOutcome::TimedOut => {
                // Not a rejection, and never a resubmit (R1.3). The order parks
                // in UNKNOWN — which also holds the instrument (R1.4) — and the
                // only thing that moves it is the venue's own answer.
                self.write_event(client_order_id, trace_id, OrderEvent::SubmissionTimedOut)?;
                self.begin_reconcile(client_order_id, trace_id, ReconcileCause::SubmissionTimeout)?;
            }
        }
        Ok(())
    }

    fn on_cancelled(
        &mut self,
        client_order_id: ClientOrderId,
        outcome: CancelOutcome,
    ) -> Result<()> {
        let trace_id = self.trace_of(client_order_id);
        match outcome {
            // The confirmation arrives on the stream, not here.
            CancelOutcome::Requested => Ok(()),
            CancelOutcome::TimedOut | CancelOutcome::Absent => {
                // The cancel's outcome is unprovable: the order may fill, may
                // already be cancelled, or may never have existed. Ask.
                self.begin_reconcile(client_order_id, trace_id, ReconcileCause::CancelTimeout)
            }
        }
    }

    fn on_looked_up(
        &mut self,
        client_order_id: ClientOrderId,
        outcome: LookupOutcome,
    ) -> Result<()> {
        let trace_id = self.trace_of(client_order_id);
        match outcome {
            LookupOutcome::TimedOut => {
                // Nothing is concluded. The order stays where it is and the
                // question is asked again later, with a longer gap each time.
                self.defer_reconcile(client_order_id);
                Ok(())
            }
            LookupOutcome::Found(snapshot) => {
                self.ensure_reconciling(
                    client_order_id,
                    trace_id,
                    ReconcileCause::PositionDisagreement,
                )?;
                self.book_the_difference(
                    client_order_id,
                    trace_id,
                    snapshot.filled_qty,
                    snapshot.avg_price,
                )?;
                self.write_event(
                    client_order_id,
                    trace_id,
                    OrderEvent::ReconcileResolved {
                        state: snapshot.state,
                        venue_order_id: snapshot.venue_order_id,
                        filled_qty: snapshot.filled_qty,
                    },
                )?;
                self.reconciling.remove(&client_order_id);
                Ok(())
            }
            LookupOutcome::Absent => {
                // Conclusively absent: the intent never became exposure. Any
                // resubmission is a new order under policy, never a reuse of
                // this one (R1.3).
                self.ensure_reconciling(
                    client_order_id,
                    trace_id,
                    ReconcileCause::SubmissionTimeout,
                )?;
                self.write_event(
                    client_order_id,
                    trace_id,
                    OrderEvent::ReconcileResolved {
                        state: OrderState::Canceled,
                        venue_order_id: None,
                        filled_qty: Qty::ZERO,
                    },
                )?;
                self.reconciling.remove(&client_order_id);
                Ok(())
            }
        }
    }

    fn on_streamed(&mut self, event: VenueEvent) -> Result<()> {
        match event {
            VenueEvent::Mark { instrument, price } => {
                self.write(Record::Mark { instrument, price })?;
                Ok(())
            }
            VenueEvent::Balance { asset, amount } => {
                self.write(Record::Balance { asset, amount })?;
                Ok(())
            }
            VenueEvent::Fill {
                client_order_id,
                exec_id,
                qty,
                price,
            } => {
                // Dedup before writing. An at-least-once stream redelivers
                // constantly, and a log that recorded every redelivery would
                // grow without recording anything.
                if self.book.fill(&exec_id).is_some() {
                    return Ok(());
                }
                if self.book.order(&client_order_id).is_none() {
                    return self.disown(client_order_id);
                }
                let trace_id = self.trace_of(client_order_id);
                let applied = self.write_event(
                    client_order_id,
                    trace_id,
                    OrderEvent::FillReceived {
                        exec_id,
                        qty,
                        price,
                    },
                )?;
                if let Applied::NeedsReconciliation { .. } = applied {
                    // A fill for an order we believed finished (R1.7). It is in
                    // the log as evidence and has moved nothing; the venue
                    // decides what it meant.
                    self.begin_reconcile(client_order_id, trace_id, ReconcileCause::LateFill)?;
                }
                Ok(())
            }
            VenueEvent::CancelConfirmed { client_order_id } => {
                if self.book.order(&client_order_id).is_none() {
                    return self.disown(client_order_id);
                }
                let trace_id = self.trace_of(client_order_id);
                let applied =
                    self.write_event(client_order_id, trace_id, OrderEvent::CancelConfirmed)?;
                if let Applied::NeedsReconciliation { .. } = applied {
                    self.begin_reconcile(
                        client_order_id,
                        trace_id,
                        ReconcileCause::LateCancelConfirm,
                    )?;
                }
                Ok(())
            }
        }
    }

    /// An execution for an order we have no intent for.
    ///
    /// Surfaced loudly and then dropped. We never book exposure we did not
    /// create, and one stray id — an orphan another process left on the
    /// account — must not take the whole system down. Any real exposure it
    /// produced is caught by position reconciliation against the venue, which
    /// is a check that does not depend on knowing the order.
    fn disown(&mut self, client_order_id: ClientOrderId) -> Result<()> {
        self.decide(
            Instrument::empty(),
            Actor::Reconciler,
            DecisionKind::ReconcileScheduled,
            TraceId::NONE,
            Money::ZERO,
            Note::new(&format!(
                "disowned event for unknown order {client_order_id}"
            ))
            .unwrap_or_default(),
        )?;
        self.request(VenueRequest::Positions);
        Ok(())
    }

    fn on_positions(&mut self, positions: &[Option<VenuePosition>; 8]) -> Result<()> {
        // The venue has spoken, so entries are unblocked from here. An empty
        // report counts: "you hold nothing" is an answer, and the guard exists
        // to distinguish an answer from silence rather than to require a
        // non-zero one.
        //
        // Whether this is the *first* answer of the process is what makes
        // adoption safe, so it is read before it is set.
        let first_report = !self.position_confirmed;
        self.position_confirmed = true;

        // R1.12 at the account level. The venue's number wins, and a
        // disagreement is recorded rather than quietly overwritten — the
        // divergence is the thing an operator needs to see.
        for reported in positions.iter().flatten() {
            let ours = self.book.position(&reported.instrument).qty;
            if ours == reported.qty {
                continue;
            }

            // A position we have never had a fill for is not a disagreement.
            // It is an inheritance: a fresh journal against an account that was
            // already holding something. Halting on it would mean a rebuilt
            // machine could never take over an open book — which is exactly
            // what a rebuilt machine is for.
            //
            // The distinction is *no fills at all*, not *a different number*.
            // Once anything has been booked here, the venue disagreeing with us
            // is the real thing and it stops the account.
            //
            // And only on the first report of this process. Positions are
            // polled on every sweep, so from the second report onward a book
            // that is merely *behind* — a fill lost in a stream gap, not yet
            // recovered — looks exactly like an inheritance from here. Adopting
            // then would book the position twice: once as an adoption and again
            // when the lookup recovers the fill it was always going to find.
            if first_report && self.has_never_traded(reported.instrument) {
                self.adopt_position(reported)?;
                continue;
            }

            self.decide(
                reported.instrument,
                Actor::Reconciler,
                DecisionKind::ReconcileScheduled,
                TraceId::NONE,
                Money::from_raw(reported.qty.raw()),
                Note::new(&format!("venue holds {}, book holds {ours}", reported.qty))
                    .unwrap_or_default(),
            )?;
            // Halting rather than adjusting. A position we cannot explain from
            // our own fills is not something to book — it is something to stop
            // for, because the arithmetic that produced it is what we would be
            // trading on next.
            self.on_halt(
                Note::new(&format!(
                    "{} position diverged: venue {}, book {ours}",
                    reported.instrument, reported.qty
                ))
                .unwrap_or_default(),
            )?;
        }
        Ok(())
    }

    /// Whether this instrument has no history in the book at all.
    /// Whether the log has never heard of this instrument at all.
    ///
    /// Stronger than "no fills": an order in any state, including one still
    /// waiting for its first fill, means this process has a claim on the
    /// instrument and any difference from the venue is a divergence to halt on
    /// rather than a position to inherit.
    fn has_never_traded(&self, instrument: Instrument) -> bool {
        self.book.position(&instrument).qty.is_zero()
            && !self
                .book
                .orders()
                .any(|order| order.core.instrument == instrument)
            && !self
                .book
                .all_fills()
                .iter()
                .any(|fill| fill.instrument == instrument)
    }

    /// Take over a position the venue was already holding.
    ///
    /// Written as a real order that was reconciled, because that is what it is:
    /// an intent this system recorded and immediately resolved against venue
    /// truth. The lifecycle goes `Created -> Reconciling -> Filled` and never
    /// through `Submitting`, because nothing was submitted — claiming otherwise
    /// would put a submission in the log that never happened.
    ///
    /// Booked at the venue's entry price, so the cost basis and every P&L
    /// number after it are the account's real ones rather than a guess from
    /// whatever the mark happened to be at boot.
    fn adopt_position(&mut self, reported: &VenuePosition) -> Result<()> {
        let instrument = reported.instrument;
        let side = if reported.qty.is_positive() {
            sentinel_types::Side::Buy
        } else {
            sentinel_types::Side::Sell
        };
        let qty = reported.qty.abs();
        let price = reported
            .entry_price
            .or_else(|| self.book.mark(&instrument))
            .unwrap_or(sentinel_types::Price::ZERO);

        // An adoption has an audit path — the decision record written below —
        // so it gets a real trace rather than `NONE`. The invariant that every
        // order is traceable to what authorised it is not one to bend for a
        // convenient case: exposure appearing with no chain behind it is the
        // exact thing it exists to catch.
        //
        // The marker makes an adoption recognisable at a glance in a log, and
        // the epoch says which run took it on.
        let trace_id = TraceId::from_u128(
            (ADOPTION_TRACE_MARKER << 64) | u128::from(self.journal.epoch().as_u64()),
        );

        // Named for what it is, and stamped with the epoch that adopted it, so
        // two boots cannot collide and a reader can see which run took it on.
        let suffix = format!("-{}", self.journal.epoch());
        let head = &instrument.as_str()[..instrument.len().min(20)];
        let client_order_id =
            ClientOrderId::new(&format!("ADOPT-{head}{suffix}")).unwrap_or_default();

        let intent = match EconomicOrderIntent::market(
            client_order_id,
            instrument,
            side,
            qty,
            sentinel_types::Authority::Entry,
            trace_id,
        ) {
            Ok(intent) => intent,
            // A position we cannot even describe as an intent is one to stop
            // for rather than to approximate.
            Err(e) => {
                return self.on_halt(
                    Note::new(&format!("{instrument}: cannot adopt {}: {e}", reported.qty))
                        .unwrap_or_default(),
                );
            }
        };

        self.write(Record::IntentPersisted(intent))?;
        self.write_event(
            client_order_id,
            trace_id,
            OrderEvent::ReconcileStarted {
                cause: ReconcileCause::PositionDisagreement,
            },
        )?;
        self.write_event(
            client_order_id,
            trace_id,
            OrderEvent::FillReceived {
                exec_id: sentinel_types::ExecId::new(&format!("ADOPT-{head}{suffix}"))
                    .unwrap_or_default(),
                qty,
                price,
            },
        )?;
        self.write_event(
            client_order_id,
            trace_id,
            OrderEvent::ReconcileResolved {
                state: sentinel_domain::OrderState::Filled,
                venue_order_id: None,
                filled_qty: qty,
            },
        )?;
        self.decide(
            instrument,
            Actor::Reconciler,
            DecisionKind::EntryPlaced,
            trace_id,
            Money::from_raw(reported.qty.raw()),
            Note::new(&format!(
                "adopted {} held at the venue, booked at {price}",
                reported.qty
            ))
            .unwrap_or_default(),
        )
    }

    fn on_sweep(&mut self) -> Result<()> {
        // Ask what the account actually holds. Every other route to a position
        // report presumes an event arrives — a fill, or an execution for an
        // order we do not know. A position that predates the log produces no
        // event at all, so without this the account-level check (R1.12) only
        // ever ran by accident, and a cutover would boot believing it was flat.
        self.request(VenueRequest::Positions);

        for client_order_id in self.book.stale_orders(self.config.stale_after_nanos) {
            if self.reconciling.contains_key(&client_order_id) {
                continue;
            }
            let trace_id = self.trace_of(client_order_id);
            self.begin_reconcile(client_order_id, trace_id, ReconcileCause::StaleSweep)?;
        }
        Ok(())
    }

    fn on_halt(&mut self, note: Note) -> Result<()> {
        if self.halted {
            return Ok(());
        }
        self.halted = true;
        // Durable evidence. The in-memory flag and the log line both go away;
        // this record is what is still there in five days' time, which is the
        // only reason the previous system's outage was diagnosable at all.
        self.decide(
            Instrument::empty(),
            Actor::Supervisor,
            DecisionKind::Halted,
            TraceId::NONE,
            Money::ZERO,
            note,
        )
    }

    fn on_resume(&mut self, note: Note) -> Result<()> {
        if !self.halted {
            return Ok(());
        }
        self.halted = false;
        self.decide(
            Instrument::empty(),
            Actor::Supervisor,
            DecisionKind::Resumed,
            TraceId::NONE,
            Money::ZERO,
            note,
        )
    }

    // -------------------------------------------------------- reconciliation

    /// Move an order into reconciliation and ask the venue.
    fn begin_reconcile(
        &mut self,
        client_order_id: ClientOrderId,
        trace_id: TraceId,
        cause: ReconcileCause,
    ) -> Result<()> {
        if self.reconciling.contains_key(&client_order_id) {
            // One queued reconciliation resolves the order against venue truth
            // however many triggers fired. Duplicates are pure load.
            return Ok(());
        }
        self.ensure_reconciling(client_order_id, trace_id, cause)?;
        self.reconciling.insert(
            client_order_id,
            Pending {
                retry_at: self.book.now(),
                backoff: self.config.reconcile_backoff_nanos,
                in_flight: true,
            },
        );
        self.request(VenueRequest::Lookup { client_order_id });
        Ok(())
    }

    /// Move the position to what the venue says this order actually traded.
    ///
    /// Resolving an order corrects *its* state; without this, it would leave
    /// the position behind. The gap is real and routine: a fill lost in a
    /// stream gap, or one the venue over-matched, is known to the venue and
    /// missing from us, and R1.12 says the venue wins.
    ///
    /// Written as an ordinary fill rather than by adjusting the position
    /// directly, for two reasons. It keeps `positions = fills` true — the
    /// invariant that catches a posting applied twice — and it puts the
    /// correction in the log as something with a size, a price and a cause,
    /// rather than a number that changed for no visible reason.
    ///
    /// The execution id is derived from the order and the venue's figure, so
    /// resolving twice to the same number books the correction once: the second
    /// is a duplicate execution and the book already knows what to do with one.
    ///
    /// The order is `Reconciling` here, which is the state that ingests fills
    /// as evidence without concluding from them — so the correction lands
    /// before the resolution rather than fighting it.
    fn book_the_difference(
        &mut self,
        client_order_id: ClientOrderId,
        trace_id: TraceId,
        venue_filled: Qty,
        venue_avg_price: Option<sentinel_types::Price>,
    ) -> Result<()> {
        let Some(order) = self.book.order(&client_order_id) else {
            return Ok(());
        };
        let instrument = order.core.instrument;
        let ours = self
            .book
            .fills_for(client_order_id)
            .iter()
            .fold(Qty::ZERO, |sum, f| sum.checked_add(f.qty).unwrap_or(sum));

        if venue_filled == ours {
            return Ok(());
        }

        if venue_filled < ours {
            // We have booked exposure the venue does not have. There is no
            // correcting fill for that — a negative execution is not a thing —
            // and the arithmetic that produced it is what we would trade on
            // next, so stop.
            return self.on_halt(
                Note::new(&format!(
                    "{client_order_id}: booked {ours}, venue says {venue_filled}"
                ))
                .unwrap_or_default(),
            );
        }

        let missing = venue_filled.checked_sub(ours).unwrap_or(Qty::ZERO);
        let exec_id = reconciliation_exec_id(client_order_id, venue_filled);
        // Priced at what the venue says the order actually averaged, when it
        // says. Falling back to the last mark moves the position by the right
        // quantity and the P&L by an estimate — right about exposure, wrong
        // about money — so the decision record below says which it was.
        let priced_by_venue = venue_avg_price.is_some();
        let price = venue_avg_price
            .or_else(|| self.book.mark(&instrument))
            .unwrap_or(sentinel_types::Price::ZERO);
        self.write_event(
            client_order_id,
            trace_id,
            OrderEvent::FillReceived {
                exec_id,
                qty: missing,
                price,
            },
        )?;
        self.decide(
            instrument,
            Actor::Reconciler,
            DecisionKind::ReconcileScheduled,
            trace_id,
            Money::from_raw(missing.raw()),
            Note::new(&format!(
                "booked {missing} the venue had and we did not, at {price} ({})",
                if priced_by_venue {
                    "venue average"
                } else {
                    "last mark"
                }
            ))
            .unwrap_or_default(),
        )
    }

    /// Write `ReconcileStarted` unless the order is already reconciling.
    fn ensure_reconciling(
        &mut self,
        client_order_id: ClientOrderId,
        trace_id: TraceId,
        cause: ReconcileCause,
    ) -> Result<()> {
        let already = self
            .book
            .order(&client_order_id)
            .is_some_and(|o| o.core.state == OrderState::Reconciling);
        if already {
            return Ok(());
        }
        self.write_event(
            client_order_id,
            trace_id,
            OrderEvent::ReconcileStarted { cause },
        )?;
        self.decide(
            self.book
                .order(&client_order_id)
                .map_or_else(Instrument::empty, |o| o.core.instrument),
            Actor::Reconciler,
            DecisionKind::ReconcileScheduled,
            trace_id,
            Money::ZERO,
            Note::new(cause.as_str()).unwrap_or_default(),
        )
    }

    /// A lookup produced no answer. Wait longer before asking again.
    fn defer_reconcile(&mut self, client_order_id: ClientOrderId) {
        let cap = self.config.reconcile_backoff_cap_nanos;
        let now = self.book.now();
        if let Some(pending) = self.reconciling.get_mut(&client_order_id) {
            pending.in_flight = false;
            pending.retry_at = now.saturating_add(pending.backoff);
            pending.backoff = pending.backoff.saturating_mul(2).min(cap);
        }
    }

    /// Re-ask about everything whose backoff has expired.
    fn retry_due_reconciliations(&mut self, now: Nanos) {
        let mut due: Vec<ClientOrderId> = self
            .reconciling
            .iter()
            .filter(|(_, p)| !p.in_flight && p.retry_at <= now)
            .map(|(id, _)| *id)
            .collect();
        // Sorted: iteration order over a hash map varies, and this decides the
        // order requests reach the venue, which reaches an output.
        due.sort_unstable();
        for client_order_id in due {
            if let Some(pending) = self.reconciling.get_mut(&client_order_id) {
                pending.in_flight = true;
            }
            self.request(VenueRequest::Lookup { client_order_id });
        }
    }

    fn trace_of(&self, client_order_id: ClientOrderId) -> TraceId {
        self.book
            .order(&client_order_id)
            .map_or(TraceId::NONE, |o| o.trace_id)
    }
}

/// The execution id of a reconciliation correction.
///
/// Derived from the order and the venue's figure, so the same correction
/// derived twice carries the same id and dedups to a no-op. Prefixed so nobody
/// reading the log mistakes it for something the venue sent.
fn reconciliation_exec_id(
    client_order_id: ClientOrderId,
    venue_filled: Qty,
) -> sentinel_types::ExecId {
    let suffix = format!(":{}", venue_filled.raw());
    let room = sentinel_types::ExecId::CAPACITY - "RCN-".len() - suffix.len();
    let head = &client_order_id.as_str()[..client_order_id.len().min(room)];
    sentinel_types::ExecId::new(&format!("RCN-{head}{suffix}")).unwrap_or_default()
}

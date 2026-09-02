//! The book: every projection the engine reads, folded from the journal.
//!
//! There is no database here and nothing to query. Applying a record is a hash
//! lookup and a struct update, which is the whole reason the hot path stopped
//! costing ten seconds — the previous system paid a Postgres round trip, inside
//! a transaction holding an advisory lock, for each one of these.
//!
//! Rebuilding is the same function run over the log from the start. That is not
//! a fallback path kept warm for emergencies; it is the *only* way this
//! structure is ever populated, so the recovery path is exercised by every
//! test in this crate.

use std::collections::HashMap;

use sentinel_domain::{OrderCore, OrderEvent, OrderState, transition};
use sentinel_journal::{FrameHeader, RecordKind, Tailer};
use sentinel_record::Record;
use sentinel_types::{
    Authority, ClientOrderId, Epoch, ExecId, Instrument, Money, Nanos, OrderKind, Price, Qty, Seq,
    Side, TraceId,
};

use crate::error::{LedgerError, Result};
use crate::position::Position;

/// An order, with everything the engine needs that the domain does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    /// The lifecycle state.
    pub core: OrderCore,
    /// Whose authority it exists under (R1.13).
    pub authority: Authority,
    /// How the venue was asked to execute it.
    pub kind: OrderKind,
    /// The resting price, for a limit order.
    pub limit_price: Option<Price>,
    /// The trigger price, for a venue-native stop.
    pub stop_price: Option<Price>,
    /// The quote observed when the decision was made.
    pub quote_at_decision: Option<Price>,
    /// The chain that authorised it.
    pub trace_id: TraceId,
    /// When the intent was made durable.
    pub created_at: Nanos,
    /// When the venue last said anything about it.
    ///
    /// Only venue evidence counts. Our own events prove we are still trying,
    /// not that anyone answered — and a sweep that counted them would never
    /// fire, which is the bug it exists to catch.
    pub last_venue_evidence: Nanos,
}

impl Order {
    /// Quantity still outstanding, as far as we believe.
    #[must_use]
    pub fn remaining_qty(&self) -> Qty {
        self.core.remaining_qty()
    }

    /// Whether this order is a protective exit.
    #[must_use]
    pub const fn is_exit(&self) -> bool {
        matches!(self.authority, Authority::ProtectiveExit)
    }
}

/// One execution, kept because its id is the dedup key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fill {
    /// Which order.
    pub client_order_id: ClientOrderId,
    /// What was traded.
    pub instrument: Instrument,
    /// Which way.
    pub side: Side,
    /// How much. The venue's number, not the order's — a tolerated over-match
    /// is clamped on the order and carried in full into the position.
    pub qty: Qty,
    /// At what price.
    pub price: Price,
    /// When.
    pub at: Nanos,
}

/// What applying a record did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// The book moved.
    Changed,
    /// The record had already been applied, and was a complete no-op.
    ///
    /// The R1.2 outcome. A duplicate execution moves nothing: not the order,
    /// not the position, not the P&L.
    Duplicate,
    /// The record cannot be applied from this state; the order must go through
    /// reconciliation first (R1.7).
    NeedsReconciliation {
        /// Which order.
        client_order_id: ClientOrderId,
    },
}

/// Every projection, folded from the log.
///
/// `PartialEq` so a rebuild can be compared against the live book as a whole,
/// rather than field by field in whichever test remembered to check them.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Book {
    orders: HashMap<ClientOrderId, Order>,
    fills: HashMap<ExecId, Fill>,
    positions: HashMap<Instrument, Position>,
    balances: HashMap<sentinel_record::Asset, Money>,
    marks: HashMap<Instrument, Price>,
    /// Insertion order, so listings are stable without sorting a hash map.
    order_sequence: Vec<ClientOrderId>,
    epoch: Epoch,
    last_seq: Seq,
    now: Nanos,
    halted: bool,
}

impl Book {
    /// An empty book.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild by folding the whole journal.
    ///
    /// # Errors
    /// [`LedgerError`] when a record cannot be decoded or applied. A log that
    /// does not fold is a log we cannot trade from, so this fails loudly rather
    /// than skipping the record and carrying on with a book that is quietly
    /// missing a fill.
    pub fn rebuild(tailer: &mut Tailer) -> Result<Self> {
        let mut book = Self::new();
        while let Some(record) = tailer.next_record().map_err(LedgerError::Journal)? {
            let decoded =
                Record::decode(record.header.kind, &record.payload).map_err(|source| {
                    LedgerError::Codec {
                        seq: record.header.seq,
                        source,
                    }
                })?;
            book.apply(&record.header, decoded)?;
        }
        Ok(book)
    }

    /// Apply one record.
    ///
    /// # Errors
    /// [`LedgerError`] when the record contradicts the book in a way that is
    /// not routine — an unknown order, an illegal transition, an overfill.
    pub fn apply(&mut self, header: &FrameHeader, record: Record) -> Result<Applied> {
        self.last_seq = header.seq;
        self.epoch = header.epoch;
        // Time advances with the log, never from a clock. Every age this book
        // reports — a stale order, a position's holding period — is measured
        // against journal time, so a replay measures the same ages the live run
        // did.
        self.now = self.now.max(header.nanos);

        match record {
            Record::EpochClaimed { epoch, .. } => {
                self.epoch = epoch;
                Ok(Applied::Changed)
            }
            Record::IntentPersisted(intent) => {
                // R1.2: a replayed intent finds the order already here and
                // creates nothing. This is the durable half of idempotency —
                // it survives a restart because the book is rebuilt from the
                // same log the first intent was written to.
                if self.orders.contains_key(&intent.client_order_id) {
                    return Ok(Applied::Duplicate);
                }
                let order = Order {
                    core: OrderCore::created(
                        intent.client_order_id,
                        intent.instrument,
                        intent.side,
                        intent.qty,
                    ),
                    authority: intent.authority,
                    kind: intent.kind,
                    limit_price: intent.limit_price,
                    stop_price: intent.stop_price,
                    quote_at_decision: intent.quote_at_decision,
                    trace_id: intent.trace_id,
                    created_at: header.nanos,
                    last_venue_evidence: header.nanos,
                };
                self.order_sequence.push(intent.client_order_id);
                self.orders.insert(intent.client_order_id, order);
                Ok(Applied::Changed)
            }
            Record::OrderEvent {
                client_order_id,
                from_state,
                to_state,
                event,
                ..
            } => self.apply_order_event(header, client_order_id, from_state, to_state, event),
            Record::Decision { kind, .. } => {
                match kind {
                    sentinel_record::DecisionKind::Halted => self.halted = true,
                    sentinel_record::DecisionKind::Resumed => self.halted = false,
                    _ => {}
                }
                Ok(Applied::Changed)
            }
            Record::Mark { instrument, price } => {
                self.marks.insert(instrument, price);
                Ok(Applied::Changed)
            }
            Record::Balance { asset, amount } => {
                self.balances.insert(asset, amount);
                Ok(Applied::Changed)
            }
            Record::Ticked => Ok(Applied::Changed),
        }
    }

    fn apply_order_event(
        &mut self,
        header: &FrameHeader,
        client_order_id: ClientOrderId,
        from_state: OrderState,
        to_state: OrderState,
        event: OrderEvent,
    ) -> Result<Applied> {
        let order = *self
            .orders
            .get(&client_order_id)
            .ok_or(LedgerError::UnknownOrder { client_order_id })?;

        // Dedup before anything else. A duplicate execution is a complete
        // no-op: the order does not move, the position does not move, and no
        // P&L is booked (R1.2 / R1.7). Doing this first is what makes that
        // true rather than approximately true.
        if let OrderEvent::FillReceived { exec_id, .. } = event
            && self.fills.contains_key(&exec_id)
        {
            return Ok(Applied::Duplicate);
        }

        // The record says where the order was and where it went. Both are
        // checked against the transition rather than trusted: a log written by
        // a different build of the state machine would otherwise be folded into
        // a book that quietly disagrees with the one that wrote it, and the
        // disagreement would be about a position.
        if order.core.state != from_state {
            return Err(LedgerError::StateDisagrees {
                client_order_id,
                recorded: from_state,
                held: order.core.state,
            });
        }

        let next = match transition(order.core, event) {
            Ok(next) => next,
            Err(e) if e.needs_reconciliation() => {
                if to_state != from_state {
                    return Err(LedgerError::StateDisagrees {
                        client_order_id,
                        recorded: to_state,
                        held: from_state,
                    });
                }
                return Ok(Applied::NeedsReconciliation { client_order_id });
            }
            Err(source) => {
                return Err(LedgerError::Transition {
                    client_order_id,
                    source,
                });
            }
        };
        if next.state != to_state {
            return Err(LedgerError::StateDisagrees {
                client_order_id,
                recorded: to_state,
                held: next.state,
            });
        }

        // The position moves off the fill's own quantity, not the order's
        // clamped one. The venue is the source of truth about exposure, so a
        // tolerated over-match lands in full here while the order's record
        // stays inside its authorised size.
        if let OrderEvent::FillReceived {
            exec_id,
            qty,
            price,
        } = event
        {
            self.fills.insert(
                exec_id,
                Fill {
                    client_order_id,
                    instrument: order.core.instrument,
                    side: order.core.side,
                    qty,
                    price,
                    at: header.nanos,
                },
            );
            self.positions
                .entry(order.core.instrument)
                .or_default()
                .apply_fill(order.core.side, qty, price)
                .map_err(|_| LedgerError::PositionOverflow {
                    instrument: order.core.instrument,
                })?;
        }

        let entry = self
            .orders
            .get_mut(&client_order_id)
            .ok_or(LedgerError::UnknownOrder { client_order_id })?;
        entry.core = next;
        if event.is_venue_evidence() {
            entry.last_venue_evidence = header.nanos;
        }
        Ok(Applied::Changed)
    }

    // ------------------------------------------------------------- reading

    /// One order.
    #[must_use]
    pub fn order(&self, client_order_id: &ClientOrderId) -> Option<&Order> {
        self.orders.get(client_order_id)
    }

    /// Every order, in the order its intent was written.
    ///
    /// Insertion order rather than hash order: a listing that reshuffles
    /// between reads is unreadable on a screen, and iteration order that varies
    /// is nondeterminism that must not reach an output.
    pub fn orders(&self) -> impl Iterator<Item = &Order> {
        self.order_sequence
            .iter()
            .filter_map(|id| self.orders.get(id))
    }

    /// Orders that are not finished.
    pub fn live_orders(&self) -> impl Iterator<Item = &Order> {
        self.orders().filter(|o| !o.core.is_terminal())
    }

    /// Live orders on one instrument.
    pub fn live_orders_on<'a>(
        &'a self,
        instrument: &'a Instrument,
    ) -> impl Iterator<Item = &'a Order> {
        self.live_orders()
            .filter(move |o| o.core.instrument == *instrument)
    }

    /// Whether anything on this instrument is in a state whose truth we cannot
    /// prove, and which therefore blocks new entries (R1.4).
    #[must_use]
    pub fn instrument_locked(&self, instrument: &Instrument) -> bool {
        self.live_orders_on(instrument)
            .any(|o| o.core.locks_instrument())
    }

    /// The order holding the instrument, when one does. For the operator, who
    /// needs to know *which* order to go and look at.
    #[must_use]
    pub fn locking_order<'a>(&'a self, instrument: &'a Instrument) -> Option<&'a Order> {
        self.live_orders_on(instrument)
            .find(|o| o.core.locks_instrument())
    }

    /// The position in one instrument.
    #[must_use]
    pub fn position(&self, instrument: &Instrument) -> Position {
        self.positions.get(instrument).copied().unwrap_or_default()
    }

    /// Every instrument holding a position, sorted.
    #[must_use]
    pub fn open_positions(&self) -> Vec<(Instrument, Position)> {
        let mut out: Vec<_> = self
            .positions
            .iter()
            .filter(|(_, p)| !p.is_flat())
            .map(|(i, p)| (*i, *p))
            .collect();
        out.sort_unstable_by_key(|(i, _)| *i);
        out
    }

    /// Outstanding quantity across live protective exits on one instrument.
    ///
    /// What R1.10 is bounded against: a new exit may only be for the position
    /// that is not already spoken for.
    #[must_use]
    pub fn committed_to_exits(&self, instrument: &Instrument) -> Qty {
        self.live_orders_on(instrument)
            .filter(|o| o.is_exit())
            .fold(Qty::ZERO, |sum, o| {
                sum.checked_add(o.remaining_qty()).unwrap_or(Qty::MAX)
            })
    }

    /// One execution, by the venue's id for it.
    #[must_use]
    pub fn fill(&self, exec_id: &ExecId) -> Option<&Fill> {
        self.fills.get(exec_id)
    }

    /// Executions recorded.
    #[must_use]
    pub fn fill_count(&self) -> usize {
        self.fills.len()
    }

    /// Every execution, in execution-id order.
    ///
    /// Sorted rather than in hash order: this feeds the invariant check, whose
    /// output an operator reads, and a list that reshuffles between runs is a
    /// list nobody can diff.
    #[must_use]
    pub fn all_fills(&self) -> Vec<Fill> {
        let mut ids: Vec<_> = self.fills.keys().collect();
        ids.sort_unstable();
        ids.iter()
            .filter_map(|id| self.fills.get(*id))
            .copied()
            .collect()
    }

    /// Every execution belonging to one order.
    ///
    /// The book indexes fills by execution id, because that is the dedup key
    /// the hot path needs. Walking them per order costs only at read time,
    /// which keeps a second index out of the write path.
    #[must_use]
    pub fn fills_for(&self, client_order_id: ClientOrderId) -> Vec<Fill> {
        self.all_fills()
            .into_iter()
            .filter(|f| f.client_order_id == client_order_id)
            .collect()
    }

    /// Every position, flat ones included, sorted.
    ///
    /// [`Self::open_positions`] is for a screen; this is for the invariant
    /// check, which has to see a flat position still carrying a cost basis.
    #[must_use]
    pub fn all_positions(&self) -> Vec<(Instrument, Position)> {
        let mut out: Vec<_> = self.positions.iter().map(|(i, p)| (*i, *p)).collect();
        out.sort_unstable_by_key(|(i, _)| *i);
        out
    }

    /// The last price seen for an instrument.
    #[must_use]
    pub fn mark(&self, instrument: &Instrument) -> Option<Price> {
        self.marks.get(instrument).copied()
    }

    /// A balance.
    #[must_use]
    pub fn balance(&self, asset: &sentinel_record::Asset) -> Money {
        self.balances.get(asset).copied().unwrap_or(Money::ZERO)
    }

    /// Every balance, sorted by asset.
    #[must_use]
    pub fn balances(&self) -> Vec<(sentinel_record::Asset, Money)> {
        let mut out: Vec<_> = self.balances.iter().map(|(a, m)| (*a, *m)).collect();
        out.sort_unstable_by_key(|(a, _)| *a);
        out
    }

    /// Realised P&L across every instrument.
    #[must_use]
    pub fn realized(&self) -> Money {
        let mut sorted: Vec<_> = self.positions.iter().collect();
        sorted.sort_unstable_by_key(|(i, _)| **i);
        sorted.iter().fold(Money::ZERO, |sum, (_, p)| {
            sum.checked_add(p.realized).unwrap_or(sum)
        })
    }

    /// Unrealised P&L at the last marks seen.
    #[must_use]
    pub fn unrealized(&self) -> Money {
        self.open_positions()
            .iter()
            .filter_map(|(i, p)| self.mark(i).map(|m| p.unrealized(m)))
            .fold(Money::ZERO, |sum, v| sum.checked_add(v).unwrap_or(sum))
    }

    /// The writer epoch the book was folded under.
    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The last sequence applied.
    #[must_use]
    pub const fn last_seq(&self) -> Seq {
        self.last_seq
    }

    /// Journal time, as of the last record applied.
    #[must_use]
    pub const fn now(&self) -> Nanos {
        self.now
    }

    /// Whether the account has stopped trading.
    #[must_use]
    pub const fn is_halted(&self) -> bool {
        self.halted
    }

    /// Live orders the venue has not spoken about for longer than `max_age`.
    ///
    /// The liveness backstop for lost venue events. Every reactive trigger —
    /// a timeout, a late fill, a duplicate — presumes an event *arrives*; a
    /// fill dropped in a stream gap produces no trigger at all, and the order
    /// sits in a live state forever. This is what finds it.
    #[must_use]
    pub fn stale_orders(&self, max_age_nanos: u64) -> Vec<ClientOrderId> {
        let now = self.now;
        let mut out: Vec<_> = self
            .live_orders()
            // An order still reconciling is being dealt with; sweeping it
            // again would queue the same work twice. The Python system did
            // exactly that and measured 11,409 sweep enqueues against 2,159
            // completed reconciliations, each redundant attempt hammering an
            // already-throttling venue.
            .filter(|o| o.core.state != OrderState::Reconciling)
            .filter(|o| now.saturating_since(o.last_venue_evidence) > max_age_nanos)
            .map(|o| o.core.client_order_id)
            .collect();
        out.sort_unstable();
        out
    }
}

/// The record kinds a book folds. Anything else in the log is for a consumer
/// downstream and is skipped here rather than being an error.
#[must_use]
pub const fn affects_book(kind: RecordKind) -> bool {
    !matches!(kind, RecordKind::Ticked)
}

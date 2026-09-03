//! The journal's vocabulary: what a record can be, and its bytes.
//!
//! Every record has a fixed layout. Reading one is a walk down known offsets,
//! and a record that decodes with bytes left over is refused — because trailing
//! bytes mean this is a record from a different version of the format wearing
//! the same kind byte, and guessing which fields it shares is how a log stops
//! meaning what it says.

use sentinel_domain::{EconomicOrderIntent, OrderEvent, OrderState, ReconcileCause, RejectReason};
use sentinel_journal::RecordKind;
use sentinel_types::{
    Authority, ClientOrderId, Epoch, InlineStr, Instrument, Money, OrderKind, Price, Seq, Side,
    TraceId, VenueOrderId, wire_enum,
};

use crate::buf::{CodecError, Reader, Result, Writer};

wire_enum! {
    /// Who made a decision.
    ///
    /// The set is closed because every value of it is a component with its own
    /// tests. A free-text actor field would let a new caller invent a seventh
    /// without anyone deciding it should exist.
    Actor {
        /// Command intake.
        Gateway = 1 => "gateway",
        /// Exposure guards.
        Guards = 2 => "guards",
        /// The reconciler.
        Reconciler = 3 => "reconciler",
        /// The protective-exit supervisor.
        Protection = 4 => "protection",
        /// A strategy.
        Strategy = 5 => "strategy",
        /// The supervisor itself.
        Supervisor = 6 => "supervisor",
        /// Position sizing.
        Sizing = 7 => "sizing",
    }
}

wire_enum! {
    /// What was decided.
    ///
    /// These are the events an order lifecycle structurally cannot record:
    /// the trades that did not happen. A blocked entry produces no order and
    /// therefore no order event, which is exactly why it needs its own record —
    /// without one, "the bot did nothing for six hours" has no explanation in
    /// the log.
    DecisionKind {
        /// An entry was authorised and submitted.
        EntryPlaced = 1 => "ENTRY_PLACED",
        /// An entry was refused before submission.
        EntryBlocked = 2 => "ENTRY_BLOCKED",
        /// An exit was authorised.
        ExitPlaced = 3 => "EXIT_PLACED",
        /// An exit was reduced to the position actually held (R1.10).
        ExitClamped = 4 => "EXIT_CLAMPED",
        /// Sizing produced a quantity.
        SizingComputed = 5 => "SIZING_COMPUTED",
        /// An order was queued for reconciliation.
        ReconcileScheduled = 6 => "RECONCILE_SCHEDULED",
        /// The account stopped trading.
        Halted = 7 => "HALTED",
        /// The account resumed.
        Resumed = 8 => "RESUMED",
        /// A strategy said what it wants to hold.
        ///
        /// Most bars ask for the position that is already held, so this is
        /// usually the record of nothing happening — which is the case the log
        /// was otherwise silent about. Without it, a strategy that has quietly
        /// stopped deciding looks exactly like one that keeps deciding to hold.
        StanceDeclared = 9 => "STANCE_DECLARED",
    }
}

/// Free text attached to a decision. Short, because it is read by a person.
pub type Note = InlineStr<96>;

/// The currency or asset a balance is denominated in.
pub type Asset = InlineStr<8>;

/// Anything the journal can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Record {
    /// A writer claimed the journal.
    EpochClaimed {
        /// The epoch this run holds.
        epoch: Epoch,
        /// The last sequence already durable when it arrived.
        resumed_from: Seq,
    },
    /// An authorised order, durable before the venue sees it (R1.1).
    ///
    /// This record is the whole of R1.1 and half of R1.2: it lands before any
    /// submission, so a crash anywhere after it leaves the intent as the
    /// recovery authority, and a replay of the same decision collapses onto the
    /// same client order id instead of creating a second order.
    IntentPersisted(EconomicOrderIntent),
    /// An order lifecycle transition.
    OrderEvent {
        /// Which order.
        client_order_id: ClientOrderId,
        /// The causal chain.
        trace_id: TraceId,
        /// Where the order was.
        from_state: OrderState,
        /// Where it ended up.
        ///
        /// Written down rather than left to be re-derived. Two bytes, and they
        /// mean a consumer of this log — the Go gateway, the projector, anyone
        /// reading it in five years — does not need its own copy of the
        /// transition function to know what happened. A second implementation
        /// of that function is a second thing that can be wrong, and it would
        /// be wrong silently, in a display, about money.
        ///
        /// This is *not* a reliable sign of whether the event was applied —
        /// see `applied`. An applied fill can leave the state where it found
        /// it, which is exactly what happens while an order is reconciling.
        to_state: OrderState,
        /// Whether the ledger booked this event, or only recorded it.
        ///
        /// Written down because it cannot be inferred. The obvious inference —
        /// "the state did not change, so nothing happened" — is wrong in the
        /// case that matters most: a fill booked during reconciliation moves
        /// the position while leaving the order in `Reconciling`, and an
        /// adopted position is booked that way too. A consumer that guessed
        /// would under-report exposure, silently, in a display, about money.
        ///
        /// False means recorded as evidence and applied to nothing: a fill for
        /// an order believed terminal, or a cancel-confirm landing mid-reconcile.
        applied: bool,
        /// What happened.
        event: OrderEvent,
    },
    /// Why something happened, or did not.
    Decision {
        /// What it concerned. Empty for account-wide decisions.
        instrument: Instrument,
        /// Who decided.
        actor: Actor,
        /// What was decided.
        kind: DecisionKind,
        /// The causal chain.
        trace_id: TraceId,
        /// A number the decision turned on — a size, a limit, a shortfall.
        value: Money,
        /// For a person.
        note: Note,
    },
    /// A price observation.
    Mark {
        /// What was priced.
        instrument: Instrument,
        /// The price.
        price: Price,
    },
    /// An account balance.
    Balance {
        /// What is held.
        asset: Asset,
        /// How much.
        amount: Money,
    },
    /// Time entering the engine.
    ///
    /// The only way a clock reaches anything below the gateway. The instant
    /// itself is in the frame header, so this record has no payload at all.
    Ticked,
}

impl Record {
    /// The journal kind this record is stored under.
    #[must_use]
    pub const fn kind(&self) -> RecordKind {
        match self {
            Self::EpochClaimed { .. } => RecordKind::EpochClaimed,
            Self::IntentPersisted(_) => RecordKind::IntentPersisted,
            Self::OrderEvent { .. } => RecordKind::OrderEvent,
            Self::Decision { .. } => RecordKind::Decision,
            Self::Mark { .. } => RecordKind::Mark,
            Self::Balance { .. } => RecordKind::Balance,
            Self::Ticked => RecordKind::Ticked,
        }
    }

    /// Append this record's payload to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let mut w = Writer::new(out);
        match *self {
            Self::EpochClaimed {
                epoch,
                resumed_from,
            } => {
                w.u64(epoch.as_u64()).u64(resumed_from.as_u64());
            }
            Self::IntentPersisted(intent) => {
                w.text(intent.client_order_id)
                    .text(intent.instrument)
                    .u8(intent.side.as_u8())
                    .u8(intent.kind.as_u8())
                    .u8(intent.authority.as_u8())
                    .qty(intent.qty)
                    .opt_price(intent.limit_price)
                    .opt_price(intent.stop_price)
                    .opt_price(intent.quote_at_decision)
                    .trace(intent.trace_id);
            }
            Self::OrderEvent {
                client_order_id,
                trace_id,
                from_state,
                to_state,
                applied,
                event,
            } => {
                w.text(client_order_id)
                    .trace(trace_id)
                    .u8(from_state.as_u8())
                    .u8(to_state.as_u8())
                    .u8(u8::from(applied))
                    .u8(event.kind().as_u8());
                encode_event(&mut w, event);
            }
            Self::Decision {
                instrument,
                actor,
                kind,
                trace_id,
                value,
                note,
            } => {
                w.text(instrument)
                    .u8(actor.as_u8())
                    .u8(kind.as_u8())
                    .trace(trace_id)
                    .money(value)
                    .text(note);
            }
            Self::Mark { instrument, price } => {
                w.text(instrument).price(price);
            }
            Self::Balance { asset, amount } => {
                w.text(asset).money(amount);
            }
            Self::Ticked => {}
        }
    }

    /// This record's payload as a fresh buffer. For tests and one-off callers;
    /// the engine appends into a reused buffer instead.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }

    /// Decode a payload, given the kind the frame header carried.
    ///
    /// # Errors
    /// [`CodecError`] when the bytes are short, hold a discriminant that is not
    /// a value, or have anything left over.
    pub fn decode(kind: RecordKind, payload: &[u8]) -> Result<Self> {
        let mut r = Reader::new(payload);
        let record = match kind {
            RecordKind::EpochClaimed => Self::EpochClaimed {
                epoch: Epoch::from_u64(r.u64()?),
                resumed_from: Seq::from_u64(r.u64()?),
            },
            RecordKind::IntentPersisted => {
                let client_order_id = r.text::<36>()?;
                let instrument = r.text::<16>()?;
                let side = Side::from_u8(r.u8()?)?;
                let order_kind = OrderKind::from_u8(r.u8()?)?;
                let authority = Authority::from_u8(r.u8()?)?;
                let qty = r.qty()?;
                let limit_price = r.opt_price()?;
                let stop_price = r.opt_price()?;
                let quote_at_decision = r.opt_price()?;
                let trace_id = r.trace()?;
                // Rebuilt through the validating constructor, so a record that
                // decodes cleanly but describes an intent this system would
                // never authorise is caught at the boundary rather than
                // downstream. A log is an input like any other.
                let intent = EconomicOrderIntent::new(
                    client_order_id,
                    instrument,
                    side,
                    qty,
                    order_kind,
                    limit_price,
                    stop_price,
                    authority,
                    trace_id,
                    quote_at_decision,
                )
                .map_err(|_| CodecError::BadText)?;
                Self::IntentPersisted(intent)
            }
            RecordKind::OrderEvent => Self::OrderEvent {
                client_order_id: r.text::<36>()?,
                trace_id: r.trace()?,
                from_state: OrderState::from_u8(r.u8()?)?,
                to_state: OrderState::from_u8(r.u8()?)?,
                applied: r.u8()? != 0,
                event: decode_event(&mut r)?,
            },
            RecordKind::Decision => Self::Decision {
                instrument: r.text::<16>()?,
                actor: Actor::from_u8(r.u8()?)?,
                kind: DecisionKind::from_u8(r.u8()?)?,
                trace_id: r.trace()?,
                value: r.money()?,
                note: r.text::<96>()?,
            },
            RecordKind::Mark => Self::Mark {
                instrument: r.text::<16>()?,
                price: r.price()?,
            },
            RecordKind::Balance => Self::Balance {
                asset: r.text::<8>()?,
                amount: r.money()?,
            },
            RecordKind::Ticked => Self::Ticked,
        };
        r.finish()?;
        Ok(record)
    }
}

fn encode_event(w: &mut Writer<'_>, event: OrderEvent) {
    match event {
        OrderEvent::SubmissionStarted
        | OrderEvent::SubmissionTimedOut
        | OrderEvent::CancelRequested
        | OrderEvent::CancelConfirmed => {}
        OrderEvent::VenueAcked { venue_order_id } => {
            w.text(venue_order_id);
        }
        OrderEvent::VenueRejected { reason } => {
            w.i32(reason.code).text(reason.text);
        }
        OrderEvent::FillReceived {
            exec_id,
            qty,
            price,
        } => {
            w.text(exec_id).qty(qty).price(price);
        }
        OrderEvent::ReconcileStarted { cause } => {
            w.u8(cause_to_u8(cause));
        }
        OrderEvent::ReconcileResolved {
            state,
            venue_order_id,
            filled_qty,
        } => {
            w.u8(state.as_u8())
                .flag(venue_order_id.is_some())
                .text(venue_order_id.unwrap_or_default())
                .qty(filled_qty);
        }
    }
}

fn decode_event(r: &mut Reader<'_>) -> Result<OrderEvent> {
    use sentinel_domain::EventKind;
    Ok(match EventKind::from_u8(r.u8()?)? {
        EventKind::SubmissionStarted => OrderEvent::SubmissionStarted,
        EventKind::VenueAcked => OrderEvent::VenueAcked {
            venue_order_id: r.text::<32>()?,
        },
        EventKind::VenueRejected => {
            let code = r.i32()?;
            let text = r.text::<64>()?;
            OrderEvent::VenueRejected {
                reason: RejectReason { code, text },
            }
        }
        EventKind::SubmissionTimedOut => OrderEvent::SubmissionTimedOut,
        EventKind::FillReceived => OrderEvent::FillReceived {
            exec_id: r.text::<40>()?,
            qty: r.qty()?,
            price: r.price()?,
        },
        EventKind::CancelRequested => OrderEvent::CancelRequested,
        EventKind::CancelConfirmed => OrderEvent::CancelConfirmed,
        EventKind::ReconcileStarted => OrderEvent::ReconcileStarted {
            cause: cause_from_u8(r.u8()?)?,
        },
        EventKind::ReconcileResolved => {
            let state = OrderState::from_u8(r.u8()?)?;
            let present = r.flag()?;
            let id: VenueOrderId = r.text::<32>()?;
            OrderEvent::ReconcileResolved {
                state,
                venue_order_id: present.then_some(id),
                filled_qty: r.qty()?,
            }
        }
    })
}

// `ReconcileCause` is a plain enum in the domain rather than a `wire_enum`,
// because down there it is a reason a human reads and not a byte. It becomes a
// byte here, at the boundary, where the mapping can be pinned by a test.
const fn cause_to_u8(cause: ReconcileCause) -> u8 {
    match cause {
        ReconcileCause::SubmissionTimeout => 1,
        ReconcileCause::CancelTimeout => 2,
        ReconcileCause::ReplayedMidSubmission => 3,
        ReconcileCause::LateFill => 4,
        ReconcileCause::LateCancelConfirm => 5,
        ReconcileCause::StaleSweep => 6,
        ReconcileCause::PositionDisagreement => 7,
    }
}

fn cause_from_u8(v: u8) -> Result<ReconcileCause> {
    Ok(match v {
        1 => ReconcileCause::SubmissionTimeout,
        2 => ReconcileCause::CancelTimeout,
        3 => ReconcileCause::ReplayedMidSubmission,
        4 => ReconcileCause::LateFill,
        5 => ReconcileCause::LateCancelConfirm,
        6 => ReconcileCause::StaleSweep,
        7 => ReconcileCause::PositionDisagreement,
        value => {
            return Err(CodecError::BadDiscriminant(
                sentinel_types::UnknownDiscriminant {
                    type_name: "ReconcileCause",
                    value,
                },
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel_types::{ExecId, Qty};

    fn coid() -> ClientOrderId {
        ClientOrderId::new("UI-4bb5ba826e2e1").unwrap()
    }

    fn btc() -> Instrument {
        Instrument::new("BTCUSD").unwrap()
    }

    fn intent() -> EconomicOrderIntent {
        EconomicOrderIntent::new(
            coid(),
            btc(),
            Side::Buy,
            Qty::whole(3),
            OrderKind::Limit,
            Some(Price::parse("109432.5").unwrap()),
            None,
            Authority::Entry,
            TraceId::from_u128(0x6440_ba96),
            Some(Price::parse("109430").unwrap()),
        )
        .unwrap()
    }

    /// Every record shape, so a new variant that is not encoded fails here.
    fn every_record() -> Vec<Record> {
        let mut records = vec![
            Record::EpochClaimed {
                epoch: Epoch::from_u64(12),
                resumed_from: Seq::from_u64(5_698),
            },
            Record::IntentPersisted(intent()),
            Record::Decision {
                instrument: btc(),
                actor: Actor::Guards,
                kind: DecisionKind::EntryBlocked,
                trace_id: TraceId::from_u128(7),
                value: Money::parse("-118.81").unwrap(),
                note: Note::new("instrument locked by UI-4bb5ba826e2e1").unwrap(),
            },
            Record::Mark {
                instrument: btc(),
                price: Price::parse("109432.5").unwrap(),
            },
            Record::Balance {
                asset: Asset::new("USD").unwrap(),
                amount: Money::parse("5942.65").unwrap(),
            },
            Record::Ticked,
        ];
        for event in every_event() {
            records.push(Record::OrderEvent {
                client_order_id: coid(),
                trace_id: TraceId::from_u128(0x8f82_b303),
                from_state: OrderState::Working,
                to_state: OrderState::Partial,
                applied: true,
                event,
            });
        }
        records
    }

    fn every_event() -> Vec<OrderEvent> {
        vec![
            OrderEvent::SubmissionStarted,
            OrderEvent::VenueAcked {
                venue_order_id: VenueOrderId::new("V-77").unwrap(),
            },
            OrderEvent::VenueRejected {
                reason: RejectReason::new(-2019, "insufficient margin"),
            },
            OrderEvent::SubmissionTimedOut,
            OrderEvent::FillReceived {
                exec_id: ExecId::new("E-6440ba96").unwrap(),
                qty: Qty::whole(2),
                price: Price::parse("109432.5").unwrap(),
            },
            OrderEvent::CancelRequested,
            OrderEvent::CancelConfirmed,
            OrderEvent::ReconcileStarted {
                cause: ReconcileCause::SubmissionTimeout,
            },
            OrderEvent::ReconcileResolved {
                state: OrderState::Filled,
                venue_order_id: Some(VenueOrderId::new("V-77").unwrap()),
                filled_qty: Qty::whole(3),
            },
            OrderEvent::ReconcileResolved {
                state: OrderState::Canceled,
                venue_order_id: None,
                filled_qty: Qty::ZERO,
            },
        ]
    }

    #[test]
    fn every_record_round_trips() {
        for record in every_record() {
            let bytes = record.to_bytes();
            let back = Record::decode(record.kind(), &bytes)
                .unwrap_or_else(|e| panic!("{record:?} failed to decode: {e}"));
            assert_eq!(back, record);
        }
    }

    #[test]
    fn encoding_is_deterministic() {
        // The log is compared, hashed and replayed. Encoding the same value
        // twice must produce the same bytes, or none of that works.
        for record in every_record() {
            assert_eq!(record.to_bytes(), record.to_bytes());
        }
    }

    #[test]
    fn every_reconcile_cause_survives_the_byte_boundary() {
        // The domain enum is not wire-typed, so this mapping is the only thing
        // holding it to the log. Adding a cause without extending both halves
        // fails here rather than at three in the morning.
        for cause in [
            ReconcileCause::SubmissionTimeout,
            ReconcileCause::CancelTimeout,
            ReconcileCause::ReplayedMidSubmission,
            ReconcileCause::LateFill,
            ReconcileCause::LateCancelConfirm,
            ReconcileCause::StaleSweep,
            ReconcileCause::PositionDisagreement,
        ] {
            assert_eq!(cause_from_u8(cause_to_u8(cause)).unwrap(), cause);
        }
        assert!(cause_from_u8(0).is_err(), "zero is not a cause");
        assert!(cause_from_u8(8).is_err());
    }

    #[test]
    fn a_truncated_payload_is_refused() {
        for record in every_record() {
            let bytes = record.to_bytes();
            for cut in 0..bytes.len() {
                assert!(
                    Record::decode(record.kind(), &bytes[..cut]).is_err(),
                    "{record:?} decoded from {cut} of {} bytes",
                    bytes.len()
                );
            }
        }
    }

    #[test]
    fn extra_bytes_are_refused() {
        // A record from another version of the format wearing the same kind
        // byte. Sharing the fields that happen to line up would be a guess.
        for record in every_record() {
            let mut bytes = record.to_bytes();
            bytes.push(0);
            assert!(
                matches!(
                    Record::decode(record.kind(), &bytes),
                    Err(CodecError::TrailingBytes { .. })
                ),
                "{record:?} accepted a trailing byte"
            );
        }
    }

    #[test]
    fn an_intent_that_would_not_be_authorised_is_refused_on_the_way_in() {
        // A log is an input. A record whose bytes are intact but whose meaning
        // is an order this system would never place must not become one just
        // because it was found on disk.
        let mut bytes = Record::IntentPersisted(intent()).to_bytes();
        // Zero the quantity: 36 id + 16 instrument + 3 discriminants = 55.
        bytes[55..63].copy_from_slice(&0i64.to_le_bytes());
        assert!(Record::decode(RecordKind::IntentPersisted, &bytes).is_err());
    }

    #[test]
    fn a_timeout_record_carries_nothing() {
        // Structural, not stylistic: there is no field in the bytes for a
        // reader to form an opinion from about what the venue actually did.
        let record = Record::OrderEvent {
            client_order_id: coid(),
            trace_id: TraceId::NONE,
            from_state: OrderState::Submitting,
            to_state: OrderState::Unknown,
            applied: true,
            event: OrderEvent::SubmissionTimedOut,
        };
        // 36 id + 16 trace + 2 states + 1 applied + 1 event kind, and nothing
        // else.
        assert_eq!(record.to_bytes().len(), 56);
    }

    #[test]
    fn record_kinds_match_the_journal_kinds() {
        for record in every_record() {
            let bytes = record.to_bytes();
            // Decoding under the wrong kind must not succeed by coincidence.
            for &kind in RecordKind::ALL {
                if kind == record.kind() {
                    continue;
                }
                if let Ok(other) = Record::decode(kind, &bytes) {
                    assert_ne!(other, record, "{record:?} decoded as {kind}");
                }
            }
        }
    }
}

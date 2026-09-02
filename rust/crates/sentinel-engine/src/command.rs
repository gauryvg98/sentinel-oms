//! Everything that can reach the writer.
//!
//! One enum, and the engine is a function of it. That is what makes a replay
//! possible: feed the same commands in the same order and the same log comes
//! out, because there is no other way in.
//!
//! Note what is *not* here — there is no `Now`. Time arrives as
//! [`Command::Tick`] and in no other way, so nothing below this line reads a
//! clock and every age the engine measures is measured against the log.

use sentinel_domain::EconomicOrderIntent;
use sentinel_record::{DecisionKind, Note};
use sentinel_types::{ClientOrderId, Money, Nanos, TraceId};
use sentinel_venue::{CancelOutcome, LookupOutcome, SubmitOutcome, VenueEvent, VenuePosition};

/// Something for the writer to do.
///
/// The variants differ a lot in size — a placement carries a whole intent, a
/// tick carries a timestamp — and that is left alone deliberately. Boxing the
/// large one would put an allocation on the order path to save a few bytes on
/// a channel that is never the bottleneck, and `Copy` is what makes a command
/// impossible to mutate between being queued and being applied.
#[expect(
    clippy::large_enum_variant,
    reason = "Copy and allocation-free matter more here than the padding"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Time has passed.
    ///
    /// Drives every deadline in the system: reconcile retries, the stale sweep,
    /// and any age a decision turns on. Nothing else advances the clock.
    Tick {
        /// The instant.
        now: Nanos,
    },

    /// Authorise an order.
    ///
    /// Makes the intent durable *before* the venue is asked (R1.1), which is
    /// what leaves something to recover from if the process dies in the window
    /// between deciding and submitting.
    Place {
        /// What to place.
        intent: EconomicOrderIntent,
    },

    /// Ask the venue to cancel an order.
    Cancel {
        /// Which order.
        client_order_id: ClientOrderId,
        /// The chain asking.
        trace_id: TraceId,
    },

    /// What a submission actually did.
    Submitted {
        /// Which order.
        client_order_id: ClientOrderId,
        /// What the venue said, or failed to say.
        outcome: SubmitOutcome,
    },

    /// What a cancel request actually did.
    Cancelled {
        /// Which order.
        client_order_id: ClientOrderId,
        /// What the venue said, or failed to say.
        outcome: CancelOutcome,
    },

    /// What the venue says an order really is.
    ///
    /// The only thing allowed to overwrite local belief (R1.12).
    LookedUp {
        /// Which order.
        client_order_id: ClientOrderId,
        /// The venue's answer.
        outcome: LookupOutcome,
    },

    /// Something the venue pushed.
    Streamed {
        /// The event.
        event: VenueEvent,
    },

    /// What the venue says the account really holds.
    PositionsReported {
        /// Up to eight instruments, which is more than this deployment trades.
        /// A fixed array rather than a `Vec` keeps [`Command`] `Copy`, and a
        /// `Copy` command cannot be mutated between being queued and applied.
        positions: [Option<VenuePosition>; 8],
    },

    /// Look for orders the venue has stopped talking about.
    ///
    /// The liveness backstop for lost events. Every other reconcile trigger
    /// presumes an event *arrives*; a fill dropped in a stream gap produces no
    /// trigger at all, and only this finds the order it stranded.
    SweepStale,

    /// Stop trading.
    ///
    /// Blocks entries. Does **not** block protective exits: R1.13 says exits
    /// run under independent authority, and a halt that also disarms the stops
    /// turns a bookkeeping problem into an unbounded position.
    Halt {
        /// Why, for the operator who will read it in five days' time.
        note: Note,
    },

    /// Resume trading.
    Resume {
        /// Why.
        note: Note,
    },

    /// Record something that happened, or did not.
    Decide {
        /// Who decided it.
        ///
        /// Stated rather than assumed to be the gateway: a stance a strategy
        /// declared and a refusal the guards raised are both decisions, and a
        /// log that filed them under the same actor would answer "who stopped
        /// trading" with the name of the socket they arrived on.
        actor: sentinel_record::Actor,
        /// What it concerned.
        instrument: sentinel_types::Instrument,
        /// What was decided.
        kind: DecisionKind,
        /// The chain.
        trace_id: TraceId,
        /// A number the decision turned on.
        value: Money,
        /// For a person.
        note: Note,
    },
}

impl Command {
    /// Pack a position list into the fixed array [`Command::PositionsReported`]
    /// carries, dropping anything beyond the eighth.
    ///
    /// Dropping rather than growing is deliberate and bounded: this deployment
    /// trades one instrument, and a venue reporting more than eight is telling
    /// us something we should look at rather than something we should quietly
    /// resize for.
    #[must_use]
    pub fn positions_reported(positions: &[VenuePosition]) -> Self {
        let mut packed = [None; 8];
        for (slot, position) in packed.iter_mut().zip(positions) {
            *slot = Some(*position);
        }
        Self::PositionsReported { positions: packed }
    }
}

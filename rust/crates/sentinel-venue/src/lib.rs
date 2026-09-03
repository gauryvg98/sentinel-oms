//! The venue boundary, and a simulator behind it.
//!
//! **Nothing here is called by the engine.** That is the whole point. In the
//! system this replaces, the instrument lock was held across the venue's HTTP
//! round trip, so every inbound fill for a symbol queued behind a network call
//! and a convoy formed that never drained — measured at ten seconds to place an
//! order on an account trading one symbol.
//!
//! Here the engine emits a [`VenueRequest`], flushes, and moves on. An I/O
//! worker performs the request and posts the answer back as another command.
//! The writer never blocks on a network, and the venue never sees an order
//! whose intent is not already durable.
//!
//! The simulator is the first implementation, not a test double bolted on
//! afterwards. Every failure in R1 is scriptable through it, which is what
//! makes those scenarios reproducible rather than hoped for.

#![forbid(unsafe_code)]

mod sim;

pub use sim::{Fault, SimVenue};

use sentinel_domain::{EconomicOrderIntent, OrderState, RejectReason};
use sentinel_types::{ClientOrderId, ExecId, Instrument, Money, Nanos, Price, Qty, VenueOrderId};

/// What the venue says an order actually is.
///
/// The answer to a reconciliation, and the only thing allowed to overwrite what
/// we believe (R1.12).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderSnapshot {
    /// What the venue says its state is.
    pub state: OrderState,
    /// What the venue calls it.
    pub venue_order_id: Option<VenueOrderId>,
    /// What the venue says has filled.
    pub filled_qty: Qty,
    /// The average price of those fills, when anything has filled.
    ///
    /// Carried so a reconciliation that finds executions we missed can book
    /// them at what they actually cost. Without it the correction has to be
    /// priced at the last mark, which moves the position by the right quantity
    /// and the P&L by an estimate — and an estimate that looks like a
    /// measurement is worse than an obviously absent number.
    pub avg_price: Option<Price>,
}

/// How a submission ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// The venue acknowledged and named it.
    Acked(VenueOrderId),
    /// The venue refused it.
    Rejected(RejectReason),
    /// No answer, and no venue order id.
    ///
    /// Carries nothing, deliberately. There is no field here from which a
    /// caller could form an opinion about what the venue did — because it does
    /// not know, and neither do we (R1.3).
    TimedOut,
}

/// How a cancel request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The venue took the request. Confirmation arrives on the stream.
    Requested,
    /// No answer. The order may still fill, or may already be cancelled.
    TimedOut,
    /// The venue has never heard of it.
    Absent,
}

/// How a lookup ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupOutcome {
    /// The venue knows this order and says this about it.
    Found(OrderSnapshot),
    /// The venue has no such order.
    ///
    /// Conclusive, and therefore usable: the intent never became exposure. Any
    /// resubmission is a new order under policy, never a reuse of this one.
    Absent,
    /// No answer. Nothing is concluded and the order stays where it is.
    TimedOut,
}

/// Something the venue pushed at us.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueEvent {
    /// An execution.
    Fill {
        /// Which order.
        client_order_id: ClientOrderId,
        /// The venue's id for this execution — the dedup key.
        exec_id: ExecId,
        /// How much.
        qty: Qty,
        /// At what price.
        price: Price,
    },
    /// The venue killed everything conclusively remaining on an order.
    ///
    /// Not necessarily because we asked: self-trade prevention and exchange-side
    /// cancels arrive here too, and the lifecycle accepts both.
    CancelConfirmed {
        /// Which order.
        client_order_id: ClientOrderId,
    },
    /// A price.
    Mark {
        /// What was priced.
        instrument: Instrument,
        /// The price.
        price: Price,
    },
    /// An account balance.
    Balance {
        /// What is held.
        asset: sentinel_record::Asset,
        /// How much.
        amount: Money,
    },
}

/// Something the engine wants the venue asked.
///
/// Emitted by the engine and performed by an I/O worker, *after* the records
/// that authorised it are durable. A request that reached the venue before its
/// intent reached the disk would be exposure we could not prove we authorised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueRequest {
    /// Place this order.
    Submit(EconomicOrderIntent),
    /// Cancel this order.
    Cancel {
        /// Which order.
        client_order_id: ClientOrderId,
        /// What the venue calls it, when we know.
        venue_order_id: Option<VenueOrderId>,
    },
    /// Ask what this order really is.
    Lookup {
        /// Which order.
        client_order_id: ClientOrderId,
    },
    /// Ask what positions the account really holds.
    Positions,
}

/// Why a venue call could not be made at all.
#[derive(Debug, Clone)]
pub enum VenueError {
    /// The transport failed.
    Transport(String),
    /// The venue answered with something we cannot parse.
    Malformed(String),
    /// The venue refused our credentials.
    Unauthorized,
    /// The venue is rate limiting us.
    ///
    /// Distinct from a transport failure because the response is different:
    /// backing off helps, and retrying immediately makes it worse. The Python
    /// system's fixed 2-second reconcile retry did exactly that — every attempt
    /// worsening the throttling that caused the timeouts.
    RateLimited {
        /// How long the venue asked us to wait, when it said.
        retry_after: Option<Nanos>,
    },
}

impl core::fmt::Display for VenueError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(m) => write!(f, "transport: {m}"),
            Self::Malformed(m) => write!(f, "malformed venue response: {m}"),
            Self::Unauthorized => f.write_str("venue refused our credentials"),
            Self::RateLimited {
                retry_after: Some(t),
            } => {
                write!(f, "rate limited, retry after {t}ns")
            }
            Self::RateLimited { retry_after: None } => f.write_str("rate limited"),
        }
    }
}

impl std::error::Error for VenueError {}

impl VenueError {
    /// Whether waiting is the right response.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Transport(_) | Self::RateLimited { .. })
    }
}

/// What the account really holds, according to the venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenuePosition {
    /// Which instrument.
    pub instrument: Instrument,
    /// Signed size.
    pub qty: Qty,
    /// What the venue says it was opened at, when it says.
    ///
    /// Carried so a position adopted at first boot is booked at what it
    /// actually cost. Without it the cost basis would have to be guessed from
    /// the current mark, and every P&L number after that would inherit the
    /// guess.
    pub entry_price: Option<Price>,
}

/// A venue.
///
/// Blocking, because it is called from an I/O worker and not from the writer.
/// Making it `async` would push a runtime into every crate above it to describe
/// something none of them do.
pub trait Venue {
    /// Place an order.
    ///
    /// # Errors
    /// [`VenueError`] when the call could not be made. A call that *was* made
    /// and produced no answer is [`SubmitOutcome::TimedOut`], not an error —
    /// the distinction is the difference between retrying and reconciling.
    fn submit(&mut self, intent: &EconomicOrderIntent) -> Result<SubmitOutcome, VenueError>;

    /// Cancel an order.
    ///
    /// # Errors
    /// [`VenueError`] when the call could not be made.
    fn cancel(
        &mut self,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
    ) -> Result<CancelOutcome, VenueError>;

    /// Ask what an order really is, by *our* id for it.
    ///
    /// By client order id, always. The venue's own id is absent for exactly the
    /// orders that most need looking up — the ones whose submission produced no
    /// answer — so keying recovery on it would leave the important case
    /// unrecoverable.
    ///
    /// # Errors
    /// [`VenueError`] when the call could not be made.
    fn lookup(&mut self, client_order_id: ClientOrderId) -> Result<LookupOutcome, VenueError>;

    /// Ask what the account really holds.
    ///
    /// # Errors
    /// [`VenueError`] when the call could not be made.
    fn positions(&mut self) -> Result<Vec<VenuePosition>, VenueError>;

    /// Take everything the venue has pushed since the last call.
    ///
    /// Never blocks. The stream is filled by the transport on its own thread;
    /// this moves what has arrived.
    fn drain_events(&mut self, out: &mut Vec<VenueEvent>);
}

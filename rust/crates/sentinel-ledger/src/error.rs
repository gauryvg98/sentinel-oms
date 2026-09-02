//! What the book refuses.

use sentinel_domain::OrderState;
use sentinel_types::{ClientOrderId, Instrument, Seq};

/// A ledger result.
pub type Result<T> = core::result::Result<T, LedgerError>;

/// Something the book would not apply.
#[derive(Debug)]
pub enum LedgerError {
    /// The journal could not be read.
    Journal(sentinel_journal::JournalError),
    /// A record's bytes are not the record they claim to be.
    Codec {
        /// Where in the log.
        seq: Seq,
        /// What was wrong.
        source: sentinel_record::CodecError,
    },
    /// A lifecycle event for an order we have no intent for.
    ///
    /// Not survivable inside the fold: the intent is the authority for
    /// everything that follows it, so an event without one means the log is
    /// missing a record it must have. Live, the engine disowns a stray venue
    /// id instead — one orphan on the account must not take the fleet down —
    /// but that decision belongs upstream, not in the projection.
    UnknownOrder {
        /// The id the event carried.
        client_order_id: ClientOrderId,
    },
    /// The lifecycle refused the event.
    Transition {
        /// Which order.
        client_order_id: ClientOrderId,
        /// Why.
        source: sentinel_domain::TransitionError,
    },
    /// Position arithmetic left the representable range.
    PositionOverflow {
        /// Where.
        instrument: Instrument,
    },
    /// The record's own account of where the order was, or went, does not match
    /// what the transition function says.
    ///
    /// Which means the log was written by a different build of the state
    /// machine. Folding it anyway would produce a book that quietly disagrees
    /// with the one that wrote the log, and the disagreement would be about a
    /// position.
    StateDisagrees {
        /// Which order.
        client_order_id: ClientOrderId,
        /// What the record says.
        recorded: OrderState,
        /// What this build computes.
        held: OrderState,
    },
}

impl core::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Journal(e) => write!(f, "journal: {e}"),
            Self::Codec { seq, source } => write!(f, "record {seq}: {source}"),
            Self::UnknownOrder { client_order_id } => {
                write!(f, "{client_order_id}: no intent for this order")
            }
            Self::Transition {
                client_order_id,
                source,
            } => {
                write!(f, "{client_order_id}: {source}")
            }
            Self::PositionOverflow { instrument } => {
                write!(f, "{instrument}: position arithmetic overflowed")
            }
            Self::StateDisagrees {
                client_order_id,
                recorded,
                held,
            } => write!(
                f,
                "{client_order_id}: the log says {recorded}, this build says {held}"
            ),
        }
    }
}

impl std::error::Error for LedgerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(e) => Some(e),
            Self::Codec { source, .. } => Some(source),
            Self::Transition { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl LedgerError {
    /// Whether this means the account's accounting cannot be trusted, and
    /// trading must stop rather than continue on numbers already shown wrong.
    #[must_use]
    pub const fn is_integrity_violation(&self) -> bool {
        match self {
            Self::PositionOverflow { .. }
            | Self::UnknownOrder { .. }
            | Self::StateDisagrees { .. } => true,
            Self::Transition { source, .. } => source.is_integrity_violation(),
            _ => false,
        }
    }
}

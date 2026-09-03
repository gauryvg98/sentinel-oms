//! The order lifecycle, as a pure function.
//!
//! No I/O, no clock, no randomness, no allocation. Everything this system
//! enforces about how an order may move lives in [`transition`], and it can be
//! exercised in full before a journal, a venue or a network exists — which is
//! the only reason the failure scenarios are testable at all.
//!
//! Two states carry the design.
//!
//! [`OrderState::Unknown`] is where a submission goes when it timed out with no
//! venue order id. Its outcome is *unprovable*, so calling it rejected would be
//! a guess, and the retry that follows a guess is how a position gets doubled.
//! Its only exit is reconciliation.
//!
//! [`OrderState::Reconciling`] is where an order sits while the venue is being
//! asked what really happened. Evidence arriving during that window — a fill, a
//! cancel confirmation — is recorded and *not acted on*. Only
//! [`OrderEvent::ReconcileResolved`] concludes anything.

#![forbid(unsafe_code)]

mod error;
mod event;
mod intent;
mod order;
mod product;
mod state;
mod transition;

pub use error::{IntentError, TransitionError};
pub use event::{EventKind, OrderEvent, ReconcileCause, RejectReason};
pub use intent::EconomicOrderIntent;
pub use order::OrderCore;
pub use product::{
    ContractKind, Exercise, Expiration, ProductDefinition, ProductError, Settlement,
};
pub use state::OrderState;
pub use transition::transition;

#[cfg(test)]
mod scenarios;

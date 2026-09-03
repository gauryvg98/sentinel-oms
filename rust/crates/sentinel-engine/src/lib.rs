//! The single writer, and the guards in front of it.
//!
//! [`Engine`] owns the journal and the book. Commands go in, records and
//! [`VenueRequest`](sentinel_venue::VenueRequest)s come out, and the venue is
//! never called from inside — which is the structural fix for the convoy that
//! made the previous system take ten seconds to place an order.
//!
//! Everything here is synchronous and deterministic. There is no runtime, no
//! task and no clock: time arrives as [`Command::Tick`] and the venue's answers
//! arrive as commands like any other. That is what makes replay possible, and
//! it is why the scenario tests can drive a full failure chain without a
//! network, a thread or a sleep anywhere in them.

#![forbid(unsafe_code)]

mod command;
mod engine;
mod error;
mod guards;

pub use command::Command;
pub use engine::{Config, Engine};
pub use error::{EngineError, Result};
pub use guards::{Limits, Refusal, Verdict, evaluate};

#[cfg(test)]
mod tests;

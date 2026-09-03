//! The book, and the invariants over it.
//!
//! Everything the engine reads is here, in memory, folded from the journal.
//! There is no query, no connection pool and no transaction — applying a record
//! is a hash lookup and a struct update. That is the whole reason placing an
//! order stopped costing ten seconds: the system it replaces paid a Postgres
//! round trip, inside a transaction holding an advisory lock, for every one of
//! these updates.
//!
//! Rebuilding is the same fold run from the start of the log. It is not a
//! recovery path kept warm for emergencies — it is the only way a [`Book`] is
//! ever populated, so every test in this crate exercises it.

#![forbid(unsafe_code)]

mod book;
mod error;
mod invariants;
mod position;

pub use book::{Applied, Book, Fill, Order, affects_book};
pub use error::{LedgerError, Result};
pub use invariants::{Report, Violation, check, signed};
pub use position::{Position, PositionOverflow};

#[cfg(test)]
mod tests;

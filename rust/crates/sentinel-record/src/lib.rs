//! What the journal holds, and how it is laid out.
//!
//! [`sentinel_journal`] frames and checksums opaque bytes; this crate decides
//! what those bytes mean. The split is deliberate — durability and vocabulary
//! change for different reasons, and a new record kind should not be able to
//! touch the code that makes records durable.
//!
//! The layout is hand-written rather than derived. A derive that quietly
//! changes its field order or its integer encoding between versions
//! reinterprets every log already on disk, and this log is the system of
//! record. `testdata/journal-golden` pins the result.

#![forbid(unsafe_code)]

mod buf;
mod record;

pub use buf::{CodecError, Reader, Result, Writer};
pub use record::{Actor, Asset, DecisionKind, Note, Record};

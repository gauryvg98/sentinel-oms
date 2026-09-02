//! The system of record.
//!
//! An append-only log of length-prefixed, checksummed frames, written by one
//! process holding a `flock` and read by any number that do not. It replaces a
//! Postgres transaction per state transition with one `fsync` per *group* of
//! transitions, which is the whole of the performance story: framing a record
//! costs tens of nanoseconds, making it durable costs milliseconds, so the only
//! question is how many records share one barrier — and the answer improves as
//! load rises.
//!
//! `docs/rewrite/JOURNAL-FORMAT.md` specifies the bytes. Go reads them too, and
//! the golden file in `tests/` is what keeps the two implementations honest.

#![forbid(unsafe_code)]

mod crc32c;
mod error;
mod format;
mod reader;
mod segment;
mod writer;

pub use crc32c::{checksum, update};
pub use error::{JournalError, Result};
pub use format::{
    FRAME_HEADER_LEN, FormatError, FrameHeader, MAGIC, MAX_PAYLOAD, RecordKind, SEGMENT_HEADER_LEN,
    SEGMENT_TARGET_BYTES, SegmentHeader, VERSION, decode_frame_header, decode_segment_header,
    encode_frame_header, encode_segment_header, finish_frame_header,
};
pub use reader::{Cursor, Record, Tailer};
pub use segment::{ScanResult, list_segments, scan_segment, segment_name};
pub use sentinel_types::Seq;
pub use writer::{CURSOR_DIR, ClaimReport, Journal, Stats};

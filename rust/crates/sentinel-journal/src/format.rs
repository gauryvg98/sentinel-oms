//! The on-disk layout. `docs/rewrite/JOURNAL-FORMAT.md` is the specification;
//! this is the implementation of it, and the golden test in `tests/` is what
//! keeps the two from drifting apart.
//!
//! Everything is little-endian, and nothing is padded. A reader that has
//! consumed `len` payload bytes is positioned exactly at the next frame header.

use sentinel_types::{Epoch, Nanos, Seq, wire_enum};

/// `"SENTJRN"` and a format byte.
pub const MAGIC: [u8; 8] = *b"SENTJRN\x01";
/// Format version.
pub const VERSION: u32 = 2;
/// Bytes at the head of every segment file.
pub const SEGMENT_HEADER_LEN: usize = 32;
/// Bytes at the head of every record.
pub const FRAME_HEADER_LEN: usize = 36;
/// Largest payload a single record may carry.
///
/// A bound rather than a limit anyone should reach: the largest real record is
/// a few hundred bytes, so a length claiming more than this is a corrupt frame
/// rather than a big one, and refusing it early stops a reader allocating a
/// gigabyte because four bytes were garbage.
pub const MAX_PAYLOAD: usize = 1 << 20;
/// Segments roll once they pass this.
pub const SEGMENT_TARGET_BYTES: u64 = 64 << 20;

wire_enum! {
    /// What a record holds.
    RecordKind {
        /// A writer claimed the journal. Carries the new epoch.
        EpochClaimed = 1 => "EPOCH_CLAIMED",
        /// An authorised order, durable before the venue sees it (R1.1).
        IntentPersisted = 2 => "INTENT_PERSISTED",
        /// An order lifecycle transition.
        OrderEvent = 3 => "ORDER_EVENT",
        /// Why something happened, or did not.
        Decision = 4 => "DECISION",
        /// A price observation.
        Mark = 5 => "MARK",
        /// An account balance snapshot.
        Balance = 6 => "BALANCE",
        /// Time entering the engine.
        Ticked = 7 => "TICKED",
    }
}

/// Everything about a record except its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Position in the journal. Monotonic from 1, gap-free.
    pub seq: Seq,
    /// Which run of the writer produced it.
    pub epoch: Epoch,
    /// When it was appended.
    pub nanos: Nanos,
    /// What it holds.
    pub kind: RecordKind,
    /// Payload length in bytes.
    pub len: u32,
}

impl FrameHeader {
    /// Total bytes this frame occupies on disk.
    #[must_use]
    pub const fn frame_len(&self) -> usize {
        FRAME_HEADER_LEN + self.len as usize
    }
}

/// Write a segment header into `out`.
pub fn encode_segment_header(base_seq: Seq, created: Nanos, out: &mut [u8; SEGMENT_HEADER_LEN]) {
    out[0..8].copy_from_slice(&MAGIC);
    out[8..12].copy_from_slice(&VERSION.to_le_bytes());
    out[12..16].copy_from_slice(&0u32.to_le_bytes());
    out[16..24].copy_from_slice(&base_seq.as_u64().to_le_bytes());
    out[24..32].copy_from_slice(&created.as_u64().to_le_bytes());
}

/// What a segment header said.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Sequence of the first record in this segment.
    pub base_seq: Seq,
    /// When the segment was created.
    pub created: Nanos,
}

/// Read a segment header.
///
/// # Errors
/// [`FormatError`] when the magic or version does not match — this file is not
/// one of ours, or is from a format we do not speak.
pub fn decode_segment_header(
    bytes: &[u8; SEGMENT_HEADER_LEN],
) -> Result<SegmentHeader, FormatError> {
    if bytes[0..8] != MAGIC {
        return Err(FormatError::NotAJournal);
    }
    let version = u32::from_le_bytes(take4(bytes, 8));
    if version != VERSION {
        return Err(FormatError::UnsupportedVersion { found: version });
    }
    Ok(SegmentHeader {
        base_seq: Seq::from_u64(u64::from_le_bytes(take8(bytes, 16))),
        created: Nanos::from_u64(u64::from_le_bytes(take8(bytes, 24))),
    })
}

/// Write a frame header into `out`, leaving the checksum field zero.
///
/// The checksum covers bytes 8 onward — the header after the checksum itself,
/// and then the payload — so it cannot be computed until the header is laid
/// out. [`finish_frame_header`] fills it in.
pub fn encode_frame_header(header: &FrameHeader, out: &mut [u8; FRAME_HEADER_LEN]) {
    out[0..4].copy_from_slice(&header.len.to_le_bytes());
    out[4..8].copy_from_slice(&0u32.to_le_bytes());
    out[8..16].copy_from_slice(&header.seq.as_u64().to_le_bytes());
    out[16..24].copy_from_slice(&header.epoch.as_u64().to_le_bytes());
    out[24..32].copy_from_slice(&header.nanos.as_u64().to_le_bytes());
    out[32..34].copy_from_slice(&u16::from(header.kind.as_u8()).to_le_bytes());
    out[34..36].copy_from_slice(&0u16.to_le_bytes());
}

/// Compute and store the checksum over the laid-out header and its payload.
pub fn finish_frame_header(out: &mut [u8; FRAME_HEADER_LEN], payload: &[u8]) {
    let crc = crate::crc32c::update(crate::crc32c::checksum(&out[8..]), payload);
    out[4..8].copy_from_slice(&crc.to_le_bytes());
}

/// Read a frame header and verify it against its payload.
///
/// # Errors
/// [`FormatError`] when the length is impossible, the kind is not one we know,
/// or the checksum does not match. Every one of those means the frame is torn
/// or corrupt, and the caller stops rather than searching for the next one.
pub fn decode_frame_header(
    bytes: &[u8; FRAME_HEADER_LEN],
    payload: &[u8],
) -> Result<FrameHeader, FormatError> {
    let len = u32::from_le_bytes(take4(bytes, 0));
    if len as usize > MAX_PAYLOAD {
        return Err(FormatError::PayloadTooLarge { len });
    }
    if payload.len() != len as usize {
        return Err(FormatError::Truncated {
            want: len as usize,
            have: payload.len(),
        });
    }

    let stored = u32::from_le_bytes(take4(bytes, 4));
    let mut without_crc = *bytes;
    without_crc[4..8].copy_from_slice(&0u32.to_le_bytes());
    let computed = crate::crc32c::update(crate::crc32c::checksum(&without_crc[8..]), payload);
    if stored != computed {
        return Err(FormatError::ChecksumMismatch { stored, computed });
    }

    let kind_raw = u16::from_le_bytes(take2(bytes, 32));
    let kind = u8::try_from(kind_raw)
        .ok()
        .and_then(|k| RecordKind::from_u8(k).ok())
        .ok_or(FormatError::UnknownKind { kind: kind_raw })?;

    Ok(FrameHeader {
        seq: Seq::from_u64(u64::from_le_bytes(take8(bytes, 8))),
        epoch: Epoch::from_u64(u64::from_le_bytes(take8(bytes, 16))),
        nanos: Nanos::from_u64(u64::from_le_bytes(take8(bytes, 24))),
        kind,
        len,
    })
}

/// Why some bytes are not a valid frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatError {
    /// The segment magic is not ours.
    NotAJournal,
    /// A format we do not speak.
    UnsupportedVersion {
        /// What the file claimed.
        found: u32,
    },
    /// A length no real record has. Almost certainly a torn header.
    PayloadTooLarge {
        /// What the frame claimed.
        len: u32,
    },
    /// The frame claims more payload than the file holds.
    Truncated {
        /// Bytes claimed.
        want: usize,
        /// Bytes available.
        have: usize,
    },
    /// The bytes changed after they were written.
    ChecksumMismatch {
        /// What the frame carried.
        stored: u32,
        /// What the bytes actually sum to.
        computed: u32,
    },
    /// A record kind this build does not know, zero included.
    UnknownKind {
        /// The kind field as read.
        kind: u16,
    },
}

impl core::fmt::Display for FormatError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotAJournal => f.write_str("not a sentinel journal segment"),
            Self::UnsupportedVersion { found } => {
                write!(f, "journal format version {found}, expected {VERSION}")
            }
            Self::PayloadTooLarge { len } => {
                write!(
                    f,
                    "frame claims {len} payload bytes, over the {MAX_PAYLOAD} cap"
                )
            }
            Self::Truncated { want, have } => {
                write!(f, "frame claims {want} payload bytes, {have} available")
            }
            Self::ChecksumMismatch { stored, computed } => {
                write!(f, "checksum {stored:#010x} does not match {computed:#010x}")
            }
            Self::UnknownKind { kind } => write!(f, "record kind {kind} is not known"),
        }
    }
}

impl core::error::Error for FormatError {}

// Fixed-width reads. `copy_from_slice` on a known-size array panics on a length
// mismatch, and every call site here is at a constant offset inside an array
// whose size is also constant — so the panic is unreachable and the compiler
// can see it.
fn take2(bytes: &[u8], at: usize) -> [u8; 2] {
    let mut out = [0u8; 2];
    out.copy_from_slice(&bytes[at..at + 2]);
    out
}

fn take4(bytes: &[u8], at: usize) -> [u8; 4] {
    let mut out = [0u8; 4];
    out.copy_from_slice(&bytes[at..at + 4]);
    out
}

fn take8(bytes: &[u8], at: usize) -> [u8; 8] {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[at..at + 8]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> FrameHeader {
        FrameHeader {
            seq: Seq::from_u64(5_698),
            epoch: Epoch::from_u64(12),
            nanos: Nanos::from_u64(1_787_761_830_475_726_400),
            kind: RecordKind::OrderEvent,
            len: 11,
        }
    }

    fn round_trip(h: &FrameHeader, payload: &[u8]) -> Result<FrameHeader, FormatError> {
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        encode_frame_header(h, &mut bytes);
        finish_frame_header(&mut bytes, payload);
        decode_frame_header(&bytes, payload)
    }

    #[test]
    fn a_frame_header_round_trips() {
        let h = header();
        assert_eq!(round_trip(&h, b"hello world").unwrap(), h);
    }

    #[test]
    fn a_segment_header_round_trips() {
        let mut bytes = [0u8; SEGMENT_HEADER_LEN];
        encode_segment_header(Seq::from_u64(104_729), Nanos::from_u64(42), &mut bytes);
        let decoded = decode_segment_header(&bytes).unwrap();
        assert_eq!(decoded.base_seq, Seq::from_u64(104_729));
        assert_eq!(decoded.created, Nanos::from_u64(42));
    }

    #[test]
    fn the_magic_and_version_are_pinned() {
        // These bytes identify a file that is the system of record. Changing
        // them reinterprets every journal already written.
        assert_eq!(&MAGIC, b"SENTJRN\x01");
        assert_eq!(VERSION, 2);
        assert_eq!(SEGMENT_HEADER_LEN, 32);
        assert_eq!(FRAME_HEADER_LEN, 36);
    }

    #[test]
    fn a_foreign_file_is_refused() {
        let mut bytes = [0u8; SEGMENT_HEADER_LEN];
        encode_segment_header(Seq::ZERO, Nanos::ZERO, &mut bytes);
        bytes[3] = b'X';
        assert_eq!(decode_segment_header(&bytes), Err(FormatError::NotAJournal));
    }

    #[test]
    fn a_future_format_is_refused_rather_than_guessed_at() {
        let mut bytes = [0u8; SEGMENT_HEADER_LEN];
        encode_segment_header(Seq::ZERO, Nanos::ZERO, &mut bytes);
        // Any version but ours, in either direction: a v1 segment cannot be
        // read as v2 either, because the byte v2 added is exactly the one a v1
        // record cannot supply.
        for other in [VERSION - 1, VERSION + 1] {
            bytes[8..12].copy_from_slice(&other.to_le_bytes());
            assert_eq!(
                decode_segment_header(&bytes),
                Err(FormatError::UnsupportedVersion { found: other })
            );
        }
    }

    #[test]
    fn any_flipped_bit_in_the_header_is_caught() {
        let h = header();
        let payload = b"hello world";
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        encode_frame_header(&h, &mut bytes);
        finish_frame_header(&mut bytes, payload);

        // Every bit outside the length field, which fails its own check first.
        for i in 4..FRAME_HEADER_LEN {
            for bit in 0..8u8 {
                let mut corrupt = bytes;
                corrupt[i] ^= 1 << bit;
                assert!(
                    decode_frame_header(&corrupt, payload).is_err(),
                    "byte {i} bit {bit} went unnoticed"
                );
            }
        }
    }

    #[test]
    fn any_flipped_bit_in_the_payload_is_caught() {
        let h = header();
        let mut payload = *b"hello world";
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        encode_frame_header(&h, &mut bytes);
        finish_frame_header(&mut bytes, &payload);

        for i in 0..payload.len() {
            payload[i] ^= 0b0100_0000;
            assert!(
                matches!(
                    decode_frame_header(&bytes, &payload),
                    Err(FormatError::ChecksumMismatch { .. })
                ),
                "payload byte {i} went unnoticed"
            );
            payload[i] ^= 0b0100_0000;
        }
    }

    #[test]
    fn a_kind_byte_that_was_never_written_is_refused() {
        let h = FrameHeader { len: 0, ..header() };
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        encode_frame_header(&h, &mut bytes);
        bytes[32..34].copy_from_slice(&0u16.to_le_bytes());
        finish_frame_header(&mut bytes, b"");
        assert_eq!(
            decode_frame_header(&bytes, b""),
            Err(FormatError::UnknownKind { kind: 0 })
        );
    }

    #[test]
    fn an_impossible_length_is_refused_before_anything_is_allocated() {
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        bytes[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            decode_frame_header(&bytes, b""),
            Err(FormatError::PayloadTooLarge { len: u32::MAX })
        );
    }

    #[test]
    fn a_short_payload_is_reported_as_truncation() {
        let h = header();
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        encode_frame_header(&h, &mut bytes);
        finish_frame_header(&mut bytes, b"hello world");
        assert_eq!(
            decode_frame_header(&bytes, b"hello"),
            Err(FormatError::Truncated { want: 11, have: 5 })
        );
    }

    #[test]
    fn an_empty_payload_is_a_valid_frame() {
        let h = FrameHeader {
            kind: RecordKind::Ticked,
            len: 0,
            ..header()
        };
        assert_eq!(round_trip(&h, b"").unwrap(), h);
    }

    #[test]
    fn frame_length_accounts_for_the_header() {
        assert_eq!(header().frame_len(), FRAME_HEADER_LEN + 11);
    }
}

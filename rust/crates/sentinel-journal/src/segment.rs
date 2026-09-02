//! Segment files: naming, listing, and the scan that finds a torn tail.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use sentinel_types::{Epoch, Nanos, Seq};

use crate::error::{JournalError, Result};
use crate::format::{
    FRAME_HEADER_LEN, FrameHeader, SEGMENT_HEADER_LEN, decode_frame_header, decode_segment_header,
    encode_segment_header,
};

/// The extension every segment carries.
pub const SEGMENT_EXT: &str = "jrnl";

/// A segment's file name: its base sequence in sixteen hex digits.
///
/// Fixed width so lexical order is numeric order, which is what makes listing a
/// directory enough to know the segment order without opening anything.
#[must_use]
pub fn segment_name(base_seq: Seq) -> String {
    format!("{:016x}.{SEGMENT_EXT}", base_seq.as_u64())
}

/// Parse a base sequence back out of a file name.
fn parse_segment_name(name: &str) -> Option<Seq> {
    let stem = name.strip_suffix(&format!(".{SEGMENT_EXT}"))?;
    if stem.len() != 16 {
        return None;
    }
    u64::from_str_radix(stem, 16).ok().map(Seq::from_u64)
}

/// Every segment in `dir`, in sequence order.
///
/// # Errors
/// [`JournalError::Io`] when the directory cannot be read.
pub fn list_segments(dir: &Path) -> Result<Vec<(Seq, PathBuf)>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|e| JournalError::io("read journal dir", e))? {
        let entry = entry.map_err(|e| JournalError::io("read journal dir entry", e))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(seq) = parse_segment_name(name) {
            found.push((seq, entry.path()));
        }
    }
    found.sort_unstable_by_key(|(seq, _)| seq.as_u64());
    Ok(found)
}

/// Create a segment file and write its header.
///
/// # Errors
/// [`JournalError::Io`] when the file cannot be created or written.
pub fn create_segment(dir: &Path, base_seq: Seq, created: Nanos) -> Result<File> {
    let path = dir.join(segment_name(base_seq));
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(&path)
        .map_err(|e| JournalError::io("create segment", e))?;
    let mut header = [0u8; SEGMENT_HEADER_LEN];
    encode_segment_header(base_seq, created, &mut header);
    std::io::Write::write_all(&mut file, &header)
        .map_err(|e| JournalError::io("write segment header", e))?;
    Ok(file)
}

/// What a scan of a segment found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanResult {
    /// Sequence of the last intact record, or `Seq::ZERO` if the segment holds
    /// only its header.
    pub last_seq: Seq,
    /// Epoch of the last intact record.
    pub last_epoch: Epoch,
    /// Byte offset just past the last intact record. Anything beyond this is a
    /// torn tail and is truncated on claim.
    pub good_end: u64,
    /// Whether bytes were found past `good_end`.
    pub torn: bool,
    /// Records read.
    pub records: u64,
}

/// Scan a segment for its last intact record.
///
/// Stops at the first frame that is short, over-long, or fails its checksum,
/// and never searches forward for the next plausible frame. A log with a hole
/// in it is not a log, and stepping over one silently is how a replay stops
/// reproducing the run it exists to reproduce.
///
/// # Errors
/// [`JournalError::Format`] when the file is not a segment of a version we
/// speak, or [`JournalError::Io`] on a read failure. A torn tail is *not* an
/// error — it is the expected shape of a crash, and it is reported in
/// [`ScanResult::torn`].
pub fn scan_segment(path: &Path) -> Result<ScanResult> {
    let mut file = File::open(path).map_err(|e| JournalError::io("open segment", e))?;
    let size = file
        .metadata()
        .map_err(|e| JournalError::io("stat segment", e))?
        .len();

    let mut segment_header = [0u8; SEGMENT_HEADER_LEN];
    file.read_exact(&mut segment_header)
        .map_err(|e| JournalError::io("read segment header", e))?;
    let segment = decode_segment_header(&segment_header).map_err(JournalError::Format)?;

    let mut offset = SEGMENT_HEADER_LEN as u64;
    let mut last_seq = Seq::ZERO;
    let mut last_epoch = Epoch::GENESIS;
    let mut records = 0u64;
    let mut expected_seq = segment.base_seq;

    let mut frame_header = [0u8; FRAME_HEADER_LEN];
    let mut payload = Vec::new();

    loop {
        if size.saturating_sub(offset) < FRAME_HEADER_LEN as u64 {
            break;
        }
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| JournalError::io("seek segment", e))?;
        if file.read_exact(&mut frame_header).is_err() {
            break;
        }
        let claimed = u32::from_le_bytes([
            frame_header[0],
            frame_header[1],
            frame_header[2],
            frame_header[3],
        ]);
        let frame_end = offset + FRAME_HEADER_LEN as u64 + u64::from(claimed);
        if u64::from(claimed) > crate::format::MAX_PAYLOAD as u64 || frame_end > size {
            break; // torn: the frame runs past what was actually written
        }
        payload.resize(claimed as usize, 0);
        if file.read_exact(&mut payload).is_err() {
            break;
        }
        let Ok(header) = decode_frame_header(&frame_header, &payload) else {
            break; // torn or corrupt: stop here, do not hunt forward
        };
        // A sequence that jumps is a hole, and a hole is not something to read
        // past. This is the check that catches a segment that was concatenated,
        // reordered, or written by two epochs interleaving.
        if header.seq != expected_seq {
            return Err(JournalError::SequenceGap {
                expected: expected_seq,
                found: header.seq,
            });
        }
        last_seq = header.seq;
        last_epoch = header.epoch;
        expected_seq = header.seq.next();
        records += 1;
        offset = frame_end;
    }

    Ok(ScanResult {
        last_seq,
        last_epoch,
        good_end: offset,
        torn: offset < size,
        records,
    })
}

/// Read one frame starting at `offset`.
///
/// Returns `Ok(None)` when the bytes there are not yet a whole frame, which
/// during live tailing means "the writer has not flushed this far", not
/// "damaged".
///
/// # Errors
/// [`JournalError::Format`] when a complete-looking frame fails its checks, or
/// [`JournalError::Io`] on a read failure.
pub fn read_frame_at(
    file: &mut File,
    offset: u64,
    size: u64,
    payload: &mut Vec<u8>,
) -> Result<Option<FrameHeader>> {
    if size.saturating_sub(offset) < FRAME_HEADER_LEN as u64 {
        return Ok(None);
    }
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| JournalError::io("seek segment", e))?;
    let mut frame_header = [0u8; FRAME_HEADER_LEN];
    if file.read_exact(&mut frame_header).is_err() {
        return Ok(None);
    }
    let claimed = u32::from_le_bytes([
        frame_header[0],
        frame_header[1],
        frame_header[2],
        frame_header[3],
    ]);
    if u64::from(claimed) > crate::format::MAX_PAYLOAD as u64 {
        return Err(JournalError::Format(
            crate::format::FormatError::PayloadTooLarge { len: claimed },
        ));
    }
    if offset + FRAME_HEADER_LEN as u64 + u64::from(claimed) > size {
        return Ok(None);
    }
    payload.resize(claimed as usize, 0);
    if file.read_exact(payload).is_err() {
        return Ok(None);
    }
    decode_frame_header(&frame_header, payload)
        .map(Some)
        .map_err(JournalError::Format)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_sort_lexically_in_sequence_order() {
        let mut names = [
            segment_name(Seq::from_u64(104_729)),
            segment_name(Seq::from_u64(1)),
            segment_name(Seq::from_u64(u64::MAX)),
            segment_name(Seq::from_u64(16)),
        ];
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "0000000000000001.jrnl",
                "0000000000000010.jrnl",
                "0000000000019919.jrnl",
                "ffffffffffffffff.jrnl",
            ]
        );
    }

    #[test]
    fn names_round_trip() {
        for seq in [1u64, 16, 104_729, u64::MAX] {
            let name = segment_name(Seq::from_u64(seq));
            assert_eq!(parse_segment_name(&name), Some(Seq::from_u64(seq)));
        }
    }

    #[test]
    fn unrelated_files_are_not_segments() {
        assert_eq!(parse_segment_name("LOCK"), None);
        assert_eq!(parse_segment_name("cursors"), None);
        assert_eq!(parse_segment_name("1.jrnl"), None, "wrong width");
        assert_eq!(parse_segment_name("0000000000000001.log"), None);
        assert_eq!(parse_segment_name("zzzzzzzzzzzzzzzz.jrnl"), None);
    }
}

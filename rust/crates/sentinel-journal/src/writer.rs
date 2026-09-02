//! The writer: claim, append, flush.
//!
//! Three properties, in the order they matter.
//!
//! **One writer.** Claiming takes an exclusive `flock` on `LOCK` and holds it
//! for the process's life. A second process is refused. The kernel releases it
//! when the holder exits, however it exits — which is the whole reason this is
//! a file lock and not a database advisory lock. Every record also carries the
//! writer's epoch, so an interleave would be *visible in the log* rather than
//! merely improbable.
//!
//! **Apply, then commit, then acknowledge.** `append` writes to a buffer;
//! `flush` reaches the disk. Nothing is acknowledged before its records are
//! durable, and a crash between the two loses records that were never acked.
//!
//! **Group commit.** `flush` writes everything buffered since the last one with
//! a single `fsync`. Framing a record costs tens of nanoseconds; making it
//! durable costs milliseconds — so the only performance question this module
//! has is how many records share one barrier, and the answer improves as load
//! rises. That is the property the Postgres-per-transition design did not have.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use sentinel_types::{Epoch, Nanos, Seq};

use crate::error::{JournalError, Result};
use crate::format::{
    FRAME_HEADER_LEN, FrameHeader, MAX_PAYLOAD, RecordKind, SEGMENT_HEADER_LEN,
    SEGMENT_TARGET_BYTES, encode_frame_header, finish_frame_header,
};
use crate::segment;

/// The lock file name.
const LOCK_FILE: &str = "LOCK";
/// Where consumer cursors live.
pub const CURSOR_DIR: &str = "cursors";

/// What claiming the journal found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimReport {
    /// The epoch this writer runs under.
    pub epoch: Epoch,
    /// The last sequence already durable when we arrived.
    pub resumed_from: Seq,
    /// Records already in the journal.
    pub existing_records: u64,
    /// Bytes truncated from a torn tail left by a crash.
    pub truncated_bytes: u64,
}

/// Counters worth watching. Cheap enough to update on every record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Records appended by this writer.
    pub records: u64,
    /// Payload bytes appended.
    pub payload_bytes: u64,
    /// Flushes performed — that is, `fsync` calls.
    pub flushes: u64,
    /// Segments rolled.
    pub segments_rolled: u64,
}

impl Stats {
    /// Mean records per `fsync`.
    ///
    /// The one number that says whether group commit is engaging. A value
    /// pinned near 1.0 under load means the writer is blocking on the disk
    /// between records and the batch never forms — which is exactly what the
    /// Python system did, at 350 orders a second regardless of concurrency.
    #[must_use]
    pub fn records_per_flush(&self) -> f64 {
        if self.flushes == 0 {
            return 0.0;
        }
        #[expect(
            clippy::cast_precision_loss,
            clippy::float_arithmetic,
            reason = "a diagnostic ratio for a human to read, never an input to \
                      a decision; f64 holds both counters exactly at any rate \
                      this system reaches"
        )]
        let ratio = self.records as f64 / self.flushes as f64;
        ratio
    }
}

/// The single writer.
#[derive(Debug)]
pub struct Journal {
    dir: PathBuf,
    /// Held open for the writer's life. Dropping it releases the claim, so it
    /// is never touched after construction and never handed out.
    _lock: File,
    epoch: Epoch,
    next_seq: Seq,
    segment_file: File,
    segment_base: Seq,
    segment_bytes: u64,
    /// Records appended and not yet durable.
    buffer: Vec<u8>,
    buffered_records: u32,
    /// Set by the first failed flush, and never cleared.
    poisoned: Option<&'static str>,
    stats: Stats,
    claim_report: ClaimReport,
}

impl Journal {
    /// Claim the journal, recovering whatever is there.
    ///
    /// Truncates a torn tail left by a crash, bumps the epoch, and records the
    /// claim as the first entry of this run — so the log itself says when each
    /// writer took over.
    ///
    /// # Errors
    /// [`JournalError::AlreadyClaimed`] when another process holds it,
    /// [`JournalError::SequenceGap`] when the existing log has a hole, or
    /// [`JournalError::Io`] / [`JournalError::Format`] when the directory
    /// cannot be read or is not a journal.
    pub fn claim(dir: impl AsRef<Path>, now: Nanos) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| JournalError::io("create journal dir", e))?;
        std::fs::create_dir_all(dir.join(CURSOR_DIR))
            .map_err(|e| JournalError::io("create cursor dir", e))?;

        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(LOCK_FILE))
            .map_err(|e| JournalError::io("open journal lock", e))?;
        lock.try_lock()
            .map_err(|_| JournalError::AlreadyClaimed { dir: dir.clone() })?;

        let segments = segment::list_segments(&dir)?;
        let mut truncated_bytes = 0u64;
        let mut existing_records = 0u64;
        let mut last_seq = Seq::ZERO;
        let mut last_epoch = Epoch::GENESIS;

        // Every segment but the last must be intact: a torn tail can only be at
        // the end of the log, and one anywhere else is a hole.
        for (index, (_, path)) in segments.iter().enumerate() {
            let scan = segment::scan_segment(path)?;
            existing_records += scan.records;
            if scan.records > 0 {
                last_seq = scan.last_seq;
                last_epoch = scan.last_epoch;
            }
            if scan.torn {
                if index + 1 != segments.len() {
                    return Err(JournalError::SequenceGap {
                        expected: scan.last_seq.next(),
                        found: scan.last_seq,
                    });
                }
                let file = OpenOptions::new()
                    .write(true)
                    .open(path)
                    .map_err(|e| JournalError::io("open segment to truncate", e))?;
                let size = file
                    .metadata()
                    .map_err(|e| JournalError::io("stat segment", e))?
                    .len();
                truncated_bytes = size - scan.good_end;
                file.set_len(scan.good_end)
                    .map_err(|e| JournalError::io("truncate torn tail", e))?;
                file.sync_all()
                    .map_err(|e| JournalError::io("sync truncation", e))?;
            }
        }

        let epoch = last_epoch.next();
        let next_seq = last_seq.next();

        // Reopen the last segment for appending, or start the first one.
        let (segment_file, segment_base, segment_bytes) = match segments.last() {
            Some((base, path)) => {
                let file = OpenOptions::new()
                    .append(true)
                    .read(true)
                    .open(path)
                    .map_err(|e| JournalError::io("open segment for append", e))?;
                let size = file
                    .metadata()
                    .map_err(|e| JournalError::io("stat segment", e))?
                    .len();
                (file, *base, size)
            }
            None => {
                let file = segment::create_segment(&dir, next_seq, now)?;
                (file, next_seq, SEGMENT_HEADER_LEN as u64)
            }
        };

        let mut journal = Self {
            dir,
            _lock: lock,
            epoch,
            next_seq,
            segment_file,
            segment_base,
            segment_bytes,
            buffer: Vec::with_capacity(64 * 1024),
            buffered_records: 0,
            poisoned: None,
            stats: Stats::default(),
            claim_report: ClaimReport {
                epoch,
                resumed_from: last_seq,
                existing_records,
                truncated_bytes,
            },
        };

        // The claim is the first record of the run. A log that says which epoch
        // wrote what needs the epoch boundary to be in the log.
        let mut payload = [0u8; 16];
        payload[0..8].copy_from_slice(&epoch.as_u64().to_le_bytes());
        payload[8..16].copy_from_slice(&last_seq.as_u64().to_le_bytes());
        journal.append(RecordKind::EpochClaimed, now, &payload)?;
        journal.flush()?;

        // The claim record is bookkeeping, not work. Counting it would put a
        // permanent 1 in the numerator of `records_per_flush`, which is the one
        // number this struct exists to report honestly.
        Ok(Self {
            stats: Stats::default(),
            ..journal
        })
    }

    /// This writer's epoch.
    #[must_use]
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The sequence the next appended record will get.
    #[must_use]
    pub const fn next_seq(&self) -> Seq {
        self.next_seq
    }

    /// The last sequence made durable.
    #[must_use]
    pub fn durable_seq(&self) -> Seq {
        let pending = u64::from(self.buffered_records) + 1;
        Seq::from_u64(self.next_seq.as_u64().saturating_sub(pending))
    }

    /// What claiming the journal found: the epoch, where it resumed, and how
    /// much of a torn tail a previous crash left behind.
    #[must_use]
    pub const fn claim_report(&self) -> ClaimReport {
        self.claim_report
    }

    /// Counters.
    #[must_use]
    pub const fn stats(&self) -> Stats {
        self.stats
    }

    /// The journal directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Records appended and not yet durable.
    #[must_use]
    pub const fn buffered(&self) -> u32 {
        self.buffered_records
    }

    /// Append a record. It is durable only after [`Self::flush`].
    ///
    /// # Errors
    /// [`JournalError::Poisoned`] once a flush has failed,
    /// [`JournalError::PayloadTooLarge`] for a payload over the cap.
    pub fn append(&mut self, kind: RecordKind, now: Nanos, payload: &[u8]) -> Result<Seq> {
        if let Some(doing) = self.poisoned {
            return Err(JournalError::Poisoned { doing });
        }
        if payload.len() > MAX_PAYLOAD {
            return Err(JournalError::PayloadTooLarge { len: payload.len() });
        }
        let len = u32::try_from(payload.len())
            .map_err(|_| JournalError::PayloadTooLarge { len: payload.len() })?;

        let seq = self.next_seq;
        let header = FrameHeader {
            seq,
            epoch: self.epoch,
            nanos: now,
            kind,
            len,
        };
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        encode_frame_header(&header, &mut bytes);
        finish_frame_header(&mut bytes, payload);

        self.buffer.extend_from_slice(&bytes);
        self.buffer.extend_from_slice(payload);
        self.buffered_records += 1;
        self.next_seq = seq.next();
        self.stats.records += 1;
        self.stats.payload_bytes += u64::from(len);
        Ok(seq)
    }

    /// Make everything appended since the last flush durable.
    ///
    /// One `write` and one `fsync` for the whole group, which is what makes the
    /// per-record cost fall as the batch grows.
    ///
    /// # Errors
    /// [`JournalError::Poisoned`] if a previous flush failed. A failure here
    /// poisons the journal permanently: memory is ahead of a log still being
    /// served from, and the only safe move is for the process to restart.
    pub fn flush(&mut self) -> Result<Seq> {
        if let Some(doing) = self.poisoned {
            return Err(JournalError::Poisoned { doing });
        }
        if self.buffer.is_empty() {
            return Ok(self.durable_seq());
        }

        if let Err(e) = self.segment_file.write_all(&self.buffer) {
            return Err(self.poison("write records", e));
        }
        // `sync_data` rather than `sync_all`: the file's length is data as far
        // as durability is concerned, and the timestamps are not. On macOS this
        // still issues a full barrier; on Linux it is `fdatasync`.
        if let Err(e) = self.segment_file.sync_data() {
            return Err(self.poison("sync records", e));
        }

        self.segment_bytes += self.buffer.len() as u64;
        self.buffer.clear();
        self.buffered_records = 0;
        self.stats.flushes += 1;

        // Roll between groups, never inside one, so no frame straddles a
        // segment boundary and a reader never has to stitch one together.
        if self.segment_bytes >= SEGMENT_TARGET_BYTES {
            self.roll()?;
        }
        Ok(self.durable_seq())
    }

    /// Append one record and make it durable. For callers with nothing to batch.
    ///
    /// # Errors
    /// As [`Self::append`] and [`Self::flush`].
    pub fn append_and_flush(
        &mut self,
        kind: RecordKind,
        now: Nanos,
        payload: &[u8],
    ) -> Result<Seq> {
        let seq = self.append(kind, now, payload)?;
        self.flush()?;
        Ok(seq)
    }

    fn roll(&mut self) -> Result<()> {
        let now = Nanos::ZERO;
        let file = segment::create_segment(&self.dir, self.next_seq, now)?;
        self.segment_file = file;
        self.segment_base = self.next_seq;
        self.segment_bytes = SEGMENT_HEADER_LEN as u64;
        self.stats.segments_rolled += 1;
        Ok(())
    }

    fn poison(&mut self, doing: &'static str, source: std::io::Error) -> JournalError {
        self.poisoned = Some(doing);
        JournalError::Io { doing, source }
    }

    /// Whether a flush has failed and the process must restart.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::Tailer;

    /// A scratch directory that removes itself.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let base =
                std::env::temp_dir().join(format!("sentinel-journal-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&base);
            std::fs::create_dir_all(&base).unwrap();
            Self(base)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn at(n: u64) -> Nanos {
        Nanos::from_u64(n)
    }

    #[test]
    fn a_fresh_journal_starts_at_epoch_one_and_records_its_claim() {
        let dir = TempDir::new("fresh");
        let j = Journal::claim(dir.path(), at(1)).unwrap();
        assert_eq!(j.epoch(), Epoch::from_u64(1));
        // The claim itself is record 1, so the next append is 2.
        assert_eq!(j.next_seq(), Seq::from_u64(2));
        assert_eq!(j.durable_seq(), Seq::from_u64(1));
    }

    #[test]
    fn a_second_writer_is_refused_while_the_first_holds_it() {
        let dir = TempDir::new("two-writers");
        let _first = Journal::claim(dir.path(), at(1)).unwrap();
        let second = Journal::claim(dir.path(), at(2));
        assert!(matches!(second, Err(JournalError::AlreadyClaimed { .. })));
    }

    #[test]
    fn the_claim_is_released_when_the_writer_is_dropped() {
        let dir = TempDir::new("release");
        {
            let _first = Journal::claim(dir.path(), at(1)).unwrap();
        }
        // No manual cleanup, no stale lock: the successor just takes it.
        let second = Journal::claim(dir.path(), at(2)).unwrap();
        assert_eq!(second.epoch(), Epoch::from_u64(2), "the epoch advances");
    }

    #[test]
    fn records_are_invisible_until_flushed() {
        let dir = TempDir::new("visibility");
        let mut j = Journal::claim(dir.path(), at(1)).unwrap();
        j.append(RecordKind::Ticked, at(2), b"pending").unwrap();

        let mut tailer = Tailer::open(dir.path(), Seq::ZERO).unwrap();
        let mut seen = 0;
        while tailer.next_record().unwrap().is_some() {
            seen += 1;
        }
        assert_eq!(seen, 1, "only the claim is durable");

        j.flush().unwrap();
        assert!(tailer.next_record().unwrap().is_some(), "now it is there");
    }

    #[test]
    fn one_flush_covers_the_whole_group() {
        let dir = TempDir::new("group-commit");
        let mut j = Journal::claim(dir.path(), at(1)).unwrap();
        for i in 0..32u64 {
            j.append(RecordKind::Ticked, at(i), &i.to_le_bytes())
                .unwrap();
        }
        assert_eq!(j.buffered(), 32);
        j.flush().unwrap();

        // One fsync for thirty-two records. The claim's own flush is not
        // counted in `records`, which is reset after it.
        assert_eq!(j.stats().records, 32);
        assert_eq!(j.stats().flushes, 1);
        assert!(j.stats().records_per_flush() >= 32.0);
    }

    #[test]
    fn sequences_are_gap_free_across_a_restart() {
        let dir = TempDir::new("restart-seq");
        let last = {
            let mut j = Journal::claim(dir.path(), at(1)).unwrap();
            for i in 0..5u64 {
                j.append(RecordKind::Ticked, at(i), &i.to_le_bytes())
                    .unwrap();
            }
            j.flush().unwrap()
        };
        let j = Journal::claim(dir.path(), at(100)).unwrap();
        // The successor's own claim record occupies the next sequence.
        assert_eq!(j.durable_seq(), last.next());
        assert_eq!(j.epoch(), Epoch::from_u64(2));
    }

    #[test]
    fn a_torn_tail_is_truncated_and_the_rest_survives() {
        let dir = TempDir::new("torn");
        {
            let mut j = Journal::claim(dir.path(), at(1)).unwrap();
            for i in 0..4u64 {
                j.append(RecordKind::Ticked, at(i), b"payload").unwrap();
            }
            j.flush().unwrap();
        }

        // Simulate a crash mid-append: half a frame at the end.
        let (_, path) = segment::list_segments(dir.path()).unwrap().pop().unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0xAB; 20]);
        std::fs::write(&path, &bytes).unwrap();

        let j = Journal::claim(dir.path(), at(50)).unwrap();
        assert_eq!(j.claim_report().truncated_bytes, 20);
        assert_eq!(
            j.claim_report().existing_records,
            5,
            "the claim plus four ticks"
        );
        assert_eq!(j.claim_report().resumed_from, Seq::from_u64(5));

        // Everything written before the crash is still readable, in order, and
        // the successor's claim is appended after it rather than over it.
        let mut tailer = Tailer::open(dir.path(), Seq::ZERO).unwrap();
        let mut kinds = Vec::new();
        while let Some(r) = tailer.next_record().unwrap() {
            kinds.push(r.header.kind);
        }
        assert_eq!(kinds[0], RecordKind::EpochClaimed);
        assert_eq!(kinds[1..5], [RecordKind::Ticked; 4]);
        assert_eq!(kinds[5], RecordKind::EpochClaimed, "the new writer's claim");
        assert_eq!(kinds.len(), 6);
        let _ = j;
    }

    #[test]
    fn a_payload_over_the_cap_is_refused_rather_than_written() {
        let dir = TempDir::new("oversize");
        let mut j = Journal::claim(dir.path(), at(1)).unwrap();
        let huge = vec![0u8; MAX_PAYLOAD + 1];
        assert!(matches!(
            j.append(RecordKind::Decision, at(2), &huge),
            Err(JournalError::PayloadTooLarge { .. })
        ));
        // And the journal is still usable.
        j.append(RecordKind::Ticked, at(3), b"fine").unwrap();
        j.flush().unwrap();
    }

    #[test]
    fn flushing_nothing_is_a_no_op() {
        let dir = TempDir::new("empty-flush");
        let mut j = Journal::claim(dir.path(), at(1)).unwrap();
        let before = j.stats().flushes;
        j.flush().unwrap();
        assert_eq!(j.stats().flushes, before);
    }

    #[test]
    fn an_empty_payload_is_a_valid_record() {
        let dir = TempDir::new("empty-payload");
        let mut j = Journal::claim(dir.path(), at(1)).unwrap();
        let seq = j.append_and_flush(RecordKind::Ticked, at(2), b"").unwrap();
        assert_eq!(seq, Seq::from_u64(2));
    }
}

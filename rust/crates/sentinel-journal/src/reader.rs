//! Reading the journal, and following it as it grows.
//!
//! The journal is the fan-out. It is an ordered log with an independent cursor
//! per consumer, which is the abstraction a message broker sells — so nothing
//! is introduced to sit in the middle and provide something that already
//! exists. `gatewayd` tails it for the browser; `projectord` tails it for
//! Supabase; neither can slow the writer down, because neither touches it.

use std::fs::File;
use std::path::{Path, PathBuf};

use sentinel_types::Seq;

use crate::error::{JournalError, Result};
use crate::format::FrameHeader;
use crate::segment;

/// One record, read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Sequence, epoch, time, kind and length.
    pub header: FrameHeader,
    /// The kind-specific bytes.
    pub payload: Vec<u8>,
}

/// A cursor over the journal that follows the writer.
///
/// [`Tailer::next_record`] returns `Ok(None)` when it has caught up — not an error and
/// not the end, just everything durable so far. Callers block on a notification
/// and ask again; nothing here polls.
#[derive(Debug)]
pub struct Tailer {
    dir: PathBuf,
    segments: Vec<(Seq, PathBuf)>,
    index: usize,
    file: Option<File>,
    offset: u64,
    next_seq: Seq,
    payload: Vec<u8>,
}

impl Tailer {
    /// Open a tailer positioned to deliver everything after `after`.
    ///
    /// `Seq::ZERO` means from the beginning, which is always a correct place to
    /// start: every consumer of this log is building a projection, and a
    /// projection is a fold that can be re-run.
    ///
    /// # Errors
    /// [`JournalError::Io`] when the directory cannot be listed.
    pub fn open(dir: impl AsRef<Path>, after: Seq) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let segments = segment::list_segments(&dir)?;
        let mut tailer = Self {
            dir,
            segments,
            index: 0,
            file: None,
            offset: 0,
            next_seq: after.next(),
            payload: Vec::with_capacity(1024),
        };
        tailer.seek_to_start()?;
        Ok(tailer)
    }

    /// The sequence this tailer will deliver next.
    #[must_use]
    pub const fn next_seq(&self) -> Seq {
        self.next_seq
    }

    /// The next record, or `Ok(None)` when caught up.
    ///
    /// # Errors
    /// [`JournalError::Format`] when a complete frame fails its checks — a
    /// corrupt log, which is never read past — or [`JournalError::Io`].
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        loop {
            if self.file.is_none() && !self.open_current()? {
                // No segment at this index yet. The writer may have rolled
                // since we listed, so look again before giving up.
                if !self.rescan()? {
                    return Ok(None);
                }
                continue;
            }

            let Some(file) = self.file.as_mut() else {
                return Ok(None);
            };
            let size = file
                .metadata()
                .map_err(|e| JournalError::io("stat segment", e))?
                .len();

            match segment::read_frame_at(file, self.offset, size, &mut self.payload)? {
                Some(header) => {
                    if header.seq != self.next_seq {
                        return Err(JournalError::SequenceGap {
                            expected: self.next_seq,
                            found: header.seq,
                        });
                    }
                    self.offset += header.frame_len() as u64;
                    self.next_seq = header.seq.next();
                    return Ok(Some(Record {
                        header,
                        payload: core::mem::take(&mut self.payload),
                    }));
                }
                None => {
                    // Nothing more in this segment. If a later one exists we
                    // have reached its end; otherwise we have caught up.
                    if self.index + 1 < self.segments.len() {
                        self.index += 1;
                        self.file = None;
                        self.offset = 0;
                        continue;
                    }
                    if self.rescan()? {
                        continue;
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Read everything available now, appending to `out`.
    ///
    /// # Errors
    /// As [`Self::next_record`].
    pub fn drain_into(&mut self, out: &mut Vec<Record>) -> Result<usize> {
        let before = out.len();
        while let Some(record) = self.next_record()? {
            out.push(record);
        }
        Ok(out.len() - before)
    }

    /// Position at the segment holding `next_seq`, and scan to its offset.
    fn seek_to_start(&mut self) -> Result<()> {
        if self.segments.is_empty() {
            return Ok(());
        }
        // The last segment whose base sequence is at or below what we want.
        self.index = self
            .segments
            .partition_point(|(base, _)| base.as_u64() <= self.next_seq.as_u64())
            .saturating_sub(1);
        self.file = None;
        self.offset = 0;
        if !self.open_current()? {
            return Ok(());
        }

        // Walk forward inside the segment to the record we want. Segments are
        // bounded, so this is a scan of at most one file and only at startup.
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        let size = file
            .metadata()
            .map_err(|e| JournalError::io("stat segment", e))?
            .len();
        while let Some(header) = segment::read_frame_at(file, self.offset, size, &mut self.payload)?
        {
            if header.seq >= self.next_seq {
                self.next_seq = header.seq;
                return Ok(());
            }
            self.offset += header.frame_len() as u64;
        }
        Ok(())
    }

    /// Open the segment at `index`, positioning past its header on first open.
    fn open_current(&mut self) -> Result<bool> {
        let Some((_, path)) = self.segments.get(self.index) else {
            return Ok(false);
        };
        let file = File::open(path).map_err(|e| JournalError::io("open segment", e))?;
        if self.offset == 0 {
            self.offset = crate::format::SEGMENT_HEADER_LEN as u64;
        }
        self.file = Some(file);
        Ok(true)
    }

    /// Re-list the directory; report whether a segment we had not seen appeared.
    fn rescan(&mut self) -> Result<bool> {
        let found = segment::list_segments(&self.dir)?;
        let grew = found.len() > self.segments.len();
        self.segments = found;
        if grew && self.index + 1 < self.segments.len() {
            self.index += 1;
            self.file = None;
            self.offset = 0;
            return Ok(true);
        }
        Ok(false)
    }
}

/// A consumer's position, persisted next to the journal.
///
/// Written by rename, so a cursor file is never observed half-updated. The
/// value is ASCII decimal with a trailing newline: a human debugging a stuck
/// consumer should be able to `cat` it.
#[derive(Debug, Clone)]
pub struct Cursor {
    path: PathBuf,
    seq: Seq,
}

impl Cursor {
    /// Load a consumer's cursor, or start from the beginning if it has none.
    ///
    /// # Errors
    /// [`JournalError::Io`] when the cursor directory cannot be read.
    pub fn load(dir: impl AsRef<Path>, consumer: &str) -> Result<Self> {
        let path = dir.as_ref().join(crate::writer::CURSOR_DIR).join(consumer);
        let seq = match std::fs::read_to_string(&path) {
            Ok(text) => text
                .trim()
                .parse::<u64>()
                .map(Seq::from_u64)
                .unwrap_or(Seq::ZERO),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Seq::ZERO,
            Err(e) => return Err(JournalError::io("read cursor", e)),
        };
        Ok(Self { path, seq })
    }

    /// The last sequence this consumer finished with.
    #[must_use]
    pub const fn seq(&self) -> Seq {
        self.seq
    }

    /// Record that everything up to `seq` is done.
    ///
    /// # Errors
    /// [`JournalError::Io`] when the file cannot be written or renamed.
    pub fn commit(&mut self, seq: Seq) -> Result<()> {
        // A cursor that goes backwards would replay work already done. Harmless
        // for a projection, but it hides the bug that caused it.
        if seq < self.seq {
            return Ok(());
        }
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, format!("{}\n", seq.as_u64()))
            .map_err(|e| JournalError::io("write cursor", e))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| JournalError::io("rename cursor", e))?;
        self.seq = seq;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::RecordKind;
    use crate::writer::Journal;
    use sentinel_types::Nanos;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let base =
                std::env::temp_dir().join(format!("sentinel-reader-{tag}-{}", std::process::id()));
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

    fn write_n(dir: &Path, n: u64) -> Journal {
        let mut j = Journal::claim(dir, Nanos::from_u64(1)).unwrap();
        for i in 0..n {
            j.append(RecordKind::Ticked, Nanos::from_u64(i), &i.to_le_bytes())
                .unwrap();
        }
        j.flush().unwrap();
        j
    }

    #[test]
    fn a_tailer_reads_everything_in_order() {
        let dir = TempDir::new("order");
        let _j = write_n(dir.path(), 10);

        let mut tailer = Tailer::open(dir.path(), Seq::ZERO).unwrap();
        let mut records = Vec::new();
        tailer.drain_into(&mut records).unwrap();

        assert_eq!(records.len(), 11, "ten ticks plus the claim");
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.header.seq.as_u64(), i as u64 + 1);
        }
    }

    #[test]
    fn two_tailers_observe_identical_order() {
        // The property that makes the journal usable as a fan-out: consumers
        // do not coordinate, and cannot disagree about what happened.
        let dir = TempDir::new("two-tailers");
        let _j = write_n(dir.path(), 20);

        let mut a = Vec::new();
        let mut b = Vec::new();
        Tailer::open(dir.path(), Seq::ZERO)
            .unwrap()
            .drain_into(&mut a)
            .unwrap();
        Tailer::open(dir.path(), Seq::ZERO)
            .unwrap()
            .drain_into(&mut b)
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn a_tailer_follows_the_writer() {
        let dir = TempDir::new("follow");
        let mut j = write_n(dir.path(), 3);

        let mut tailer = Tailer::open(dir.path(), Seq::ZERO).unwrap();
        let mut first = Vec::new();
        tailer.drain_into(&mut first).unwrap();
        assert_eq!(first.len(), 4);

        // Caught up: not an error, not the end.
        assert_eq!(tailer.next_record().unwrap(), None);

        j.append_and_flush(RecordKind::Mark, Nanos::from_u64(99), b"later")
            .unwrap();
        let record = tailer.next_record().unwrap().expect("the new record");
        assert_eq!(record.header.kind, RecordKind::Mark);
        assert_eq!(record.payload, b"later");
    }

    #[test]
    fn a_tailer_can_start_partway_through() {
        let dir = TempDir::new("resume");
        let _j = write_n(dir.path(), 10);

        let mut tailer = Tailer::open(dir.path(), Seq::from_u64(6)).unwrap();
        let record = tailer.next_record().unwrap().unwrap();
        assert_eq!(record.header.seq, Seq::from_u64(7));
    }

    #[test]
    fn an_empty_journal_directory_yields_nothing_rather_than_failing() {
        let dir = TempDir::new("empty");
        let mut tailer = Tailer::open(dir.path(), Seq::ZERO).unwrap();
        assert_eq!(tailer.next_record().unwrap(), None);
    }

    #[test]
    fn a_cursor_survives_a_restart() {
        let dir = TempDir::new("cursor");
        let _j = write_n(dir.path(), 10);

        let mut cursor = Cursor::load(dir.path(), "projectord").unwrap();
        assert_eq!(
            cursor.seq(),
            Seq::ZERO,
            "a consumer with no cursor starts over"
        );
        cursor.commit(Seq::from_u64(7)).unwrap();

        let reloaded = Cursor::load(dir.path(), "projectord").unwrap();
        assert_eq!(reloaded.seq(), Seq::from_u64(7));

        let mut tailer = Tailer::open(dir.path(), reloaded.seq()).unwrap();
        assert_eq!(
            tailer.next_record().unwrap().unwrap().header.seq,
            Seq::from_u64(8)
        );
    }

    #[test]
    fn a_cursor_never_moves_backwards() {
        let dir = TempDir::new("cursor-back");
        let _j = write_n(dir.path(), 3);
        let mut cursor = Cursor::load(dir.path(), "gatewayd").unwrap();
        cursor.commit(Seq::from_u64(3)).unwrap();
        cursor.commit(Seq::from_u64(1)).unwrap();
        assert_eq!(cursor.seq(), Seq::from_u64(3));
    }

    #[test]
    fn consumers_keep_separate_cursors() {
        let dir = TempDir::new("cursor-sep");
        let _j = write_n(dir.path(), 5);
        let mut a = Cursor::load(dir.path(), "gatewayd").unwrap();
        let mut b = Cursor::load(dir.path(), "projectord").unwrap();
        a.commit(Seq::from_u64(5)).unwrap();
        b.commit(Seq::from_u64(2)).unwrap();
        assert_eq!(
            Cursor::load(dir.path(), "gatewayd").unwrap().seq(),
            Seq::from_u64(5)
        );
        assert_eq!(
            Cursor::load(dir.path(), "projectord").unwrap().seq(),
            Seq::from_u64(2)
        );
    }
}

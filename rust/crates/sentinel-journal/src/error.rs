//! What can go wrong with a log that is the system of record.

use std::path::PathBuf;

use sentinel_types::Seq;

use crate::format::FormatError;

/// A journal result.
pub type Result<T> = std::result::Result<T, JournalError>;

/// Something the journal refused to do.
#[derive(Debug)]
pub enum JournalError {
    /// An operating system call failed. Carries what was being attempted,
    /// because `No such file or directory` on its own has never helped anyone
    /// at three in the morning.
    Io {
        /// What was being done.
        doing: &'static str,
        /// What the OS said.
        source: std::io::Error,
    },
    /// The bytes on disk are not a frame we can read.
    Format(FormatError),
    /// Another process holds the journal.
    ///
    /// The single-writer proof. Unlike the database advisory lock this replaces,
    /// it cannot report a rival that is actually us: the kernel releases a
    /// `flock` when the holding process exits, however it exits.
    AlreadyClaimed {
        /// The journal directory.
        dir: PathBuf,
    },
    /// The log skips a sequence. A hole, not a gap to read past.
    SequenceGap {
        /// What the next record should have been.
        expected: Seq,
        /// What was found.
        found: Seq,
    },
    /// A payload larger than a record may carry.
    PayloadTooLarge {
        /// What was offered.
        len: usize,
    },
    /// The journal has failed a flush and will not accept further work.
    ///
    /// Permanent, and deliberately so. Memory is ahead of a log that is still
    /// being served from, which is the one state that cannot be recovered by
    /// restarting the thing that is wrong — so the process must restart and
    /// rebuild from what is actually durable.
    Poisoned {
        /// What the failed flush was.
        doing: &'static str,
    },
}

impl JournalError {
    pub(crate) fn io(doing: &'static str, source: std::io::Error) -> Self {
        Self::Io { doing, source }
    }
}

impl core::fmt::Display for JournalError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io { doing, source } => write!(f, "{doing}: {source}"),
            Self::Format(e) => write!(f, "{e}"),
            Self::AlreadyClaimed { dir } => {
                write!(f, "journal at {} is held by another writer", dir.display())
            }
            Self::SequenceGap { expected, found } => {
                write!(f, "journal skips from {expected} to {found}")
            }
            Self::PayloadTooLarge { len } => {
                write!(f, "{len} bytes exceeds the record payload cap")
            }
            Self::Poisoned { doing } => {
                write!(
                    f,
                    "journal is poisoned: {doing} failed and cannot be retried"
                )
            }
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Format(e) => Some(e),
            _ => None,
        }
    }
}

impl JournalError {
    /// Whether this error means the process must restart.
    ///
    /// A poisoned journal cannot be nursed back: the only safe move is to die
    /// and rebuild memory from what is durable.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::Poisoned { .. } | Self::SequenceGap { .. })
    }
}

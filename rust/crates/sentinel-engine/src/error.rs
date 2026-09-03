//! What the writer refuses.

/// An engine result.
pub type Result<T> = core::result::Result<T, EngineError>;

/// Something the engine could not do.
#[derive(Debug)]
pub enum EngineError {
    /// The journal refused the append, or a flush failed.
    ///
    /// A failed flush is permanent: memory is ahead of a log that is still
    /// being served from, and the only safe move is for the process to restart
    /// and rebuild from what is actually durable.
    Journal(sentinel_journal::JournalError),
    /// The book refused the record.
    Ledger(sentinel_ledger::LedgerError),
}

impl core::fmt::Display for EngineError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Journal(e) => write!(f, "journal: {e}"),
            Self::Ledger(e) => write!(f, "ledger: {e}"),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(e) => Some(e),
            Self::Ledger(e) => Some(e),
        }
    }
}

impl EngineError {
    /// Whether the process must restart rather than carry on.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        match self {
            Self::Journal(e) => e.is_fatal(),
            Self::Ledger(e) => e.is_integrity_violation(),
        }
    }
}

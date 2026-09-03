//! Identifiers, sized inline.
//!
//! Every id in the hot path is `Copy` and heap-free. Not for the allocation —
//! at this order rate the allocator would never notice — but because a `String`
//! in an event payload is a pointer, and a pointer is a thing that can dangle,
//! be shared, or be mutated between the moment an event is appended and the
//! moment it is flushed. A fixed array cannot.
//!
//! The lengths are the venue's, not ours: Delta India accepts a 36-character
//! client order id and its symbols fit in 16.

use core::fmt;

/// Text that did not fit its inline capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooLong {
    /// Bytes offered.
    pub len: usize,
    /// Bytes the type can hold.
    pub capacity: usize,
}

impl fmt::Display for TooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes exceeds capacity {}", self.len, self.capacity)
    }
}

impl core::error::Error for TooLong {}

/// A short, immutable, inline-stored ASCII string.
///
/// Padded with zero bytes; the padding is part of the value's identity, so two
/// equal strings always have equal bytes and the on-disk form of an event is
/// byte-stable without a separate canonicalisation step.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InlineStr<const N: usize> {
    bytes: [u8; N],
    len: u8,
}

impl<const N: usize> InlineStr<N> {
    /// Capacity in bytes.
    pub const CAPACITY: usize = N;

    /// The empty string.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            bytes: [0; N],
            len: 0,
        }
    }

    /// Build from text.
    ///
    /// # Errors
    /// [`TooLong`] when the text exceeds `N` bytes. Truncating an id is worse
    /// than refusing it: a truncated client order id collides with its
    /// neighbours, and collision on that key means two orders share a
    /// reconciliation identity.
    pub const fn new(s: &str) -> Result<Self, TooLong> {
        let src = s.as_bytes();
        // u8 holds the length, so a capacity above 255 would silently truncate
        // the length field itself. Caught at compile time in every monomorphisation.
        const { assert!(N <= u8::MAX as usize, "InlineStr capacity must fit in u8") };
        if src.len() > N {
            return Err(TooLong {
                len: src.len(),
                capacity: N,
            });
        }
        let mut bytes = [0u8; N];
        let mut i = 0;
        while i < src.len() {
            bytes[i] = src[i];
            i += 1;
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "src.len() <= N is checked above, and N <= u8::MAX is a \
                      const assertion at the top of this function"
        )]
        let len = src.len() as u8;
        Ok(Self { bytes, len })
    }

    /// The text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // Only ever built from a `&str`, so the prefix is valid UTF-8 and the
        // boundary is a byte count taken from that same `&str`.
        core::str::from_utf8(&self.bytes[..usize::from(self.len)]).unwrap_or_default()
    }

    /// The padded bytes, as they appear on disk.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; N] {
        &self.bytes
    }

    /// Rebuild from padded bytes read off disk.
    ///
    /// # Errors
    /// [`TooLong`] carrying the offending length when the padding is not a
    /// valid trailing run of zero bytes, or the content is not UTF-8.
    pub fn from_padded(bytes: [u8; N]) -> Result<Self, TooLong> {
        let len = bytes.iter().position(|&b| b == 0).unwrap_or(N);
        // A zero byte followed by a non-zero byte is not padding — it is a
        // corrupt record claiming to be a short string.
        if bytes[len..].iter().any(|&b| b != 0) {
            return Err(TooLong {
                len: N,
                capacity: N,
            });
        }
        if core::str::from_utf8(&bytes[..len]).is_err() {
            return Err(TooLong { len, capacity: N });
        }
        Ok(Self {
            bytes,
            len: u8::try_from(len).map_err(|_| TooLong { len, capacity: N })?,
        })
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> Default for InlineStr<N> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<const N: usize> fmt::Display for InlineStr<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<const N: usize> fmt::Debug for InlineStr<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl<const N: usize> core::str::FromStr for InlineStr<N> {
    type Err = TooLong;
    fn from_str(s: &str) -> Result<Self, TooLong> {
        Self::new(s)
    }
}

/// What the venue knows us by, and the key every reconciliation is done on.
///
/// Generated by us, before submission, and durable before the venue sees it —
/// which is what makes a duplicate submission structurally impossible rather
/// than merely unlikely (R1.1, R1.2).
pub type ClientOrderId = InlineStr<36>;

/// The venue's own order identifier. Absent until the venue acknowledges, which
/// is precisely why it cannot be the reconciliation key.
pub type VenueOrderId = InlineStr<32>;

/// The venue's identifier for a single execution. The dedup key for fills
/// (R1.2 / R1.7): a stream that delivers the same fill twice must not move the
/// position twice.
pub type ExecId = InlineStr<40>;

/// A tradable instrument, as the venue names it. `BTCUSD`, `ETHUSD`.
pub type Instrument = InlineStr<16>;

/// Correlates every event produced by one causal chain.
///
/// A `u128` rather than a UUID type: it is generated at the edge — where clocks
/// and randomness are allowed — and below the gateway it is an opaque number
/// that is only ever copied and compared.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TraceId(u128);

impl TraceId {
    /// The trace that means "no chain" — recovery, boot, replay.
    pub const NONE: Self = Self(0);

    #[must_use]
    pub const fn from_u128(v: u128) -> Self {
        Self(v)
    }

    #[must_use]
    pub const fn as_u128(self) -> u128 {
        self.0
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The first eight hex digits are what appears in a log line; the full
        // value is what appears in the ledger.
        write!(f, "{:032x}", self.0)
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId({self})")
    }
}

/// Which run of the writer produced a record.
///
/// The fencing token. One machine, one journal, one `flock` — but a log whose
/// records carry an epoch makes a two-writer interleave *detectable after the
/// fact*, which a lock alone does not. The epoch increments once per successful
/// journal claim and never repeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(u64);

impl Epoch {
    /// The epoch of a journal that has never been claimed.
    pub const GENESIS: Self = Self(0);

    #[must_use]
    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// The next epoch. Saturates: an epoch that wrapped would let an old
    /// writer's records pass as current, which is the one thing the token exists
    /// to prevent. At one claim per boot, saturation is not reachable.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Position of a record in the journal. Monotonic, gap-free, from 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Seq(u64);

impl Seq {
    /// Before the first record.
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// The next sequence. Saturates for the same reason [`Epoch::next`] does;
    /// at any conceivable rate `u64` outlives the venue.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_a_real_client_order_id() {
        let id = ClientOrderId::new("UI-4bb5ba826e2e").unwrap();
        assert_eq!(id.as_str(), "UI-4bb5ba826e2e");
        assert_eq!(id.len(), 15);
    }

    #[test]
    fn refuses_to_truncate_an_id() {
        // Truncation would collide two orders onto one reconciliation key.
        let long = "x".repeat(ClientOrderId::CAPACITY + 1);
        assert_eq!(
            ClientOrderId::new(&long),
            Err(TooLong {
                len: 37,
                capacity: 36
            })
        );
        // Exactly at capacity still fits.
        assert!(ClientOrderId::new(&"x".repeat(ClientOrderId::CAPACITY)).is_ok());
    }

    #[test]
    fn equal_strings_have_equal_bytes() {
        // Byte stability is what lets an event be compared, hashed and written
        // without a canonicalisation pass.
        let a = Instrument::new("BTCUSD").unwrap();
        let b = Instrument::new("BTCUSD").unwrap();
        assert_eq!(a.as_bytes(), b.as_bytes());
        assert_eq!(a.as_bytes()[6..], [0u8; 10]);
    }

    #[test]
    fn round_trips_through_padded_bytes() {
        let id = ClientOrderId::new("UI-af1d562998f7").unwrap();
        assert_eq!(ClientOrderId::from_padded(*id.as_bytes()).unwrap(), id);
        assert_eq!(
            ClientOrderId::from_padded([0; 36]).unwrap(),
            ClientOrderId::empty()
        );
    }

    #[test]
    fn rejects_padding_that_is_not_padding() {
        // A record claiming "AB\0D" is corrupt, not a two-character string.
        let mut bytes = [0u8; 16];
        bytes[0] = b'A';
        bytes[1] = b'B';
        bytes[3] = b'D';
        assert!(Instrument::from_padded(bytes).is_err());
    }

    #[test]
    fn rejects_bytes_that_are_not_utf8() {
        let mut bytes = [0u8; 16];
        bytes[0] = 0xff;
        assert!(Instrument::from_padded(bytes).is_err());
    }

    #[test]
    fn epoch_and_seq_advance_without_wrapping() {
        assert_eq!(Epoch::GENESIS.next(), Epoch::from_u64(1));
        assert_eq!(Epoch::from_u64(u64::MAX).next(), Epoch::from_u64(u64::MAX));
        assert_eq!(Seq::ZERO.next(), Seq::from_u64(1));
    }

    #[test]
    fn trace_id_renders_full_width() {
        assert_eq!(TraceId::NONE.to_string(), "0".repeat(32));
        assert_eq!(
            TraceId::from_u128(0x6440_ba96).to_string(),
            "0000000000000000000000006440ba96"
        );
    }
}

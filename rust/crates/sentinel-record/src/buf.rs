//! Checked little-endian reads and writes over a byte buffer.
//!
//! Hand-rolled rather than derived. The journal is the system of record, so its
//! layout is a thing to decide and pin, not a thing to inherit from whatever a
//! serialisation library does this year — a derive that silently changes its
//! field order or its integer encoding reinterprets every log already written.
//!
//! Reads are checked. A short buffer returns [`CodecError::Truncated`] rather
//! than panicking, because the bytes arrive from disk and a corrupt record must
//! be an error the caller handles, not a crash in a reader.

use sentinel_types::{InlineStr, Money, Price, Qty, TraceId};

/// Why some bytes are not the record they claim to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    /// The buffer ended before the record did.
    Truncated {
        /// Bytes wanted.
        want: usize,
        /// Bytes left.
        have: usize,
    },
    /// A discriminant byte that is not a value of its type.
    BadDiscriminant(sentinel_types::UnknownDiscriminant),
    /// Padded text that is not a valid inline string.
    BadText,
    /// A boolean field that was neither 0 nor 1.
    ///
    /// Refused rather than coerced: `2` means the writer and the reader
    /// disagree about the layout, and treating it as `true` hides that.
    BadFlag {
        /// The byte as read.
        value: u8,
    },
    /// Bytes remained after the record was fully decoded.
    ///
    /// A record is a fixed shape. Trailing bytes mean this is a record from a
    /// different version of the format wearing the same kind byte.
    TrailingBytes {
        /// How many were left.
        extra: usize,
    },
}

impl core::fmt::Display for CodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Truncated { want, have } => {
                write!(f, "record wants {want} more bytes, {have} remain")
            }
            Self::BadDiscriminant(e) => write!(f, "{e}"),
            Self::BadText => f.write_str("padded text is not a valid inline string"),
            Self::BadFlag { value } => write!(f, "flag byte {value} is neither 0 nor 1"),
            Self::TrailingBytes { extra } => {
                write!(f, "{extra} bytes remain after the record")
            }
        }
    }
}

impl core::error::Error for CodecError {}

impl From<sentinel_types::UnknownDiscriminant> for CodecError {
    fn from(e: sentinel_types::UnknownDiscriminant) -> Self {
        Self::BadDiscriminant(e)
    }
}

/// A codec result.
pub type Result<T> = core::result::Result<T, CodecError>;

/// Appends fixed-width fields to a buffer.
#[derive(Debug)]
pub struct Writer<'a> {
    out: &'a mut Vec<u8>,
}

impl<'a> Writer<'a> {
    /// Wrap a buffer.
    pub fn new(out: &'a mut Vec<u8>) -> Self {
        Self { out }
    }

    /// A discriminant.
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.out.push(v);
        self
    }

    /// A boolean, as one byte.
    pub fn flag(&mut self, v: bool) -> &mut Self {
        self.out.push(u8::from(v));
        self
    }

    /// A signed 32-bit field.
    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.out.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// A signed 64-bit field.
    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.out.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// An unsigned 64-bit field.
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.out.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// A trace id.
    pub fn trace(&mut self, v: TraceId) -> &mut Self {
        self.out.extend_from_slice(&v.as_u128().to_le_bytes());
        self
    }

    /// Padded inline text, exactly `N` bytes.
    pub fn text<const N: usize>(&mut self, v: InlineStr<N>) -> &mut Self {
        self.out.extend_from_slice(v.as_bytes());
        self
    }

    /// A price.
    pub fn price(&mut self, v: Price) -> &mut Self {
        self.i64(v.raw())
    }

    /// A quantity.
    pub fn qty(&mut self, v: Qty) -> &mut Self {
        self.i64(v.raw())
    }

    /// An amount of money.
    pub fn money(&mut self, v: Money) -> &mut Self {
        self.i64(v.raw())
    }

    /// An optional price, as a flag byte and a value.
    ///
    /// The value is written even when absent, so the record keeps a fixed width
    /// and a reader can be laid out by offset rather than by parsing forward.
    pub fn opt_price(&mut self, v: Option<Price>) -> &mut Self {
        self.flag(v.is_some());
        self.price(v.unwrap_or(Price::ZERO))
    }
}

/// Reads fixed-width fields from a buffer, checking every one.
#[derive(Debug)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// Wrap a buffer.
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    /// Bytes not yet read.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    /// Assert the record is exactly consumed.
    ///
    /// # Errors
    /// [`CodecError::TrailingBytes`] when bytes remain.
    pub const fn finish(&self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes {
                extra: self.remaining(),
            })
        }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(CodecError::Truncated {
                want: n,
                have: self.remaining(),
            });
        }
        let slice = &self.bytes[self.at..self.at + n];
        self.at += n;
        Ok(slice)
    }

    /// A discriminant.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// A boolean.
    ///
    /// # Errors
    /// [`CodecError::Truncated`], or [`CodecError::BadFlag`] for any byte other
    /// than 0 or 1.
    pub fn flag(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(CodecError::BadFlag { value }),
        }
    }

    /// A signed 32-bit field.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn i32(&mut self) -> Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// A signed 64-bit field.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    /// An unsigned 64-bit field.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    /// A trace id.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn trace(&mut self) -> Result<TraceId> {
        let b = self.take(16)?;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(b);
        Ok(TraceId::from_u128(u128::from_le_bytes(bytes)))
    }

    /// Padded inline text.
    ///
    /// # Errors
    /// [`CodecError::Truncated`], or [`CodecError::BadText`] when the padding
    /// is not a trailing run of zero bytes or the content is not UTF-8.
    pub fn text<const N: usize>(&mut self) -> Result<InlineStr<N>> {
        let b = self.take(N)?;
        let mut bytes = [0u8; N];
        bytes.copy_from_slice(b);
        InlineStr::from_padded(bytes).map_err(|_| CodecError::BadText)
    }

    /// A price.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn price(&mut self) -> Result<Price> {
        Ok(Price::from_raw(self.i64()?))
    }

    /// A quantity.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn qty(&mut self) -> Result<Qty> {
        Ok(Qty::from_raw(self.i64()?))
    }

    /// An amount of money.
    ///
    /// # Errors
    /// [`CodecError::Truncated`].
    pub fn money(&mut self) -> Result<Money> {
        Ok(Money::from_raw(self.i64()?))
    }

    /// An optional price.
    ///
    /// # Errors
    /// [`CodecError::Truncated`] or [`CodecError::BadFlag`].
    pub fn opt_price(&mut self) -> Result<Option<Price>> {
        let present = self.flag()?;
        let value = self.price()?;
        Ok(present.then_some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_round_trip_in_order() {
        let mut bytes = Vec::new();
        Writer::new(&mut bytes)
            .u8(7)
            .flag(true)
            .i32(-2019)
            .i64(i64::MIN)
            .u64(u64::MAX)
            .trace(TraceId::from_u128(0xdead_beef))
            .text(InlineStr::<16>::new("BTCUSD").unwrap())
            .price(Price::parse("109432.5").unwrap())
            .opt_price(None);

        let mut r = Reader::new(&bytes);
        assert_eq!(r.u8().unwrap(), 7);
        assert!(r.flag().unwrap());
        assert_eq!(r.i32().unwrap(), -2019);
        assert_eq!(r.i64().unwrap(), i64::MIN);
        assert_eq!(r.u64().unwrap(), u64::MAX);
        assert_eq!(r.trace().unwrap(), TraceId::from_u128(0xdead_beef));
        assert_eq!(r.text::<16>().unwrap().as_str(), "BTCUSD");
        assert_eq!(r.price().unwrap(), Price::parse("109432.5").unwrap());
        assert_eq!(r.opt_price().unwrap(), None);
        r.finish().unwrap();
    }

    #[test]
    fn an_optional_field_keeps_a_fixed_width() {
        // Present and absent must cost the same, or a record's layout depends
        // on its contents and cannot be read by offset.
        let mut with = Vec::new();
        Writer::new(&mut with).opt_price(Some(Price::whole(5)));
        let mut without = Vec::new();
        Writer::new(&mut without).opt_price(None);
        assert_eq!(with.len(), without.len());
        assert_eq!(with.len(), 9);
    }

    #[test]
    fn a_short_buffer_is_an_error_not_a_panic() {
        let bytes = [1u8, 2, 3];
        let mut r = Reader::new(&bytes);
        assert_eq!(r.i64(), Err(CodecError::Truncated { want: 8, have: 3 }));
    }

    #[test]
    fn trailing_bytes_are_refused() {
        let bytes = [1u8, 2, 3];
        let mut r = Reader::new(&bytes);
        r.u8().unwrap();
        assert_eq!(r.finish(), Err(CodecError::TrailingBytes { extra: 2 }));
    }

    #[test]
    fn a_flag_that_is_not_a_flag_is_refused() {
        // 2 means the writer and the reader disagree about the layout.
        // Coercing it to `true` would hide exactly that.
        let bytes = [2u8];
        let mut r = Reader::new(&bytes);
        assert_eq!(r.flag(), Err(CodecError::BadFlag { value: 2 }));
    }

    #[test]
    fn text_that_is_not_padding_is_refused() {
        let mut bytes = vec![0u8; 16];
        bytes[0] = b'A';
        bytes[3] = b'D'; // a gap: not padding, a corrupt record
        let mut r = Reader::new(&bytes);
        assert_eq!(r.text::<16>(), Err(CodecError::BadText));
    }

    #[test]
    fn every_value_of_every_field_survives_the_trip() {
        for raw in [0i64, 1, -1, i64::MAX, i64::MIN, 10_943_250_000_000] {
            let mut bytes = Vec::new();
            Writer::new(&mut bytes)
                .qty(Qty::from_raw(raw))
                .money(Money::from_raw(raw));
            let mut r = Reader::new(&bytes);
            assert_eq!(r.qty().unwrap().raw(), raw);
            assert_eq!(r.money().unwrap().raw(), raw);
            r.finish().unwrap();
        }
    }
}

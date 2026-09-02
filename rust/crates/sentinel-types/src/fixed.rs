//! Fixed-point decimal at 1e8, and the three newtypes built on it.
//!
//! There is deliberately no `Decimal` and no `f64`. A price times a quantity is
//! computed in `i128` and rescaled once, so the only rounding in the system
//! happens at a place named `rescale` and nowhere else.
//!
//! Why 1e8: it holds every tick and lot size the venues we speak use (Delta
//! India quotes BTCUSD in 0.5-dollar ticks and sizes in whole contracts; the
//! smallest thing on the roadmap is a 1e-8 BTC funding accrual), and `i64` at
//! 1e8 reaches ±92 billion units — nine orders of magnitude above any position
//! this system is allowed to hold.

use core::fmt;
use core::ops::{Add, AddAssign, Neg, Sub, SubAssign};

/// Units per whole. Every `E8`-backed type shares it.
pub const SCALE: i64 = 100_000_000;
/// Decimal places `SCALE` represents.
pub const SCALE_DIGITS: u32 = 8;

/// Why a decimal string could not become a fixed-point value.
///
/// Parsing refuses rather than rounds. A venue that sends more precision than
/// we model is telling us our model is wrong, and silently truncating its
/// message is how that stays hidden until settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    /// Nothing, or nothing but a sign.
    Empty,
    /// A character that is not a digit, a single leading sign, or one point.
    Malformed,
    /// More fractional digits than `SCALE` can hold, so the value would round.
    TooPrecise,
    /// The integral part does not fit in `i64` at this scale.
    OutOfRange,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Empty => "empty",
            Self::Malformed => "malformed decimal",
            Self::TooPrecise => "more than 8 decimal places",
            Self::OutOfRange => "out of range for i64 at 1e8",
        };
        f.write_str(s)
    }
}

impl core::error::Error for ParseError {}

/// Arithmetic that could not be represented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Overflow;

impl fmt::Display for Overflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fixed-point overflow")
    }
}

impl core::error::Error for Overflow {}

/// The magnitude of the most negative representable value. `i64::MIN` has no
/// positive counterpart, so parsing accumulates an unsigned magnitude in `i128`
/// and applies the sign last — otherwise `"-92233720368.54775808"`, which
/// `Display` produces, would fail to parse back.
const MAX_MAGNITUDE: i128 = i64::MAX as i128 + 1;

/// Largest whole-unit count that still fits once multiplied by `SCALE`.
/// Asserted rather than divided, so the relationship is visible at the constant.
const MAX_WHOLE: i128 = 92_233_720_368;
const _: () = assert!(MAX_WHOLE * SCALE as i128 <= MAX_MAGNITUDE);
const _: () = assert!((MAX_WHOLE + 1) * SCALE as i128 > MAX_MAGNITUDE);

/// Parse a decimal string into raw 1e8 units.
///
/// Accepts `-12`, `12.5`, `.5`, `+0.00000001`. Rejects `1e5`, `1.234567891`,
/// `12.` and anything with a separator, because no venue we speak sends those
/// and accepting them would mean guessing what they meant.
///
/// # Errors
/// [`ParseError`] describing which of those rules the text broke.
pub const fn parse_e8(s: &str) -> Result<i64, ParseError> {
    let b = s.as_bytes();
    if b.is_empty() {
        return Err(ParseError::Empty);
    }

    let mut i = 0usize;
    let negative = match b[0] {
        b'-' => {
            i = 1;
            true
        }
        b'+' => {
            i = 1;
            false
        }
        _ => false,
    };
    if i >= b.len() {
        return Err(ParseError::Empty);
    }

    // Unsigned magnitude throughout; the sign is applied once, at the end.
    let mut magnitude: i128 = 0;
    let mut seen_digit = false;

    // Integral part, counted in whole units and scaled once below.
    while i < b.len() && b[i] != b'.' {
        let d = b[i];
        if !d.is_ascii_digit() {
            return Err(ParseError::Malformed);
        }
        seen_digit = true;
        magnitude = magnitude * 10 + (d - b'0') as i128;
        if magnitude > MAX_WHOLE {
            return Err(ParseError::OutOfRange);
        }
        i += 1;
    }
    magnitude *= SCALE as i128;

    // Fractional part.
    if i < b.len() {
        i += 1; // consume '.'
        let mut place = SCALE;
        let mut digits = 0u32;
        while i < b.len() {
            let d = b[i];
            if !d.is_ascii_digit() {
                return Err(ParseError::Malformed);
            }
            digits += 1;
            if digits > SCALE_DIGITS {
                // A trailing zero beyond our precision carries no value, so it
                // is not a loss of information and is accepted.
                if d != b'0' {
                    return Err(ParseError::TooPrecise);
                }
                i += 1;
                continue;
            }
            seen_digit = true;
            // Walk down the decimal places. SCALE is a power of ten, so every
            // step is exact and `place` reaches 1 at the eighth digit.
            place /= 10;
            magnitude += (d - b'0') as i128 * place as i128;
            i += 1;
        }
        if digits == 0 {
            // "12." — a point promising digits that never came.
            return Err(ParseError::Malformed);
        }
    }

    if !seen_digit {
        return Err(ParseError::Empty);
    }
    if negative {
        if magnitude > MAX_MAGNITUDE {
            return Err(ParseError::OutOfRange);
        }
        // Negate in i128 first: -MAX_MAGNITUDE is i64::MIN and representable,
        // while +MAX_MAGNITUDE is not.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "-magnitude >= i64::MIN, checked on the line above"
        )]
        let narrowed = (-magnitude) as i64;
        Ok(narrowed)
    } else {
        if magnitude > i64::MAX as i128 {
            return Err(ParseError::OutOfRange);
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "magnitude <= i64::MAX, checked on the line above"
        )]
        let narrowed = magnitude as i64;
        Ok(narrowed)
    }
}

/// Render raw 1e8 units as the shortest exact decimal string.
///
/// `100_000_000` is `"1"`, not `"1.00000000"`. Exactness is about the value,
/// not the digit count, and the short form is what crosses the wire.
fn format_e8(raw: i64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let negative = raw < 0;
    // i64::MIN has no positive counterpart, so widen before taking the modulus.
    let magnitude = i128::from(raw).unsigned_abs();
    let scale = u128::from(SCALE.unsigned_abs());
    #[expect(
        clippy::integer_division,
        reason = "splitting units into whole and fractional parts; the remainder \
                  is taken on the next line, so nothing is discarded"
    )]
    let whole = magnitude / scale;
    let frac = magnitude % scale;

    if negative {
        f.write_str("-")?;
    }
    write!(f, "{whole}")?;
    if frac != 0 {
        // Zero-pad to 8, then trim the trailing zeros the value does not need.
        let mut s = format!("{frac:0width$}", width = SCALE_DIGITS as usize);
        while s.ends_with('0') {
            s.pop();
        }
        write!(f, ".{s}")?;
    }
    Ok(())
}

/// Declare a newtype over raw 1e8 units.
///
/// Prices, quantities and money are all fixed-point at the same scale and are
/// still not interchangeable: adding a price to a quantity is a bug the type
/// system should catch, so each gets its own type rather than an alias.
macro_rules! e8_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(i64);

        impl $name {
            /// Zero.
            pub const ZERO: Self = Self(0);
            /// The largest representable value.
            pub const MAX: Self = Self(i64::MAX);
            /// The smallest representable value.
            pub const MIN: Self = Self(i64::MIN);
            /// One whole unit.
            pub const ONE: Self = Self(SCALE);

            /// Wrap raw 1e8 units. Prefer [`Self::whole`] or parsing.
            #[must_use]
            pub const fn from_raw(raw: i64) -> Self {
                Self(raw)
            }

            /// The raw 1e8 units.
            #[must_use]
            pub const fn raw(self) -> i64 {
                self.0
            }

            /// From a whole number of units, saturating at the representable
            /// range rather than wrapping.
            #[must_use]
            pub const fn whole(n: i64) -> Self {
                Self(n.saturating_mul(SCALE))
            }

            /// Parse an exact decimal string.
            ///
            /// # Errors
            /// [`ParseError`] when the text is not an exact decimal this scale
            /// can hold.
            pub const fn parse(s: &str) -> Result<Self, ParseError> {
                match parse_e8(s) {
                    Ok(raw) => Ok(Self(raw)),
                    Err(e) => Err(e),
                }
            }

            #[must_use]
            pub const fn is_zero(self) -> bool {
                self.0 == 0
            }

            #[must_use]
            pub const fn is_positive(self) -> bool {
                self.0 > 0
            }

            #[must_use]
            pub const fn is_negative(self) -> bool {
                self.0 < 0
            }

            /// Magnitude. `MIN` saturates to `MAX`, because its true absolute
            /// value is not representable and wrapping to a negative would be
            /// worse than being one unit wrong at a bound nothing reaches.
            #[must_use]
            pub const fn abs(self) -> Self {
                Self(self.0.saturating_abs())
            }

            #[must_use]
            pub const fn min(self, other: Self) -> Self {
                if self.0 <= other.0 { self } else { other }
            }

            #[must_use]
            pub const fn max(self, other: Self) -> Self {
                if self.0 >= other.0 { self } else { other }
            }

            /// Addition that reports overflow instead of panicking.
            ///
            /// # Errors
            /// [`Overflow`] when the sum leaves the representable range.
            pub const fn checked_add(self, other: Self) -> Result<Self, Overflow> {
                match self.0.checked_add(other.0) {
                    Some(v) => Ok(Self(v)),
                    None => Err(Overflow),
                }
            }

            /// Subtraction that reports overflow instead of panicking.
            ///
            /// # Errors
            /// [`Overflow`] when the difference leaves the representable range.
            pub const fn checked_sub(self, other: Self) -> Result<Self, Overflow> {
                match self.0.checked_sub(other.0) {
                    Some(v) => Ok(Self(v)),
                    None => Err(Overflow),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                format_e8(self.0, f)
            }
        }

        // Debug renders the value, not the struct. A log line reading
        // `Qty(2500000000)` costs the reader a division every time.
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }

        impl Add for $name {
            type Output = Self;
            fn add(self, other: Self) -> Self {
                Self(self.0 + other.0)
            }
        }

        impl Sub for $name {
            type Output = Self;
            fn sub(self, other: Self) -> Self {
                Self(self.0 - other.0)
            }
        }

        impl Neg for $name {
            type Output = Self;
            fn neg(self) -> Self {
                Self(-self.0)
            }
        }

        impl AddAssign for $name {
            fn add_assign(&mut self, other: Self) {
                self.0 += other.0;
            }
        }

        impl SubAssign for $name {
            fn sub_assign(&mut self, other: Self) {
                self.0 -= other.0;
            }
        }

        impl core::str::FromStr for $name {
            type Err = ParseError;
            fn from_str(s: &str) -> Result<Self, ParseError> {
                Self::parse(s)
            }
        }
    };
}

e8_newtype! {
    /// A price, in the instrument's quote currency.
    Price
}

e8_newtype! {
    /// A quantity, in the instrument's contract unit.
    ///
    /// Signed: a position is a `Qty`, and short is negative. An *order* quantity
    /// is always positive and carries its direction in [`crate::Side`].
    Qty
}

e8_newtype! {
    /// An amount of money, in the account's settlement currency.
    Money
}

impl Price {
    /// Notional value of `qty` at this price.
    ///
    /// Computed in `i128` and rescaled once. This is the only multiplication of
    /// two fixed-point values in the system, which is why it is the only place
    /// that can round — and it rounds toward zero, so a notional is never
    /// reported larger than it is.
    ///
    /// # Errors
    /// [`Overflow`] when the product does not fit in `Money`.
    pub const fn notional(self, qty: Qty) -> Result<Money, Overflow> {
        let product = (self.0 as i128) * (qty.0 as i128);
        // Rescale: two 1e8 values multiplied are at 1e16.
        #[expect(
            clippy::integer_division,
            reason = "the single rescale point; truncation toward zero is the \
                      documented rounding rule"
        )]
        let scaled = product / (SCALE as i128);
        if scaled > i64::MAX as i128 || scaled < i64::MIN as i128 {
            return Err(Overflow);
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "range checked against both i64 bounds on the line above"
        )]
        let narrowed = scaled as i64;
        Ok(Money(narrowed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_forms_venues_actually_send() {
        assert_eq!(Price::parse("0").unwrap().raw(), 0);
        assert_eq!(Price::parse("1").unwrap().raw(), SCALE);
        assert_eq!(Price::parse("-1").unwrap().raw(), -SCALE);
        assert_eq!(Price::parse("+1").unwrap().raw(), SCALE);
        assert_eq!(Price::parse("12.5").unwrap().raw(), 1_250_000_000);
        assert_eq!(Price::parse(".5").unwrap().raw(), 50_000_000);
        assert_eq!(Price::parse("0.00000001").unwrap().raw(), 1);
        assert_eq!(Price::parse("-0.00000001").unwrap().raw(), -1);
        assert_eq!(Price::parse("109432.5").unwrap().raw(), 10_943_250_000_000);
    }

    #[test]
    fn refuses_rather_than_rounds() {
        assert_eq!(Price::parse("1.234567891"), Err(ParseError::TooPrecise));
        assert_eq!(Price::parse("1e5"), Err(ParseError::Malformed));
        assert_eq!(Price::parse("1,000"), Err(ParseError::Malformed));
        assert_eq!(Price::parse("12."), Err(ParseError::Malformed));
        assert_eq!(Price::parse(""), Err(ParseError::Empty));
        assert_eq!(Price::parse("-"), Err(ParseError::Empty));
        assert_eq!(Price::parse("."), Err(ParseError::Malformed));
        assert_eq!(Price::parse("99999999999.0"), Err(ParseError::OutOfRange));
    }

    #[test]
    fn trailing_zeros_beyond_precision_are_not_a_loss() {
        // Venues pad. "1.500000000000" is 1.5 and nothing has been discarded.
        assert_eq!(
            Price::parse("1.500000000000").unwrap(),
            Price::parse("1.5").unwrap()
        );
        assert_eq!(Price::parse("0.000000000").unwrap(), Price::ZERO);
    }

    #[test]
    fn display_round_trips_through_parse() {
        for raw in [
            0i64,
            1,
            -1,
            SCALE,
            -SCALE,
            1_250_000_000,
            10_943_250_000_000,
            i64::MAX,
            i64::MIN,
        ] {
            let p = Price::from_raw(raw);
            let text = p.to_string();
            assert_eq!(
                Price::parse(&text).unwrap(),
                p,
                "round trip failed for {text}"
            );
        }
    }

    #[test]
    fn display_is_the_short_form() {
        assert_eq!(Price::from_raw(SCALE).to_string(), "1");
        assert_eq!(Price::from_raw(1_250_000_000).to_string(), "12.5");
        assert_eq!(Price::from_raw(1).to_string(), "0.00000001");
        assert_eq!(Price::from_raw(-1).to_string(), "-0.00000001");
        assert_eq!(Price::ZERO.to_string(), "0");
    }

    #[test]
    fn notional_is_exact_at_venue_scale() {
        // 3 contracts of BTCUSD at 109,432.50.
        let px = Price::parse("109432.5").unwrap();
        let qty = Qty::whole(3);
        assert_eq!(px.notional(qty).unwrap().to_string(), "328297.5");
    }

    #[test]
    fn notional_truncates_toward_zero_and_says_so() {
        // 1e-8 at 0.5 is 5e-9 — below the scale. Toward zero, both signs.
        let half = Price::parse("0.5").unwrap();
        assert_eq!(half.notional(Qty::from_raw(1)).unwrap(), Money::ZERO);
        assert_eq!(half.notional(Qty::from_raw(-1)).unwrap(), Money::ZERO);
        // And a value that does not need rounding is untouched.
        assert_eq!(half.notional(Qty::from_raw(2)).unwrap(), Money::from_raw(1));
    }

    #[test]
    fn notional_reports_overflow_instead_of_wrapping() {
        assert_eq!(Price::MAX.notional(Qty::MAX), Err(Overflow));
        assert_eq!(Price::MAX.notional(Qty::whole(1)), Ok(Money::MAX));
    }

    #[test]
    fn checked_arithmetic_reports_the_edges() {
        assert_eq!(Qty::MAX.checked_add(Qty::from_raw(1)), Err(Overflow));
        assert_eq!(Qty::MIN.checked_sub(Qty::from_raw(1)), Err(Overflow));
        assert_eq!(Qty::whole(2).checked_sub(Qty::whole(3)), Ok(Qty::whole(-1)));
    }

    #[test]
    fn abs_saturates_at_min_rather_than_wrapping() {
        // i64::MIN.abs() would wrap to itself — negative. Saturating is one
        // unit wrong at a bound no position reaches; wrapping is sign-wrong.
        assert_eq!(Qty::MIN.abs(), Qty::MAX);
        assert_eq!(Qty::whole(-3).abs(), Qty::whole(3));
    }

    #[test]
    fn whole_saturates_rather_than_wrapping() {
        assert_eq!(Qty::whole(i64::MAX), Qty::MAX);
        assert_eq!(Qty::whole(i64::MIN), Qty::MIN);
    }

    #[test]
    fn parsing_is_available_in_const_context() {
        // The venue specs are consts. If parsing were not const they would be
        // lazily-initialised statics, and a malformed one would surface at
        // first use rather than at compile time.
        const TICK: Price = match Price::parse("0.5") {
            Ok(p) => p,
            Err(_) => panic!("bad const price"),
        };
        assert_eq!(TICK.raw(), 50_000_000);
    }
}

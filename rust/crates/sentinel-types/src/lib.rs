//! The units every other crate is built from.
//!
//! Three rules hold here and are inherited by everything above:
//!
//! **Money is fixed point.** [`Price`], [`Qty`] and [`Money`] are `i64` at 1e8.
//! There is no `Decimal` and no `f64` below the gateway, and the single place
//! two fixed-point values are multiplied — [`Price::notional`] — rounds toward
//! zero and says so.
//!
//! **Ids are inline and `Copy`.** A [`ClientOrderId`] is a fixed array, not a
//! pointer, so an event's payload cannot alias, dangle, or change between being
//! appended and being flushed.
//!
//! **No enum's zero value is valid.** A discriminant byte that was never
//! written decodes as an error rather than as the first variant.
//!
//! There are no dependencies, no clock, and no allocation on any path here.

#![forbid(unsafe_code)]

mod enums;
mod fixed;
mod ids;

pub use enums::{Authority, OrderKind, Side, TimeInForce, UnknownDiscriminant};
pub use fixed::{Money, Overflow, ParseError, Price, Qty, SCALE, SCALE_DIGITS, parse_e8};
pub use ids::{
    ClientOrderId, Epoch, ExecId, InlineStr, Instrument, Seq, TooLong, TraceId, VenueOrderId,
};

/// Nanoseconds since the Unix epoch.
///
/// The only representation of time below the gateway, and it never comes from a
/// clock down here — it arrives inside an event. `SystemTime::now()` in the
/// engine, the ledger or the domain is a bug, because reading a clock is
/// exactly what makes a replay stop reproducing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Nanos(u64);

impl Nanos {
    /// The Unix epoch itself. Used by replay and by tests, never by live code.
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Whole seconds since the epoch.
    #[must_use]
    pub const fn as_secs(self) -> u64 {
        #[expect(
            clippy::integer_division,
            reason = "seconds from nanoseconds; the remainder is deliberately dropped"
        )]
        let secs = self.0 / 1_000_000_000;
        secs
    }

    /// Nanoseconds elapsed since `earlier`, saturating at zero.
    ///
    /// Saturating rather than signed because time below the gateway is
    /// monotonic by construction — it only advances through `Ticked` — so a
    /// negative interval means the log is out of order, and every caller of
    /// this wants an age rather than a direction.
    #[must_use]
    pub const fn saturating_since(self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }

    /// This instant advanced by `nanos`.
    #[must_use]
    pub const fn saturating_add(self, nanos: u64) -> Self {
        Self(self.0.saturating_add(nanos))
    }
}

impl core::fmt::Display for Nanos {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nanos_measure_age_not_direction() {
        let t0 = Nanos::from_u64(1_000_000_000);
        let t1 = Nanos::from_u64(3_500_000_000);
        assert_eq!(t1.saturating_since(t0), 2_500_000_000);
        // Backwards is zero, not a negative or a wrap.
        assert_eq!(t0.saturating_since(t1), 0);
    }

    #[test]
    fn nanos_convert_to_seconds() {
        assert_eq!(Nanos::from_u64(3_999_999_999).as_secs(), 3);
        assert_eq!(Nanos::ZERO.as_secs(), 0);
    }

    #[test]
    fn the_pieces_compose_into_an_order() {
        // A smoke test that the vocabulary actually describes the thing it is
        // for: three BTCUSD contracts, bought, at a real price.
        let instrument = Instrument::new("BTCUSD").unwrap();
        let coid = ClientOrderId::new("UI-4bb5ba826e2e").unwrap();
        let qty = Qty::whole(3);
        let px = Price::parse("109432.5").unwrap();

        assert_eq!(instrument.as_str(), "BTCUSD");
        assert_eq!(coid.len(), 15);
        assert_eq!(px.notional(qty).unwrap().to_string(), "328297.5");
        assert_eq!(Side::Buy.sign() * qty.raw(), qty.raw());
    }
}

//! The closed vocabularies.
//!
//! Every enum here has an explicit discriminant starting at 1, and none of them
//! implements `Default`. A zero discriminant is reserved as "not a value", so a
//! record whose byte was never written decodes as an error rather than as
//! whichever variant happened to be listed first. `Side` defaulting to `Buy`
//! because a field was missed is the class of bug this prevents.

use core::fmt;

/// A byte that is not a valid discriminant for the type it was read as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownDiscriminant {
    /// The type the byte was being decoded as.
    pub type_name: &'static str,
    /// The byte.
    pub value: u8,
}

impl fmt::Display for UnknownDiscriminant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} is not a valid {}", self.value, self.type_name)
    }
}

impl core::error::Error for UnknownDiscriminant {}

/// Declare a wire-stable enum: explicit discriminants, no `Default`, a total
/// `TryFrom<u8>`, and `Display`/`FromStr` over the venue-facing spelling.
///
/// Exported because every crate above this one writes bytes to the same journal,
/// and an enum that decodes its own zero byte as a valid variant is the same bug
/// wherever it is declared.
///
/// Every path in the expansion is absolute. A crate that defines its own
/// `Result<T>` alias — which several above this one do — would otherwise have
/// the macro expand against it and fail to compile for reasons that point at
/// the wrong file.
///
/// ```
/// sentinel_types::wire_enum! {
///     /// Which way the tide is going.
///     Tide { In = 1 => "IN", Out = 2 => "OUT" }
/// }
/// assert!(Tide::from_u8(0).is_err());
/// assert_eq!(Tide::from_u8(2).unwrap(), Tide::Out);
/// ```
#[macro_export]
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $name:ident { $( $(#[$vmeta:meta])* $variant:ident = $disc:literal => $text:literal ),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        #[repr(u8)]
        pub enum $name {
            $( $(#[$vmeta])* $variant = $disc ),+
        }

        impl $name {
            /// Every variant, in discriminant order. The oracle for exhaustiveness
            /// tests — adding a variant without extending a match is caught by
            /// iterating this rather than by remembering to.
            pub const ALL: &'static [Self] = &[ $( Self::$variant ),+ ];

            /// The on-disk and on-wire discriminant.
            #[must_use]
            pub const fn as_u8(self) -> u8 {
                self as u8
            }

            /// The canonical text form.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $( Self::$variant => $text ),+ }
            }

            /// Decode a discriminant.
            ///
            /// # Errors
            /// [`$crate::UnknownDiscriminant`] for any byte not listed, zero included.
            pub const fn from_u8(v: u8) -> ::core::result::Result<Self, $crate::UnknownDiscriminant> {
                match v {
                    $( $disc => Ok(Self::$variant), )+
                    other => Err($crate::UnknownDiscriminant {
                        type_name: stringify!($name),
                        value: other,
                    }),
                }
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl TryFrom<u8> for $name {
            type Error = $crate::UnknownDiscriminant;
            fn try_from(v: u8) -> ::core::result::Result<Self, $crate::UnknownDiscriminant> {
                Self::from_u8(v)
            }
        }

        impl ::core::str::FromStr for $name {
            type Err = $crate::UnknownDiscriminant;
            fn from_str(s: &str) -> ::core::result::Result<Self, $crate::UnknownDiscriminant> {
                match s {
                    $( $text => Ok(Self::$variant), )+
                    _ => Err($crate::UnknownDiscriminant { type_name: stringify!($name), value: 0 }),
                }
            }
        }
    };
}

wire_enum! {
    /// Direction of an order. A *position* carries its direction in the sign of
    /// its [`crate::Qty`]; an order carries it here, so an order quantity is
    /// always positive and "negative size" is unrepresentable.
    Side {
        Buy = 1 => "BUY",
        Sell = 2 => "SELL",
    }
}

impl Side {
    /// The opposite direction — what flattens a position this side opened.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    /// `+1` for a buy, `-1` for a sell: the multiplier that turns an order
    /// quantity into a signed position delta.
    #[must_use]
    pub const fn sign(self) -> i64 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }
}

wire_enum! {
    /// Under whose authority an order exists.
    ///
    /// The distinction is not cosmetic. Protective exits run under their own
    /// supervisor and stay operational when entries are blocked or upstream
    /// components fail (R1.13), and exposure guards bound exits by the
    /// reconciled position rather than the requested one (R1.10). Both rules
    /// are keyed off this field.
    Authority {
        /// Opens or increases exposure. Subject to every entry guard.
        Entry = 1 => "ENTRY",
        /// Reduces exposure. Never blocked by an entry guard.
        ProtectiveExit = 2 => "PROTECTIVE_EXIT",
    }
}

impl Authority {
    /// Whether this authority may create new economic exposure.
    #[must_use]
    pub const fn opens_exposure(self) -> bool {
        matches!(self, Self::Entry)
    }
}

wire_enum! {
    /// How the venue should execute the order.
    OrderKind {
        /// Cross the spread now. Price is whatever the book gives.
        Market = 1 => "MARKET",
        /// Rest at a price, or better.
        Limit = 2 => "LIMIT",
        /// Rest at the venue as a stop; becomes a market order when triggered.
        /// The catastrophe backstop — it survives this process dying.
        StopMarket = 3 => "STOP_MARKET",
    }
}

impl OrderKind {
    /// Whether the venue needs a limit price for this kind.
    #[must_use]
    pub const fn needs_limit_price(self) -> bool {
        matches!(self, Self::Limit)
    }

    /// Whether the venue needs a trigger price for this kind.
    #[must_use]
    pub const fn needs_stop_price(self) -> bool {
        matches!(self, Self::StopMarket)
    }
}

wire_enum! {
    /// How long the order lives.
    TimeInForce {
        /// Rests until filled or cancelled.
        GoodTilCancelled = 1 => "GTC",
        /// Takes what is available now; the remainder is cancelled.
        ImmediateOrCancel = 2 => "IOC",
        /// All of it now, or none of it.
        FillOrKill = 3 => "FOK",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::str::FromStr;

    #[test]
    fn zero_is_never_a_valid_discriminant() {
        // The whole point: a byte that was never written decodes as an error.
        assert!(Side::from_u8(0).is_err());
        assert!(Authority::from_u8(0).is_err());
        assert!(OrderKind::from_u8(0).is_err());
        assert!(TimeInForce::from_u8(0).is_err());
    }

    #[test]
    fn discriminants_round_trip() {
        for &s in Side::ALL {
            assert_eq!(Side::from_u8(s.as_u8()).unwrap(), s);
        }
        for &a in Authority::ALL {
            assert_eq!(Authority::from_u8(a.as_u8()).unwrap(), a);
        }
        for &k in OrderKind::ALL {
            assert_eq!(OrderKind::from_u8(k.as_u8()).unwrap(), k);
        }
        for &t in TimeInForce::ALL {
            assert_eq!(TimeInForce::from_u8(t.as_u8()).unwrap(), t);
        }
    }

    #[test]
    fn text_round_trips() {
        for &s in Side::ALL {
            assert_eq!(Side::from_str(s.as_str()).unwrap(), s);
        }
        for &a in Authority::ALL {
            assert_eq!(Authority::from_str(a.as_str()).unwrap(), a);
        }
    }

    #[test]
    fn discriminants_are_pinned() {
        // These bytes are written to a log that is the system of record. A
        // renumbering reinterprets every record already on disk, so the values
        // are asserted here rather than left to declaration order.
        assert_eq!(Side::Buy.as_u8(), 1);
        assert_eq!(Side::Sell.as_u8(), 2);
        assert_eq!(Authority::Entry.as_u8(), 1);
        assert_eq!(Authority::ProtectiveExit.as_u8(), 2);
        assert_eq!(OrderKind::Market.as_u8(), 1);
        assert_eq!(OrderKind::Limit.as_u8(), 2);
        assert_eq!(OrderKind::StopMarket.as_u8(), 3);
    }

    #[test]
    fn side_flips_and_signs() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
        assert_eq!(Side::Buy.sign(), 1);
        assert_eq!(Side::Sell.sign(), -1);
        for &s in Side::ALL {
            assert_eq!(s.opposite().opposite(), s);
        }
    }

    #[test]
    fn only_entries_open_exposure() {
        assert!(Authority::Entry.opens_exposure());
        assert!(!Authority::ProtectiveExit.opens_exposure());
    }

    #[test]
    fn each_kind_declares_the_prices_it_needs() {
        assert!(!OrderKind::Market.needs_limit_price());
        assert!(OrderKind::Limit.needs_limit_price());
        assert!(OrderKind::StopMarket.needs_stop_price());
        assert!(!OrderKind::Limit.needs_stop_price());
    }
}

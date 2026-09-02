//! First-class product definitions (R4).
//!
//! Settlement, exercise style and expiration are configuration the domain
//! reads, never `if symbol == ...` scattered through logic. The difference
//! between a cash-settled European index option and a physically delivered
//! American equity option is data here and behaviour everywhere else.
//!
//! Delta India lists perpetual futures, dated futures and options on the same
//! account, and they do not expire, settle or assign alike. Modelling that as
//! configuration is what keeps a perpetual from acquiring an expiry branch.

use sentinel_types::{Instrument, Nanos, Price, Qty};

use sentinel_types::wire_enum;

wire_enum! {
    /// What the contract pays at the end.
    Settlement {
        /// Settles to cash. No delivery leg.
        Cash = 1 => "CASH",
        /// Delivers the underlying. Assignment creates positions.
        Physical = 2 => "PHYSICAL",
    }
}

wire_enum! {
    /// When the holder may exercise.
    Exercise {
        /// Only at expiration.
        European = 1 => "EUROPEAN",
        /// Any time — early assignment is possible.
        American = 2 => "AMERICAN",
        /// Never. Futures and perpetuals do not have an exercise decision, and
        /// giving them one would mean every exercise path had a "not
        /// applicable" branch running through it.
        NotApplicable = 3 => "NONE",
    }
}

wire_enum! {
    /// What kind of contract this is.
    ContractKind {
        /// No expiry. Funding is exchanged periodically instead.
        Perpetual = 1 => "PERPETUAL",
        /// Expires and settles on a date.
        Future = 2 => "FUTURE",
        /// Expires, and carries an exercise decision.
        Option = 3 => "OPTION",
    }
}

/// When a contract stops being tradable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiration {
    /// It does not. A perpetual.
    Never,
    /// At an instant.
    At(Nanos),
}

/// Everything about an instrument that changes how it behaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductDefinition {
    /// The venue's name for it.
    pub instrument: Instrument,
    /// Perpetual, dated future, or option.
    pub contract: ContractKind,
    /// Cash or physical.
    pub settlement: Settlement,
    /// Exercise style, or that the question does not apply.
    pub exercise: Exercise,
    /// Units of underlying per contract.
    pub multiplier: Qty,
    /// Smallest price increment the venue accepts.
    pub tick_size: Price,
    /// Smallest quantity increment the venue accepts.
    pub lot_size: Qty,
    /// When it stops trading.
    pub expiration: Expiration,
    /// When it was listed. Reads before this are refused by the replay contract
    /// (R4) — a backtest that can see a contract before it existed is a
    /// backtest that has already lied to you once.
    pub listed_at: Nanos,
}

/// A product definition that does not describe a tradable thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductError {
    /// Multiplier, tick or lot was zero or negative.
    NonPositiveIncrement,
    /// A perpetual with an expiry, or a dated contract without one.
    ExpirationContradictsKind,
    /// An option with no exercise style, or a future with one.
    ExerciseContradictsKind,
    /// The contract expires before it was listed.
    ExpiresBeforeListing,
}

impl core::fmt::Display for ProductError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let s = match self {
            Self::NonPositiveIncrement => "multiplier, tick and lot must be positive",
            Self::ExpirationContradictsKind => {
                "a perpetual cannot expire, and a dated contract must"
            }
            Self::ExerciseContradictsKind => {
                "an option needs an exercise style, and a future cannot have one"
            }
            Self::ExpiresBeforeListing => "expiry precedes listing",
        };
        f.write_str(s)
    }
}

impl core::error::Error for ProductError {}

impl ProductDefinition {
    /// Validate a definition.
    ///
    /// # Errors
    /// [`ProductError`] when the fields contradict each other. A perpetual with
    /// an expiry is not a thing that can be traded correctly by code that
    /// believes either field.
    pub const fn validate(&self) -> Result<(), ProductError> {
        if !self.multiplier.is_positive()
            || !self.tick_size.is_positive()
            || !self.lot_size.is_positive()
        {
            return Err(ProductError::NonPositiveIncrement);
        }
        match (self.contract, self.expiration) {
            (ContractKind::Perpetual, Expiration::At(_))
            | (ContractKind::Future | ContractKind::Option, Expiration::Never) => {
                return Err(ProductError::ExpirationContradictsKind);
            }
            _ => {}
        }
        match (self.contract, self.exercise) {
            (ContractKind::Option, Exercise::NotApplicable)
            | (
                ContractKind::Perpetual | ContractKind::Future,
                Exercise::European | Exercise::American,
            ) => return Err(ProductError::ExerciseContradictsKind),
            _ => {}
        }
        if let Expiration::At(t) = self.expiration
            && t.as_u64() <= self.listed_at.as_u64()
        {
            return Err(ProductError::ExpiresBeforeListing);
        }
        Ok(())
    }

    /// Whether a short position can be assigned before expiry.
    ///
    /// A risk path that simply does not exist for European or non-option
    /// products, and must therefore be modelled, monitored and tested
    /// separately rather than guarded by a symbol check.
    #[must_use]
    pub const fn early_assignment_possible(&self) -> bool {
        matches!(self.exercise, Exercise::American)
    }

    /// Whether holding this to expiry can produce an underlying position.
    #[must_use]
    pub const fn has_delivery_risk(&self) -> bool {
        matches!(self.settlement, Settlement::Physical)
    }

    /// Whether the contract is tradable at `now`.
    ///
    /// The contract-lifetime gate (R4): an instrument is queryable only within
    /// its listed lifetime, and the check lives on the product rather than at
    /// each call site so there is one place for it to be right.
    #[must_use]
    pub const fn is_tradable_at(&self, now: Nanos) -> bool {
        if now.as_u64() < self.listed_at.as_u64() {
            return false;
        }
        match self.expiration {
            Expiration::Never => true,
            Expiration::At(t) => now.as_u64() < t.as_u64(),
        }
    }

    /// Whether funding is exchanged on this contract instead of it expiring.
    #[must_use]
    pub const fn accrues_funding(&self) -> bool {
        matches!(self.contract, ContractKind::Perpetual)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perp() -> ProductDefinition {
        ProductDefinition {
            instrument: Instrument::new("BTCUSD").unwrap(),
            contract: ContractKind::Perpetual,
            settlement: Settlement::Cash,
            exercise: Exercise::NotApplicable,
            multiplier: Qty::parse("0.001").unwrap(),
            tick_size: Price::parse("0.5").unwrap(),
            lot_size: Qty::whole(1),
            expiration: Expiration::Never,
            listed_at: Nanos::from_u64(1_000),
        }
    }

    fn american_option() -> ProductDefinition {
        ProductDefinition {
            contract: ContractKind::Option,
            settlement: Settlement::Physical,
            exercise: Exercise::American,
            expiration: Expiration::At(Nanos::from_u64(9_000)),
            ..perp()
        }
    }

    #[test]
    fn a_perpetual_validates_and_accrues_funding() {
        let p = perp();
        p.validate().unwrap();
        assert!(p.accrues_funding());
        assert!(!p.early_assignment_possible());
        assert!(!p.has_delivery_risk());
    }

    #[test]
    fn a_perpetual_that_expires_is_refused() {
        let p = ProductDefinition {
            expiration: Expiration::At(Nanos::from_u64(9_000)),
            ..perp()
        };
        assert_eq!(p.validate(), Err(ProductError::ExpirationContradictsKind));
    }

    #[test]
    fn a_dated_contract_without_an_expiry_is_refused() {
        let p = ProductDefinition {
            contract: ContractKind::Future,
            expiration: Expiration::Never,
            ..perp()
        };
        assert_eq!(p.validate(), Err(ProductError::ExpirationContradictsKind));
    }

    #[test]
    fn exercise_style_must_match_the_contract_kind() {
        let opt_without_style = ProductDefinition {
            exercise: Exercise::NotApplicable,
            ..american_option()
        };
        assert_eq!(
            opt_without_style.validate(),
            Err(ProductError::ExerciseContradictsKind)
        );

        let perp_with_style = ProductDefinition {
            exercise: Exercise::European,
            ..perp()
        };
        assert_eq!(
            perp_with_style.validate(),
            Err(ProductError::ExerciseContradictsKind)
        );
    }

    #[test]
    fn increments_must_be_positive() {
        for p in [
            ProductDefinition {
                multiplier: Qty::ZERO,
                ..perp()
            },
            ProductDefinition {
                tick_size: Price::ZERO,
                ..perp()
            },
            ProductDefinition {
                lot_size: Qty::whole(-1),
                ..perp()
            },
        ] {
            assert_eq!(p.validate(), Err(ProductError::NonPositiveIncrement));
        }
    }

    #[test]
    fn a_contract_cannot_expire_before_it_was_listed() {
        let p = ProductDefinition {
            contract: ContractKind::Future,
            expiration: Expiration::At(Nanos::from_u64(500)),
            ..perp()
        };
        assert_eq!(p.validate(), Err(ProductError::ExpiresBeforeListing));
    }

    /// R4 — the contract-lifetime gate, both ends.
    #[test]
    fn a_contract_is_untradable_outside_its_lifetime() {
        let p = ProductDefinition {
            contract: ContractKind::Future,
            exercise: Exercise::NotApplicable,
            expiration: Expiration::At(Nanos::from_u64(9_000)),
            ..perp()
        };
        p.validate().unwrap();
        assert!(!p.is_tradable_at(Nanos::from_u64(999)), "before listing");
        assert!(p.is_tradable_at(Nanos::from_u64(1_000)), "at listing");
        assert!(p.is_tradable_at(Nanos::from_u64(8_999)));
        assert!(!p.is_tradable_at(Nanos::from_u64(9_000)), "at expiry");
        assert!(!p.is_tradable_at(Nanos::from_u64(9_001)), "after expiry");
    }

    #[test]
    fn a_perpetual_is_tradable_forever_but_not_before_listing() {
        let p = perp();
        assert!(!p.is_tradable_at(Nanos::from_u64(999)));
        assert!(p.is_tradable_at(Nanos::from_u64(u64::MAX)));
    }

    #[test]
    fn an_american_option_carries_early_assignment_and_delivery_risk() {
        let p = american_option();
        p.validate().unwrap();
        assert!(p.early_assignment_possible());
        assert!(p.has_delivery_risk());
        assert!(!p.accrues_funding());

        // The European twin differs only in data, and only in the risk path.
        let euro = ProductDefinition {
            exercise: Exercise::European,
            ..p
        };
        euro.validate().unwrap();
        assert!(!euro.early_assignment_possible());
        assert!(euro.has_delivery_risk());
    }
}

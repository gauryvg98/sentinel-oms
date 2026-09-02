//! Symbol rules, and the grid an order has to sit on.
//!
//! Binance futures size in the base asset — 0.003 BTC, not three contracts — so
//! unlike Delta there is no contract conversion. What there is instead is a
//! **step**: `LOT_SIZE.stepSize` says the quantity must be a whole multiple of
//! it, and an order that is not is refused with `-1111` and nothing useful.
//!
//! Snapping is to a multiple of the step, **not** to its decimal places. Those
//! coincide for a step like `0.001` and do not for one like `0.0025`, and the
//! second case is the one that silently places an order the venue will refuse.
//!
//! All of it read from `/fapi/v1/exchangeInfo` and never hardcoded. A constant
//! here would be right until the day Binance retunes a filter, and wrong in a
//! way that only shows up in the rejection.

use std::collections::HashMap;

use sentinel_types::{Instrument, Money, Price, Qty};

/// What the venue will accept for one symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolRules {
    /// The symbol.
    pub symbol: Instrument,
    /// Quantity must be a whole multiple of this (`LOT_SIZE.stepSize`).
    pub step_size: Qty,
    /// Smallest quantity the venue will take (`LOT_SIZE.minQty`).
    pub min_qty: Qty,
    /// Price must be a whole multiple of this (`PRICE_FILTER.tickSize`).
    pub tick_size: Price,
    /// Smallest order value (`MIN_NOTIONAL.notional`).
    ///
    /// The one that catches a correctly-stepped order that is simply too small
    /// to be worth the venue's time — `-4164`, which reads like a size problem
    /// and is a value problem.
    pub min_notional: Money,
    /// Decimal places the venue will accept in a quantity (`quantityPrecision`).
    pub quantity_precision: u32,
}

/// Why an order would be refused before it is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unplaceable {
    /// Not a whole multiple of the step.
    OffStep {
        /// What was asked for.
        qty: Qty,
        /// The grid it has to sit on.
        step: Qty,
    },
    /// Below the venue's minimum quantity.
    BelowMinQty {
        /// What was asked for.
        qty: Qty,
        /// The minimum.
        min: Qty,
    },
    /// Worth less than the venue's minimum notional.
    BelowMinNotional {
        /// What the order is worth.
        notional: Money,
        /// The minimum.
        min: Money,
    },
    /// Zero or negative.
    NonPositive,
}

impl core::fmt::Display for Unplaceable {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OffStep { qty, step } => {
                write!(f, "qty {qty} is not a multiple of the step {step}")
            }
            Self::BelowMinQty { qty, min } => {
                write!(f, "qty {qty} is below the minimum {min}")
            }
            Self::BelowMinNotional { notional, min } => {
                write!(f, "notional {notional} is below the minimum {min}")
            }
            Self::NonPositive => f.write_str("qty must be positive"),
        }
    }
}

impl core::error::Error for Unplaceable {}

impl SymbolRules {
    /// Whether the venue will take this order.
    ///
    /// Checked before sending rather than after being refused: a rejection
    /// costs a round trip, a log line and an order that has to be reconciled,
    /// and every one of these conditions is knowable here.
    ///
    /// # Errors
    /// [`Unplaceable`] saying which rule it breaks.
    pub fn check(&self, qty: Qty, price: Price) -> Result<(), Unplaceable> {
        if !qty.is_positive() {
            return Err(Unplaceable::NonPositive);
        }
        if self.step_size.is_positive() && qty.raw() % self.step_size.raw() != 0 {
            return Err(Unplaceable::OffStep {
                qty,
                step: self.step_size,
            });
        }
        if qty < self.min_qty {
            return Err(Unplaceable::BelowMinQty {
                qty,
                min: self.min_qty,
            });
        }
        if self.min_notional.is_positive() && price.is_positive() {
            let notional = price.notional(qty).unwrap_or(Money::ZERO);
            if notional < self.min_notional {
                return Err(Unplaceable::BelowMinNotional {
                    notional,
                    min: self.min_notional,
                });
            }
        }
        Ok(())
    }

    /// The largest placeable quantity at or below `qty`.
    ///
    /// Snapped to a *multiple of the step*, not to its decimal places. Those
    /// coincide for a step of `0.001` and do not for one of `0.0025`, and the
    /// second is the case that silently places an order the venue refuses.
    ///
    /// Always rounds down. Risk sizing produces arbitrary decimals, and a
    /// hundredth light is free while a hundredth heavy is the side that fails a
    /// margin check.
    #[must_use]
    pub fn round_down(&self, qty: Qty) -> Qty {
        if !qty.is_positive() || !self.step_size.is_positive() {
            return Qty::ZERO;
        }
        Qty::from_raw(qty.raw() - qty.raw() % self.step_size.raw())
    }

    /// The nearest price at or below `price` that sits on the tick grid.
    #[must_use]
    pub fn round_price_down(&self, price: Price) -> Price {
        if !price.is_positive() || !self.tick_size.is_positive() {
            return Price::ZERO;
        }
        Price::from_raw(price.raw() - price.raw() % self.tick_size.raw())
    }
}

/// The symbols this adapter knows about.
#[derive(Debug, Default)]
pub struct Symbols {
    rules: HashMap<Instrument, SymbolRules>,
}

impl Symbols {
    /// Empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a symbol's rules.
    pub fn insert(&mut self, rules: SymbolRules) {
        self.rules.insert(rules.symbol, rules);
    }

    /// By symbol.
    #[must_use]
    pub fn get(&self, symbol: &Instrument) -> Option<SymbolRules> {
        self.rules.get(symbol).copied()
    }

    /// How many are known.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether none are known.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Every symbol, sorted.
    #[must_use]
    pub fn all(&self) -> Vec<Instrument> {
        let mut out: Vec<_> = self.rules.keys().copied().collect();
        out.sort_unstable();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// BTCUSDT as Binance actually filters it.
    fn btcusdt() -> SymbolRules {
        SymbolRules {
            symbol: Instrument::new("BTCUSDT").unwrap(),
            step_size: Qty::parse("0.001").unwrap(),
            min_qty: Qty::parse("0.001").unwrap(),
            tick_size: Price::parse("0.1").unwrap(),
            min_notional: Money::whole(100),
            quantity_precision: 3,
        }
    }

    fn px(s: &str) -> Price {
        Price::parse(s).unwrap()
    }

    #[test]
    fn a_well_formed_order_passes() {
        assert_eq!(
            btcusdt().check(Qty::parse("0.003").unwrap(), px("109432.5")),
            Ok(())
        );
    }

    #[test]
    fn an_off_step_quantity_is_caught_before_it_is_sent() {
        // Binance refuses this with -1111 and nothing useful. Catching it here
        // costs no round trip and no order to reconcile afterwards.
        assert_eq!(
            btcusdt().check(Qty::parse("0.0035").unwrap(), px("109432.5")),
            Err(Unplaceable::OffStep {
                qty: Qty::parse("0.0035").unwrap(),
                step: Qty::parse("0.001").unwrap(),
            })
        );
    }

    #[test]
    fn a_quantity_below_the_minimum_is_caught() {
        let rules = SymbolRules {
            min_qty: Qty::parse("0.002").unwrap(),
            ..btcusdt()
        };
        assert!(matches!(
            rules.check(Qty::parse("0.001").unwrap(), px("109432.5")),
            Err(Unplaceable::BelowMinQty { .. })
        ));
    }

    #[test]
    fn an_order_worth_too_little_is_caught() {
        // -4164 reads like a size problem and is a value problem: the quantity
        // is on the grid and above the minimum, and the order is still refused.
        assert!(matches!(
            btcusdt().check(Qty::parse("0.001").unwrap(), px("50")),
            Err(Unplaceable::BelowMinNotional { .. })
        ));
    }

    #[test]
    fn a_non_positive_quantity_is_refused() {
        for qty in [Qty::ZERO, Qty::whole(-1)] {
            assert_eq!(
                btcusdt().check(qty, px("109432.5")),
                Err(Unplaceable::NonPositive)
            );
        }
    }

    #[test]
    fn rounding_snaps_to_a_multiple_of_the_step() {
        let rules = btcusdt();
        assert_eq!(
            rules.round_down(Qty::parse("0.0035").unwrap()),
            Qty::parse("0.003").unwrap()
        );
        assert_eq!(
            rules.round_down(Qty::parse("0.003").unwrap()),
            Qty::parse("0.003").unwrap()
        );
        assert_eq!(rules.round_down(Qty::parse("0.0009").unwrap()), Qty::ZERO);
        assert_eq!(rules.round_down(Qty::whole(-1)), Qty::ZERO);
    }

    #[test]
    fn rounding_is_to_the_step_not_to_its_decimal_places() {
        // The case the distinction exists for. A step of 0.0025 has four
        // decimal places, so snapping to decimals would accept 0.0031 — which
        // has four places and is not a multiple of anything.
        let rules = SymbolRules {
            step_size: Qty::parse("0.0025").unwrap(),
            min_qty: Qty::parse("0.0025").unwrap(),
            min_notional: Money::ZERO,
            ..btcusdt()
        };
        assert_eq!(
            rules.round_down(Qty::parse("0.0031").unwrap()),
            Qty::parse("0.0025").unwrap()
        );
        assert!(rules.check(Qty::parse("0.0031").unwrap(), px("1")).is_err());
        assert!(rules.check(Qty::parse("0.005").unwrap(), px("1")).is_ok());
    }

    #[test]
    fn a_rounded_quantity_is_always_on_the_grid() {
        let rules = SymbolRules {
            min_qty: Qty::ZERO,
            min_notional: Money::ZERO,
            ..btcusdt()
        };
        for raw in [1i64, 99_999, 350_000, 1_234_567, 99_999_999] {
            let rounded = rules.round_down(Qty::from_raw(raw));
            if rounded.is_positive() {
                assert_eq!(
                    rules.check(rounded, px("109432.5")),
                    Ok(()),
                    "{rounded} did not land on the grid"
                );
            }
        }
    }

    #[test]
    fn prices_snap_to_the_tick() {
        let rules = btcusdt();
        assert_eq!(rules.round_price_down(px("109432.55")), px("109432.5"));
        assert_eq!(rules.round_price_down(px("109432.5")), px("109432.5"));
    }

    #[test]
    fn symbols_are_findable_and_listed_in_a_stable_order() {
        let mut symbols = Symbols::new();
        assert!(symbols.is_empty());
        for name in ["ETHUSDT", "BTCUSDT"] {
            symbols.insert(SymbolRules {
                symbol: Instrument::new(name).unwrap(),
                ..btcusdt()
            });
        }
        assert_eq!(symbols.len(), 2);
        assert!(symbols.get(&Instrument::new("BTCUSDT").unwrap()).is_some());
        assert!(symbols.get(&Instrument::new("SOLUSDT").unwrap()).is_none());
        let names: Vec<_> = symbols.all().iter().map(ToString::to_string).collect();
        assert_eq!(names, ["BTCUSDT", "ETHUSDT"]);
    }
}

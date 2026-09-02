//! Products, and the conversion that gets people 100x wrong.
//!
//! Delta sizes orders in **contracts**, integers. Sentinel deals in decimal base
//! quantity everywhere else. One contract of `BTCUSD` is `contract_value` of
//! BTC — 0.001 — so three contracts is 0.003 BTC, and confusing the two is a
//! thousand-fold error in position size.
//!
//! So the conversion refuses rather than rounds. A quantity that is not an exact
//! multiple of the contract value is a bug upstream, and rounding it silently
//! changes real exposure by an amount nobody chose.
//!
//! `contract_value` and `product_id` are read from the venue and never
//! hardcoded. A constant here would be right until the day the venue relists
//! something, and wrong in a way that only shows up in the fill.

use std::collections::HashMap;

use sentinel_types::{Instrument, Price, Qty};

/// What the venue says about one tradable product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Product {
    /// The venue's numeric id, needed to cancel and to set leverage.
    pub id: i64,
    /// The symbol.
    pub symbol: Instrument,
    /// Base units per contract.
    pub contract_value: Qty,
    /// Smallest price increment.
    pub tick_size: Price,
}

/// A quantity that is not a whole number of contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffGrid {
    /// What was asked for.
    pub qty: Qty,
    /// The grid it had to sit on.
    pub contract_value: Qty,
}

impl core::fmt::Display for OffGrid {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "qty {} is not a multiple of the contract value {}",
            self.qty, self.contract_value
        )
    }
}

impl core::error::Error for OffGrid {}

impl Product {
    /// Base quantity to a whole number of contracts.
    ///
    /// # Errors
    /// [`OffGrid`] when the quantity is not an exact multiple. Refused, never
    /// rounded: a silent round changes real position size, and at a contract
    /// value of 0.001 the direction of the rounding decides whether the account
    /// is over-exposed or under-filled.
    pub fn to_contracts(&self, qty: Qty) -> Result<i64, OffGrid> {
        if !self.contract_value.is_positive() {
            return Err(OffGrid {
                qty,
                contract_value: self.contract_value,
            });
        }
        let numerator = i128::from(qty.raw());
        let denominator = i128::from(self.contract_value.raw());
        if numerator % denominator != 0 {
            return Err(OffGrid {
                qty,
                contract_value: self.contract_value,
            });
        }
        #[expect(
            clippy::integer_division,
            reason = "exact by the remainder check on the line above"
        )]
        let contracts = numerator / denominator;
        i64::try_from(contracts).map_err(|_| OffGrid {
            qty,
            contract_value: self.contract_value,
        })
    }

    /// Contracts back to base quantity. Signed, because a position is.
    #[must_use]
    pub fn to_qty(&self, contracts: i64) -> Qty {
        let product = i128::from(contracts) * i128::from(self.contract_value.raw());
        i64::try_from(product).map_or(Qty::ZERO, Qty::from_raw)
    }

    /// The largest quantity at or below `qty` that sits on the contract grid.
    ///
    /// For a caller that has computed a size and needs a *placeable* one — risk
    /// sizing produces arbitrary decimals, and rounding down is the safe
    /// direction. Distinct from [`Self::to_contracts`] on purpose: this one is
    /// asked to round, and that one is asked not to.
    #[must_use]
    pub fn round_down(&self, qty: Qty) -> Qty {
        if !self.contract_value.is_positive() || !qty.is_positive() {
            return Qty::ZERO;
        }
        let remainder = qty.raw() % self.contract_value.raw();
        Qty::from_raw(qty.raw() - remainder)
    }
}

/// The products this adapter knows about.
///
/// Loaded from the venue on demand and never guessed at.
#[derive(Debug, Default)]
pub struct Products {
    by_symbol: HashMap<Instrument, Product>,
    by_id: HashMap<i64, Product>,
}

impl Products {
    /// Empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a product.
    pub fn insert(&mut self, product: Product) {
        self.by_symbol.insert(product.symbol, product);
        self.by_id.insert(product.id, product);
    }

    /// By symbol.
    #[must_use]
    pub fn get(&self, symbol: &Instrument) -> Option<Product> {
        self.by_symbol.get(symbol).copied()
    }

    /// By the venue's numeric id — which is how the user stream identifies
    /// things, since its frames carry `product_id` and not always a symbol.
    #[must_use]
    pub fn by_id(&self, id: i64) -> Option<Product> {
        self.by_id.get(&id).copied()
    }

    /// How many are known.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_symbol.len()
    }

    /// Whether none are known.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_symbol.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn btcusd() -> Product {
        Product {
            id: 27,
            symbol: Instrument::new("BTCUSD").unwrap(),
            // Delta's BTCUSD perpetual: one contract is 0.001 BTC.
            contract_value: Qty::parse("0.001").unwrap(),
            tick_size: Price::parse("0.5").unwrap(),
        }
    }

    #[test]
    fn base_quantity_converts_to_whole_contracts() {
        let p = btcusd();
        assert_eq!(p.to_contracts(Qty::parse("0.003").unwrap()).unwrap(), 3);
        assert_eq!(p.to_contracts(Qty::parse("0.001").unwrap()).unwrap(), 1);
        assert_eq!(p.to_contracts(Qty::ZERO).unwrap(), 0);
        assert_eq!(p.to_contracts(Qty::parse("1").unwrap()).unwrap(), 1_000);
    }

    #[test]
    fn an_off_grid_quantity_is_refused_not_rounded() {
        // The thousand-fold error, prevented at the boundary. 0.0035 BTC is
        // three and a half contracts, and neither three nor four is what the
        // caller asked for.
        let p = btcusd();
        assert!(p.to_contracts(Qty::parse("0.0035").unwrap()).is_err());
        assert!(p.to_contracts(Qty::parse("0.00000001").unwrap()).is_err());
    }

    #[test]
    fn contracts_convert_back_to_base_quantity() {
        let p = btcusd();
        assert_eq!(p.to_qty(3), Qty::parse("0.003").unwrap());
        assert_eq!(
            p.to_qty(-5),
            Qty::parse("-0.005").unwrap(),
            "a short position"
        );
        assert_eq!(p.to_qty(0), Qty::ZERO);
    }

    #[test]
    fn the_conversion_round_trips() {
        let p = btcusd();
        for contracts in [-1_000i64, -1, 0, 1, 3, 76, 100_000] {
            assert_eq!(p.to_contracts(p.to_qty(contracts)).unwrap(), contracts);
        }
    }

    #[test]
    fn rounding_down_is_a_separate_and_explicit_request() {
        // Risk sizing produces arbitrary decimals. This is the one place a
        // caller may ask for them to be made placeable, and it always rounds
        // toward less exposure.
        let p = btcusd();
        assert_eq!(
            p.round_down(Qty::parse("0.0035").unwrap()),
            Qty::parse("0.003").unwrap()
        );
        assert_eq!(
            p.round_down(Qty::parse("0.003").unwrap()),
            Qty::parse("0.003").unwrap()
        );
        assert_eq!(p.round_down(Qty::parse("0.0009").unwrap()), Qty::ZERO);
        assert_eq!(p.round_down(Qty::whole(-1)), Qty::ZERO, "unsigned only");
    }

    #[test]
    fn a_rounded_quantity_is_always_placeable() {
        let p = btcusd();
        for raw in [1i64, 99_999, 350_000, 1_234_567, 99_999_999] {
            let rounded = p.round_down(Qty::from_raw(raw));
            assert!(
                p.to_contracts(rounded).is_ok(),
                "{rounded} did not land on the grid"
            );
        }
    }

    #[test]
    fn a_product_is_findable_by_symbol_and_by_id() {
        // The user stream identifies instruments by numeric id, so both
        // directions have to work off one load.
        let mut products = Products::new();
        assert!(products.is_empty());
        products.insert(btcusd());
        assert_eq!(products.len(), 1);
        assert_eq!(
            products.get(&Instrument::new("BTCUSD").unwrap()),
            Some(btcusd())
        );
        assert_eq!(products.by_id(27), Some(btcusd()));
        assert_eq!(products.by_id(28), None);
        assert_eq!(products.get(&Instrument::new("ETHUSD").unwrap()), None);
    }

    #[test]
    fn a_product_with_no_contract_value_converts_nothing() {
        let broken = Product {
            contract_value: Qty::ZERO,
            ..btcusd()
        };
        assert!(broken.to_contracts(Qty::whole(1)).is_err());
        assert_eq!(broken.round_down(Qty::whole(1)), Qty::ZERO);
    }
}

//! Position accounting: signed size, cost basis, realised P&L.
//!
//! Cost basis is stored rather than average price. An average is a division,
//! and storing one means every subsequent fill compounds the rounding of every
//! fill before it. Storing the total cost means there is exactly one division
//! in the whole model — the one that closes part of a position — and it is
//! named, documented, and tested at its edges.

use sentinel_types::{Money, Price, Qty, Side};

/// What is held in one instrument, and what it has cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Position {
    /// Signed size. Negative is short.
    pub qty: Qty,
    /// Total cost of the open size, in quote currency. Signed the same way as
    /// `qty`: a short position has a negative basis, being money received
    /// rather than money paid.
    pub cost_basis: Money,
    /// Realised profit and loss, cumulative and never reset.
    pub realized: Money,
}

/// Position arithmetic that could not be represented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionOverflow;

impl core::fmt::Display for PositionOverflow {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("position arithmetic overflowed")
    }
}

impl core::error::Error for PositionOverflow {}

impl Position {
    /// Whether nothing is held.
    #[must_use]
    pub const fn is_flat(&self) -> bool {
        self.qty.is_zero()
    }

    /// Size without direction.
    #[must_use]
    pub const fn abs_qty(&self) -> Qty {
        self.qty.abs()
    }

    /// Average entry price of the open size, or `None` when flat.
    ///
    /// Derived on demand rather than stored, so it is always consistent with
    /// the basis it comes from and never accumulates error of its own.
    #[must_use]
    pub fn avg_entry(&self) -> Option<Price> {
        if self.qty.is_zero() {
            return None;
        }
        // Both are signed the same way, so the quotient is positive.
        let scaled = i128::from(self.cost_basis.raw()) * i128::from(sentinel_types::SCALE);
        #[expect(
            clippy::integer_division,
            reason = "cost per unit; truncation toward zero is the documented rule, \
                      and the value is a display and P&L input, never a size"
        )]
        let per_unit = scaled / i128::from(self.qty.raw());
        i64::try_from(per_unit).map(Price::from_raw).ok()
    }

    /// Unrealised profit and loss at `mark`.
    #[must_use]
    pub fn unrealized(&self, mark: Price) -> Money {
        if self.qty.is_zero() {
            return Money::ZERO;
        }
        let value = mark.notional(self.qty).unwrap_or(Money::ZERO);
        value.checked_sub(self.cost_basis).unwrap_or(Money::ZERO)
    }

    /// Apply a fill.
    ///
    /// Three cases, and the third is the one that gets written wrong: a fill
    /// larger than the open position closes it *and opens the other side*, and
    /// the opening leg must be priced at this fill rather than inheriting the
    /// old basis.
    ///
    /// # Errors
    /// [`PositionOverflow`] when the arithmetic leaves the representable range.
    /// Reported rather than wrapped — a wrapped position is worse than a
    /// stopped system.
    pub fn apply_fill(
        &mut self,
        side: Side,
        qty: Qty,
        price: Price,
    ) -> Result<(), PositionOverflow> {
        let signed = Qty::from_raw(qty.raw().checked_mul(side.sign()).ok_or(PositionOverflow)?);
        let cost = price.notional(qty).map_err(|_| PositionOverflow)?;
        let signed_cost = Money::from_raw(
            cost.raw()
                .checked_mul(side.sign())
                .ok_or(PositionOverflow)?,
        );

        let opening = self.qty.is_zero() || (self.qty.is_positive() == signed.is_positive());
        if opening {
            self.qty = self.qty.checked_add(signed).map_err(|_| PositionOverflow)?;
            self.cost_basis = self
                .cost_basis
                .checked_add(signed_cost)
                .map_err(|_| PositionOverflow)?;
            return Ok(());
        }

        // Reducing. Close up to what is actually held.
        let held = self.qty.abs();
        let closing = qty.min(held);
        // A magnitude, not a signed value: the direction is applied once, below.
        let closed_cost = self.slice_of_basis(closing, held)?;
        let proceeds = price.notional(closing).map_err(|_| PositionOverflow)?;

        // The position's direction decides the sign of the result. Closing a
        // long books (exit - entry); closing a short books (entry - exit); the
        // two are one expression with the sign flipped.
        let direction = if self.qty.is_positive() { 1 } else { -1 };
        let gross = proceeds
            .checked_sub(closed_cost)
            .map_err(|_| PositionOverflow)?;
        let pnl = Money::from_raw(gross.raw().checked_mul(direction).ok_or(PositionOverflow)?);
        self.realized = self
            .realized
            .checked_add(pnl)
            .map_err(|_| PositionOverflow)?;

        // The basis is signed the same way the position is, so removing the
        // closed share means removing `direction * closed_cost`.
        self.cost_basis = self
            .cost_basis
            .checked_sub(Money::from_raw(
                closed_cost
                    .raw()
                    .checked_mul(direction)
                    .ok_or(PositionOverflow)?,
            ))
            .map_err(|_| PositionOverflow)?;
        self.qty = self
            .qty
            .checked_sub(Qty::from_raw(
                closing
                    .raw()
                    .checked_mul(direction)
                    .ok_or(PositionOverflow)?,
            ))
            .map_err(|_| PositionOverflow)?;

        if self.qty.is_zero() {
            // Flat means flat. Rounding in `slice_of_basis` can leave a unit or
            // two behind, and a residual basis on a zero position is reported
            // as unrealised P&L on nothing.
            self.cost_basis = Money::ZERO;
        }

        // A fill larger than the position flips it. The new side opens at this
        // fill's price, carrying nothing across.
        let flipped = qty.checked_sub(closing).map_err(|_| PositionOverflow)?;
        if flipped.is_positive() {
            let opened = price.notional(flipped).map_err(|_| PositionOverflow)?;
            self.qty = Qty::from_raw(
                flipped
                    .raw()
                    .checked_mul(side.sign())
                    .ok_or(PositionOverflow)?,
            );
            self.cost_basis = Money::from_raw(
                opened
                    .raw()
                    .checked_mul(side.sign())
                    .ok_or(PositionOverflow)?,
            );
        }
        Ok(())
    }

    /// The share of the cost basis attributable to `closing` out of `held`,
    /// as a positive magnitude.
    ///
    /// The one division in the model. Truncates toward zero, so a partial close
    /// never books more cost than it should — the remainder stays with the open
    /// size and is settled when that closes.
    fn slice_of_basis(&self, closing: Qty, held: Qty) -> Result<Money, PositionOverflow> {
        if held.is_zero() {
            return Ok(Money::ZERO);
        }
        if closing == held {
            // Closing everything takes everything, with no rounding at all.
            return Ok(self.cost_basis.abs());
        }
        let numerator = i128::from(self.cost_basis.abs().raw()) * i128::from(closing.raw());
        #[expect(
            clippy::integer_division,
            reason = "apportioning cost basis to the closed share; truncation \
                      toward zero leaves the remainder with the open size"
        )]
        let slice = numerator / i128::from(held.raw());
        i64::try_from(slice)
            .map(Money::from_raw)
            .map_err(|_| PositionOverflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn px(s: &str) -> Price {
        Price::parse(s).unwrap()
    }

    fn money(s: &str) -> Money {
        Money::parse(s).unwrap()
    }

    #[test]
    fn a_flat_position_has_no_average_and_no_pnl() {
        let p = Position::default();
        assert!(p.is_flat());
        assert_eq!(p.avg_entry(), None);
        assert_eq!(p.unrealized(px("109432.5")), Money::ZERO);
    }

    #[test]
    fn buying_opens_a_long_at_the_fill_price() {
        let mut p = Position::default();
        p.apply_fill(Side::Buy, Qty::whole(3), px("100")).unwrap();
        assert_eq!(p.qty, Qty::whole(3));
        assert_eq!(p.cost_basis, money("300"));
        assert_eq!(p.avg_entry(), Some(px("100")));
        assert_eq!(p.realized, Money::ZERO);
    }

    #[test]
    fn adding_to_a_long_averages_the_entry() {
        let mut p = Position::default();
        p.apply_fill(Side::Buy, Qty::whole(2), px("100")).unwrap();
        p.apply_fill(Side::Buy, Qty::whole(2), px("110")).unwrap();
        assert_eq!(p.qty, Qty::whole(4));
        assert_eq!(p.cost_basis, money("420"));
        assert_eq!(p.avg_entry(), Some(px("105")));
    }

    #[test]
    fn selling_a_long_books_the_difference() {
        let mut p = Position::default();
        p.apply_fill(Side::Buy, Qty::whole(4), px("100")).unwrap();
        p.apply_fill(Side::Sell, Qty::whole(1), px("110")).unwrap();
        assert_eq!(p.qty, Qty::whole(3));
        assert_eq!(p.realized, money("10"), "one unit, ten better");
        assert_eq!(p.cost_basis, money("300"));
        assert_eq!(p.avg_entry(), Some(px("100")), "the rest is untouched");
    }

    #[test]
    fn a_short_books_profit_when_price_falls() {
        let mut p = Position::default();
        p.apply_fill(Side::Sell, Qty::whole(2), px("100")).unwrap();
        assert_eq!(p.qty, Qty::whole(-2));
        assert_eq!(p.cost_basis, money("-200"), "money received, not paid");
        assert_eq!(p.avg_entry(), Some(px("100")));

        p.apply_fill(Side::Buy, Qty::whole(2), px("90")).unwrap();
        assert!(p.is_flat());
        assert_eq!(p.realized, money("20"), "sold at 100, bought back at 90");
        assert_eq!(p.cost_basis, Money::ZERO);
    }

    #[test]
    fn a_short_books_a_loss_when_price_rises() {
        let mut p = Position::default();
        p.apply_fill(Side::Sell, Qty::whole(2), px("100")).unwrap();
        p.apply_fill(Side::Buy, Qty::whole(2), px("110")).unwrap();
        assert_eq!(p.realized, money("-20"));
    }

    #[test]
    fn partially_closing_a_short_leaves_the_rest_short() {
        let mut p = Position::default();
        p.apply_fill(Side::Sell, Qty::whole(4), px("100")).unwrap();
        p.apply_fill(Side::Buy, Qty::whole(1), px("90")).unwrap();
        assert_eq!(p.qty, Qty::whole(-3));
        assert_eq!(p.realized, money("10"));
        assert_eq!(p.cost_basis, money("-300"));
        assert_eq!(p.avg_entry(), Some(px("100")));
    }

    #[test]
    fn closing_flat_leaves_no_residual_basis() {
        // A basis left on a zero position shows up as unrealised P&L on
        // nothing, which is how a flat account reports a running number.
        let mut p = Position::default();
        p.apply_fill(Side::Buy, Qty::whole(3), px("109432.5"))
            .unwrap();
        p.apply_fill(Side::Sell, Qty::whole(1), px("109500"))
            .unwrap();
        p.apply_fill(Side::Sell, Qty::whole(1), px("109400"))
            .unwrap();
        p.apply_fill(Side::Sell, Qty::whole(1), px("109450"))
            .unwrap();
        assert!(p.is_flat());
        assert_eq!(p.cost_basis, Money::ZERO);
        assert_eq!(p.unrealized(px("109432.5")), Money::ZERO);
    }

    #[test]
    fn a_fill_larger_than_the_position_flips_it() {
        // The case that gets written wrong: the new side must open at this
        // fill's price and carry none of the old basis.
        let mut p = Position::default();
        p.apply_fill(Side::Buy, Qty::whole(2), px("100")).unwrap();
        p.apply_fill(Side::Sell, Qty::whole(5), px("110")).unwrap();

        assert_eq!(p.qty, Qty::whole(-3), "two closed, three opened short");
        assert_eq!(p.realized, money("20"), "the two longs, ten better each");
        assert_eq!(p.cost_basis, money("-330"), "three short at 110");
        assert_eq!(p.avg_entry(), Some(px("110")), "not the old 100");
    }

    #[test]
    fn a_short_flipped_long_carries_nothing_across_either() {
        let mut p = Position::default();
        p.apply_fill(Side::Sell, Qty::whole(2), px("100")).unwrap();
        p.apply_fill(Side::Buy, Qty::whole(5), px("90")).unwrap();
        assert_eq!(p.qty, Qty::whole(3));
        assert_eq!(p.realized, money("20"));
        assert_eq!(p.cost_basis, money("270"));
        assert_eq!(p.avg_entry(), Some(px("90")));
    }

    #[test]
    fn unrealised_tracks_the_mark_in_both_directions() {
        let mut long = Position::default();
        long.apply_fill(Side::Buy, Qty::whole(2), px("100"))
            .unwrap();
        assert_eq!(long.unrealized(px("110")), money("20"));
        assert_eq!(long.unrealized(px("90")), money("-20"));

        let mut short = Position::default();
        short
            .apply_fill(Side::Sell, Qty::whole(2), px("100"))
            .unwrap();
        assert_eq!(short.unrealized(px("90")), money("20"));
        assert_eq!(short.unrealized(px("110")), money("-20"));
    }

    #[test]
    fn a_round_trip_at_the_same_price_books_nothing() {
        let mut p = Position::default();
        p.apply_fill(Side::Buy, Qty::whole(7), px("109432.5"))
            .unwrap();
        p.apply_fill(Side::Sell, Qty::whole(7), px("109432.5"))
            .unwrap();
        assert!(p.is_flat());
        assert_eq!(p.realized, Money::ZERO);
    }

    #[test]
    fn partial_closes_sum_to_the_whole_close() {
        // Whatever `slice_of_basis` rounds away on the way down must come back
        // when the last unit closes, or a position that was fully round-tripped
        // reports a P&L it never earned.
        let mut piecemeal = Position::default();
        piecemeal
            .apply_fill(Side::Buy, Qty::whole(3), px("109432.5"))
            .unwrap();
        for _ in 0..3 {
            piecemeal
                .apply_fill(Side::Sell, Qty::whole(1), px("109500"))
                .unwrap();
        }

        let mut wholesale = Position::default();
        wholesale
            .apply_fill(Side::Buy, Qty::whole(3), px("109432.5"))
            .unwrap();
        wholesale
            .apply_fill(Side::Sell, Qty::whole(3), px("109500"))
            .unwrap();

        assert_eq!(piecemeal.realized, wholesale.realized);
        assert!(piecemeal.is_flat() && wholesale.is_flat());
    }

    #[test]
    fn fractional_sizes_stay_exact() {
        // Delta India sizes BTCUSD in contracts of 0.001 BTC.
        let mut p = Position::default();
        p.apply_fill(Side::Buy, Qty::parse("0.001").unwrap(), px("109432.5"))
            .unwrap();
        assert_eq!(p.cost_basis, money("109.4325"));
        p.apply_fill(Side::Sell, Qty::parse("0.001").unwrap(), px("109532.5"))
            .unwrap();
        assert_eq!(p.realized, money("0.1"));
        assert!(p.is_flat());
    }

    #[test]
    fn overflow_is_reported_rather_than_wrapped() {
        let mut p = Position {
            qty: Qty::MAX,
            ..Position::default()
        };
        assert_eq!(
            p.apply_fill(Side::Buy, Qty::whole(1), px("1")),
            Err(PositionOverflow)
        );
    }
}

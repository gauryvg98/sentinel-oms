//! Position sizing and protective-exit geometry.
//!
//! The money layer, kept apart from alpha. A strategy says *which way* and *how
//! convinced*; this says *how much that is worth*, and the whole of it is one
//! idea: size so that being wrong costs a fixed fraction of equity.
//!
//! ```text
//! risk_amount = risk_pct · equity · conviction     what we are willing to lose
//! stop_dist   = stop_atr_mult · ATR   (or a %)     where the thesis breaks
//! qty         = risk_amount / stop_dist            so a stop-out ≈ risk_amount
//! qty         = min(qty, equity · max_leverage / price)
//! qty         = min(qty, every cap the caller knows about)
//! ```
//!
//! Size is therefore tied to *where the stop is* — to volatility — and not to a
//! flat notional. That is the single thing most naive bots get wrong: a fixed
//! size means a quiet market and a violent one cost different amounts to be
//! wrong in, and only one of them was sized for.
//!
//! Pure and unsigned. No clock, no I/O, no venue: the caller applies the
//! direction, and every rule here is testable before a feed exists.

#![forbid(unsafe_code)]

use sentinel_types::{Money, Price, Qty, SCALE};

/// A fraction, fixed-point at 1e8. `0.02` is two percent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Ratio(i64);

impl Ratio {
    /// Nothing.
    pub const ZERO: Self = Self(0);
    /// The whole.
    pub const ONE: Self = Self(SCALE);

    /// Wrap raw 1e8 units.
    #[must_use]
    pub const fn from_raw(raw: i64) -> Self {
        Self(raw)
    }

    /// The raw units.
    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }

    /// Parse an exact decimal fraction.
    ///
    /// # Errors
    /// [`sentinel_types::ParseError`] when the text is not an exact decimal at
    /// this scale.
    pub const fn parse(s: &str) -> Result<Self, sentinel_types::ParseError> {
        match sentinel_types::parse_e8(s) {
            Ok(raw) => Ok(Self(raw)),
            Err(e) => Err(e),
        }
    }

    /// Clamped into `[0, 1]`.
    #[must_use]
    pub const fn clamped(self) -> Self {
        if self.0 < 0 {
            Self::ZERO
        } else if self.0 > SCALE {
            Self::ONE
        } else {
            self
        }
    }

    #[must_use]
    const fn is_positive(self) -> bool {
        self.0 > 0
    }
}

impl core::fmt::Display for Ratio {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", Price::from_raw(self.0))
    }
}

/// `amount · ratio`, rescaled once.
fn scale(amount: Money, ratio: Ratio) -> Money {
    let product = i128::from(amount.raw()) * i128::from(ratio.raw());
    #[expect(
        clippy::integer_division,
        reason = "one rescale after a fixed-point multiply; truncation toward \
                  zero never sizes larger than intended"
    )]
    let scaled = product / i128::from(SCALE);
    i64::try_from(scaled).map_or(Money::ZERO, Money::from_raw)
}

/// `amount / price`, as a quantity.
///
/// The sizing division. Truncates toward zero, so a rounded quantity is always
/// the smaller one — being a hundredth of a contract light is free, and being a
/// hundredth heavy is the side that breaks a margin check.
fn divide(amount: Money, price: Price) -> Qty {
    if !price.is_positive() {
        return Qty::ZERO;
    }
    let numerator = i128::from(amount.raw()) * i128::from(SCALE);
    #[expect(
        clippy::integer_division,
        reason = "the sizing division; truncation toward zero rounds size down"
    )]
    let quotient = numerator / i128::from(price.raw());
    i64::try_from(quotient).map_or(Qty::ZERO, Qty::from_raw)
}

/// The knobs. Everything here is a decision someone made with money on the line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RiskParams {
    /// Equity fraction risked per trade at full conviction.
    pub risk_pct: Ratio,
    /// Cap: `|notional| <= equity · max_leverage`.
    pub max_leverage: Ratio,
    /// Stop distance as a multiple of ATR.
    pub stop_atr_mult: Ratio,
    /// Stop distance as a fraction of price, when the ATR is not yet warm.
    pub fallback_stop_pct: Ratio,
    /// Reward to risk. The take-profit sits `rr · stop_dist` the other way.
    ///
    /// Zero disables the fixed take-profit entirely: a trend-follower then
    /// rides to its own signal flip, protected only by the stop.
    pub rr: Ratio,
    /// Ratcheting trail: the stop follows the best price at `stop_dist` and
    /// only ever tightens, and there is no fixed take-profit — the trail *is*
    /// the profit-taker, so a run is ridden and the giveback is bounded.
    pub trail: bool,
}

/// Optional ceilings the caller knows about and this crate does not.
///
/// All of them are "how much can actually be paid for", and every one exists
/// because a venue refused an order for want of it. `None` leaves sizing
/// open-loop, which is what a backtest wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Caps {
    /// The most quantity the caller's own estimate of free margin can carry.
    pub margin: Option<Qty>,
    /// The most quantity whose notional stays inside the venue's per-symbol
    /// cap at the configured leverage.
    pub bracket: Option<Qty>,
    /// The most quantity whose initial margin fits the venue's *own*
    /// net-of-everything available balance.
    ///
    /// Unlike `margin`, which is our estimate, this is the exchange's figure —
    /// so an entry under it cannot be refused for insufficient margin.
    pub available: Option<Qty>,
}

impl Caps {
    /// The tightest of the caps applied to `qty`.
    ///
    /// A zero or negative cap means no headroom, and sizes to zero rather than
    /// refusing: there is nothing wrong with the decision, only with the money
    /// available to act on it, and the caller finds out by getting nothing.
    #[must_use]
    pub fn apply(self, qty: Qty) -> Qty {
        [self.margin, self.bracket, self.available]
            .into_iter()
            .flatten()
            .fold(qty, |q, cap| q.min(cap.max(Qty::ZERO)))
    }
}

/// One bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Candle {
    /// Open.
    pub open: Price,
    /// High.
    pub high: Price,
    /// Low.
    pub low: Price,
    /// Close.
    pub close: Price,
}

/// Average true range over the last `period` closed bars.
///
/// `None` until there are enough. The caller then falls back to a fixed-percent
/// stop rather than sizing off a half-warm indicator — a number that is
/// technically available and not yet meaningful is worse than no number, because
/// only one of the two gets used by accident.
#[must_use]
pub fn atr(candles: &[Candle], period: usize) -> Option<Price> {
    if period == 0 || candles.len() < period + 1 {
        return None;
    }
    let window = &candles[candles.len() - period - 1..];
    let mut total: i128 = 0;
    let mut prev_close = window[0].close;
    for candle in &window[1..] {
        let range = candle.high.raw().saturating_sub(candle.low.raw());
        let up_gap = candle.high.raw().saturating_sub(prev_close.raw()).abs();
        let down_gap = candle.low.raw().saturating_sub(prev_close.raw()).abs();
        total += i128::from(range.max(up_gap).max(down_gap));
        prev_close = candle.close;
    }
    #[expect(
        clippy::integer_division,
        reason = "the mean of the true ranges; one price tick of truncation on \
                  an average of prices is below the tick size of every venue we \
                  speak"
    )]
    let mean = total / i128::try_from(window.len() - 1).unwrap_or(1);
    i64::try_from(mean).ok().map(Price::from_raw)
}

/// Price distance to the stop.
///
/// ATR-scaled when the ATR is warm; a fixed fraction of price when it is not.
#[must_use]
pub fn stop_distance(params: RiskParams, price: Price, atr_value: Option<Price>) -> Price {
    match atr_value {
        Some(value) if value.is_positive() => {
            Price::from_raw(scale(Money::from_raw(value.raw()), params.stop_atr_mult).raw())
        }
        _ => Price::from_raw(scale(Money::from_raw(price.raw()), params.fallback_stop_pct).raw()),
    }
}

/// The protective prices for a position entered at `entry`.
///
/// The stop sits `stop_dist` on the losing side; the target `rr · stop_dist` on
/// the winning side. A long stops below and targets above; a short mirrors it.
/// With `rr` at zero there is no target at all — the thesis exits on its own
/// signal, not at a multiple somebody picked.
#[must_use]
pub fn brackets(
    entry: Price,
    is_long: bool,
    stop_dist: Price,
    rr: Ratio,
) -> (Price, Option<Price>) {
    let take_dist = rr
        .is_positive()
        .then(|| scale(Money::from_raw(stop_dist.raw()), rr).raw());
    if is_long {
        (
            Price::from_raw(entry.raw().saturating_sub(stop_dist.raw())),
            take_dist.map(|d| Price::from_raw(entry.raw().saturating_add(d))),
        )
    } else {
        (
            Price::from_raw(entry.raw().saturating_add(stop_dist.raw())),
            take_dist.map(|d| Price::from_raw(entry.raw().saturating_sub(d))),
        )
    }
}

/// Which bracket the mark has reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Breach {
    /// The stop.
    Stop,
    /// The take-profit.
    Take,
}

/// Whether the mark has reached a bracket.
///
/// A `None` take means the target is disabled, so only the stop can fire and a
/// winner rides until the strategy itself flips.
#[must_use]
pub fn breached(is_long: bool, mark: Price, stop: Price, take: Option<Price>) -> Option<Breach> {
    if is_long {
        if mark <= stop {
            return Some(Breach::Stop);
        }
        if take.is_some_and(|t| mark >= t) {
            return Some(Breach::Take);
        }
    } else {
        if mark >= stop {
            return Some(Breach::Stop);
        }
        if take.is_some_and(|t| mark <= t) {
            return Some(Breach::Take);
        }
    }
    None
}

/// Quantity such that hitting the stop costs about `risk_pct · conviction` of
/// equity, capped by leverage and by whatever the caller knows.
///
/// Unsigned: the caller applies the direction. Takes the stop distance directly
/// rather than deriving it, so sizing and the stop that gets placed use the same
/// number — if they diverged, the loss at the stop would not be the loss that
/// was sized for, and that is the one thing this function exists to guarantee.
#[must_use]
pub fn risk_sized_qty(
    params: RiskParams,
    equity: Money,
    price: Price,
    stop_dist: Option<Price>,
    conviction: Ratio,
    caps: Caps,
) -> Qty {
    let Some(stop_dist) = stop_dist else {
        return Qty::ZERO;
    };
    if !equity.is_positive() || !price.is_positive() || !stop_dist.is_positive() {
        return Qty::ZERO;
    }

    let risk_amount = scale(scale(equity, params.risk_pct), conviction.clamped());
    let qty = divide(risk_amount, stop_dist);

    // Never over-lever, whatever the stop distance implies.
    let leverage_cap = divide(scale(equity, params.max_leverage), price);
    caps.apply(qty.min(leverage_cap))
}

/// Where a ratcheting trailing stop is now.
///
/// Tracks the position's best price and keeps the stop `stop_dist` behind it,
/// monotonically: the stop may only tighten, never loosen, even if `stop_dist`
/// widens later. Floored at the entry-anchored initial stop, so it is never
/// looser than the static bracket would have been.
///
/// Returns the new watermark and the new stop.
#[must_use]
pub fn trail_ratchet(
    is_long: bool,
    entry: Price,
    price: Price,
    stop_dist: Price,
    watermark: Option<Price>,
    previous_stop: Option<Price>,
) -> (Price, Price) {
    if is_long {
        let wm = watermark.map_or(price, |w| w.max(price));
        let anchored = Price::from_raw(entry.raw().saturating_sub(stop_dist.raw()));
        let trailing = Price::from_raw(wm.raw().saturating_sub(stop_dist.raw()));
        let mut stop = anchored.max(trailing);
        if let Some(prev) = previous_stop {
            stop = stop.max(prev); // up only
        }
        (wm, stop)
    } else {
        let wm = watermark.map_or(price, |w| w.min(price));
        let anchored = Price::from_raw(entry.raw().saturating_add(stop_dist.raw()));
        let trailing = Price::from_raw(wm.raw().saturating_add(stop_dist.raw()));
        let mut stop = anchored.min(trailing);
        if let Some(prev) = previous_stop {
            stop = stop.min(prev); // down only
        }
        (wm, stop)
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

    fn ratio(s: &str) -> Ratio {
        Ratio::parse(s).unwrap()
    }

    /// The live configuration, as `fly.toml` currently sets it.
    fn params() -> RiskParams {
        RiskParams {
            risk_pct: ratio("0.02"),
            max_leverage: ratio("100"),
            stop_atr_mult: ratio("2"),
            fallback_stop_pct: ratio("0.005"),
            rr: ratio("1.5"),
            trail: true,
        }
    }

    fn candle(high: &str, low: &str, close: &str) -> Candle {
        Candle {
            open: px(close),
            high: px(high),
            low: px(low),
            close: px(close),
        }
    }

    // ------------------------------------------------------------- sizing

    #[test]
    fn a_stop_out_costs_what_was_risked() {
        // The whole point, stated as arithmetic: risk 2% of 10,000 with the
        // stop 100 away, and the loss at the stop is 200.
        let qty = risk_sized_qty(
            params(),
            money("10000"),
            px("50000"),
            Some(px("100")),
            Ratio::ONE,
            Caps::default(),
        );
        assert_eq!(qty, Qty::whole(2));

        let loss = px("100").notional(qty).unwrap();
        assert_eq!(loss, money("200"), "2% of 10,000");
    }

    #[test]
    fn a_wider_stop_buys_less() {
        // Volatility-scaled sizing: the same risk budget, a stop twice as far,
        // half the position. A fixed size would have doubled the loss instead.
        let size = |dist: &str| {
            risk_sized_qty(
                params(),
                money("10000"),
                px("50000"),
                Some(px(dist)),
                Ratio::ONE,
                Caps::default(),
            )
        };
        assert_eq!(size("200"), Qty::whole(1));
        assert_eq!(size("100"), Qty::whole(2));
        assert_eq!(size("50"), Qty::whole(4));
    }

    #[test]
    fn conviction_scales_the_risk_and_is_clamped() {
        let size = |c: Ratio| {
            risk_sized_qty(
                params(),
                money("10000"),
                px("50000"),
                Some(px("100")),
                c,
                Caps::default(),
            )
        };
        assert_eq!(size(ratio("0.5")), Qty::whole(1));
        assert_eq!(size(Ratio::ZERO), Qty::ZERO);
        // Beyond full conviction is still full conviction. A strategy returning
        // 3 must not be able to triple the risk budget by accident.
        assert_eq!(size(ratio("3")), size(Ratio::ONE));
        assert_eq!(size(ratio("-1")), Qty::ZERO);
    }

    #[test]
    fn leverage_caps_a_stop_that_is_absurdly_tight() {
        // A one-cent stop implies a colossal position. The leverage cap is what
        // stops the risk model from turning a tight stop into a blow-up.
        let params = RiskParams {
            max_leverage: ratio("2"),
            ..params()
        };
        let qty = risk_sized_qty(
            params,
            money("10000"),
            px("50000"),
            Some(px("0.01")),
            Ratio::ONE,
            Caps::default(),
        );
        // 10,000 × 2 / 50,000 = 0.4
        assert_eq!(qty, Qty::parse("0.4").unwrap());
    }

    #[test]
    fn the_tightest_cap_wins() {
        let base = risk_sized_qty(
            params(),
            money("10000"),
            px("50000"),
            Some(px("100")),
            Ratio::ONE,
            Caps::default(),
        );
        assert_eq!(base, Qty::whole(2));

        let capped = risk_sized_qty(
            params(),
            money("10000"),
            px("50000"),
            Some(px("100")),
            Ratio::ONE,
            Caps {
                margin: Some(Qty::parse("1.5").unwrap()),
                bracket: Some(Qty::whole(3)),
                available: Some(Qty::parse("0.75").unwrap()),
            },
        );
        assert_eq!(capped, Qty::parse("0.75").unwrap());
    }

    #[test]
    fn no_headroom_sizes_to_zero_rather_than_refusing() {
        // Nothing is wrong with the decision, only with the money available to
        // act on it. The caller finds out by getting nothing back.
        let qty = risk_sized_qty(
            params(),
            money("10000"),
            px("50000"),
            Some(px("100")),
            Ratio::ONE,
            Caps {
                available: Some(Qty::whole(-5)),
                ..Caps::default()
            },
        );
        assert_eq!(qty, Qty::ZERO);
    }

    #[test]
    fn missing_inputs_size_to_zero() {
        for (equity, price, stop) in [
            (money("0"), px("50000"), Some(px("100"))),
            (money("10000"), px("0"), Some(px("100"))),
            (money("10000"), px("50000"), Some(px("0"))),
            (money("10000"), px("50000"), None),
            (money("-1"), px("50000"), Some(px("100"))),
        ] {
            assert_eq!(
                risk_sized_qty(params(), equity, price, stop, Ratio::ONE, Caps::default()),
                Qty::ZERO
            );
        }
    }

    #[test]
    fn sizing_rounds_down() {
        // A hundredth of a contract light is free. A hundredth heavy is the
        // side that fails a margin check.
        let qty = risk_sized_qty(
            params(),
            money("10000"),
            px("100"),
            Some(px("3")),
            Ratio::ONE,
            Caps::default(),
        );
        // 200 / 3 = 66.666..., truncated at the eighth decimal. Priced at 100
        // so the leverage cap (10,000 x 100 / 100 = 10,000) is nowhere near.
        assert_eq!(qty, Qty::parse("66.66666666").unwrap());
    }

    // -------------------------------------------------------------- stops

    #[test]
    fn the_stop_falls_back_to_a_percentage_until_the_atr_is_warm() {
        assert_eq!(stop_distance(params(), px("50000"), None), px("250"));
        assert_eq!(
            stop_distance(params(), px("50000"), Some(px("0"))),
            px("250"),
            "a zero ATR is not a warm one"
        );
        assert_eq!(
            stop_distance(params(), px("50000"), Some(px("300"))),
            px("600"),
            "2 x ATR once it is"
        );
    }

    #[test]
    fn the_atr_is_none_until_there_are_enough_bars() {
        let bars: Vec<_> = (0..14).map(|_| candle("101", "99", "100")).collect();
        assert_eq!(atr(&bars, 14), None, "fourteen bars is one short");
        let bars: Vec<_> = (0..15).map(|_| candle("101", "99", "100")).collect();
        assert_eq!(atr(&bars, 14), Some(px("2")));
        assert_eq!(atr(&[], 14), None);
    }

    #[test]
    fn the_atr_counts_gaps_as_range() {
        // A bar that opens far from the previous close has a true range larger
        // than its own high-low, and a stop sized off the smaller number would
        // sit inside the noise.
        let bars = vec![
            candle("100", "100", "100"),
            candle("100", "100", "100"),
            candle("120", "119", "120"),
        ];
        // Ranges: 0, then max(1, |120-100|, |119-100|) = 20. Mean = 10.
        assert_eq!(atr(&bars, 2), Some(px("10")));
    }

    #[test]
    fn brackets_sit_the_right_side_for_each_direction() {
        let (stop, take) = brackets(px("100"), true, px("2"), ratio("1.5"));
        assert_eq!(stop, px("98"));
        assert_eq!(take, Some(px("103")));

        let (stop, take) = brackets(px("100"), false, px("2"), ratio("1.5"));
        assert_eq!(stop, px("102"));
        assert_eq!(take, Some(px("97")));
    }

    #[test]
    fn a_zero_reward_ratio_disables_the_target_entirely() {
        // A trend-follower rides to its own signal flip; a fixed multiple would
        // cut the trades it exists to hold.
        let (stop, take) = brackets(px("100"), true, px("2"), Ratio::ZERO);
        assert_eq!(stop, px("98"));
        assert_eq!(take, None);
        assert_eq!(breached(true, px("1000"), stop, take), None, "never takes");
        assert_eq!(breached(true, px("98"), stop, take), Some(Breach::Stop));
    }

    #[test]
    fn breaches_are_inclusive_at_the_price() {
        // A stop that only fires strictly past its level does not fire on the
        // print that touches it, which is the print it was placed for.
        assert_eq!(breached(true, px("98"), px("98"), None), Some(Breach::Stop));
        assert_eq!(
            breached(false, px("102"), px("102"), None),
            Some(Breach::Stop)
        );
        assert_eq!(
            breached(true, px("103"), px("98"), Some(px("103"))),
            Some(Breach::Take)
        );
    }

    #[test]
    fn a_stop_takes_precedence_over_a_target() {
        // If a bar reached both, assume the worse one. Anything else is a
        // backtest telling you it made money it did not make.
        assert_eq!(
            breached(true, px("90"), px("98"), Some(px("103"))),
            Some(Breach::Stop)
        );
    }

    // -------------------------------------------------------------- trail

    #[test]
    fn the_trail_follows_the_high_and_never_loosens() {
        let (wm, stop) = trail_ratchet(true, px("100"), px("100"), px("2"), None, None);
        assert_eq!((wm, stop), (px("100"), px("98")));

        let (wm, stop) = trail_ratchet(true, px("100"), px("110"), px("2"), Some(wm), Some(stop));
        assert_eq!((wm, stop), (px("110"), px("108")), "it followed");

        // Price falls back. The stop stays where it ratcheted to.
        let (wm, stop) = trail_ratchet(true, px("100"), px("104"), px("2"), Some(wm), Some(stop));
        assert_eq!((wm, stop), (px("110"), px("108")), "up only");
    }

    #[test]
    fn a_widening_stop_distance_cannot_loosen_the_trail() {
        // Volatility rising later must not hand back profit already locked in.
        let (wm, stop) = trail_ratchet(true, px("100"), px("120"), px("2"), None, None);
        assert_eq!(stop, px("118"));
        let (_, stop) = trail_ratchet(true, px("100"), px("120"), px("10"), Some(wm), Some(stop));
        assert_eq!(stop, px("118"), "not 110");
    }

    #[test]
    fn the_short_trail_mirrors_the_long_one() {
        let (wm, stop) = trail_ratchet(false, px("100"), px("100"), px("2"), None, None);
        assert_eq!((wm, stop), (px("100"), px("102")));

        let (wm, stop) = trail_ratchet(false, px("100"), px("90"), px("2"), Some(wm), Some(stop));
        assert_eq!((wm, stop), (px("90"), px("92")), "it followed down");

        let (wm, stop) = trail_ratchet(false, px("100"), px("96"), px("2"), Some(wm), Some(stop));
        assert_eq!((wm, stop), (px("90"), px("92")), "down only");
    }

    #[test]
    fn the_trail_is_never_looser_than_the_static_bracket() {
        // Anchored at the entry: an immediate adverse move must not produce a
        // stop further away than the bracket the position was sized against.
        let (_, trailed) = trail_ratchet(true, px("100"), px("90"), px("2"), None, None);
        let (static_stop, _) = brackets(px("100"), true, px("2"), Ratio::ZERO);
        assert_eq!(trailed, static_stop);
    }

    #[test]
    fn sizing_and_the_stop_use_the_same_distance() {
        // If they diverged, the loss at the stop would not be the loss that was
        // sized for — which is the one guarantee this crate makes.
        let params = params();
        let equity = money("10000");
        let price = px("50000");
        let bars: Vec<_> = (0..20).map(|_| candle("50300", "49700", "50000")).collect();

        let dist = stop_distance(params, price, atr(&bars, 14));
        let qty = risk_sized_qty(
            params,
            equity,
            price,
            Some(dist),
            Ratio::ONE,
            Caps::default(),
        );
        let (stop, _) = brackets(price, true, dist, params.rr);

        let loss_at_stop = Price::from_raw(price.raw() - stop.raw())
            .notional(qty)
            .unwrap();
        let budget = scale(equity, params.risk_pct);

        // Under budget, never over: sizing truncates, so the realised loss is
        // at most one quantity tick's worth of stop distance short of what was
        // risked, and never a unit more than it.
        let one_tick = dist.notional(Qty::from_raw(1)).unwrap();
        let shortfall = budget.raw() - loss_at_stop.raw();
        assert!(
            (0..=one_tick.raw()).contains(&shortfall),
            "loss at stop {loss_at_stop} against a budget of {budget}, \
             tolerance {one_tick}"
        );
    }
}

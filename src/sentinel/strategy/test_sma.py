"""SMA-cross tests: warm-up (no opinion), LONG/FLAT stance, determinism."""

from decimal import Decimal

import pytest

from sentinel.strategy import Decision, SmaCross, Stance, Strategy


def feed(strat, prices):
    return [strat.on_bar(Decimal(str(p))).stance for p in prices]


def test_conforms_to_strategy_protocol():
    assert isinstance(SmaCross(3, 5), Strategy)


def test_rejects_bad_periods():
    with pytest.raises(ValueError):
        SmaCross(fast=5, slow=5)


def test_no_opinion_until_slow_window_is_full():
    s = SmaCross(fast=2, slow=4)
    # Not enough for the slow window -> stance None (runner does nothing).
    assert feed(s, [10, 11, 12]) == [None, None, None]


def test_wants_long_in_an_uptrend():
    s = SmaCross(fast=2, slow=4)
    stances = feed(s, [10, 10, 10, 10, 11, 12, 13, 14])
    # once warm and rising, the fast SMA sits above the slow -> LONG, and it
    # STAYS long through the trend (target-position, not an edge).
    assert stances[-1] is Stance.LONG
    assert stances[-2] is Stance.LONG


def test_wants_flat_in_a_downtrend():
    s = SmaCross(fast=2, slow=4)
    stances = feed(s, [20, 20, 20, 20, 18, 16, 14, 12])
    assert stances[-1] is Stance.FLAT


def test_flips_long_to_flat_across_a_reversal():
    s = SmaCross(fast=2, slow=4)
    stances = feed(s, [10, 10, 10, 10, 12, 14, 16,   # up   -> LONG
                       14, 12, 10, 8])                # down -> FLAT
    assert Stance.LONG in stances
    last_long = len(stances) - 1 - stances[::-1].index(Stance.LONG)
    # after the last LONG the reversal turns it FLAT
    assert Stance.FLAT in stances[last_long + 1:]


def test_long_short_variant_shorts_below_the_cross():
    # short=True -> below the cross is SHORT (not FLAT): always in the market,
    # can profit in a downtrend on a shorting venue.
    plain = feed(SmaCross(fast=2, slow=4), [10, 11, 12, 5, 4, 3])
    ls = feed(SmaCross(fast=2, slow=4, short=True), [10, 11, 12, 5, 4, 3])
    assert Stance.FLAT in plain and Stance.SHORT not in plain
    assert Stance.SHORT in ls and Stance.FLAT not in ls


def test_is_deterministic():
    prices = [10, 11, 9, 12, 8, 13, 7, 14, 15, 16, 5, 4, 3]

    def run():
        return feed(SmaCross(3, 6), prices)

    assert run() == run()


def test_decision_carries_sma_values_for_the_ui():
    s = SmaCross(fast=2, slow=3)
    for p in (10, 11):
        s.on_bar(Decimal(p))
    d = s.on_bar(Decimal(12))
    assert isinstance(d, Decision)
    assert "fast" in d.detail and "slow" in d.detail


# ---- strategy-level stop geometry -------------------------------------------

def test_emits_stop_dist_when_directional():
    # In a clear uptrend the strategy wants LONG and reports a stop distance:
    # the gap from price back to the slow SMA (floored at stop_floor_pct). Cap
    # disabled here so the raw gap-to-SMA geometry is what's asserted.
    s = SmaCross(fast=2, slow=4, stop_floor_pct=Decimal("0.005"),
                 stop_cap_pct=Decimal("0"))
    d = None
    for p in [10, 11, 12, 13, 20]:            # strong uptrend
        d = s.on_bar(Decimal(str(p)))
    assert d.stance is Stance.LONG
    sd = Decimal(d.detail["stop_dist"])
    slow = Decimal(d.detail["slow"])
    # stop distance = max(|price - slow|, 0.5% of price); here the gap dominates.
    assert sd == max(abs(Decimal("20") - slow), Decimal("20") * Decimal("0.005"))
    assert sd > 0


def test_stop_dist_capped_in_a_strong_trend():
    # When the trend has run far from the slow SMA, the tight cap bounds the stop
    # instead of letting it blow out to the (wide) distance to the line.
    s = SmaCross(fast=2, slow=4, stop_floor_pct=Decimal("0.005"),
                 stop_cap_pct=Decimal("0.01"))
    d = None
    for p in [10, 11, 12, 13, 20]:            # price 20, slow SMA ~14 -> gap ~6
        d = s.on_bar(Decimal(str(p)))
    assert d.stance is Stance.LONG
    sd = Decimal(d.detail["stop_dist"])
    # raw gap (~6) far exceeds the 1% cap, so the stop is capped at 1% of price.
    assert sd == Decimal("20") * Decimal("0.01")


def test_stop_dist_floored_near_a_cross():
    # Price hugging the SMA -> the raw gap is ~0, so the floor kicks in and the
    # stop is never absurdly tight.
    s = SmaCross(fast=2, slow=4, stop_floor_pct=Decimal("0.01"))
    d = None
    for p in [100, 100, 100, 100, 100.5]:     # basically flat, tiny up-tick
        d = s.on_bar(Decimal(str(p)))
    if d.stance in (Stance.LONG, Stance.SHORT):
        sd = Decimal(d.detail["stop_dist"])
        assert sd >= Decimal("100.5") * Decimal("0.01")   # floored


def test_no_stop_dist_while_warming_up():
    s = SmaCross(fast=2, slow=4)
    d = s.on_bar(Decimal("10"))
    assert d.stance is None and "stop_dist" not in d.detail

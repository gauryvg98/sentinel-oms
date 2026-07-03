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

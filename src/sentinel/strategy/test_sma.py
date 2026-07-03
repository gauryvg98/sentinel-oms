"""SMA-cross tests: warm-up, edge-only signals, determinism, protocol shape."""

from decimal import Decimal

import pytest

from sentinel.strategy import Decision, SmaCross, Signal, Strategy


def feed(strat, prices):
    return [strat.on_bar(Decimal(str(p))).signal for p in prices]


def test_conforms_to_strategy_protocol():
    assert isinstance(SmaCross(3, 5), Strategy)


def test_rejects_bad_periods():
    with pytest.raises(ValueError):
        SmaCross(fast=5, slow=5)


def test_holds_until_slow_window_is_full():
    s = SmaCross(fast=2, slow=4)
    # first 3 bars: not enough for the slow window -> HOLD (warming up)
    assert feed(s, [10, 11, 12]) == [Signal.HOLD, Signal.HOLD, Signal.HOLD]


def test_golden_cross_enters_once_then_holds_the_trend():
    s = SmaCross(fast=2, slow=4)
    # Rising series: once warm, fast pulls above slow -> exactly one ENTER,
    # then HOLD while the uptrend persists (no over-trading).
    signals = feed(s, [10, 10, 10, 10, 11, 12, 13, 14, 15])
    assert signals.count(Signal.ENTER) == 1
    first_enter = signals.index(Signal.ENTER)
    assert all(x is Signal.HOLD for x in signals[first_enter + 1:])


def test_death_cross_exits_after_entering():
    s = SmaCross(fast=2, slow=4)
    # Up then down: expect an ENTER on the way up and an EXIT on the way down.
    signals = feed(s, [10, 10, 10, 10, 12, 14, 16,   # up  -> ENTER
                       14, 12, 10, 8])                # down -> EXIT
    assert Signal.ENTER in signals
    assert Signal.EXIT in signals
    assert signals.index(Signal.ENTER) < signals.index(Signal.EXIT)


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

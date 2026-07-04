"""RegimeTrendMR (strategy v2) — pure, deterministic behaviour tests.

Small params keep warm-up short (max(entry+1, vol+1, z, 3*wilder) = 9 bars)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from sentinel.strategy import Bar, Params, RegimeTrendMR, Stance, Strategy


def P(**kw) -> Params:
    d = dict(donchian_entry=5, donchian_exit=2, wilder_period=3,
             adx_trend=25.0, adx_range=20.0, vol_window=4, z_window=4,
             z_enter=2.0, z_exit=0.5)
    d.update(kw)
    return Params(**d)


def feed(strat, bars):
    """bars: list of (high, low, close). Returns the list of Decisions."""
    return [strat.on_bar_ohlcv(Bar(Decimal(str(h)), Decimal(str(l)), Decimal(str(c))))
            for h, l, c in bars]


def uptrend(n=16):
    # Strictly rising: each close clears the prior bar's high -> genuine
    # Donchian breakouts, and clean +DM -> high ADX (TREND).
    return [(100 + 2 * i + 1, 100 + 2 * i - 1, 100 + 2 * i + 1) for i in range(n)]


def downtrend(n=16):
    return [(100 - 2 * i + 1, 100 - 2 * i - 1, 100 - 2 * i - 1) for i in range(n)]


def chop(n=20):
    # Tight oscillation: +DM and -DM cancel -> low ADX (RANGE), no new highs.
    return [(101, 99, 101 if i % 2 else 99) for i in range(n)]


# ---------------------------------------------------------------- contract

def test_conforms_to_strategy_protocol():
    assert isinstance(RegimeTrendMR(P()), Strategy)


def test_view_spec_declares_regime_rows_and_a_donchian_band():
    vs = RegimeTrendMR(P()).view_spec()
    assert {"regime", "trend_strength", "target_weight"} <= {r["key"] for r in vs["rows"]}
    assert any(o["kind"] == "band" and o["upper"] == "upper" for o in vs["overlays"])


@pytest.mark.parametrize("bad", [
    dict(donchian_entry=5, donchian_exit=5),     # exit not shorter than entry
    dict(adx_trend=20.0, adx_range=25.0),        # range band above trend
    dict(z_enter=1.0, z_exit=1.0),               # no hysteresis
])
def test_bad_params_are_rejected(bad):
    with pytest.raises(ValueError):
        P(**bad)


def test_no_opinion_until_warm():
    strat = RegimeTrendMR(P())
    decisions = feed(strat, uptrend(8))          # warm-up needs 9
    assert all(d.stance is None for d in decisions)


# ---------------------------------------------------------------- regime logic

def test_trending_breakout_goes_long():
    d = feed(RegimeTrendMR(P()), uptrend())[-1]
    assert d.stance is Stance.LONG
    assert d.detail["regime"] == "TREND"


def test_downtrend_stays_flat_long_only():
    # No new highs -> no long entry, even though it's strongly trending.
    assert feed(RegimeTrendMR(P()), downtrend())[-1].stance is Stance.FLAT


def test_chop_is_ranging_and_flat_with_mr_off():
    d = feed(RegimeTrendMR(P()), chop())[-1]
    assert d.detail["regime"] == "RANGE" and d.stance is Stance.FLAT


def test_mr_overlay_off_by_default_never_buys_dips():
    # Same chop, MR explicitly off (default): a dip must NOT open a position.
    assert all(d.stance is not Stance.LONG
               for d in feed(RegimeTrendMR(P(enable_mean_reversion=False)), chop()))


# ---------------------------------------------------------------- sizing

def test_conviction_weight_is_in_unit_interval_when_long():
    d = feed(RegimeTrendMR(P()), uptrend())[-1]
    assert d.stance is Stance.LONG
    w = float(d.detail["target_weight"])
    assert 0.0 <= w <= 1.0


def test_flat_prices_do_not_crash_on_zero_vol():
    # sigma -> 0; conviction uses max(sigma, floor) so no divide-by-zero.
    decisions = feed(RegimeTrendMR(P()), [(100, 100, 100)] * 15)
    assert decisions[-1].stance in (Stance.FLAT, None)


# ---------------------------------------------------------------- purity

def test_deterministic_same_stream_same_stances():
    bars = uptrend()
    a = [d.stance for d in feed(RegimeTrendMR(P()), bars)]
    b = [d.stance for d in feed(RegimeTrendMR(P()), bars)]
    assert a == b


def test_on_bar_matches_on_bar_ohlcv_for_degenerate_bars():
    closes = [c for _, _, c in uptrend()]
    s1, s2 = RegimeTrendMR(P()), RegimeTrendMR(P())
    a = [s1.on_bar(Decimal(str(c))).stance for c in closes]
    b = [s2.on_bar_ohlcv(Bar(Decimal(str(c)), Decimal(str(c)), Decimal(str(c)))).stance
         for c in closes]
    assert a == b

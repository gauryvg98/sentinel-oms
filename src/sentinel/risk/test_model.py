"""Risk-sizing tests — pure, no I/O. Proves a stop-out costs ~risk_pct of
equity, that conviction scales it, that the leverage cap binds, and that ATR
falls back to a fixed-% stop until it's warm."""

from __future__ import annotations

from decimal import Decimal

from sentinel.risk import RiskParams, atr, risk_sized_qty, stop_distance

P = RiskParams(risk_pct=Decimal("0.01"), max_leverage=Decimal("3"),
               stop_atr_mult=Decimal("2"), fallback_stop_pct=Decimal("0.02"))


def _candles(closes, hi_lo=1.0):
    # simple OHLC where each bar spans hi_lo around the close
    return [{"t": i, "o": c, "h": c + hi_lo, "l": c - hi_lo, "c": c}
            for i, c in enumerate(closes)]


# ---- sizing -----------------------------------------------------------------

def test_stop_out_loses_about_risk_pct_of_equity():
    equity, price, atrv = Decimal("10000"), Decimal("60000"), Decimal("500")
    qty = risk_sized_qty(P, equity=equity, price=price, atr_value=atrv)
    # stop distance = 2*500 = 1000; a stop-out loses qty * 1000
    loss_at_stop = qty * (P.stop_atr_mult * atrv)
    assert abs(loss_at_stop - P.risk_pct * equity) < Decimal("0.01")   # ~100 USDT


def test_conviction_scales_size_linearly():
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"), atr_value=Decimal("500"))
    full = risk_sized_qty(P, conviction=Decimal("1"), **kw)
    half = risk_sized_qty(P, conviction=Decimal("0.5"), **kw)
    assert abs(half - full / 2) < Decimal("1e-9")


def test_conviction_is_clamped_to_unit_interval():
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"), atr_value=Decimal("500"))
    full = risk_sized_qty(P, conviction=Decimal("1"), **kw)
    assert risk_sized_qty(P, conviction=Decimal("5"), **kw) == full     # clamped to 1
    assert risk_sized_qty(P, conviction=Decimal("-1"), **kw) == Decimal(0)  # clamped to 0


def test_leverage_cap_binds_when_stop_is_tight():
    # A very tight stop would size huge; the leverage cap must clip notional to
    # equity * max_leverage.
    equity, price = Decimal("10000"), Decimal("60000")
    qty = risk_sized_qty(P, equity=equity, price=price, atr_value=Decimal("1"))
    assert qty * price <= equity * P.max_leverage + Decimal("1e-6")
    assert qty == (equity * P.max_leverage) / price     # exactly the cap


def test_zero_or_negative_inputs_are_safe():
    assert risk_sized_qty(P, equity=Decimal(0), price=Decimal("60000"),
                          atr_value=Decimal("500")) == 0
    assert risk_sized_qty(P, equity=Decimal("10000"), price=Decimal(0),
                          atr_value=Decimal("500")) == 0


# ---- stop distance / ATR ----------------------------------------------------

def test_stop_falls_back_to_pct_without_atr():
    assert stop_distance(P, Decimal("60000"), None) == Decimal("0.02") * 60000
    assert stop_distance(P, Decimal("60000"), Decimal("500")) == Decimal("2") * 500


def test_atr_none_until_enough_bars_then_positive():
    assert atr(_candles([100, 101, 102]), period=14) is None
    a = atr(_candles([100 + i for i in range(20)], hi_lo=1.0), period=14)
    assert a is not None and a > 0


# ---- brackets / breach --------------------------------------------------------

def test_brackets_long_and_short_are_mirrored():
    from sentinel.risk import brackets
    # long entered at 100, stop 5 away, rr 2 -> stop 95, take 110
    assert brackets(Decimal("100"), True, Decimal("5"), Decimal("2")) == \
        (Decimal("95"), Decimal("110"))
    # short: stop above, take below
    assert brackets(Decimal("100"), False, Decimal("5"), Decimal("2")) == \
        (Decimal("105"), Decimal("90"))


def test_breach_detects_stop_and_take_for_both_sides():
    from sentinel.risk import breached
    # long, stop 95, take 110
    assert breached(True, Decimal("94"), Decimal("95"), Decimal("110")) == "STOP"
    assert breached(True, Decimal("111"), Decimal("95"), Decimal("110")) == "TAKE"
    assert breached(True, Decimal("100"), Decimal("95"), Decimal("110")) is None
    # short, stop 105, take 90
    assert breached(False, Decimal("106"), Decimal("105"), Decimal("90")) == "STOP"
    assert breached(False, Decimal("89"), Decimal("105"), Decimal("90")) == "TAKE"
    assert breached(False, Decimal("100"), Decimal("105"), Decimal("90")) is None

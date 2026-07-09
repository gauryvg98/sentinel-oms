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
    equity, price, sd = Decimal("10000"), Decimal("60000"), Decimal("1000")
    qty = risk_sized_qty(P, equity=equity, price=price, stop_dist=sd)
    loss_at_stop = qty * sd                        # a stop-out loses qty * stop_dist
    assert abs(loss_at_stop - P.risk_pct * equity) < Decimal("0.01")   # ~100 USDT


def test_conviction_scales_size_linearly():
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"), stop_dist=Decimal("1000"))
    full = risk_sized_qty(P, conviction=Decimal("1"), **kw)
    half = risk_sized_qty(P, conviction=Decimal("0.5"), **kw)
    assert abs(half - full / 2) < Decimal("1e-9")


def test_conviction_is_clamped_to_unit_interval():
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"), stop_dist=Decimal("1000"))
    full = risk_sized_qty(P, conviction=Decimal("1"), **kw)
    assert risk_sized_qty(P, conviction=Decimal("5"), **kw) == full     # clamped to 1
    assert risk_sized_qty(P, conviction=Decimal("-1"), **kw) == Decimal(0)  # clamped to 0


def test_leverage_cap_binds_when_stop_is_tight():
    # A very tight stop would size huge; the leverage cap must clip notional to
    # equity * max_leverage.
    equity, price = Decimal("10000"), Decimal("60000")
    qty = risk_sized_qty(P, equity=equity, price=price, stop_dist=Decimal("2"))
    assert qty * price <= equity * P.max_leverage + Decimal("1e-6")
    assert qty == (equity * P.max_leverage) / price     # exactly the cap


def test_zero_or_negative_inputs_are_safe():
    assert risk_sized_qty(P, equity=Decimal(0), price=Decimal("60000"),
                          stop_dist=Decimal("1000")) == 0
    assert risk_sized_qty(P, equity=Decimal("10000"), price=Decimal(0),
                          stop_dist=Decimal("1000")) == 0
    assert risk_sized_qty(P, equity=Decimal("10000"), price=Decimal("60000"),
                          stop_dist=None) == 0


# ---- closed-loop margin clamp -------------------------------------------------

def test_margin_cap_binds_when_tighter_than_risk_and_leverage():
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"),
              stop_dist=Decimal("1000"))
    uncapped = risk_sized_qty(P, **kw)
    cap = uncapped / 2                             # real free margin allows less
    assert risk_sized_qty(P, margin_qty_cap=cap, **kw) == cap


def test_margin_cap_absent_or_loose_leaves_sizing_unchanged():
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"),
              stop_dist=Decimal("1000"))
    uncapped = risk_sized_qty(P, **kw)
    # None (open-loop callers, backtests) must be byte-identical to before.
    assert risk_sized_qty(P, margin_qty_cap=None, **kw) == uncapped
    # A cap wider than the risk-sized qty never inflates the size.
    assert risk_sized_qty(P, margin_qty_cap=uncapped * 10, **kw) == uncapped


def test_margin_cap_zero_or_negative_free_margin_sizes_to_zero():
    # Exhausted (or over-drawn after drift) margin -> qty 0, a quiet no-op —
    # never a negative qty and never an order the exchange would -2019.
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"),
              stop_dist=Decimal("1000"))
    assert risk_sized_qty(P, margin_qty_cap=Decimal(0), **kw) == 0
    assert risk_sized_qty(P, margin_qty_cap=Decimal("-0.5"), **kw) == 0


# ---- Binance leverage-bracket clamp (kills -2027) ---------------------------

def test_bracket_cap_binds_when_tighter_than_risk_and_leverage():
    # The bracket cap (max qty whose notional stays under the symbol's leverage
    # bracket) is smaller than the risk-sized qty -> it wins.
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"),
              stop_dist=Decimal("1000"))
    uncapped = risk_sized_qty(P, **kw)
    cap = uncapped / 2
    assert risk_sized_qty(P, bracket_qty_cap=cap, **kw) == cap


def test_bracket_cap_absent_or_loose_leaves_sizing_unchanged():
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"),
              stop_dist=Decimal("1000"))
    uncapped = risk_sized_qty(P, **kw)
    # None (spot/sim/fail-open, backtests) is byte-identical to before.
    assert risk_sized_qty(P, bracket_qty_cap=None, **kw) == uncapped
    # A cap wider than the sized qty never inflates it.
    assert risk_sized_qty(P, bracket_qty_cap=uncapped * 10, **kw) == uncapped
    # Zero/negative (defensive) -> size to zero, never negative.
    assert risk_sized_qty(P, bracket_qty_cap=Decimal(0), **kw) == 0
    assert risk_sized_qty(P, bracket_qty_cap=Decimal("-1"), **kw) == 0


def test_bracket_and_margin_caps_both_present_tighter_wins():
    kw = dict(equity=Decimal("10000"), price=Decimal("60000"),
              stop_dist=Decimal("1000"))
    uncapped = risk_sized_qty(P, **kw)
    tight, loose = uncapped / 4, uncapped / 2
    # Bracket tighter -> bracket wins.
    assert risk_sized_qty(P, margin_qty_cap=loose, bracket_qty_cap=tight,
                          **kw) == tight
    # Margin tighter -> margin wins.
    assert risk_sized_qty(P, margin_qty_cap=tight, bracket_qty_cap=loose,
                          **kw) == tight


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


def test_brackets_no_take_profit_when_rr_not_positive():
    from sentinel.risk import breached, brackets
    # rr=0 -> ride to the signal flip: a stop, but NO take-profit.
    stop, take = brackets(Decimal("100"), True, Decimal("5"), Decimal("0"))
    assert (stop, take) == (Decimal("95"), None)
    stop, take = brackets(Decimal("100"), False, Decimal("5"), Decimal("0"))
    assert (stop, take) == (Decimal("105"), None)
    # with take=None only the stop can fire; upside never triggers a flatten.
    assert breached(True, Decimal("94"), Decimal("95"), None) == "STOP"
    assert breached(True, Decimal("1000"), Decimal("95"), None) is None
    assert breached(False, Decimal("106"), Decimal("105"), None) == "STOP"
    assert breached(False, Decimal("1"), Decimal("105"), None) is None


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


def test_trail_ratchet_long_only_tightens():
    from sentinel.risk.model import trail_ratchet
    e, sd = Decimal("100"), Decimal("1")
    wm, s1 = trail_ratchet(True, e, Decimal("100"), sd, None, None)
    assert s1 == Decimal("99")                    # entry-anchored start
    wm, s2 = trail_ratchet(True, e, Decimal("105"), sd, wm, s1)
    assert s2 == Decimal("104")                   # trails the new peak
    wm, s3 = trail_ratchet(True, e, Decimal("102"), sd, wm, s2)
    assert s3 == Decimal("104") and wm == Decimal("105")  # NEVER loosens


def test_trail_ratchet_short_mirror():
    from sentinel.risk.model import trail_ratchet
    e, sd = Decimal("100"), Decimal("1")
    wm, s1 = trail_ratchet(False, e, Decimal("100"), sd, None, None)
    assert s1 == Decimal("101")
    wm, s2 = trail_ratchet(False, e, Decimal("95"), sd, wm, s1)
    assert s2 == Decimal("96")
    wm, s3 = trail_ratchet(False, e, Decimal("98"), sd, wm, s2)
    assert s3 == Decimal("96") and wm == Decimal("95")


def test_trail_ratchet_widening_stop_dist_cannot_loosen():
    from sentinel.risk.model import trail_ratchet
    e = Decimal("100")
    wm, s1 = trail_ratchet(True, e, Decimal("105"), Decimal("1"), None, None)
    wm, s2 = trail_ratchet(True, e, Decimal("105"), Decimal("3"), wm, s1)
    assert s2 == s1                               # wider sd -> ratchet holds

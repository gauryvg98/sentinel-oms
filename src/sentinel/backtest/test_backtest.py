"""Backtest engine tests on synthetic bars — accounting, no-lookahead, costs."""

from __future__ import annotations

from decimal import Decimal

from sentinel.strategy import Decision, Stance
from sentinel.backtest.engine import run_backtest


def bars(closes, *, spacing=3600):
    # o == prior close (gap-free) so open-fill == the price we expect.
    out = []
    prev = closes[0]
    for i, c in enumerate(closes):
        out.append({"t": i * spacing, "o": prev, "h": max(prev, c) + 1,
                    "l": min(prev, c) - 1, "c": c})
        prev = c
    return out


class _Always:
    name = "always-long"

    def on_bar(self, close):
        return Decision(Stance.LONG)


class _Warming:
    name = "warm"

    def on_bar(self, close):
        return Decision(None)


class _AlwaysShort:
    name = "always-short"

    def on_bar(self, close):
        return Decision(Stance.SHORT)


class _LongThenFlat:
    """LONG on the first close, FLAT forever after — to check trade timing."""
    name = "long-then-flat"

    def __init__(self):
        self._n = 0

    def on_bar(self, close):
        self._n += 1
        return Decision(Stance.LONG if self._n == 1 else Stance.FLAT)


def test_no_opinion_never_trades():
    r = run_backtest(_Warming(), bars([100, 101, 102, 103]))
    assert r.trades == 0
    assert abs(r.net_return) < 1e-9          # equity == budget, untouched


def test_decision_executes_on_the_next_bar_open_no_lookahead():
    # LONG decided on bar 0's close -> the BUY must land on bar 1 (its open),
    # never bar 0. And the FLAT on bar 1's close exits on bar 2.
    b = bars([100, 110, 120])
    r = run_backtest(_LongThenFlat(), b, cost_bps=Decimal(0))
    assert [t for t, *_ in r.trades_log] == [b[1]["t"], b[2]["t"]]
    assert r.trades_log[0][1] == "BUY" and r.trades_log[0][3] == b[1]["o"]
    assert r.trades_log[1][1] == "SELL"


def test_costs_drag_net_below_gross():
    b = bars([100, 105, 95, 108, 92, 110])
    free = run_backtest(_LongThenFlat(), b, cost_bps=Decimal(0))
    paid = run_backtest(_LongThenFlat(), b, cost_bps=Decimal(50))
    assert paid.fees_paid > 0
    assert paid.net_return < free.net_return          # fees erode return
    assert abs(free.gross_return - free.net_return) < 1e-9   # no fees -> equal


def test_flat_market_flat_pnl_before_costs():
    r = run_backtest(_Always(), bars([100, 100, 100, 100]), cost_bps=Decimal(0))
    assert abs(r.gross_return) < 1e-6                  # bought and held, no move


def test_short_profits_when_price_falls():
    r = run_backtest(_AlwaysShort(), bars([100, 98, 96, 94, 92]), cost_bps=Decimal(0))
    assert r.net_return > 0                            # short a falling market -> gain


def test_short_loses_when_price_rises():
    r = run_backtest(_AlwaysShort(), bars([100, 102, 104, 106, 108]), cost_bps=Decimal(0))
    assert r.net_return < 0


def test_deterministic():
    b = bars([100, 101, 99, 103, 97, 105])
    a = run_backtest(_LongThenFlat(), b)
    c = run_backtest(_LongThenFlat(), b)
    assert a.net_return == c.net_return and a.trades == c.trades

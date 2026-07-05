"""Backtest engine.

Decisions are made on a bar's CLOSE and executed on the NEXT bar's OPEN — no
lookahead (you never trade at a price you used to decide). Fills are modelled as
TAKERS at the open plus cost_bps: deliberately conservative vs the live maker
peg, because a strategy that only survives on perfect maker fills isn't robust.

Sizing/reconciliation is the SAME plan_action the live runner uses (with
entry=None, since a backtest has no resting order to re-price) — so the position
the backtest holds is the position the live system would target. Note the
consequence: the target is constant-NOTIONAL (weight x budget / price), so a
rising market trims the position — the strategy will trail buy-and-hold in a
strong uptrend by design. That's honest, not a bug.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal

from sentinel.strategy import Bar, Stance
from sentinel.ui.strategy_runner import plan_action


def _feed(strategy, b: dict):
    ohlcv = getattr(strategy, "on_bar_ohlcv", None)
    if callable(ohlcv):
        return ohlcv(Bar(Decimal(str(b["h"])), Decimal(str(b["l"])),
                         Decimal(str(b["c"]))))
    return strategy.on_bar(Decimal(str(b["c"])))


def _target(decision, price: Decimal, budget: Decimal, lot: Decimal):
    """Same stance+conviction -> target-qty mapping the runner uses."""
    if decision is None or decision.stance is None:
        return None
    if decision.stance is Stance.FLAT:
        return Decimal(0)
    if price <= 0:
        return None
    try:
        w = Decimal(str(decision.detail.get("target_weight", "1")))
    except Exception:  # noqa: BLE001
        w = Decimal(1)
    w = max(Decimal(0), min(Decimal(1), w))
    return (w * budget / price).quantize(lot, rounding=ROUND_DOWN)


@dataclass(slots=True)
class BacktestResult:
    symbol: str
    interval: str
    bars: int
    trades: int
    gross_return: float      # before fees
    net_return: float        # after fees
    buy_hold_return: float   # benchmark: hold from first open
    fees_paid: float
    turnover: float          # sum |trade notional|
    sharpe: float            # annualized, net
    max_drawdown: float
    final_equity: float
    budget: float
    equity_curve: list = field(default_factory=list)   # [(t, net_equity)]
    trades_log: list = field(default_factory=list)      # [(t, side, qty, price)]


def run_backtest(strategy, bars, *, symbol="", interval="",
                 budget=Decimal("1000"), cost_bps=Decimal("10"),
                 lot=Decimal("0.00001")) -> BacktestResult:
    budget = Decimal(budget)
    cost = Decimal(cost_bps) / Decimal(10000)
    gross_cash = budget          # cash IGNORING fees; fees tracked separately
    position = Decimal(0)
    fees = Decimal(0)
    turnover = Decimal(0)
    curve: list = []
    trades_log: list = []
    rets: list[float] = []
    prev = None
    pending = None               # decision from the previous bar's close

    def fill(t, qty, price, sign):   # sign +1 buy, -1 sell
        nonlocal gross_cash, position, fees, turnover
        notional = qty * price
        gross_cash -= sign * notional
        position += sign * qty
        fees += notional * cost
        turnover += notional
        trades_log.append((t, "BUY" if sign > 0 else "SELL",
                           float(qty), float(price)))

    for b in bars:
        po = Decimal(str(b["o"]))
        if pending is not None:                      # execute prior decision now
            target = _target(pending, po, budget, lot)
            plan = plan_action(position, po, None, target)
            if plan.kind == "place":
                fill(b["t"], plan.qty, po, +1)
            elif plan.kind == "trim":
                fill(b["t"], plan.qty, po, -1)
            elif plan.kind == "exit" and position > 0:
                fill(b["t"], position, po, -1)
        pc = Decimal(str(b["c"]))
        net_equity = gross_cash + position * pc - fees
        curve.append((b["t"], float(net_equity)))
        if prev is not None and prev > 0:
            rets.append(float(net_equity / prev - 1))
        prev = net_equity
        pending = _feed(strategy, b)                 # decide on this close

    budget_f = float(budget)
    net_final = curve[-1][1] if curve else budget_f
    gross_final = (float(gross_cash + position * Decimal(str(bars[-1]["c"])))
                   if bars else budget_f)
    bh = (float(bars[-1]["c"]) / float(bars[0]["o"]) - 1) if bars else 0.0
    return BacktestResult(
        symbol=symbol, interval=interval, bars=len(bars), trades=len(trades_log),
        gross_return=gross_final / budget_f - 1,
        net_return=net_final / budget_f - 1,
        buy_hold_return=bh,
        fees_paid=float(fees), turnover=float(turnover),
        sharpe=_sharpe(rets, _periods_per_year(bars)),
        max_drawdown=_max_drawdown(curve),
        final_equity=net_final, budget=budget_f,
        equity_curve=curve, trades_log=trades_log,
    )


def _periods_per_year(bars) -> float:
    if len(bars) < 2:
        return 365.0
    spacing = bars[1]["t"] - bars[0]["t"]
    return 31_536_000 / spacing if spacing > 0 else 365.0


def _sharpe(rets, ppy) -> float:
    if len(rets) < 2:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    sd = math.sqrt(var)
    return (mean / sd * math.sqrt(ppy)) if sd > 0 else 0.0


def _max_drawdown(curve) -> float:
    peak, mdd = float("-inf"), 0.0
    for _, e in curve:
        peak = max(peak, e)
        if peak > 0:
            mdd = max(mdd, (peak - e) / peak)
    return mdd

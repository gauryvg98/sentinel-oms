"""CLI: python -m sentinel.backtest --strategy regime --interval 1h --days 365

Pulls REAL mainnet klines (public, no auth), replays the pure strategy through
the live sizing/execution logic, charges realistic fees, prints a report.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal

from sentinel.strategy import Params, RegimeTrendMR, SmaCross

from .data import load_klines
from .engine import run_backtest


def _strategy(name: str):
    if name == "regime":
        return RegimeTrendMR(Params())
    return SmaCross()


def _pct(x: float) -> str:
    return f"{x * 100:+.2f}%"


def _report(args, r) -> None:
    edge = r.net_return - r.buy_hold_return
    print(f"""
  ── {r.symbol} {r.interval} · {args.strategy} · {r.bars} bars ──────────────
  budget            {r.budget:,.0f} USDT     cost {args.cost_bps} bps/side
  trades            {r.trades}   turnover {r.turnover:,.0f} USDT   fees {r.fees_paid:,.2f} USDT

  gross return      {_pct(r.gross_return)}
  net return        {_pct(r.net_return)}   <- after fees
  buy & hold        {_pct(r.buy_hold_return)}
  edge vs hold      {_pct(edge)}   {'BEATS hold' if edge > 0 else 'loses to hold'}

  Sharpe (net)      {r.sharpe:.2f}
  max drawdown      {r.max_drawdown * 100:.1f}%
  final equity      {r.final_equity:,.2f} USDT
  ─────────────────────────────────────────────────────────────────────
  Fees turned {_pct(r.gross_return)} gross into {_pct(r.net_return)} net.
""")


def main() -> None:
    ap = argparse.ArgumentParser(prog="sentinel.backtest")
    ap.add_argument("--strategy", default="regime", choices=["sma", "regime"])
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--interval", default="1h")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--budget", default="1000")
    ap.add_argument("--cost-bps", default="10", help="per-side cost (Binance taker ~10)")
    ap.add_argument("--refresh", action="store_true", help="refetch, ignore cache")
    args = ap.parse_args()

    print(f"loading {args.symbol} {args.interval}, last {args.days}d "
          f"(real mainnet public data)...")
    bars = load_klines(args.symbol, args.interval, args.days, refresh=args.refresh)
    day = lambda t: datetime.fromtimestamp(t, timezone.utc).date()
    print(f"  {len(bars)} bars [{day(bars[0]['t'])} → {day(bars[-1]['t'])}]")
    r = run_backtest(_strategy(args.strategy), bars,
                     symbol=args.symbol, interval=args.interval,
                     budget=Decimal(args.budget), cost_bps=Decimal(args.cost_bps))
    _report(args, r)


if __name__ == "__main__":
    main()

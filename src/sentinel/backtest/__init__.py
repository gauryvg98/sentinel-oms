"""Backtester — replay REAL mainnet bars through the same pure strategy and the
same sizing/reconciliation the live runner uses, with a realistic cost model.

The whole point of Sentinel's pure strategy core is that this is possible with
zero re-implementation: on_bar_ohlcv and plan_action are the exact same code the
live path runs. A strategy that looks good here is at least self-consistent with
what will actually trade; a strategy that dies here should never touch money.
"""

from .engine import BacktestResult, run_backtest

__all__ = ["BacktestResult", "run_backtest"]

"""Sizing-equity tests for InstrumentManager.equity_for — the fix for the bug
that placed ~3x-equity orders per bot and drew a flood of -2019 "Margin
insufficient". Two invariants:

  1. Single-asset margin: a bot sizes off ITS settlement asset's balance only —
     a USDC perp never sees the USDT pool (they don't cross-collateralize).
  2. The pool is SHARED: N bots on the same asset each get pool/N, so their
     summed sizing can never exceed the pool.

Pure-ish: the manager is built with stub app/venue; equity_for touches only
balances + the roster, no I/O.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from sentinel.ui.server import InstrumentManager


def _mgr(balances: dict, *, multi: bool = False) -> InstrumentManager:
    app = SimpleNamespace(latest_balances={k: Decimal(str(v))
                                           for k, v in balances.items()})
    venue = SimpleNamespace()
    m = InstrumentManager(app, venue, {}, default_strategy="sma",
                          default_usdt=Decimal("100"), caps={},
                          multi_asset_margin=multi)
    return m


def _bot(symbol: str, asset: str):
    return SimpleNamespace(spec=SimpleNamespace(quote_asset=asset, symbol=symbol))


def test_single_asset_confines_sizing_to_settlement_pool():
    m = _mgr({"USDT": 4000, "USDC": 6000})
    m.bots = {"BTCUSDT": _bot("BTCUSDT", "USDT"),
              "SOLUSDC": _bot("SOLUSDC", "USDC")}
    # each symbol sees ONLY its own pool (one bot per pool -> full pool)
    assert m.equity_for("BTCUSDT") == Decimal("4000")
    assert m.equity_for("SOLUSDC") == Decimal("6000")


def test_pool_is_split_across_bots_on_the_same_asset():
    m = _mgr({"USDT": 3000, "USDC": 6000})
    m.bots = {"BTCUSDT": _bot("BTCUSDT", "USDT"),
              "ETHUSDC": _bot("ETHUSDC", "USDC"),
              "SOLUSDC": _bot("SOLUSDC", "USDC")}
    assert m.equity_for("BTCUSDT") == Decimal("3000")          # sole USDT bot
    # two USDC bots split the 6000 pool -> 3000 each; summed = pool, never over
    assert m.equity_for("ETHUSDC") == Decimal("3000")
    assert m.equity_for("SOLUSDC") == Decimal("3000")
    assert m.equity_for("ETHUSDC") + m.equity_for("SOLUSDC") == Decimal("6000")


def test_multi_asset_pools_everything_and_splits_across_all_bots():
    m = _mgr({"USDT": 4000, "USDC": 6000}, multi=True)
    m.bots = {"BTCUSDT": _bot("BTCUSDT", "USDT"),
              "SOLUSDC": _bot("SOLUSDC", "USDC")}
    # unified 10_000 pool, 2 bots -> 5000 each regardless of settlement asset
    assert m.equity_for("BTCUSDT") == Decimal("5000")
    assert m.equity_for("SOLUSDC") == Decimal("5000")


def test_settle_asset_falls_back_to_suffix_when_spec_silent():
    m = _mgr({"USDT": 100, "USDC": 200})
    m.bots = {"SOLUSDC": _bot("SOLUSDC", "")}   # spec omitted the asset
    assert m._settle_asset("SOLUSDC") == "USDC"
    assert m._settle_asset("BTCUSDT") == "USDT"

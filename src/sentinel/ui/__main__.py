"""Run the Sentinel paper-trading terminal against Binance testnet:

    python -m sentinel.ui        ->  http://localhost:8000

Needs: docker compose postgres (:5433) and BINANCE_TESTNET_KEY/SECRET in
.env. The ledger PERSISTS across restarts — startup recovery reconciles any
non-terminal orders against the real exchange before the page goes live.
That is not a demo behavior; that is the system.
"""

from __future__ import annotations

import asyncio
import os
from decimal import Decimal
from pathlib import Path

import asyncpg
import uvicorn

from sentinel.broker.binance import BinanceFuturesAdapter, BinanceSpotAdapter
from sentinel.ledger import apply_migrations
from sentinel.runtime import SentinelApp
from sentinel.ui.bars import BarFeed
from sentinel.ui.market import MarketData
from sentinel.ui.server import build_ui

DEFAULT_DB = "postgresql://sentinel:sentinel@127.0.0.1:5433/sentinel"
SYMBOL = os.environ.get("SENTINEL_SYMBOL", "BTCUSDT")
FUT_REST = "https://demo-fapi.binance.com"       # Binance Demo Trading (demo.binance.com)
FUT_STREAM = "wss://demo-fstream.binance.com"


def _venue():
    """Select spot (long/flat) or USDT-M futures (long/short) by SENTINEL_VENUE.
    Returns (adapter, market, bar_feed, max_position, allow_short)."""
    interval = os.environ.get("SENTINEL_STRATEGY_INTERVAL", "1m")
    if os.environ.get("SENTINEL_VENUE") == "futures":
        # Default: Binance Demo Trading (demo-fapi). Override for the classic
        # auto-funded testnet: SENTINEL_FUT_REST=https://testnet.binancefuture.com
        # SENTINEL_FUT_STREAM=wss://fstream.binancefuture.com
        rest = os.environ.get("SENTINEL_FUT_REST", FUT_REST)
        stream = os.environ.get("SENTINEL_FUT_STREAM", FUT_STREAM)
        adapter = BinanceFuturesAdapter(
            os.environ["BINANCE_FUTURES_KEY"], os.environ["BINANCE_FUTURES_SECRET"],
            symbols=(SYMBOL,), base_url=rest, ws_base=stream,
            leverage=int(os.environ.get("SENTINEL_LEVERAGE", "1")))
        mkt = MarketData(SYMBOL, rest_base=rest, stream_base=stream,
                         kline_path="/fapi/v1/klines")
        bars = BarFeed(SYMBOL, interval, rest_base=rest, stream_base=stream,
                       kline_path="/fapi/v1/klines")
        cap = Decimal(os.environ.get("SENTINEL_MAX_POSITION", "0.05"))  # hard exposure cap
        return adapter, mkt, bars, cap, True
    adapter = BinanceSpotAdapter(
        os.environ["BINANCE_TESTNET_KEY"], os.environ["BINANCE_TESTNET_SECRET"],
        symbols=(SYMBOL,))
    return adapter, MarketData(SYMBOL), BarFeed(SYMBOL, interval), None, False


def load_env() -> None:
    env_file = Path(__file__).parents[3] / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def _build_registry() -> dict:
    """The strategies the operator can select from at runtime, as FACTORIES
    (each call is a fresh, unwarmed instance) — the live runner and a backtest
    each need their own. 'regime' is the v2 engine (Donchian breakout gated by
    an ADX regime filter + vol-target conviction)."""
    from sentinel.strategy import Params, RegimeTrendMR, SmaCross
    sma_fast = int(os.environ.get("SENTINEL_SMA_FAST", "5"))
    sma_slow = int(os.environ.get("SENTINEL_SMA_SLOW", "20"))
    regime_params = Params(
        donchian_entry=int(os.environ.get("SENTINEL_DONCHIAN_ENTRY", "55")),
        donchian_exit=int(os.environ.get("SENTINEL_DONCHIAN_EXIT", "20")),
        adx_trend=float(os.environ.get("SENTINEL_ADX_TREND", "25")),
        adx_range=float(os.environ.get("SENTINEL_ADX_RANGE", "20")),
        enable_mean_reversion=os.environ.get("SENTINEL_MR", "0") == "1",
    )
    return {
        "sma": lambda: SmaCross(fast=sma_fast, slow=sma_slow),
        "sma-ls": lambda: SmaCross(fast=sma_fast, slow=sma_slow, short=True),
        "regime": lambda: RegimeTrendMR(regime_params),
    }


async def _serve() -> None:
    load_env()
    dsn = os.environ.get("DATABASE_URL", DEFAULT_DB)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=6)
    async with pool.acquire() as conn:
        await apply_migrations(conn)

    adapter, market, strategy_bars, max_position, allow_short = _venue()
    # dsn -> single-writer enforcement: refuse to boot if another process
    # already owns this account/database. max_position caps futures exposure.
    app = SentinelApp(pool, adapter, dsn=dsn, max_position=max_position)

    ui = build_ui(
        app, market,
        trade_qty=Decimal(os.environ.get("SENTINEL_TRADE_QTY", "0.0002")),
        strategies=_build_registry(),
        default_strategy=os.environ.get("SENTINEL_STRATEGY", "sma"),
        strategy_usdt=Decimal(os.environ.get("SENTINEL_STRATEGY_USDT", "15")),
        strategy_bars=strategy_bars,
        allow_short=allow_short,
    )
    config = uvicorn.Config(
        ui, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")),
        log_level="info",
    )
    await uvicorn.Server(config).serve()


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()

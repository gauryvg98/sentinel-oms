"""Run the Sentinel paper-trading terminal against a testnet:

    python -m sentinel.ui        ->  http://localhost:8000

Needs: docker compose postgres (:5433) and the venue's keys in .env. The ledger
PERSISTS across restarts — startup recovery reconciles any non-terminal orders
against the real exchange before the page goes live. That is not a demo
behavior; that is the system.

Multi-bot: the terminal runs one bot per instrument, all on ONE account/ledger.
SENTINEL_SYMBOLS (comma-separated) seeds the initial roster; the UI '+' adds
more from the venue's predefined list. Every symbol's lot/tick/min rules are
fetched from the exchange — nothing is hardcoded.
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
from sentinel.ui.instruments import fetch_binance_spec, fetch_bybit_spec
from sentinel.ui.market import REST_BASE as SPOT_REST
from sentinel.ui.market import MarketData
from sentinel.ui.server import Venue, build_ui

DEFAULT_DB = "postgresql://sentinel:sentinel@127.0.0.1:5433/sentinel"
SYMBOL = os.environ.get("SENTINEL_SYMBOL", "BTCUSDT")
FUT_REST = "https://demo-fapi.binance.com"       # Binance Demo Trading (demo.binance.com)
FUT_STREAM = "wss://demo-fstream.binance.com"

# Hard exposure ceiling per bot, as a max notional (USDT). Turned into a
# base-unit cap at each symbol's price, so it means the same risk on BTC as DOGE.
MAX_NOTIONAL = Decimal(os.environ.get("SENTINEL_MAX_NOTIONAL", "5000"))

# The menu the "+" offers. These are just NAMES — each symbol's real trading
# rules (lot/tick/mins) are fetched from the exchange when it's added, and the
# add is refused if the exchange doesn't list it.
PERP_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
                "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT",
                # USDC-margined perps (need USDC collateral / multi-asset margin)
                "BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC", "XRPUSDC",
                "ADAUSDC", "AVAXUSDC", "LINKUSDC", "LTCUSDC", "SUIUSDC",
                "NEARUSDC", "DOGEUSDC", "AAVEUSDC")
SPOT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "LTCUSDT")


def _notional_cap(spec, price):
    """A fixed max notional -> this symbol's base-unit exposure cap at `price`.
    None means no cap (spot: can't short, and the budget already bounds size)."""
    if price and price > 0:
        return spec.round_qty(MAX_NOTIONAL / price)
    return None


def _venue() -> Venue:
    """Assemble the account's Venue: one shared adapter + per-symbol factories
    that fetch rules FROM the exchange. SENTINEL_VENUE picks spot (long/flat) /
    Binance-futures / Bybit (both long/short)."""
    interval = os.environ.get("SENTINEL_STRATEGY_INTERVAL", "1m")
    venue = os.environ.get("SENTINEL_VENUE")

    if venue == "bybit":
        from sentinel.broker.bybit import BybitFuturesAdapter
        from sentinel.ui.bybit_market import REST_BASE as BYBIT_REST
        from sentinel.ui.bybit_market import BybitBarFeed, BybitMarketData
        adapter = BybitFuturesAdapter(
            os.environ["BYBIT_KEY"], os.environ["BYBIT_SECRET"], symbols=PERP_SYMBOLS)
        return Venue(
            adapter=adapter, allow_short=True, predefined=PERP_SYMBOLS,
            default_symbol=SYMBOL, default_interval=interval,
            make_market=lambda s: BybitMarketData(s),
            make_bars=lambda s, iv: BybitBarFeed(s, iv),
            fetch_spec=lambda s: fetch_bybit_spec(BYBIT_REST, s),
            cap_for=_notional_cap,
        )

    if venue == "futures":
        # Default: Binance Demo Trading (demo-fapi). Override for the classic
        # auto-funded testnet: SENTINEL_FUT_REST=https://testnet.binancefuture.com
        # SENTINEL_FUT_STREAM=wss://fstream.binancefuture.com
        rest = os.environ.get("SENTINEL_FUT_REST", FUT_REST)
        stream = os.environ.get("SENTINEL_FUT_STREAM", FUT_STREAM)
        adapter = BinanceFuturesAdapter(
            os.environ["BINANCE_FUTURES_KEY"], os.environ["BINANCE_FUTURES_SECRET"],
            symbols=PERP_SYMBOLS, base_url=rest, ws_base=stream,
            leverage=int(os.environ.get("SENTINEL_LEVERAGE", "1")))
        return Venue(
            adapter=adapter, allow_short=True, predefined=PERP_SYMBOLS,
            default_symbol=SYMBOL, default_interval=interval,
            make_market=lambda s: MarketData(s, rest_base=rest, stream_base=stream,
                                             kline_path="/fapi/v1/klines"),
            make_bars=lambda s, iv: BarFeed(s, iv, rest_base=rest, stream_base=stream,
                                            kline_path="/fapi/v1/klines"),
            fetch_spec=lambda s: fetch_binance_spec(rest, "/fapi/v1/exchangeInfo", s),
            cap_for=_notional_cap,
            leverage=int(os.environ.get("SENTINEL_LEVERAGE", "1")),
        )

    adapter = BinanceSpotAdapter(
        os.environ["BINANCE_TESTNET_KEY"], os.environ["BINANCE_TESTNET_SECRET"],
        symbols=SPOT_SYMBOLS)
    return Venue(
        adapter=adapter, allow_short=False, predefined=SPOT_SYMBOLS,
        default_symbol=SYMBOL, default_interval=interval,
        make_market=lambda s: MarketData(s),
        make_bars=lambda s, iv: BarFeed(s, iv),
        fetch_spec=lambda s: fetch_binance_spec(SPOT_REST, "/api/v3/exchangeInfo", s),
        cap_for=lambda spec, price: None,
    )


def load_env() -> None:
    # Look in the CWD first (so running from the repo dir works no matter how the
    # package is installed), then next to the module. First hit wins.
    for env_file in (Path.cwd() / ".env", Path(__file__).parents[3] / ".env"):
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
            print(f"  loaded env from {env_file}", flush=True)
            return
    print("  no .env found (cwd or module dir) — using shell env + defaults",
          flush=True)


def _build_registry() -> dict:
    """The strategies the operator can select from at runtime, as FACTORIES
    (each call is a fresh, unwarmed instance) — every bot and any backtest each
    need their own. 'regime' is the v2 engine (Donchian breakout gated by an ADX
    regime filter + vol-target conviction)."""
    from sentinel.strategy import Params, RegimeTrendMR, SmaCross
    sma_fast = int(os.environ.get("SENTINEL_SMA_FAST", "5"))
    sma_slow = int(os.environ.get("SENTINEL_SMA_SLOW", "20"))
    # Tight protective stop for the SMA: cap the distance to the slow SMA at this
    # fraction of price (0 disables the cap → raw distance to the trend line).
    sma_stop_floor = Decimal(os.environ.get("SENTINEL_SMA_STOP_FLOOR_PCT", "0.005"))
    sma_stop_cap = Decimal(os.environ.get("SENTINEL_SMA_STOP_CAP_PCT", "0.01"))
    regime_params = Params(
        donchian_entry=int(os.environ.get("SENTINEL_DONCHIAN_ENTRY", "55")),
        donchian_exit=int(os.environ.get("SENTINEL_DONCHIAN_EXIT", "20")),
        adx_trend=float(os.environ.get("SENTINEL_ADX_TREND", "25")),
        adx_range=float(os.environ.get("SENTINEL_ADX_RANGE", "20")),
        enable_mean_reversion=os.environ.get("SENTINEL_MR", "0") == "1",
    )
    return {
        "sma": lambda: SmaCross(fast=sma_fast, slow=sma_slow,
                                stop_floor_pct=sma_stop_floor,
                                stop_cap_pct=sma_stop_cap),
        "sma-ls": lambda: SmaCross(fast=sma_fast, slow=sma_slow, short=True,
                                   stop_floor_pct=sma_stop_floor,
                                   stop_cap_pct=sma_stop_cap),
        "regime": lambda: RegimeTrendMR(regime_params),
    }


async def _serve() -> None:
    load_env()
    dsn = os.environ.get("DATABASE_URL", DEFAULT_DB)
    # synchronous_commit=off: don't block each COMMIT on the WAL fsync. That
    # fsync-per-commit is what saturates a small single-node Postgres under a
    # pegging fleet's fill flood (checkpoints falling tens of seconds behind ->
    # dropped connections -> halts). The only cost is a <1s window of committed
    # events that a hard pg crash could lose — which is exactly what this
    # system's reconcile-against-broker recovery re-derives on restart (fills
    # back-filled idempotently by exec_id). Durability backstop already exists.
    pool = await asyncpg.create_pool(
        dsn, min_size=2, max_size=10,
        server_settings={"synchronous_commit": "off"},
    )
    async with pool.acquire() as conn:
        await apply_migrations(conn)

    venue = _venue()
    # Loud banner so the active venue/db is never a mystery — spot vs futures vs
    # bybit is the single most confusing thing to get silently wrong.
    print(f"  VENUE: {os.environ.get('SENTINEL_VENUE') or 'spot'}  "
          f"·  adapter {type(venue.adapter).__name__}  "
          f"·  db {dsn.rsplit('/', 1)[-1]}  "
          f"·  short={'on' if venue.allow_short else 'off'}", flush=True)
    # Per-symbol exposure caps live in this shared dict; the guard resolves each
    # instrument's cap through it. The manager fills it as bots are added.
    caps: dict[str, Decimal | None] = {}
    # dsn -> single-writer enforcement: refuse to boot if another process already
    # owns this account/database.
    app = SentinelApp(pool, venue.adapter, dsn=dsn,
                      max_position=lambda sym: caps.get(sym))

    initial = tuple(
        s.strip().upper()
        for s in os.environ.get("SENTINEL_SYMBOLS", SYMBOL).split(",")
        if s.strip()
    )
    # Margin collateral: default multi-asset (all stablecoins count). For a
    # single-asset USDT-M account set SENTINEL_MARGIN_ASSETS=USDT.
    margin_env = os.environ.get("SENTINEL_MARGIN_ASSETS")
    margin_assets = ({a.strip().upper() for a in margin_env.split(",") if a.strip()}
                     if margin_env else None)
    # Does the exchange cross-collateralize (Binance "Multi-Asset Margin" ON)?
    # OFF (default) -> each perp is confined to its OWN settlement-asset balance,
    # so a USDC perp can't borrow USDT margin and sizing must not sum the pools.
    multi_asset_margin = os.environ.get(
        "SENTINEL_MULTI_ASSET_MARGIN", "false").strip().lower() in ("1", "true", "yes")
    # Risk-based sizing (opt-in): size each trade so a stop-out costs RISK_PCT of
    # equity, capped by MAX_LEVERAGE, stop at STOP_ATR_MULT×ATR. Enabled when
    # SENTINEL_RISK_PCT is set; otherwise the fixed-notional budget is used.
    risk_params = None
    if os.environ.get("SENTINEL_RISK_PCT"):
        from sentinel.risk import RiskParams
        risk_params = RiskParams(
            risk_pct=Decimal(os.environ["SENTINEL_RISK_PCT"]),
            max_leverage=Decimal(os.environ.get("SENTINEL_MAX_LEVERAGE", "3")),
            stop_atr_mult=Decimal(os.environ.get("SENTINEL_STOP_ATR_MULT", "2")),
            fallback_stop_pct=Decimal(os.environ.get("SENTINEL_STOP_FALLBACK_PCT", "0.02")),
            rr=Decimal(os.environ.get("SENTINEL_RR", "2")),
        )
        print(f"  SIZING: risk-based · {risk_params.risk_pct} equity/trade · "
              f"{risk_params.max_leverage}x max lev · stop {risk_params.stop_atr_mult}×ATR "
              f"· TP {risk_params.rr}R", flush=True)
        print(f"  MARGIN: {'multi-asset (pooled)' if multi_asset_margin else 'single-asset (per settlement pool)'} "
              f"· each bot sizes off its share of its pool", flush=True)
    else:
        print("  SIZING: fixed-notional budget  "
              "(set SENTINEL_RISK_PCT to enable risk-based sizing)", flush=True)
    ui = build_ui(
        app, venue,
        strategies=_build_registry(),
        default_strategy=os.environ.get("SENTINEL_STRATEGY", "sma"),
        strategy_usdt=Decimal(os.environ.get("SENTINEL_STRATEGY_USDT", "15")),
        caps=caps,
        initial_symbols=initial,
        margin_assets=margin_assets,
        risk_params=risk_params,
        multi_asset_margin=multi_asset_margin,
    )
    # Drop the benign "socket.send() raised exception." line the websockets lib
    # emits when a push races a client that just closed — pure noise (the send
    # simply targets a gone socket) that otherwise floods the logs. The real fix
    # for the volume is the client holding ONE socket; this just quiets the tail.
    import logging as _logging

    class _DropSocketSendNoise(_logging.Filter):
        def filter(self, record):
            return "socket.send() raised exception" not in record.getMessage()

    for _n in ("websockets", "websockets.server", "uvicorn.error"):
        _logging.getLogger(_n).addFilter(_DropSocketSendNoise())
    # the message is INFO on the per-connection websockets logger; raising the
    # parent level suppresses it there too (level is inherited, filters aren't).
    _logging.getLogger("websockets").setLevel(_logging.WARNING)

    config = uvicorn.Config(
        # bind 127.0.0.1 locally; 0.0.0.0 in a container (Fly sets HOST=0.0.0.0)
        ui, host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8000")),
        log_level="info", proxy_headers=True, forwarded_allow_ips="*",
    )
    await uvicorn.Server(config).serve()


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()

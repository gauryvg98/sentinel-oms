"""Sentinel paper-trading terminal: one page, chart first.

The buttons place REAL orders on Binance testnet through the full stack —
gateway -> guards -> ledger -> exchange -> user stream -> position. The
chart shows the same market those orders fill against; fills appear on it
as markers within a second of the exchange reporting them.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import hmac

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from sentinel.domain import Authority, EconomicOrderIntent, OrderState, Side
from sentinel.marks.pnl import compute_pnl
from sentinel.oms import PlacementBlocked
from sentinel.runtime import SentinelApp

from .instruments import InstrumentSpec
from .logbuffer import LogRing
from .market import MarketData
from .strategy_runner import StrategyRunner

STATIC = Path(__file__).parent / "static"
# Stablecoins counted 1:1 as dollar margin collateral for the equity model.
_STABLES = {"USDT", "USDC", "BUSD", "FDUSD", "DAI", "TUSD"}

_UNIT_S = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
_LIQ_SAFETY_S = 20.0   # slow safety re-anchor for cross-margin liq drift; the
                       # real refresh is event-driven off account changes
_LOGS_PUSH_MIN_S = 2.0   # max cadence for pushing the logs frame over the WS

# --- public read-only mode (deployment) --------------------------------------
# With SENTINEL_PUBLIC_READONLY set, every control (trade, start/stop, add/remove,
# size, strategy, timeframe) is refused unless the request carries the admin
# cookie. The live feed, charts, metrics, logs and backtest stay open to all.
# Admin unlocks by visiting /admin?token=<SENTINEL_ADMIN_TOKEN>.
_READONLY = os.environ.get("SENTINEL_PUBLIC_READONLY", "false").strip().lower() in (
    "1", "true", "yes")
_ADMIN_TOKEN = os.environ.get("SENTINEL_ADMIN_TOKEN", "")
_ADMIN_COOKIE = "sentinel_admin"


def _is_admin(request: "Request") -> bool:
    """A request may write when the deployment isn't read-only, or it carries the
    admin cookie matching SENTINEL_ADMIN_TOKEN (constant-time compare)."""
    if not _READONLY:
        return True
    tok = request.cookies.get(_ADMIN_COOKIE, "")
    return bool(_ADMIN_TOKEN) and hmac.compare_digest(tok, _ADMIN_TOKEN)


def _order_margin(entry: dict | None, lev: Decimal) -> Decimal:
    """Initial margin a resting limit ENTRY reserves: its remaining (unfilled)
    notional / leverage. Zero when there's no live order, no limit price, or
    nothing left to fill. Pure — the 'blocked' figure the exchange never streams
    but that its available-balance silently reflects."""
    if not entry or entry.get("limit_price") is None:
        return Decimal(0)
    remaining = entry["qty"] - entry["filled"]
    if remaining <= 0:
        return Decimal(0)
    return remaining * entry["limit_price"] / (lev or Decimal(1))


def interval_seconds(interval: str) -> int:
    """Binance interval string ('1m','4h','1d') -> seconds. Defaults to 1m."""
    try:
        return int(interval[:-1]) * _UNIT_S[interval[-1]]
    except (KeyError, ValueError):
        return 60


def consolidate_markers(fills: list[dict], interval_s: int) -> list[dict]:
    """Collapse raw fills into ONE marker per (candle, side).

    Many fills land inside a single bar — especially on coarse timeframes,
    where a whole session of trades falls in one 4h candle. Rather than stack
    arrows (or drop all but one), we bucket by the candle each fill belongs to
    and emit a single consolidated marker carrying the aggregate (count, total
    qty, VWAP) plus the individual fills for the hover tooltip. Snapping to the
    candle start is also what keeps markers ON real bars — fill wall-times are
    arbitrary seconds, candle times are interval-aligned."""
    buckets: dict[tuple[int, str], dict] = {}
    for f in fills:
        ct = (f["t"] // interval_s) * interval_s        # containing candle
        side = f["side"]
        kind = f.get("kind") or ("LONG" if side == "BUY" else "SHORT")
        b = buckets.get((ct, kind))                     # one marker per (candle, kind)
        qty, price = Decimal(f["qty"]), Decimal(f["price"])
        if b is None:
            b = buckets[(ct, kind)] = {
                "t": ct, "side": side, "kind": kind, "n": 0,
                "_qty": Decimal(0), "_notional": Decimal(0), "detail": [],
            }
        b["n"] += 1
        b["_qty"] += qty
        b["_notional"] += qty * price
        if len(b["detail"]) < 25:                       # cap hover list
            b["detail"].append({"t": f["t"], "side": side, "kind": kind,
                                 "qty": f["qty"], "price": f["price"]})
    out = []
    for b in buckets.values():
        vwap = (b["_notional"] / b["_qty"]) if b["_qty"] else Decimal(0)
        out.append({
            "t": b["t"], "side": b["side"], "kind": b["kind"], "n": b["n"],
            "qty": format(b["_qty"].normalize(), "f"),
            "price": format(vwap.quantize(Decimal("0.01")), "f"),
            "detail": sorted(b["detail"], key=lambda d: d["t"]),
        })
    out.sort(key=lambda m: m["t"])
    return out


async def check_invariants(app: SentinelApp) -> dict[str, bool]:
    pool = app.store._pool
    return {
        "positions=fills": not await pool.fetch(
            """
            SELECT p.instrument FROM positions p
            LEFT JOIN (
              SELECT o.instrument,
                     SUM(CASE WHEN o.side='BUY' THEN f.qty ELSE -f.qty END) AS q
              FROM fills f JOIN orders o ON o.order_id=f.order_id
              GROUP BY o.instrument
            ) s ON s.instrument = p.instrument
            WHERE p.qty != COALESCE(s.q, 0)
            """
        ),
        "no_overfill": not await pool.fetch(
            "SELECT 1 FROM orders WHERE filled_qty > qty"
        ),
        "exits_bounded": not await pool.fetch(
            """
            SELECT o.instrument FROM orders o
            LEFT JOIN positions p ON p.instrument = o.instrument
            WHERE o.authority='PROTECTIVE_EXIT'
              AND o.state NOT IN ('FILLED','CANCELED','REJECTED')
            GROUP BY o.instrument, p.qty
            HAVING SUM(o.qty - o.filled_qty) > COALESCE(ABS(p.qty), 0)
            """
        ),
        "audit_traced": not await pool.fetch(
            "SELECT 1 FROM events WHERE trace_id IS NULL"
        ),
    }


async def order_stats(app: SentinelApp) -> dict:
    """Cheap OMS-state counts for the terminal: orders grouped by state plus
    the running fill total. Read-only aggregates, no projection dependency."""
    pool = app.store._pool
    rows = await pool.fetch("SELECT state, COUNT(*) AS n FROM orders GROUP BY state")
    states = {r["state"]: r["n"] for r in rows}
    return {
        "states": states,
        "orders_total": sum(states.values()),
        "fills_total": await pool.fetchval("SELECT COUNT(*) FROM fills") or 0,
    }


class Terminal:
    def __init__(self, app: SentinelApp, market: MarketData,
                 spec: InstrumentSpec) -> None:
        self.app = app
        self.market = market
        self.spec = spec                # the symbol's exchange rules (lot/tick/mins)
        # A bare manual order (no usdt/btc/pct) defaults to the exchange minimum
        # quantity for this symbol — never a hardcoded qty.
        self.trade_qty = spec.min_qty or spec.lot_step

    def _intent(self, side: Side, authority: Authority, qty: Decimal,
                limit_price: Decimal | None = None) -> EconomicOrderIntent:
        mark = self.market.latest(self.market.symbol)
        return EconomicOrderIntent(
            intent_id=uuid4(),
            idempotency_key=f"UI-{uuid4().hex[:12]}",
            instrument=self.market.symbol,
            side=side,
            qty=qty,
            limit_price=limit_price,          # None -> market (instant feedback)
            authority=authority,
            trace_id=uuid4(),
            quote_at_decision=mark.price if mark else None,
        )

    async def place_limit(self, side: str, qty: Decimal, price: Decimal,
                          authority: Authority = Authority.ENTRY) -> dict:
        """Place a resting LIMIT order (maker). Same guarded path as a market
        order — only the order type differs."""
        qty = self.spec.round_qty(qty)
        price = self.spec.round_price(price, side)   # snap to tick, stay maker-side
        if qty <= 0:
            return {"blocked": "ZeroSize", "reason": "nothing to trade"}
        if not self.spec.tradeable(qty, price):
            return {"blocked": "BelowMinimum",
                    "reason": f"below exchange minimum "
                              f"(qty {self.spec.min_qty} / notional {self.spec.min_notional})"}
        try:
            with self.app.metrics.timer("place_ms"):
                stored = await self.app.gateway.place(
                    uuid4(), self._intent(Side(side), authority, qty, price)
                )
            return await self._classify(stored)
        except PlacementBlocked as e:
            self.app.metrics.inc("orders_blocked")
            return {"blocked": type(e).__name__, "reason": str(e)}

    async def cancel(self, client_order_id: str) -> dict:
        """Request cancellation of a working order (confirmation arrives on the
        broker stream). Idempotent through the gateway."""
        try:
            stored = await self.app.gateway.cancel(
                uuid4(), client_order_id, uuid4()
            )
            return {"canceling": client_order_id, "state": stored.core.state.value}
        except Exception as e:  # noqa: BLE001
            return {"error": str(e)}

    async def _classify(self, stored) -> dict:
        """A returned order is NOT proof of success — a broker rejection comes
        back as a REJECTED order, not an exception. Report the outcome the
        ledger actually recorded."""
        state = stored.core.state
        key = stored.core.client_order_id
        if state is OrderState.REJECTED:
            self.app.metrics.inc("orders_rejected")
            reason = await self.app.store.order_reject_reason(key)
            return {"rejected": key, "reason": reason or "broker rejected"}
        if state is OrderState.UNKNOWN:
            self.app.metrics.inc("orders_placed")
            return {"pending": key, "state": state.value,
                    "note": "submission unprovable — reconciling"}
        self.app.metrics.inc("orders_placed")
        return {"placed": key, "state": state.value, "qty": str(stored.core.qty)}

    async def _size(self, side: str, usdt, btc, pct) -> Decimal:
        """Resolve the order quantity (in BTC) from whatever sizing was asked:
          btc  — an explicit quantity (either side); used to cover/trim a short
                 (BUY) or shed a long (SELL), so the runner's reduce legs work.
          BUY  — usdt amount, or pct of USDT balance -> qty = spend / price
          SELL — pct of the open position
        Rounded down to the lot step. The guards still clamp a reduce to what you
        actually hold, so this can only ever undershoot."""
        mark = self.market.latest(self.market.symbol)
        if mark is None:
            return Decimal(0)
        if btc is not None:                        # explicit qty, side-agnostic
            qty = Decimal(str(btc))
        elif side == "BUY":
            if usdt is not None:
                spend = Decimal(str(usdt))
            elif pct is not None:
                bal = self.app.latest_balances.get("USDT", Decimal(0))
                spend = bal * Decimal(str(pct)) / 100
            else:
                spend = self.trade_qty * mark.price
            qty = spend / mark.price
        else:  # SELL by fraction of the open position
            pos = await self.app.store.get_position(self.market.symbol)
            frac = Decimal(str(pct)) / 100 if pct is not None else Decimal(1)
            qty = pos * frac
        return self.spec.round_qty(abs(qty))

    async def trade(self, side: str, *, usdt=None, btc=None, pct=None,
                    authority: Authority | None = None) -> dict:
        """BUY opens/extends (ENTRY); SELL reduces (PROTECTIVE_EXIT). On futures
        a cover (BUY that REDUCES a short) passes authority=PROTECTIVE_EXIT so it
        is clamped by never-over-exit, not treated as a fresh open."""
        qty = await self._size(side, usdt, btc, pct)
        if qty <= 0:
            return {"blocked": "ZeroSize",
                    "reason": "nothing to trade at that size"}
        if authority is None:
            authority = Authority.ENTRY if side == "BUY" else Authority.PROTECTIVE_EXIT
        try:
            with self.app.metrics.timer("place_ms"):
                stored = await self.app.gateway.place(
                    uuid4(), self._intent(Side(side), authority, qty)
                )
            return await self._classify(stored)
        except PlacementBlocked as e:
            self.app.metrics.inc("orders_blocked")
            return {"blocked": type(e).__name__, "reason": str(e)}


# ============================================================ multi-bot layer

@dataclass
class Venue:
    """Everything build_ui needs to run bots on one exchange account, without
    knowing which exchange it is. The shared `adapter` carries account-wide
    execution; the factories build per-symbol market data, bars and rules. This
    is where 'spot vs Binance-futures vs Bybit' lives — nothing downstream
    branches on venue, and NOTHING hardcodes a symbol's lot/tick (fetch_spec
    reads them from the exchange)."""

    adapter: object
    allow_short: bool
    predefined: tuple[str, ...]
    default_symbol: str
    make_market: Callable[[str], MarketData]
    make_bars: Callable[[str, str], object]
    fetch_spec: Callable[[str], Awaitable[InstrumentSpec]]
    # (spec, reference_price) -> hard signed exposure cap in base units, so a
    # fixed max notional becomes the right qty for BTC, ETH or DOGE alike.
    cap_for: Callable[[InstrumentSpec, "Decimal | None"], "Decimal | None"]
    default_interval: str = "1m"
    leverage: int = 1               # margin = notional / leverage (equity model)


class Bot:
    """One instrument's independent trading bot: its own market feed, bar clock,
    exchange rules (spec), order terminal and strategy runner. Bots share the
    single account/ledger/gateway but never collide — every write is keyed and
    advisory-locked by instrument, so tens of these run side by side."""

    def __init__(self, app: SentinelApp, venue: Venue, symbol: str, market,
                 bars, spec: InstrumentSpec, strategies: dict, *,
                 default_strategy: str, size_usdt: Decimal,
                 equity_fn=None, risk_params=None) -> None:
        self.app = app
        self.venue = venue
        self.symbol = symbol
        self.market = market
        self.bars = bars
        self.spec = spec
        self.strategies = strategies
        self.size = {"usdt": size_usdt}
        self.current = {"name": default_strategy}
        self.equity_fn = equity_fn        # account equity for risk-based sizing
        self.risk_params = risk_params    # None -> fixed-notional budget sizing
        self.liq_price: Decimal | None = None   # anchored liquidation price; the
                                                 # live distance is derived per
                                                 # mark tick in card()
        self.terminal = Terminal(app, market, spec)
        self.runner = self._build_runner(strategies[default_strategy]())

    # -- the peg's touch: rest at the near side; fall back to the mark --------
    def _bid(self) -> Decimal | None:
        b = self.market.best_bid()
        if b is not None:
            return b
        m = self.market.latest(self.symbol)
        return m.price if m else None

    def _ask(self) -> Decimal | None:
        a = self.market.best_ask()
        if a is not None:
            return a
        m = self.market.latest(self.symbol)
        return m.price if m else None

    def _liq_view(self, mark: Decimal | None) -> dict | None:
        """Liquidation display for the card: the anchored liq price + its live
        distance from the CURRENT mark. Pure/in-memory — recomputed on every mark
        tick (this runs in card()), so the distance tracks price with zero fetch.
        None when flat or no mark."""
        if self.liq_price is None or mark is None or mark <= 0:
            return None
        dist = abs(self.liq_price - mark) / mark * 100
        return {"liq": format(self.liq_price.normalize(), "f"),
                "mark": format(mark.normalize(), "f"),
                "dist_pct": str(round(dist, 1))}

    async def _notify_order_change(self) -> None:
        """The runner moved an order (placed / filled / canceled / bracket exit)
        — which changes the account-level wallet roll-up (blocked / committed /
        available), not just this card. Bump BOTH topics so the wallet pushes
        live over the WS instead of waiting for the dead-quiet heartbeat.
        Price-only ticks still bump the symbol alone (via market.on_change)."""
        await self.app.changes.bump(self.symbol)
        await self.app.changes.bump("account")

    def _build_runner(self, strategy) -> StrategyRunner:
        t = self.terminal
        return StrategyRunner(
            strategy, self.bars,
            position_fn=lambda: self.app.store.get_position(self.symbol),
            open_entry_fn=lambda: self.app.store.open_entry(self.symbol),
            place_entry_fn=lambda qty, price: t.place_limit("BUY", qty, price),
            reduce_sell_fn=lambda qty: t.trade("SELL", btc=float(qty)),
            place_short_fn=lambda qty, price: t.place_limit("SELL", qty, price),
            reduce_buy_fn=lambda qty: t.trade(
                "BUY", btc=float(qty), authority=Authority.PROTECTIVE_EXIT),
            cancel_fn=lambda key: t.cancel(key),
            bid_fn=self._bid, ask_fn=self._ask,
            budget_fn=lambda: self.size["usdt"],
            on_change=self._notify_order_change,
            allow_short=self.venue.allow_short,
            lot_step=self.spec.lot_step,
            equity_fn=self.equity_fn,
            risk_params=self.risk_params,
            entry_fn=self.avg_cost,
        )

    async def spawn(self) -> None:
        """Warm history and put this bot's three feeds under supervision, each
        tagged with the symbol so a change patches only this card."""
        await self.market.load_history()
        await self.bars.load_history()
        self.market.on_change = functools.partial(self.app.changes.bump, self.symbol)
        self.app.supervisor.spawn(f"market:{self.symbol}", self.market.run, restart=True)
        self.app.supervisor.spawn(f"bars:{self.symbol}", self.bars.run, restart=True)
        self.app.supervisor.spawn(f"strategy:{self.symbol}", self.runner.run, restart=True)

    def start(self) -> None:
        self.runner.start()

    def stop(self) -> None:
        self.runner.stop()

    async def select(self, name: str) -> dict:
        if name not in self.strategies:
            return {"error": "unknown strategy"}
        self.current["name"] = name
        await self.runner.set_strategy(self.strategies[name]())
        return {"selected": name}

    async def set_size(self, v: Decimal) -> None:
        self.size["usdt"] = v

    async def set_timeframe(self, interval: str) -> None:
        await self.market.set_interval(interval)

    async def close(self) -> None:
        """Graceful teardown (never disturbs other bots' orders): stop opening,
        cancel this symbol's working orders, FLATTEN the position at market, then
        cancel this bot's supervised tasks."""
        self.runner.stop()
        for o in await self.app.store.recent_orders(50, self.symbol):
            if o["state"] not in ("FILLED", "CANCELED", "REJECTED"):
                await self.terminal.cancel(o["key"])
        pos = await self.app.store.get_position(self.symbol)
        if pos > 0:
            await self.terminal.trade("SELL", btc=float(pos),
                                      authority=Authority.PROTECTIVE_EXIT)
        elif pos < 0:
            await self.terminal.trade("BUY", btc=float(-pos),
                                      authority=Authority.PROTECTIVE_EXIT)
        for kind in ("strategy", "bars", "market"):
            await self.app.supervisor.cancel(f"{kind}:{self.symbol}")

    def _spark(self, n: int = 48) -> list[float]:
        cs = self.market.candles
        if not cs:
            return []
        step = max(1, len(cs) // n)
        return [c["c"] for c in cs[::step]][-n:]

    async def _pnl(self):
        return (await compute_pnl(self.app.store._pool, self.market)).get(self.symbol)

    async def avg_cost(self) -> Decimal | None:
        """The open position's entry price — anchors the risk layer's SL/TP."""
        pnl = await self._pnl()
        return pnl.avg_cost if pnl else None

    async def card(self) -> dict:
        """Compact, always-live state for this bot's card. No candles (a
        sparkline stands in) so tens of these stay cheap on the wire."""
        pnl = await self._pnl()
        s = self.runner.snapshot()
        mark = self.market.latest(self.symbol)
        q = lambda v: str(v.quantize(Decimal("0.01"))) if v is not None else None
        working = [o for o in await self.app.store.recent_orders(20, self.symbol)
                   if o["state"] not in ("FILLED", "CANCELED", "REJECTED")]
        # Two P&L views on the position's cost basis (== margin at 1x):
        #   running (roe)  — unrealized only: how the OPEN position is doing now
        #   net (net_roe)  — realized + unrealized: total incl. closed trades
        net = roe = net_roe = None
        if pnl:
            net = (pnl.realized or Decimal(0)) + (pnl.unrealized or Decimal(0))
            basis = (abs(pnl.avg_cost * pnl.position)
                     if pnl.avg_cost and pnl.position else None)
            if basis and basis > 0:
                if pnl.unrealized is not None:
                    roe = pnl.unrealized / basis * 100
                net_roe = net / basis * 100
        return {
            "type": "card",
            "symbol": self.symbol,
            "price": str(mark.price) if mark else None,
            "price_age": self.market.price_age_s,
            "interval": self.market.interval,
            "strategy_interval": s.get("interval"),
            "running": s["running"],
            "stance": s["stance"],
            "last_action": s["last_action"],
            "strategy": s["name"],
            "selected": self.current["name"],
            "available": list(self.strategies),
            "entry_usdt": format(self.size["usdt"], "f"),
            "position": format(pnl.position.normalize(), "f") if pnl else "0",
            "avg_cost": q(pnl.avg_cost) if pnl else None,
            "realized": q(pnl.realized) if pnl else "0.00",
            "unrealized": q(pnl.unrealized) if pnl else None,
            "net": q(net),
            "roe": str(round(roe, 2)) if roe is not None else None,
            "net_roe": str(round(net_roe, 2)) if net_roe is not None else None,
            # Distance to liquidation is derived HERE, off the streamed mark, so
            # it moves live on every price tick with no extra fetch. The liq price
            # itself is re-anchored reactively (liq_refresh_loop) on account changes.
            "liq": self._liq_view(mark.price if mark else None),
            "working": len(working),
            "spark": self._spark(),
            "spec": {"lot_step": str(self.spec.lot_step),
                     "tick": str(self.spec.price_tick),
                     "min_qty": str(self.spec.min_qty),
                     "min_notional": str(self.spec.min_notional)},
        }

    async def detail(self, with_candles: bool = True) -> dict:
        """Full chart payload for an expanded card: candles, fill markers, the
        working-order list, decisions and the strategy's own overlay spec."""
        s = self.runner.snapshot()
        d = {
            "type": "detail",
            "symbol": self.symbol,
            "interval": self.market.interval,
            "bid": str(b) if (b := self.market.best_bid()) is not None else None,
            "ask": str(a) if (a := self.market.best_ask()) is not None else None,
            "candle": self.market.candles[-1] if self.market.candles else None,
            "markers": consolidate_markers(
                await self.app.store.recent_fills(
                    self.symbol, limit=1000,
                    since=(self.market.candles[0]["t"] if self.market.candles else None)),
                interval_seconds(self.market.interval)),
            "working": [o for o in await self.app.store.recent_orders(30, self.symbol)
                        if o["state"] not in ("FILLED", "CANCELED", "REJECTED")],
            "trades": await self.app.store.recent_fills(self.symbol, limit=25),
            "decisions": await self.app.store.recent_decisions(8, self.symbol),
            "strategy": {**s, "entry_usdt": format(self.size["usdt"], "f")},
        }
        if with_candles:
            d["candles"] = self.market.candles
        return d


class InstrumentManager:
    """Owns the live roster of bots on one account. add()/remove() at runtime;
    the shared caps dict feeds the exposure guard's per-symbol resolver."""

    def __init__(self, app: SentinelApp, venue: Venue, strategies: dict, *,
                 default_strategy: str, default_usdt: Decimal, caps: dict,
                 margin_assets: set[str] | None = None, risk_params=None,
                 multi_asset_margin: bool = False,
                 log_ring: LogRing | None = None) -> None:
        self.app = app
        self.venue = venue
        self.strategies = strategies
        self.default_strategy = default_strategy
        self.default_usdt = default_usdt
        self.caps = caps                       # {symbol: cap} read by the guard
        # Which balances count as margin collateral. Multi-asset mode -> all
        # stablecoins; single-asset USDT-M -> just {"USDT"}.
        self.margin_assets = margin_assets or set(_STABLES)
        # Does the EXCHANGE cross-collateralize (multiAssetsMargin=ON)? If so all
        # collateral is one pool a USDC perp can draw USDT from; if OFF (Binance
        # default) each perp is confined to its OWN settlement asset's balance —
        # summing pools would over-size and get -2019 Margin insufficient.
        self.multi_asset_margin = multi_asset_margin
        self.log_ring = log_ring               # recent server log records (logs page)
        self._started = time.time()            # for the status page's uptime
        self.risk_params = risk_params         # None -> fixed-notional budget sizing
        self.bots: dict[str, Bot] = {}
        # Initial-load progress for the boot loading screen: reveal the grid only
        # once every bot is up and in its correct live state.
        self.seed_target = 0
        self.seed_done = False
        self._lock = asyncio.Lock()

    def account_cash(self) -> Decimal:
        """Total stablecoin collateral across all pools — for the account-level
        equity display only. NOT for sizing (that must respect per-asset pools);
        use equity_for(symbol)."""
        return sum((v for a, v in self.app.latest_balances.items()
                    if a in self.margin_assets), Decimal(0))

    def _settle_asset(self, symbol: str) -> str:
        """The asset a symbol's margin/PnL settle in (USDT for *USDT perps, USDC
        for *USDC). From the exchange-fetched spec — falls back to a suffix match
        on the known collateral assets when the venue omits it."""
        bot = self.bots.get(symbol)
        qa = getattr(bot.spec, "quote_asset", "") if bot is not None else ""
        if qa:
            return qa
        for a in sorted(self.margin_assets, key=len, reverse=True):
            if symbol.endswith(a):
                return a
        return "USDT"

    def equity_for(self, symbol: str) -> Decimal:
        """Sizing equity for ONE bot: its SHARE of the margin pool it actually
        draws on. Single-asset margin -> only the symbol's settlement-asset
        balance; multi-asset -> all collateral summed. Divided across the bots
        that compete for the SAME pool, so N bots can't each size against the
        whole account (the bug that placed ~3x-equity orders per bot and drew a
        flood of -2019 rejects)."""
        bal = self.app.latest_balances
        if self.multi_asset_margin:
            pool = sum((v for a, v in bal.items() if a in self.margin_assets),
                       Decimal(0))
            sharers = len(self.bots) or 1
        else:
            asset = self._settle_asset(symbol)
            pool = bal.get(asset, Decimal(0))
            sharers = sum(1 for s in self.bots
                          if self._settle_asset(s) == asset) or 1
        return pool / sharers

    async def liq_refresh_loop(self) -> None:
        """Re-anchor each position's liquidation PRICE — event-driven, not a
        timer poll. Wakes on the change stream and re-anchors only when the
        ACCOUNT moved (an order filled / balance shifted → the liq price jumps);
        plain mark ticks are ignored here because the live distance is derived
        off the mark in Bot.card(). A slow safety wake (_LIQ_SAFETY_S) catches
        the gradual cross-margin drift from OTHER positions' P&L.

        Display-only and unsupervised: swallows everything, never halts. Futures-
        only; a venue without open_positions is silently skipped."""
        adapter = self.venue.adapter
        if not hasattr(adapter, "open_positions"):
            return
        seen = self.app.changes.revision
        while True:
            nxt = await self.app.changes.wait_past(seen, timeout=_LIQ_SAFETY_S)
            topics = self.app.changes.topics_since(seen)
            seen = nxt
            # Only the liq PRICE needs a refetch, and it only moves on an account
            # change (fill/balance) — skip pure card/mark churn. Empty topics == a
            # safety timeout, which we DO honour (cross-margin drift).
            if topics and "account" not in topics:
                continue
            try:
                positions = await adapter.open_positions()
            except Exception:  # noqa: BLE001 — display only, never propagate
                continue
            for sym, bot in list(self.bots.items()):
                bp = positions.get(sym)
                anchor = bp.liq_price if bp else None
                if anchor != bot.liq_price:
                    bot.liq_price = anchor
                    await self.app.changes.bump(sym)

    def get(self, symbol: str) -> Bot | None:
        return self.bots.get(symbol.upper())

    def roster(self) -> list[str]:
        return list(self.bots)

    async def add(self, symbol: str, *, allow_unlisted: bool = False) -> dict:
        # allow_unlisted: bring up a bot for a symbol OUTSIDE the predefined
        # menu — used on boot to surface a symbol that already carries an open
        # position, so a live trade is never left without a card.
        symbol = symbol.upper()
        async with self._lock:
            if symbol in self.bots:
                return {"error": "already added"}
            if not allow_unlisted and symbol not in self.venue.predefined:
                return {"error": f"{symbol} not in the predefined list"}
            try:
                spec = await self.venue.fetch_spec(symbol)
            except Exception as e:  # noqa: BLE001
                return {"error": f"could not fetch {symbol} rules: {e}"}
            market = self.venue.make_market(symbol)
            bars = self.venue.make_bars(symbol, self.venue.default_interval)
            # Each bot sizes off its OWN share of the settlement-asset pool
            # (equity_for), never the whole account — bound at construction to
            # this symbol.
            bot = Bot(self.app, self.venue, symbol, market, bars, spec,
                      self.strategies, default_strategy=self.default_strategy,
                      size_usdt=self.default_usdt,
                      equity_fn=functools.partial(self.equity_for, symbol),
                      risk_params=self.risk_params)
            await bot.spawn()                       # loads history -> a mark exists
            self.bots[symbol] = bot                 # register BEFORE cap so it
                                                    # counts in its own pool share
            mark = market.latest(symbol)
            price = mark.price if mark else None
            # Hard per-symbol exposure backstop. With risk-based sizing the cap is
            # the leverage limit (equity_share·maxLev/price) so it never clips a
            # correctly-risk-sized position; otherwise the venue's notional cap.
            if self.risk_params is not None and price:
                self.caps[symbol] = spec.round_qty(
                    self.equity_for(symbol) * self.risk_params.max_leverage / price)
            else:
                self.caps[symbol] = self.venue.cap_for(spec, price)
        await self.app.changes.bump("roster")
        await self.app.changes.bump(symbol)
        return {"added": symbol}

    async def remove(self, symbol: str) -> dict:
        symbol = symbol.upper()
        async with self._lock:
            bot = self.bots.get(symbol)
            if bot is None:
                return {"error": "not active"}
            await bot.close()              # flatten + cancel + stop this bot only
            del self.bots[symbol]
            self.caps.pop(symbol, None)
        await self.app.changes.bump("roster")
        return {"removed": symbol}

    async def account_snapshot(self) -> dict:
        """Account-wide state every card shares (topic 'account'/'roster'):
        liveness, metrics, order totals, invariants, roster, wallet.

        USDT-M futures margin model (matches Binance): cash is the USDT wallet
        balance; each open position ties up initial margin (notional / leverage)
        and carries unrealized P&L.
            equity    = cash + Σ unrealized              (Binance 'margin balance')
            committed = Σ position margin                 (initial margin in use)
            blocked   = Σ resting-order margin            (reserved by open orders)
            available = cash − committed − blocked        (free to open more)
        Non-USDT balances aren't marked into equity — on single-asset USDT-M
        they don't exist; pricing a stray demo dust asset only misleads."""
        balances = self.app.latest_balances
        quote = "USDT"
        q = lambda v: str(v.quantize(Decimal("0.01"))) if v is not None else None
        # Dollar collateral = every stablecoin balance at ~1:1 (multi-asset
        # margin holds USDT AND USDC/etc.). Counting only USDT would understate
        # equity vs what the exchange shows when both are funded.
        cash = sum((v for a, v in balances.items() if a in self.margin_assets),
                   Decimal(0))
        unrealized = Decimal(0)     # running: open positions, mark-to-market
        realized = Decimal(0)       # locked in from closed trades
        committed = Decimal(0)      # margin tied up by open POSITIONS
        blocked = Decimal(0)        # margin reserved by resting (unfilled) ORDERS
        lev = Decimal(self.venue.leverage or 1)
        for bot in self.bots.values():
            pnl = await bot._pnl()
            if pnl is not None:
                if pnl.unrealized is not None:
                    unrealized += pnl.unrealized
                realized += pnl.realized or Decimal(0)
                if pnl.avg_cost and pnl.position:
                    committed += abs(pnl.avg_cost * pnl.position) / lev
            # A resting limit entry ties up initial margin = remaining notional /
            # leverage until it fills or cancels. Binance never streams this
            # number (no balance event on placement) — we derive it from the
            # working order we already track.
            blocked += _order_margin(await self.app.store.open_entry(bot.symbol), lev)
        net = realized + unrealized     # total P&L across the whole account
        equity = cash + unrealized
        available = cash - committed - blocked   # free to open more
        # Leverage: the configured exchange setting (the 1x/2x/3x cap) vs. the
        # EFFECTIVE leverage actually in use = gross position notional / equity.
        # committed is notional/lev, so committed*lev recovers the gross notional.
        gross_notional = committed * lev
        eff_lev = (gross_notional / equity) if equity > 0 else Decimal(0)
        shown = {a: format(v.normalize(), "f")
                 for a, v in sorted(balances.items()) if v}
        return {
            "type": "account",
            "accepting": self.app.accepting,
            "halted": self.app.supervisor.halted.is_set(),
            "uptime_s": round(time.time() - self._started),
            "task_failures": len(self.app.supervisor.failures),
            "metrics": self.app.metrics.snapshot(),
            "orders": await order_stats(self.app),
            "invariants": await check_invariants(self.app),
            "roster": self.roster(),
            "predefined": list(self.venue.predefined),
            "strategies": list(self.strategies),
            "seeding": {"ready": len(self.bots), "target": self.seed_target,
                        "done": self.seed_done},
            "wallet": {
                "balances": shown, "quote": quote,
                "cash": q(cash), "equity": q(equity),
                "committed": q(committed), "blocked": q(blocked),
                "available": q(available),
                "leverage": str(int(lev)) if lev == lev.to_integral_value() else str(lev),
                "eff_leverage": str(eff_lev.quantize(Decimal("0.01"))),
                "unrealized": q(unrealized),
                "running": q(unrealized),     # portfolio running P&L (open)
                "realized": q(realized),
                "net": q(net),                # portfolio net P&L (realized+unreal)
            },
        }

    async def logs_snapshot(self) -> dict:
        """Feed for the logs page: the operational SYSTEM log (in-memory ring)
        and the durable EVENT ledger (order lifecycle). Both newest-first for the
        UI. Cheap enough to push every couple of seconds over the WS."""
        events = await self.app.store.recent_events(80)
        system = list(reversed(self.log_ring.tail(200))) if self.log_ring else []
        return {"type": "logs", "system": system, "events": events}


def build_ui(app: SentinelApp, venue: Venue, *, strategies: dict | None = None,
             default_strategy: str | None = None,
             strategy_usdt: Decimal = Decimal("15"),
             caps: dict | None = None,
             initial_symbols: tuple[str, ...] = (),
             margin_assets: set[str] | None = None,
             risk_params=None,
             multi_asset_margin: bool = False) -> FastAPI:
    strategies = strategies or {}
    default_strategy = default_strategy or next(iter(strategies), None)
    caps = caps if caps is not None else {}
    ui = FastAPI(title="sentinel-terminal")
    from .logbuffer import install as install_log_ring
    log_ring = install_log_ring()          # capture sentinel.* logs for the UI
    mgr = InstrumentManager(app, venue, strategies,
                            default_strategy=default_strategy,
                            default_usdt=strategy_usdt, caps=caps,
                            risk_params=risk_params,
                            margin_assets=margin_assets,
                            multi_asset_margin=multi_asset_margin,
                            log_ring=log_ring)
    ui.state.manager = mgr

    @ui.middleware("http")
    async def _readonly_guard(request: Request, call_next):
        # In a public deployment, refuse every state-changing POST unless the
        # caller is admin. /backtest is a pure read-only computation and /admin
        # is the unlock itself, so both stay open.
        if (_READONLY and request.method == "POST"
                and request.url.path != "/backtest"
                and not _is_admin(request)):
            return JSONResponse(
                {"error": "read-only demo — controls are disabled"},
                status_code=403)
        return await call_next(request)

    @ui.get("/admin")
    async def _admin(token: str = ""):
        # Visit /admin?token=<SENTINEL_ADMIN_TOKEN> once to drop the admin cookie,
        # then the controls unlock for this browser. No-op when not read-only.
        resp = RedirectResponse("/", status_code=303)
        if _READONLY and _ADMIN_TOKEN and hmac.compare_digest(token, _ADMIN_TOKEN):
            resp.set_cookie(_ADMIN_COOKIE, _ADMIN_TOKEN, httponly=True,
                            samesite="lax", max_age=30 * 86400)
        return resp

    @ui.get("/whoami")
    async def _whoami(request: Request):
        return {"readonly": _READONLY, "admin": _is_admin(request)}

    @ui.on_event("startup")
    async def _startup() -> None:
        from sentinel.runtime import AnotherWriterActive
        try:
            await app.start(arm_protection=False)     # claims the account lock
        except AnotherWriterActive as e:
            print(f"\n  REFUSING TO START: {e}\n", flush=True)
            os._exit(1)
        # Seed the roster in the BACKGROUND so the page binds immediately and
        # cards stream in as each bot's history loads — booting N bots serially
        # in the startup hook would otherwise hold the port for tens of seconds.
        app.supervisor.spawn("seed-roster", _seed_roster, restart=False)
        # Liquidation re-anchor — event-driven off the change stream, display-only,
        # deliberately NOT supervised so a hiccup can't halt the account. Held on
        # ui.state so the task isn't garbage-collected.
        ui.state.liq_task = asyncio.create_task(mgr.liq_refresh_loop())

    async def _seed_roster() -> None:
        async def _try_add(sym: str, **kw) -> None:
            try:
                await mgr.add(sym, **kw)
            except Exception as e:  # noqa: BLE001 — one bad symbol mustn't halt
                print(f"  could not add {sym}: {e!r}", flush=True)

        # Full target up front (configured roster + every symbol already holding
        # a position, recovered from the durable ledger) so the loading screen
        # can show real progress.
        positions = await app.store.load_positions()
        target = list(dict.fromkeys([*initial_symbols, *positions]))
        mgr.seed_target = len(target)
        await app.changes.bump("account")

        for sym in target:
            if mgr.get(sym) is None:
                await _try_add(sym, allow_unlisted=sym not in initial_symbols)
            await app.changes.bump("account")           # advance the % bar

        # Every bot runs by default — Sentinel is an always-on fleet, and an open
        # position MUST be managed regardless (unmanaged exposure is naked risk).
        # Arm them all BEFORE we reveal, so each card shows its true live state,
        # never a stale "off". Admin can stop or close individual bots at runtime.
        for sym in target:
            bot = mgr.get(sym)
            if bot is not None and not bot.runner.running:
                bot.start()
                await app.changes.bump(sym)

        mgr.seed_done = True
        await app.changes.bump("account")               # reveal the grid

        # Reconcile positions to their strategy target in the background, AFTER
        # the page is live — this does network I/O and must not gate the reveal.
        for sym in positions:
            bot = mgr.get(sym)
            if bot is not None:
                try:
                    await bot.runner.reconcile_now()
                except Exception:  # noqa: BLE001 — arming must never halt
                    pass

    @ui.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    @ui.get("/engineering")
    async def engineering():
        # Long-form engineering write-up served in-app (linked from the footer).
        return FileResponse(STATIC / "engineering.html")

    @ui.websocket("/ws")
    async def ws(websocket: WebSocket):
        """Topic-driven: full sync on connect (account + every card), then push
        ONLY the topics that changed — one bot card, or 'account'. A bot's tick
        patches its card alone; nothing reflushes the whole roster."""
        await websocket.accept()

        async def send_card(sym: str) -> None:
            bot = mgr.get(sym)
            if bot is not None:
                await websocket.send_text(json.dumps(await bot.card()))

        async def send_account() -> None:
            await websocket.send_text(json.dumps(await mgr.account_snapshot()))

        async def send_logs() -> None:
            await websocket.send_text(json.dumps(await mgr.logs_snapshot()))

        try:
            import asyncio as _aio
            await send_account()
            for sym in mgr.roster():
                await send_card(sym)
            await send_logs()
            seen = app.changes.revision
            last_logs = _aio.get_event_loop().time()
            while True:
                nxt = await app.changes.wait_past(seen, timeout=2.0)
                topics = app.changes.topics_since(seen)
                seen = nxt
                if not topics:                        # heartbeat (dead-quiet)
                    await send_account()
                else:
                    for topic in topics:
                        if topic in ("account", "roster"):
                            await send_account()
                        else:
                            await send_card(topic)
                # Logs (system ring + event ledger) — pushed on any wake but rate-
                # limited: a 2s tail is plenty and keeps the recent_events query
                # off the hot path. The 2s heartbeat guarantees it refreshes even
                # when idle. Server-pushed, never polled by the browser.
                now = _aio.get_event_loop().time()
                if now - last_logs >= _LOGS_PUSH_MIN_S:
                    await send_logs()
                    last_logs = now
        except WebSocketDisconnect:
            pass

    # ------------------------------------------------------------ roster ops
    @ui.post("/instrument/add/{symbol}")
    async def add_instrument(symbol: str):
        return await mgr.add(symbol)

    @ui.post("/instrument/remove/{symbol}")
    async def remove_instrument(symbol: str):
        return await mgr.remove(symbol)

    @ui.get("/instrument/{symbol}/detail")
    async def instrument_detail(symbol: str):
        bot = mgr.get(symbol)
        if bot is None:
            return {"error": "not active"}
        return await bot.detail(with_candles=True)

    # ------------------------------------------------------ per-symbol control
    @ui.post("/{symbol}/strategy/{action}")
    async def strategy_toggle(symbol: str, action: str):
        bot = mgr.get(symbol)
        if bot is None:
            return {"error": "not active"}
        if action == "start":
            bot.start()
            await bot.runner.reconcile_now()
        elif action == "stop":
            bot.stop()
        await app.changes.bump(bot.symbol)
        return bot.runner.snapshot()

    @ui.post("/{symbol}/strategy/select/{name}")
    async def strategy_select(symbol: str, name: str):
        bot = mgr.get(symbol)
        if bot is None:
            return {"error": "not active"}
        res = await bot.select(name)
        await app.changes.bump(bot.symbol)
        return res

    @ui.post("/{symbol}/size/{usdt}")
    async def set_size(symbol: str, usdt: str):
        bot = mgr.get(symbol)
        if bot is None:
            return {"error": "not active"}
        try:
            v = Decimal(usdt)
        except Exception:  # noqa: BLE001
            return {"error": "invalid amount"}
        if v <= 0:
            return {"error": "must be positive"}
        await bot.set_size(v)
        await app.changes.bump(bot.symbol)
        return {"entry_usdt": format(v, "f")}

    @ui.post("/{symbol}/timeframe/{interval}")
    async def set_timeframe(symbol: str, interval: str):
        bot = mgr.get(symbol)
        if bot is None:
            return {"error": "not active"}
        await bot.set_timeframe(interval)
        await app.changes.bump(bot.symbol)
        return {"interval": bot.market.interval}

    @ui.post("/{symbol}/cancel/{client_order_id}")
    async def cancel_order(symbol: str, client_order_id: str):
        bot = mgr.get(symbol)
        if bot is None:
            return {"error": "not active"}
        res = await bot.terminal.cancel(client_order_id)
        await app.changes.bump(bot.symbol)
        return res

    @ui.post("/{symbol}/trade/{side}")
    async def trade(symbol: str, side: str, usdt: str | None = None,
                    btc: str | None = None):
        """Manual marketable order on ONE bot, through the same guarded gateway
        the strategy uses. BUY = ENTRY, SELL = PROTECTIVE_EXIT."""
        bot = mgr.get(symbol)
        if bot is None:
            return {"error": "not active"}
        side = side.upper()
        if side not in ("BUY", "SELL"):
            return {"error": "side must be BUY or SELL"}
        kw = {}
        if usdt is not None:
            kw["usdt"] = Decimal(usdt)
        elif btc is not None:
            kw["btc"] = float(btc)
        res = await bot.terminal.trade(side, **kw)
        await app.changes.bump(bot.symbol)
        return res

    @ui.post("/backtest")
    async def backtest(symbol: str | None = None, interval: str = "1h",
                       days: int = 365, cost_bps: str = "10",
                       strategy: str | None = None, budget: str | None = None):
        """Backtest a strategy on real mainnet history — the same pure strategy +
        plan_action the live path uses. Symbol defaults to the first active bot;
        strategy/budget default to that bot's live selection."""
        sym = (symbol or (mgr.roster()[0] if mgr.roster() else venue.default_symbol)).upper()
        bot = mgr.get(sym)
        name = strategy or (bot.current["name"] if bot else default_strategy)
        if name not in strategies:
            return {"error": "no strategy"}
        factory = strategies[name]
        try:
            budget_v = Decimal(budget) if budget else (bot.size["usdt"] if bot else strategy_usdt)
        except Exception:  # noqa: BLE001
            return {"error": "invalid budget"}

        def _run():
            from sentinel.backtest.data import load_klines
            from sentinel.backtest.engine import run_backtest
            bars = load_klines(sym, interval, days)
            r = run_backtest(factory(), bars, symbol=sym, interval=interval,
                             budget=budget_v, cost_bps=Decimal(cost_bps))
            open0 = float(bars[0]["o"]) if bars else 1.0
            b = float(budget_v)
            merged = [(t, net, b * float(bars[i]["c"]) / open0)
                      for i, (t, net) in enumerate(r.equity_curve)]
            stepn = max(1, len(merged) // 300)
            curve = [{"t": t, "net": net, "hold": hold}
                     for t, net, hold in merged[::stepn]]
            return r, curve

        try:
            r, curve = await asyncio.to_thread(_run)
        except Exception as e:  # noqa: BLE001
            return {"error": f"backtest failed: {e}"}
        return {
            "strategy": name, "symbol": sym, "interval": interval, "days": days,
            "bars": r.bars, "trades": r.trades, "net_return": r.net_return,
            "gross_return": r.gross_return, "buy_hold_return": r.buy_hold_return,
            "sharpe": r.sharpe, "max_drawdown": r.max_drawdown, "fees": r.fees_paid,
            "turnover": r.turnover, "final_equity": r.final_equity,
            "budget": r.budget, "curve": curve,
        }

    return ui

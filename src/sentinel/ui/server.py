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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from sentinel.domain import Authority, EconomicOrderIntent, OrderState, Side
from sentinel.marks.pnl import compute_pnl
from sentinel.oms import PlacementBlocked
from sentinel.runtime import SentinelApp

from .instruments import InstrumentSpec
from .market import MarketData
from .strategy_runner import StrategyRunner

STATIC = Path(__file__).parent / "static"

_UNIT_S = {"m": 60, "h": 3600, "d": 86400, "w": 604800}


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
        b = buckets.get((ct, side))
        qty, price = Decimal(f["qty"]), Decimal(f["price"])
        if b is None:
            b = buckets[(ct, side)] = {
                "t": ct, "side": side, "n": 0,
                "_qty": Decimal(0), "_notional": Decimal(0), "detail": [],
            }
        b["n"] += 1
        b["_qty"] += qty
        b["_notional"] += qty * price
        if len(b["detail"]) < 25:                       # cap hover list
            b["detail"].append({"t": f["t"], "side": side,
                                 "qty": f["qty"], "price": f["price"]})
    out = []
    for b in buckets.values():
        vwap = (b["_notional"] / b["_qty"]) if b["_qty"] else Decimal(0)
        out.append({
            "t": b["t"], "side": b["side"], "n": b["n"],
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


class Bot:
    """One instrument's independent trading bot: its own market feed, bar clock,
    exchange rules (spec), order terminal and strategy runner. Bots share the
    single account/ledger/gateway but never collide — every write is keyed and
    advisory-locked by instrument, so tens of these run side by side."""

    def __init__(self, app: SentinelApp, venue: Venue, symbol: str, market,
                 bars, spec: InstrumentSpec, strategies: dict, *,
                 default_strategy: str, size_usdt: Decimal) -> None:
        self.app = app
        self.venue = venue
        self.symbol = symbol
        self.market = market
        self.bars = bars
        self.spec = spec
        self.strategies = strategies
        self.size = {"usdt": size_usdt}
        self.current = {"name": default_strategy}
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
            on_change=functools.partial(self.app.changes.bump, self.symbol),
            allow_short=self.venue.allow_short,
            lot_step=self.spec.lot_step,
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

    async def card(self) -> dict:
        """Compact, always-live state for this bot's card. No candles (a
        sparkline stands in) so tens of these stay cheap on the wire."""
        pnl = await self._pnl()
        s = self.runner.snapshot()
        mark = self.market.latest(self.symbol)
        q = lambda v: str(v.quantize(Decimal("0.01"))) if v is not None else None
        working = [o for o in await self.app.store.recent_orders(20, self.symbol)
                   if o["state"] not in ("FILLED", "CANCELED", "REJECTED")]
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
                 default_strategy: str, default_usdt: Decimal, caps: dict) -> None:
        self.app = app
        self.venue = venue
        self.strategies = strategies
        self.default_strategy = default_strategy
        self.default_usdt = default_usdt
        self.caps = caps                       # {symbol: cap} read by the guard
        self.bots: dict[str, Bot] = {}
        self._lock = asyncio.Lock()

    def get(self, symbol: str) -> Bot | None:
        return self.bots.get(symbol.upper())

    def roster(self) -> list[str]:
        return list(self.bots)

    async def add(self, symbol: str) -> dict:
        symbol = symbol.upper()
        async with self._lock:
            if symbol in self.bots:
                return {"error": "already added"}
            if symbol not in self.venue.predefined:
                return {"error": f"{symbol} not in the predefined list"}
            try:
                spec = await self.venue.fetch_spec(symbol)
            except Exception as e:  # noqa: BLE001
                return {"error": f"could not fetch {symbol} rules: {e}"}
            market = self.venue.make_market(symbol)
            bars = self.venue.make_bars(symbol, self.venue.default_interval)
            bot = Bot(self.app, self.venue, symbol, market, bars, spec,
                      self.strategies, default_strategy=self.default_strategy,
                      size_usdt=self.default_usdt)
            await bot.spawn()                       # loads history -> a mark exists
            mark = market.latest(symbol)
            self.caps[symbol] = self.venue.cap_for(
                spec, mark.price if mark else None)
            self.bots[symbol] = bot
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
        liveness, metrics, order totals, invariants, roster, wallet + equity.
        Equity prices each active base at its own bot's mark."""
        balances = self.app.latest_balances
        quote = "USDT"
        q = lambda v: str(v.quantize(Decimal("0.01"))) if v is not None else None
        equity = balances.get(quote, Decimal(0))
        bases = {quote}
        for bot in self.bots.values():
            base = bot.symbol[:-len(quote)] if bot.symbol.endswith(quote) else None
            if not base:
                continue
            bases.add(base)
            mark = bot.market.latest(bot.symbol)
            if mark and balances.get(base):
                equity += balances[base] * mark.price
        shown = {a: format(v.normalize(), "f")
                 for a, v in sorted(balances.items()) if a in bases and v}
        return {
            "type": "account",
            "accepting": self.app.accepting,
            "halted": self.app.supervisor.halted.is_set(),
            "task_failures": len(self.app.supervisor.failures),
            "metrics": self.app.metrics.snapshot(),
            "orders": await order_stats(self.app),
            "invariants": await check_invariants(self.app),
            "roster": self.roster(),
            "predefined": list(self.venue.predefined),
            "wallet": {"balances": shown, "equity": q(equity), "quote": quote},
        }


def build_ui(app: SentinelApp, venue: Venue, *, strategies: dict | None = None,
             default_strategy: str | None = None,
             strategy_usdt: Decimal = Decimal("15"),
             caps: dict | None = None,
             initial_symbols: tuple[str, ...] = ()) -> FastAPI:
    strategies = strategies or {}
    default_strategy = default_strategy or next(iter(strategies), None)
    caps = caps if caps is not None else {}
    ui = FastAPI(title="sentinel-terminal")
    mgr = InstrumentManager(app, venue, strategies,
                            default_strategy=default_strategy,
                            default_usdt=strategy_usdt, caps=caps)
    ui.state.manager = mgr

    @ui.on_event("startup")
    async def _startup() -> None:
        from sentinel.runtime import AnotherWriterActive
        try:
            await app.start(arm_protection=False)     # claims the account lock
        except AnotherWriterActive as e:
            print(f"\n  REFUSING TO START: {e}\n", flush=True)
            os._exit(1)
        for sym in initial_symbols:
            await mgr.add(sym)

    @ui.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

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

        try:
            await websocket.send_text(json.dumps(await mgr.account_snapshot()))
            for sym in mgr.roster():
                await send_card(sym)
            seen = app.changes.revision
            while True:
                nxt = await app.changes.wait_past(seen, timeout=2.0)
                topics = app.changes.topics_since(seen)
                seen = nxt
                if not topics:                        # heartbeat (dead-quiet)
                    await websocket.send_text(json.dumps(await mgr.account_snapshot()))
                    continue
                for topic in topics:
                    if topic in ("account", "roster"):
                        await websocket.send_text(json.dumps(await mgr.account_snapshot()))
                    else:
                        await send_card(topic)
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

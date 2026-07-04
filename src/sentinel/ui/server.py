"""Sentinel paper-trading terminal: one page, chart first.

The buttons place REAL orders on Binance testnet through the full stack —
gateway -> guards -> ledger -> exchange -> user stream -> position. The
chart shows the same market those orders fill against; fills appear on it
as markers within a second of the exchange reporting them.
"""

from __future__ import annotations

import asyncio
import json
import os
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from sentinel.domain import Authority, EconomicOrderIntent, OrderState, Side
from sentinel.marks.pnl import compute_pnl
from sentinel.oms import PlacementBlocked
from sentinel.runtime import SentinelApp

from .market import MarketData
from .strategy_runner import StrategyRunner

STATIC = Path(__file__).parent / "static"
LOT_STEP = Decimal("0.00001")   # BTCUSDT lot step (round order qty down to this)

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
                 trade_qty: Decimal) -> None:
        self.app = app
        self.market = market
        self.trade_qty = trade_qty

    def _intent(self, side: Side, authority: Authority,
                qty: Decimal) -> EconomicOrderIntent:
        mark = self.market.latest(self.market.symbol)
        return EconomicOrderIntent(
            intent_id=uuid4(),
            idempotency_key=f"UI-{uuid4().hex[:12]}",
            instrument=self.market.symbol,
            side=side,
            qty=qty,
            limit_price=None,                 # market order: instant feedback
            authority=authority,
            trace_id=uuid4(),
            quote_at_decision=mark.price if mark else None,
        )

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
        """Resolve the order quantity (in BTC) from whatever sizing the UI sent:
          BUY  — usdt amount, or pct of USDT balance -> qty = spend / price
          SELL — btc amount, or pct of position     -> qty = fraction of position
        Rounded down to the lot step. The guards still clamp SELL to what you
        actually hold, so this can only ever undershoot."""
        mark = self.market.latest(self.market.symbol)
        if mark is None:
            return Decimal(0)
        if side == "BUY":
            if usdt is not None:
                spend = Decimal(str(usdt))
            elif pct is not None:
                bal = self.app.latest_balances.get("USDT", Decimal(0))
                spend = bal * Decimal(str(pct)) / 100
            else:
                spend = self.trade_qty * mark.price
            qty = spend / mark.price
        else:  # SELL
            if btc is not None:
                qty = Decimal(str(btc))
            else:
                pos = await self.app.store.get_position(self.market.symbol)
                frac = Decimal(str(pct)) / 100 if pct is not None else Decimal(1)
                qty = pos * frac
        return qty.quantize(LOT_STEP, rounding=ROUND_DOWN)

    async def trade(self, side: str, *, usdt=None, btc=None, pct=None) -> dict:
        """BUY opens/extends (ENTRY); SELL reduces (PROTECTIVE_EXIT, clamped
        by the never-over-exit guard — selling flat is refused, not shorted)."""
        qty = await self._size(side, usdt, btc, pct)
        if qty <= 0:
            return {"blocked": "ZeroSize",
                    "reason": "nothing to trade at that size"}
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

    async def snapshot(self, *, with_candles: bool = True) -> dict:
        """The UI state. with_candles=False omits the ~180-bar history (sent
        once on connect) and ships only the forming bar — the bulk of the
        payload — so per-tick updates are tiny."""
        symbol = self.market.symbol
        pnl_all = await compute_pnl(self.app.store._pool, self.market)
        pnl = pnl_all.get(symbol)
        mark = self.market.latest(symbol)
        balances = self.app.latest_balances  # stream-fed, never polled
        working = [
            o for o in await self.app.store.recent_orders(20)
            if o["state"] not in ("FILLED", "CANCELED", "REJECTED")
        ]
        quantize = lambda v: str(v.quantize(Decimal("0.01"))) if v is not None else None
        # Account equity in quote terms: quote cash + base holdings at mark.
        quote = "USDT"
        base = symbol[:-len(quote)] if symbol.endswith(quote) else None
        equity = balances.get(quote, Decimal(0))
        if base and mark and balances.get(base):
            equity += balances[base] * mark.price
        # Display only the traded pair's assets: the testnet account holds
        # ~445 airdropped tokens — shipping them all bloats the payload/panel,
        # and equity only prices base+quote anyway.
        shown = {a: v for a, v in balances.items() if a in (base, quote)}
        snap = {
            "type": "state",
            "symbol": symbol,
            "interval": self.market.interval,
            "price": str(mark.price) if mark else None,
            "price_age": self.market.price_age_s,
            "candle": self.market.candles[-1] if self.market.candles else None,
            "accepting": self.app.accepting,
            "halted": self.app.supervisor.halted.is_set(),
            "task_failures": len(self.app.supervisor.failures),
            "metrics": self.app.metrics.snapshot(),
            "orders": await order_stats(self.app),
            "invariants": await check_invariants(self.app),
            # Chart markers: fills bucketed to one arrow per (candle, side),
            # covering the visible window. Consolidation bounds the payload to
            # the number of bars no matter how many fills there are, and each
            # marker carries its constituent fills for the hover tooltip.
            "markers": consolidate_markers(
                await self.app.store.recent_fills(
                    symbol, limit=1000,
                    since=(self.market.candles[0]["t"]
                           if self.market.candles else None),
                ),
                interval_seconds(self.market.interval),
            ),
            "position": format(pnl.position.normalize(), "f") if pnl else "0",
            "avg_cost": quantize(pnl.avg_cost) if pnl else None,
            "realized": quantize(pnl.realized) if pnl else "0.00",
            "unrealized": quantize(pnl.unrealized) if pnl else None,
            "working": working,
            "decisions": await self.app.store.recent_decisions(8),
            "trade_qty": str(self.trade_qty),
            "wallet": {
                "balances": {
                    a: format(v.normalize(), "f") for a, v in sorted(shown.items())
                },
                "equity": quantize(equity) if balances else None,
                "quote": quote,
            },
        }
        if with_candles:
            snap["candles"] = self.market.candles
        return snap


def build_ui(app: SentinelApp, market: MarketData,
             trade_qty: Decimal = Decimal("0.0002"),
             strategy=None, strategy_usdt: Decimal = Decimal("15")) -> FastAPI:
    ui = FastAPI(title="sentinel-terminal")
    terminal = Terminal(app, market, trade_qty)

    # The strategy is just another producer of signals into the same gateway.
    # ENTER -> a BUY sized in USDT; EXIT -> close the whole position. It faces
    # the identical guards a human does (single-writer, never-over-exit).
    # Entry size lives in a mutable holder so it can be tuned from the UI
    # without a restart. This is the ONLY position-size knob: the strategy
    # targets a single long/flat position, so "trade bigger" = bigger entry,
    # not more concurrent buys.
    size = {"usdt": strategy_usdt}

    runner = None
    if strategy is not None:
        runner = StrategyRunner(
            strategy, market,
            position_fn=lambda: app.store.get_position(market.symbol),
            enter_fn=lambda: terminal.trade("BUY", usdt=float(size["usdt"])),
            exit_fn=lambda: terminal.trade("SELL", pct=100),
            on_change=app.changes.bump,
        )

    @ui.on_event("startup")
    async def _startup() -> None:
        from sentinel.runtime import AnotherWriterActive

        market.on_change = app.changes.bump          # ticks push to the UI
        # Manual terminal: no auto-arm — a market-style protective exit on
        # boot would flatten the position. Exits stay on the SELL button.
        try:
            await app.start(arm_protection=False)    # claims the account lock
        except AnotherWriterActive as e:
            print(f"\n  REFUSING TO START: {e}\n", flush=True)
            os._exit(1)                              # clean, no traceback
        await market.load_history()
        app.supervisor.spawn("market-data", market.run, restart=True)
        if runner is not None:
            # Always-on task: keeps indicators warm; acts only when started.
            app.supervisor.spawn("strategy", runner.run, restart=True)

    @ui.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    def _snap(with_candles: bool):
        async def build():
            snap = await terminal.snapshot(with_candles=with_candles)
            if runner is not None:
                snap["strategy"] = runner.snapshot()
                snap["strategy"]["entry_usdt"] = format(size["usdt"], "f")
            return snap
        return build()

    @ui.websocket("/ws")
    async def ws(websocket: WebSocket):
        """Event-driven: send full state once, then push only when the world
        changes (fill, order update, balance, tick, strategy), with a 2s
        heartbeat so price-age keeps ticking even in dead-quiet markets."""
        await websocket.accept()
        seen = -1
        last_interval = None
        try:
            await websocket.send_text(json.dumps(await _snap(True)))
            seen = app.changes.revision
            last_interval = market.interval
            while True:
                seen = await app.changes.wait_past(seen, timeout=2.0)
                switched = market.interval != last_interval
                await websocket.send_text(json.dumps(await _snap(switched)))
                last_interval = market.interval
        except WebSocketDisconnect:
            pass

    @ui.post("/strategy/{action}")
    async def strategy_toggle(action: str):
        if runner is None:
            return {"error": "no strategy configured"}
        if action == "start":
            runner.start()
            await runner.reconcile_now()   # act on the current stance now
        elif action == "stop":
            runner.stop()
        await app.changes.bump()
        return runner.snapshot()

    # No manual /trade endpoint — this is a systematic terminal. The strategy
    # runner drives Terminal.trade() directly; the human's only control is
    # start/stop below.

    @ui.post("/strategy/size/{usdt}")
    async def strategy_size(usdt: str):
        """Set the strategy's per-entry size in USDT (live, no restart)."""
        try:
            v = Decimal(usdt)
        except Exception:
            return {"error": "invalid amount"}
        if v <= 0:
            return {"error": "must be positive"}
        size["usdt"] = v
        await app.changes.bump()
        return {"entry_usdt": format(v, "f")}

    @ui.post("/timeframe/{interval}")
    async def timeframe(interval: str):
        await market.set_interval(interval)
        return {"interval": market.interval}

    return ui

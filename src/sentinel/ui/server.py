"""Sentinel paper-trading terminal: one page, chart first.

The buttons place REAL orders on Binance testnet through the full stack —
gateway -> guards -> ledger -> exchange -> user stream -> position. The
chart shows the same market those orders fill against; fills appear on it
as markers within a second of the exchange reporting them.
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from sentinel.domain import Authority, EconomicOrderIntent, OrderState, Side
from sentinel.marks.pnl import compute_pnl
from sentinel.oms import PlacementBlocked
from sentinel.runtime import SentinelApp

from .market import MarketData

STATIC = Path(__file__).parent / "static"


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

    def _intent(self, side: Side, authority: Authority) -> EconomicOrderIntent:
        mark = self.market.latest(self.market.symbol)
        return EconomicOrderIntent(
            intent_id=uuid4(),
            idempotency_key=f"UI-{uuid4().hex[:12]}",
            instrument=self.market.symbol,
            side=side,
            qty=self.trade_qty,
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

    async def trade(self, side: str) -> dict:
        """BUY opens/extends (ENTRY); SELL reduces (PROTECTIVE_EXIT, clamped
        by the never-over-exit guard — selling flat is refused, not shorted)."""
        try:
            # Round-trip through gateway -> guards -> ledger -> exchange ack.
            with self.app.metrics.timer("place_ms"):
                if side == "BUY":
                    stored = await self.app.gateway.place(
                        uuid4(), self._intent(Side.BUY, Authority.ENTRY)
                    )
                else:
                    stored = await self.app.gateway.place(
                        uuid4(), self._intent(Side.SELL, Authority.PROTECTIVE_EXIT)
                    )
            return await self._classify(stored)
        except PlacementBlocked as e:
            self.app.metrics.inc("orders_blocked")
            return {"blocked": type(e).__name__, "reason": str(e)}

    async def flatten_all(self) -> dict:
        """Dump every open position to flat with market exits. Each is a
        PROTECTIVE_EXIT for the full |position|, so the never-over-exit guard
        bounds it to reconciled holdings — a dump can undershoot (a fill
        raced it) but can NEVER oversell or flip us short."""
        results = []
        for instrument, qty in (await self.app.store.load_positions()).items():
            if qty == 0:
                continue
            side = Side.SELL if qty > 0 else Side.BUY
            mark = self.market.latest(instrument)
            intent = EconomicOrderIntent(
                intent_id=uuid4(),
                idempotency_key=f"DUMP-{uuid4().hex[:12]}",
                instrument=instrument, side=side, qty=abs(qty),
                limit_price=None,                 # market: flatten now
                authority=Authority.PROTECTIVE_EXIT, trace_id=uuid4(),
                quote_at_decision=mark.price if mark else None,
            )
            try:
                stored = await self.app.gateway.place(uuid4(), intent)
                results.append({"instrument": instrument,
                                **await self._classify(stored)})
            except PlacementBlocked as e:
                self.app.metrics.inc("orders_blocked")
                results.append({"instrument": instrument, "blocked": str(e)})
        return {"flattened": results} if results else {"flat": "nothing to dump"}

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
            # Chart markers: only the recent window — older fills scroll off
            # the visible chart, and shipping hundreds bloats every delta.
            "markers": await self.app.store.recent_fills(symbol, limit=60),
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
             trade_qty: Decimal = Decimal("0.0002")) -> FastAPI:
    ui = FastAPI(title="sentinel-terminal")
    terminal = Terminal(app, market, trade_qty)

    @ui.on_event("startup")
    async def _startup() -> None:
        await market.load_history()
        market.on_change = app.changes.bump          # ticks push to the UI
        # Manual terminal: no auto-arm — a market-style protective exit on
        # boot would flatten the position. Exits stay on the SELL button.
        await app.start(arm_protection=False)
        app.supervisor.spawn("market-data", market.run, restart=True)

    @ui.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    @ui.websocket("/ws")
    async def ws(websocket: WebSocket):
        """Event-driven: send full state once, then push only when the world
        changes (fill, order update, balance, tick), with a 2s heartbeat so
        price-age keeps ticking even in dead-quiet markets."""
        await websocket.accept()
        seen = -1
        last_interval = None
        try:
            # Initial frame carries the candle history; deltas don't — EXCEPT
            # when the timeframe changed, which needs a full chart refresh.
            await websocket.send_text(json.dumps(await terminal.snapshot()))
            seen = app.changes.revision
            last_interval = market.interval
            while True:
                seen = await app.changes.wait_past(seen, timeout=2.0)
                switched = market.interval != last_interval
                await websocket.send_text(
                    json.dumps(await terminal.snapshot(with_candles=switched))
                )
                last_interval = market.interval
        except WebSocketDisconnect:
            pass

    @ui.post("/trade/{side}")
    async def trade(side: str):
        if side not in ("BUY", "SELL"):
            return {"error": side}
        return await terminal.trade(side)

    @ui.post("/flatten")
    async def flatten():
        return await terminal.flatten_all()

    @ui.post("/timeframe/{interval}")
    async def timeframe(interval: str):
        await market.set_interval(interval)
        return {"interval": market.interval}

    return ui

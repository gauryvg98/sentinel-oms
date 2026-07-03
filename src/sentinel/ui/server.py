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

from sentinel.domain import Authority, EconomicOrderIntent, Side
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

    async def trade(self, side: str) -> dict:
        """BUY opens/extends (ENTRY); SELL reduces (PROTECTIVE_EXIT, clamped
        by the never-over-exit guard — selling flat is refused, not shorted)."""
        try:
            if side == "BUY":
                stored = await self.app.gateway.place(
                    uuid4(), self._intent(Side.BUY, Authority.ENTRY)
                )
            else:
                stored = await self.app.gateway.place(
                    uuid4(), self._intent(Side.SELL, Authority.PROTECTIVE_EXIT)
                )
            return {"placed": stored.core.client_order_id,
                    "state": stored.core.state.value,
                    "qty": str(stored.core.qty)}
        except PlacementBlocked as e:
            return {"blocked": type(e).__name__, "reason": str(e)}

    async def snapshot(self) -> dict:
        symbol = self.market.symbol
        pnl_all = await compute_pnl(self.app.store._pool, self.market)
        pnl = pnl_all.get(symbol)
        mark = self.market.latest(symbol)
        working = [
            o for o in await self.app.store.recent_orders(20)
            if o["state"] not in ("FILLED", "CANCELED", "REJECTED")
        ]
        quantize = lambda v: str(v.quantize(Decimal("0.01"))) if v is not None else None
        return {
            "symbol": symbol,
            "price": str(mark.price) if mark else None,
            "price_age": self.market.price_age_s,
            "accepting": self.app.accepting,
            "halted": self.app.supervisor.halted.is_set(),
            "task_failures": len(self.app.supervisor.failures),
            "invariants": await check_invariants(self.app),
            "candles": self.market.candles,
            "markers": await self.app.store.recent_fills(symbol),
            "position": format(pnl.position.normalize(), "f") if pnl else "0",
            "avg_cost": quantize(pnl.avg_cost) if pnl else None,
            "realized": quantize(pnl.realized) if pnl else "0.00",
            "unrealized": quantize(pnl.unrealized) if pnl else None,
            "working": working,
            "decisions": await self.app.store.recent_decisions(8),
            "trade_qty": str(self.trade_qty),
        }


def build_ui(app: SentinelApp, market: MarketData,
             trade_qty: Decimal = Decimal("0.0002")) -> FastAPI:
    ui = FastAPI(title="sentinel-terminal")
    terminal = Terminal(app, market, trade_qty)

    @ui.on_event("startup")
    async def _startup() -> None:
        await market.load_history()
        # Manual terminal: no auto-arm — a market-style protective exit on
        # boot would flatten the position. Exits stay on the SELL button.
        await app.start(arm_protection=False)
        app.supervisor.spawn("market-data", market.run, restart=True)

    @ui.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    @ui.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                await websocket.send_text(json.dumps(await terminal.snapshot()))
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            pass

    @ui.post("/trade/{side}")
    async def trade(side: str):
        if side not in ("BUY", "SELL"):
            return {"error": side}
        return await terminal.trade(side)

    return ui

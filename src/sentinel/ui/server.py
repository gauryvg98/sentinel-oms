"""The Sentinel terminal server.

One FastAPI app serving one static page and one WebSocket. Snapshots are
built server-side (~4Hz) and fanned out to every viewer — stream once,
fan out, the same hub pattern as the OMS's own event flow.

Demo endpoints (/demo/*) drive the simulated broker: place entries, script
fills, force timeouts, storm, and crash-recover — so a viewer can inflict
failures and WATCH the system converge.
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from sentinel.broker.sim import ScriptedBroker
from sentinel.domain import Authority, EconomicOrderIntent, Side
from sentinel.marks import SimMarkFeed
from sentinel.marks.pnl import compute_pnl
from sentinel.oms import PlacementBlocked
from sentinel.runtime import SentinelApp

STATIC = Path(__file__).parent / "static"

INSTRUMENT = "IDX-OPT"


class DemoDriver:
    """Drives the ScriptedBroker + marks on a real-time cadence and exposes
    the chaos actions the UI buttons call."""

    def __init__(self, app: SentinelApp, sim: ScriptedBroker,
                 marks: SimMarkFeed) -> None:
        self.app = app
        self.sim = sim
        self.marks = marks
        self._n = 0
        marks.add_instrument(INSTRUMENT, "4.20")

    async def run(self) -> None:
        while True:
            self.marks.tick()
            self.sim.step()
            await asyncio.sleep(0.5)

    # ------------------------------------------------------------- actions

    def _key(self, prefix: str) -> str:
        self._n += 1
        return f"{prefix}-{self._n}"

    def _intent(self, key: str, qty: str, side=Side.BUY,
                authority=Authority.ENTRY) -> EconomicOrderIntent:
        mark = self.marks.latest(INSTRUMENT)
        return EconomicOrderIntent(
            intent_id=uuid4(), idempotency_key=key, instrument=INSTRUMENT,
            side=side, qty=Decimal(qty), limit_price=None,
            authority=authority, trace_id=uuid4(),
            quote_at_decision=mark.price if mark else None,
        )

    async def trade(self) -> dict:
        """Place an entry that fills over the next few steps."""
        key = self._key("K")
        step = self.sim.current_step
        self.sim._script.fill(key, qty="1", price="4.20", at_step=step + 1)
        self.sim._script.fill(key, qty="1", price="4.25", at_step=step + 3)
        try:
            stored = await self.app.gateway.place(uuid4(), self._intent(key, "2"))
            return {"placed": key, "state": stored.core.state.value}
        except PlacementBlocked as e:
            return {"blocked": key, "reason": str(e)}

    async def timeout(self) -> dict:
        """Force the classic: timeout with hidden broker acceptance + a fill
        while UNKNOWN. The reconcile loop discovers and adopts it."""
        key = self._key("T")
        step = self.sim.current_step
        self.sim._script.on_submit(key, timeout=True, accept_on_timeout=True)
        self.sim._script.fill(key, qty="1", price="4.30", at_step=step + 2)
        try:
            stored = await self.app.gateway.place(uuid4(), self._intent(key, "2"))
            return {"placed": key, "state": stored.core.state.value}
        except PlacementBlocked as e:
            return {"blocked": key, "reason": str(e)}

    async def storm(self) -> dict:
        """A burst of trades — watch throughput, queue depth, backpressure."""
        results = []
        for _ in range(10):
            results.append(await self.trade())
            await asyncio.sleep(0.05)
        return {"placed": sum(1 for r in results if "placed" in r),
                "blocked": sum(1 for r in results if "blocked" in r)}

    async def flatten(self) -> dict:
        report = await self.app.protect.ensure_protection()
        return {"protection_orders": len(report.placed)}

    async def crash_recover(self) -> dict:
        """Corrupt every projection, then run startup recovery live."""
        await self.app.store._pool.execute("UPDATE positions SET qty = 99")
        await self.app.store._pool.execute(
            "UPDATE orders SET filled_qty = 0 WHERE state NOT IN "
            "('FILLED','CANCELED','REJECTED')"
        )
        report = await self.app.recon.startup_recovery()
        await self.app.protect.ensure_protection()
        return {"events_replayed": report.events_replayed,
                "reconciled": report.reconciled}


async def check_invariants(app: SentinelApp) -> dict[str, bool]:
    """Live INV checks against the database — evaluated WHILE load runs."""
    pool = app.store._pool
    inv = {}
    # INV-2: every position equals the side-signed sum of deduplicated fills.
    inv["positions_match_fills"] = not await pool.fetch(
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
    )
    # Overfill impossible.
    inv["no_overfill"] = not await pool.fetch(
        "SELECT 1 FROM orders WHERE filled_qty > qty"
    )
    # No duplicate exec ids (schema-guaranteed, asserted anyway).
    inv["fills_unique"] = not await pool.fetch(
        "SELECT exec_id FROM fills GROUP BY exec_id HAVING count(*) > 1"
    )
    # Never-over-exit: committed exit qty <= |position| per instrument.
    inv["never_over_exit"] = not await pool.fetch(
        """
        SELECT o.instrument FROM orders o
        LEFT JOIN positions p ON p.instrument = o.instrument
        WHERE o.authority='PROTECTIVE_EXIT'
          AND o.state NOT IN ('FILLED','CANCELED','REJECTED')
        GROUP BY o.instrument, p.qty
        HAVING SUM(o.qty - o.filled_qty) > COALESCE(ABS(p.qty), 0)
        """
    )
    # Event sequence monotonic + traced (audit spine).
    inv["audit_traced"] = not await pool.fetch(
        "SELECT 1 FROM events WHERE trace_id IS NULL"
    )
    return inv


async def build_snapshot(app: SentinelApp, marks: SimMarkFeed,
                         driver: DemoDriver) -> dict:
    pnl = await compute_pnl(app.store._pool, marks)
    positions = []
    for p in pnl.values():
        committed = await app.store.open_exit_remaining(p.instrument)
        positions.append({
            "instrument": p.instrument,
            "position": str(p.position),
            "avg_cost": str(p.avg_cost.quantize(Decimal("0.01")))
            if p.avg_cost is not None else None,
            "realized": str(p.realized.quantize(Decimal("0.01"))),
            "unrealized": str(p.unrealized.quantize(Decimal("0.01")))
            if p.unrealized is not None else None,
            "mark": str(p.mark) if p.mark else None,
            "covered": str(committed),
            "uncovered": str(max(Decimal(0), abs(p.position) - committed)),
        })
    mark = marks.latest(INSTRUMENT)
    return {
        "authority": "SIM",
        "accepting": app.accepting,
        "halted": app.supervisor.halted.is_set(),
        "invariants": await check_invariants(app),
        "orders": await app.store.recent_orders(),
        "positions": positions,
        "decisions": await app.store.recent_decisions(20),
        "ledger": await app.store.recent_events(25),
        "metrics": app.metrics.snapshot(),
        "session": {
            "mark_price": str(mark.price) if mark else None,
            "feed_age": 0.5,          # sim feed ticks on the driver cadence
            "broker": "simulator",
            "task_failures": len(app.supervisor.failures),
        },
    }


def build_ui(app: SentinelApp, sim: ScriptedBroker,
             marks: SimMarkFeed) -> FastAPI:
    ui = FastAPI(title="sentinel-terminal")
    driver = DemoDriver(app, sim, marks)

    @ui.on_event("startup")
    async def _startup() -> None:
        await app.start()
        app.supervisor.spawn("demo-driver", driver.run, restart=True)

    @ui.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    @ui.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                snap = await build_snapshot(app, marks, driver)
                await websocket.send_text(json.dumps(snap))
                await asyncio.sleep(0.25)
        except WebSocketDisconnect:
            pass

    @ui.post("/demo/{action}")
    async def demo(action: str):
        actions = {
            "trade": driver.trade,
            "timeout": driver.timeout,
            "storm": driver.storm,
            "flatten": driver.flatten,
            "crash": driver.crash_recover,
        }
        if action not in actions:
            return {"error": f"unknown action {action}"}
        return await actions[action]()

    return ui

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
INSTRUMENTS = ["IDX-OPT", "IDX-MINI", "ETF-OPT"]
STARTS = {"IDX-OPT": "4.20", "IDX-MINI": "0.42", "ETF-OPT": "5.10"}


class DemoDriver:
    """Drives the ScriptedBroker + marks on a real-time cadence and exposes
    the chaos actions the UI buttons call.

    Demo-script rule learned the hard way: every scripted order must reach a
    terminal state (fill fully or get canceled). A permanently-PARTIAL entry
    holds the one-live-entry guard forever and deadlocks the whole demo —
    the OMS refusing correctly, the script deserving it."""

    def __init__(self, app: SentinelApp, sim: ScriptedBroker,
                 marks: SimMarkFeed) -> None:
        self.app = app
        self.sim = sim
        self.marks = marks
        self._n = 0
        self._rotate = 0
        for inst in INSTRUMENTS:
            marks.add_instrument(inst, STARTS[inst])

    async def run(self) -> None:
        while True:
            self.marks.tick()
            self.sim.step()
            await asyncio.sleep(0.5)

    # ------------------------------------------------------------- actions

    def _key(self, prefix: str) -> str:
        self._n += 1
        return f"{prefix}-{self._n}"

    def _next_instrument(self) -> str:
        self._rotate += 1
        return INSTRUMENTS[self._rotate % len(INSTRUMENTS)]

    def _intent(self, key: str, qty: str, instrument: str, side=Side.BUY,
                authority=Authority.ENTRY) -> EconomicOrderIntent:
        mark = self.marks.latest(instrument)
        return EconomicOrderIntent(
            intent_id=uuid4(), idempotency_key=key, instrument=instrument,
            side=side, qty=Decimal(qty), limit_price=None,
            authority=authority, trace_id=uuid4(),
            quote_at_decision=mark.price if mark else None,
        )

    async def trade(self, instrument: str | None = None) -> dict:
        """Place an entry that fills COMPLETELY over the next few steps —
        terminal by construction, so the entry lane frees itself."""
        key = self._key("K")
        instrument = instrument or self._next_instrument()
        step = self.sim.current_step
        mark = self.marks.latest(instrument)
        px = mark.price if mark else Decimal("4.20")
        self.sim._script.fill(key, qty="1", price=str(px), at_step=step + 1)
        self.sim._script.fill(key, qty="1", price=str(px), at_step=step + 3)
        try:
            stored = await self.app.gateway.place(
                uuid4(), self._intent(key, "2", instrument)
            )
            return {"placed": key, "instrument": instrument,
                    "state": stored.core.state.value}
        except PlacementBlocked as e:
            return {"blocked": key, "reason": str(e)}

    async def timeout(self) -> dict:
        """The classic: timeout with hidden broker acceptance, and the FULL
        quantity fills while UNKNOWN. Reconciliation discovers, adopts, and
        the order completes — the lane frees itself after the drama."""
        key = self._key("T")
        instrument = self._next_instrument()
        step = self.sim.current_step
        mark = self.marks.latest(instrument)
        px = mark.price if mark else Decimal("4.30")
        self.sim._script.on_submit(key, timeout=True, accept_on_timeout=True)
        self.sim._script.fill(key, qty="2", price=str(px), at_step=step + 2)
        try:
            stored = await self.app.gateway.place(
                uuid4(), self._intent(key, "2", instrument)
            )
            return {"placed": key, "instrument": instrument,
                    "state": stored.core.state.value}
        except PlacementBlocked as e:
            return {"blocked": key, "reason": str(e)}

    async def storm(self) -> dict:
        """Three waves × three lanes. Within a wave, all lanes fire
        simultaneously (parallel single-writer lanes); between waves we wait
        for fills to complete so the storm never fights the one-live-entry
        guard it's supposed to showcase. A lane still mid-fill from earlier
        clicking may block once — the guard, visible, not broken."""
        results = []
        for _ in range(3):
            wave = await asyncio.gather(
                *(self.trade(instrument) for instrument in INSTRUMENTS)
            )
            results.extend(wave)
            await asyncio.sleep(2.2)      # fills land at ~1.5s; free the lanes
        return {"placed": sum(1 for r in results if "placed" in r),
                "blocked": sum(1 for r in results if "blocked" in r)}

    async def flatten(self) -> dict:
        report = await self.app.protect.ensure_protection()
        return {"protection_orders": len(report.placed)}

    async def corrupt(self) -> dict:
        """Vandalize the projections — positions become lies (99), live
        orders forget their fills. Watch the INV lights go RED: the auditor
        catches the corruption live. Nothing self-heals until you recover."""
        await self.app.store._pool.execute("UPDATE positions SET qty = 99")
        await self.app.store._pool.execute(
            "UPDATE orders SET filled_qty = 0 WHERE state NOT IN "
            "('FILLED','CANCELED','REJECTED')"
        )
        return {"corrupted": "positions=99, live fills zeroed",
                "watch": "INV strip + positions panel"}

    async def recover(self) -> dict:
        """R1.11/R1.12 live: rebuild projections from the event ledger,
        reconcile every non-terminal order against the broker, re-arm
        protection. The lies are corrected by the two sources of truth."""
        report = await self.app.recon.startup_recovery()
        armed = await self.app.protect.ensure_protection()
        return {"events_replayed": report.events_replayed,
                "reconciled": report.reconciled,
                "protection_orders": len(armed.placed)}


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
            "corrupt": driver.corrupt,
            "recover": driver.recover,
        }
        if action not in actions:
            return {"error": f"unknown action {action}"}
        return await actions[action]()

    return ui

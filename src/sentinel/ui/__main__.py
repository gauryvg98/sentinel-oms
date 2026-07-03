"""Run the Sentinel terminal in sim mode:

    python -m sentinel.ui

Connects to DATABASE_URL (default: the docker-compose postgres on :5433),
applies migrations, assembles SentinelApp against the scripted simulator +
sim mark feed, and serves the terminal on http://localhost:8000.
"""

from __future__ import annotations

import asyncio
import os

import asyncpg
import uvicorn

from sentinel.broker.sim import BrokerScript, ScriptedBroker
from sentinel.ledger import apply_migrations
from sentinel.marks import SimMarkFeed
from sentinel.runtime import SentinelApp
from sentinel.ui.server import build_ui

DEFAULT_DB = "postgresql://sentinel:sentinel@127.0.0.1:5433/sentinel"


async def _build():
    dsn = os.environ.get("DATABASE_URL", DEFAULT_DB)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=6)
    async with pool.acquire() as conn:
        await apply_migrations(conn)
        # Sim mode: broker truth is in-memory and dies with the process, so
        # the ledger must be born with it — otherwise startup reconciliation
        # correctly HALTS on "local fills, broker absent" (R1.12 doing its
        # job against a broker that forgot). Real-broker mode never resets.
        if os.environ.get("SENTINEL_RESET_ON_BOOT", "1") == "1":
            await conn.execute(
                "TRUNCATE commands, events, orders, fills, positions, "
                "checkpoints, decision_log CASCADE"
            )
    sim = ScriptedBroker(BrokerScript())
    marks = SimMarkFeed(seed=None)
    app = SentinelApp(pool, sim)
    return build_ui(app, sim, marks)


async def _serve() -> None:
    # Pool and server must share ONE event loop — build inside it.
    ui = await _build()
    config = uvicorn.Config(
        ui, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")),
        log_level="info",
    )
    await uvicorn.Server(config).serve()


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()

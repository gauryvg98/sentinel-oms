"""Minimal migration runner: numbered .sql files, applied once, in order.

No framework: migrations are plain SQL you can read, and the applied set is
itself a table. Each migration runs in its own transaction.
"""

from __future__ import annotations

from importlib import resources

import asyncpg

_MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def _load_migrations() -> list[tuple[str, str]]:
    pkg = resources.files("sentinel.ledger.schema")
    files = sorted(
        (f.name, f.read_text()) for f in pkg.iterdir() if f.name.endswith(".sql")
    )
    if not files:
        raise RuntimeError("no migrations found in sentinel.ledger.schema")
    return files


async def apply_migrations(conn: asyncpg.Connection) -> list[str]:
    """Apply pending migrations; returns the filenames applied this run."""
    await conn.execute(_MIGRATIONS_TABLE)
    done: set[str] = {
        r["filename"] for r in await conn.fetch("SELECT filename FROM schema_migrations")
    }
    applied: list[str] = []
    for name, sql in _load_migrations():
        if name in done:
            continue
        async with conn.transaction():
            await conn.execute(sql)
            await conn.execute(
                "INSERT INTO schema_migrations (filename) VALUES ($1)", name
            )
        applied.append(name)
    return applied

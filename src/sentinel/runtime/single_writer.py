"""Single-writer enforcement — one Sentinel process per account, proven at boot.

The ledger already enforces single-writer *reactively* (per-instrument advisory
locks + an optimistic sequence guard that HALTS if two writers ever interleave).
That's the last line of defense. This is the first: a process acquires an
ACCOUNT-scoped session advisory lock at startup and holds it for its entire
life on a dedicated connection. A second process calling pg_try_advisory_lock
gets False and refuses to boot — the interleave never gets the chance to happen.

Why a session lock on a dedicated connection:
- Session advisory locks live until unlocked OR the connection closes. So if
  this process dies, Postgres drops the connection and releases the lock
  automatically — the next process takes over cleanly, no stale lock, no manual
  cleanup. That's the acquire / (heartbeat) / takeover pattern.
- A heartbeat verifies we STILL hold it; if that connection ever drops, we've
  lost exclusivity and must halt loudly rather than keep writing.

Key space: the two-int32 form pg_try_advisory_lock(classid, objid) is a SEPARATE
namespace from the ledger's single-bigint pg_advisory_xact_lock(key), so the
account lock can never collide with a per-instrument lock.
"""

from __future__ import annotations

import asyncio
import hashlib

import asyncpg

_CLASSID = 0x5E17  # "SE" — namespace for Sentinel account locks (int32)


class AnotherWriterActive(RuntimeError):
    """Another Sentinel process already owns this account. Refuse to boot."""


def _objid(account: str) -> int:
    return int.from_bytes(hashlib.sha256(account.encode()).digest()[:4],
                          "big", signed=True)


class SingleWriterLock:
    def __init__(self, dsn: str, account: str = "sentinel",
                 *, heartbeat_s: float = 15.0) -> None:
        self._dsn = dsn
        self._account = account
        self._objid = _objid(account)
        self._heartbeat_s = heartbeat_s
        self._conn: asyncpg.Connection | None = None

    async def acquire(self) -> None:
        """Take the lock or raise AnotherWriterActive. Holds the connection."""
        conn = await asyncpg.connect(self._dsn)
        got = await conn.fetchval(
            "SELECT pg_try_advisory_lock($1, $2)", _CLASSID, self._objid
        )
        if not got:
            await conn.close()
            raise AnotherWriterActive(
                f"another Sentinel process already owns account "
                f"'{self._account}' — stop it before starting a new one"
            )
        self._conn = conn  # keep the session alive => keep the lock

    async def guard(self) -> None:
        """Supervised task: confirm we still hold the lock. If the session
        drops, the query raises — we've lost exclusivity, so let it propagate
        and halt the supervisor (spawned with restart=False)."""
        assert self._conn is not None
        while True:
            await asyncio.sleep(self._heartbeat_s)
            await self._conn.execute("SELECT 1")  # raises if the session died

    async def release(self) -> None:
        if self._conn is None:
            return
        try:
            await self._conn.execute(
                "SELECT pg_advisory_unlock($1, $2)", _CLASSID, self._objid
            )
        finally:
            await self._conn.close()
            self._conn = None

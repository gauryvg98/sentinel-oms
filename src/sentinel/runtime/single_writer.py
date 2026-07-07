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
import logging

import asyncpg

log = logging.getLogger("sentinel.runtime")

_CLASSID = 0x5E17  # "SE" — namespace for Sentinel account locks (int32)
_REACQUIRE_BACKOFF_S = 2.0  # retry cadence while the DB path is transiently down


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
        """Supervised task: confirm we still hold the lock.

        A dropped session used to be treated as fatal (halt). But a managed
        Postgres proxy (e.g. Fly) RECYCLES long-lived TCP connections every
        10-20min — and our lock connection is long-lived BY DESIGN (it must stay
        open to hold the session advisory lock). A recycle is NOT a second
        writer, so halting on it is a false positive that took the fleet down.

        Instead, on a dropped connection RE-ACQUIRE the lock on a fresh session:
        - re-acquired -> nobody else held it (routine recycle) -> keep running;
        - proven taken by another connection -> the real two-writer condition
          -> halt loudly (raise, restart=False);
        - DB transiently unreachable -> retry, don't halt (connectivity is not a
          competing writer — same discipline as the reconcile loop's timeouts).
        """
        assert self._conn is not None
        while True:
            await asyncio.sleep(self._heartbeat_s)
            try:
                await self._conn.execute("SELECT 1")  # still holding?
            except Exception as e:  # noqa: BLE001 — any failure => verify via re-acquire
                log.warning(
                    "writer-lock connection lost (%r); re-acquiring", e
                )
                try:
                    await self._conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self._conn = None
                if not await self._reacquire():
                    raise AnotherWriterActive(
                        f"account '{self._account}' lock is now held by another "
                        f"writer — halting to avoid a two-writer interleave"
                    ) from e
                log.warning("writer-lock re-acquired after connection drop")

    async def _reacquire(self) -> bool:
        """Re-take the account lock after our session dropped. Retry through
        transient connectivity (a proxy recycle briefly breaks the path);
        return True once we hold it again, and False ONLY when a live connection
        proves the lock is owned by another writer."""
        while True:
            try:
                conn = await asyncpg.connect(self._dsn, timeout=10)
            except (OSError, asyncpg.PostgresError) as e:  # connectivity, not a rival
                log.warning("writer-lock re-acquire: DB unreachable (%r); retrying", e)
                await asyncio.sleep(_REACQUIRE_BACKOFF_S)
                continue
            got = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1, $2)", _CLASSID, self._objid
            )
            if got:
                self._conn = conn      # hold the new session => hold the lock
                return True
            await conn.close()
            return False               # definitively owned by another writer

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

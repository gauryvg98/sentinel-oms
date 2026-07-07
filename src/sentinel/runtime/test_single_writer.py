"""Single-writer lock: the second process on an account is refused, and the
lock frees when the holder releases (or its connection dies).

Uses the shared container's DSN directly (pg_dsn) — advisory locks are
database-scoped and need no tables, so no per-test database is required."""

from __future__ import annotations

import pytest

from sentinel.runtime import AnotherWriterActive, SingleWriterLock


async def test_second_writer_is_refused(pg_dsn):
    first = SingleWriterLock(pg_dsn, account="acct-A")
    await first.acquire()
    try:
        second = SingleWriterLock(pg_dsn, account="acct-A")
        with pytest.raises(AnotherWriterActive):
            await second.acquire()
    finally:
        await first.release()


async def test_release_frees_the_account(pg_dsn):
    a = SingleWriterLock(pg_dsn, account="acct-B")
    await a.acquire()
    await a.release()
    b = SingleWriterLock(pg_dsn, account="acct-B")   # fresh process can take it
    await b.acquire()
    await b.release()


async def test_different_accounts_dont_conflict(pg_dsn):
    a = SingleWriterLock(pg_dsn, account="acct-C")
    b = SingleWriterLock(pg_dsn, account="acct-D")
    await a.acquire()
    await b.acquire()            # different key -> no conflict
    await a.release()
    await b.release()


async def test_dropped_connection_releases_lock(pg_dsn):
    """Simulate a crash: the holder's connection closes without release ->
    Postgres frees the session lock -> a new process takes over."""
    a = SingleWriterLock(pg_dsn, account="acct-E")
    await a.acquire()
    await a._conn.close()        # noqa: SLF001 — simulate process death
    a._conn = None
    b = SingleWriterLock(pg_dsn, account="acct-E")
    await b.acquire()            # takeover succeeds
    await b.release()


async def test_reacquire_after_recycle_keeps_running(pg_dsn):
    """The Fly-proxy incident: the lock connection is RECYCLED (closed under
    us) but no rival exists. _reacquire must win the lock back on a fresh
    session — a recycle is not a second writer, and must not halt."""
    a = SingleWriterLock(pg_dsn, account="acct-F")
    await a.acquire()
    await a._conn.close()        # noqa: SLF001 — proxy recycles the connection
    a._conn = None
    assert await a._reacquire() is True     # noqa: SLF001
    # exclusivity survives the recycle: a rival is still refused
    rival = SingleWriterLock(pg_dsn, account="acct-F")
    with pytest.raises(AnotherWriterActive):
        await rival.acquire()
    await a.release()


async def test_reacquire_yields_to_a_real_rival(pg_dsn):
    """If another writer claimed the account while our connection was down,
    _reacquire must return False (-> guard raises -> HALT). The two-writer
    interleave stays impossible."""
    a = SingleWriterLock(pg_dsn, account="acct-G")
    await a.acquire()
    await a._conn.close()        # noqa: SLF001
    a._conn = None
    rival = SingleWriterLock(pg_dsn, account="acct-G")
    await rival.acquire()                    # rival takes over the freed lock
    try:
        assert await a._reacquire() is False  # noqa: SLF001 — defer, don't steal
    finally:
        await rival.release()

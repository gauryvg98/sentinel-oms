"""Shared test fixtures.

One NAMED, long-lived Postgres container (`sentinel-oms-test-pg`) is reused
across all test sessions — created on first use, never torn down between runs,
so there is no teardown/startup race between back-to-back runs. Isolation
comes from a fresh database per test, dropped on teardown.

Skips cleanly when Docker is unavailable.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

CONTAINER_NAME = "sentinel-oms-test-pg"
PG_USER = "sentinel"
PG_PASS = "sentinel"


def _ensure_shared_pg() -> int | None:
    """Return the host port of the shared Postgres container, starting or
    creating it if needed. None when Docker is unavailable."""
    try:
        import docker
        from docker.errors import NotFound

        client = docker.from_env()
        client.ping()
    except Exception:
        return None

    try:
        container = client.containers.get(CONTAINER_NAME)
        if container.status != "running":
            container.start()
    except NotFound:
        container = client.containers.run(
            "postgres:16-alpine",
            name=CONTAINER_NAME,
            environment={
                "POSTGRES_USER": PG_USER,
                "POSTGRES_PASSWORD": PG_PASS,
                "POSTGRES_DB": "postgres",
            },
            ports={"5432/tcp": None},  # random free host port, fixed for life
            detach=True,
            labels={"purpose": "sentinel-oms-tests"},
        )

    # The published port appears in attrs only once the container is fully
    # up — poll for it instead of trusting the first reload.
    import time

    for _ in range(80):
        container.reload()
        binding = container.attrs["NetworkSettings"]["Ports"].get("5432/tcp")
        if binding:
            return int(binding[0]["HostPort"])
        time.sleep(0.25)
    raise RuntimeError("shared postgres container never published its port")


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    port = _ensure_shared_pg()
    if port is None:
        pytest.skip("docker unavailable")
    return f"postgresql://{PG_USER}:{PG_PASS}@127.0.0.1:{port}/postgres"


_server_ready = False


async def _wait_ready(dsn: str) -> None:
    """First contact after (re)creating the container can race its init;
    retry briefly, once per session."""
    global _server_ready
    if _server_ready:
        return
    import asyncpg

    last: Exception | None = None
    for _ in range(60):
        try:
            conn = await asyncpg.connect(dsn)
            await conn.close()
            _server_ready = True
            return
        except Exception as e:  # noqa: BLE001 — includes CannotConnect, refused
            last = e
            await asyncio.sleep(0.5)
    raise RuntimeError(f"shared postgres never became ready: {last!r}")


@pytest.fixture
async def pool(pg_dsn):
    import asyncpg

    from sentinel.ledger import apply_migrations

    await _wait_ready(pg_dsn)
    dbname = f"t_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(pg_dsn)
    await admin.execute(f'CREATE DATABASE "{dbname}"')
    await admin.close()

    pool = await asyncpg.create_pool(
        pg_dsn.rsplit("/", 1)[0] + f"/{dbname}", min_size=1, max_size=4
    )
    async with pool.acquire() as conn:
        await apply_migrations(conn)
    yield pool
    await pool.close()

    admin = await asyncpg.connect(pg_dsn)
    await admin.execute(f'DROP DATABASE "{dbname}" WITH (FORCE)')
    await admin.close()

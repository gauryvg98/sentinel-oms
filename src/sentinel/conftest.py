"""Shared test fixtures: one Postgres container per session, a fresh database
per test (state never leaks), skip cleanly when Docker is unavailable."""

from __future__ import annotations

from uuid import uuid4

import pytest


def _docker_available() -> bool:
    try:
        import docker

        docker.from_env().ping()
        return True
    except Exception:
        return False


DOCKER_UP = _docker_available()

requires_postgres = pytest.mark.skipif(not DOCKER_UP, reason="docker unavailable")


@pytest.fixture(scope="session")
def pg_container():
    if not DOCKER_UP:
        pytest.skip("docker unavailable")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture
async def pool(pg_container):
    import asyncpg

    from sentinel.ledger import apply_migrations

    base = pg_container.get_connection_url().replace(
        "postgresql+psycopg2", "postgresql"
    )
    admin = await asyncpg.connect(base)
    dbname = f"t_{uuid4().hex[:12]}"
    await admin.execute(f'CREATE DATABASE "{dbname}"')
    await admin.close()

    pool = await asyncpg.create_pool(
        base.rsplit("/", 1)[0] + f"/{dbname}", min_size=1, max_size=4
    )
    async with pool.acquire() as conn:
        await apply_migrations(conn)
    yield pool
    await pool.close()

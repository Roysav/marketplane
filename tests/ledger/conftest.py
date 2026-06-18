import asyncio
import uuid

import asyncpg
import pytest

from apiserver.ledger.postgres import get_migrations
from apiserver.utils.migrations.postgres import PostgresMigrationClient

_ADMIN_DSN = "postgresql://user:password@localhost:5432/postgres"
_BASE = "postgresql://user:password@localhost:5432"


@pytest.fixture(scope="session")
def ledger_dsn():
    db_name = f"test_ledger_{uuid.uuid4().hex[:8]}"
    dsn = f"{_BASE}/{db_name}"

    async def _setup():
        conn = await asyncpg.connect(_ADMIN_DSN)
        await conn.execute(f'CREATE DATABASE "{db_name}"')
        await conn.close()
        pool = await asyncpg.create_pool(dsn)
        async with pool.acquire() as c:
            await PostgresMigrationClient(c, get_migrations(), "ledger_schema_migrations").apply()
        await pool.close()

    asyncio.run(_setup())
    yield dsn

    async def _teardown():
        conn = await asyncpg.connect(_ADMIN_DSN)
        await conn.execute(f'DROP DATABASE "{db_name}"')
        await conn.close()

    asyncio.run(_teardown())

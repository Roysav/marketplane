import asyncio
import uuid

import asyncpg
import pytest

from apiserver.ledger.storage.postgres import get_migrations
from apiserver.utils.migrations.postgres import PostgresMigrationClient


@pytest.fixture(scope="session")
def ledger_dsn(postgres_dsn):
    db_name = f"test_ledger_{uuid.uuid4().hex[:8]}"
    admin_dsn = f"{postgres_dsn}/postgres"
    dsn = f"{postgres_dsn}/{db_name}"

    async def _setup():
        conn = await asyncpg.connect(admin_dsn)
        await conn.execute(f'CREATE DATABASE "{db_name}"')
        await conn.close()
        pool = await asyncpg.create_pool(dsn)
        async with pool.acquire() as c:
            await PostgresMigrationClient(c, get_migrations(), "ledger_schema_migrations").apply()
        await pool.close()

    asyncio.run(_setup())
    yield dsn

    async def _teardown():
        conn = await asyncpg.connect(admin_dsn)
        await conn.execute(f'DROP DATABASE "{db_name}"')
        await conn.close()

    asyncio.run(_teardown())

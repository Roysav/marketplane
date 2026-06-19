import asyncio
import logging.config

import asyncpg
import redis.asyncio as redis

from apiserver.config import Settings
from apiserver.events import EventsClient
from apiserver.events.storage.redis import RedisEventStorage
from apiserver.ledger import LedgerClient
from apiserver.ledger.storage.postgres import PostgresLedgerStorage, get_migrations as ledger_migrations
from apiserver.records import RecordsClient
from apiserver.records.storage.postgres import PostgresRecordStorage, get_migrations as records_migrations
from apiserver.server import serve
from apiserver.service import Service
from apiserver.ticks import TicksClient
from apiserver.ticks.storage.redis import RedisTickStorage
from apiserver.utils.migrations.postgres import PostgresMigrationClient

logger = logging.getLogger(__name__)


def main() -> None:
    settings = Settings()
    logging.config.dictConfig(settings.logging)
    asyncio.run(_run(settings))


async def _run(settings: Settings) -> None:
    logger.info("apiserver starting")
    ledger_pool = await asyncpg.create_pool(settings.ledger.storage.postgres.connection_uri)
    async with ledger_pool.acquire() as conn:
        await PostgresMigrationClient(conn, ledger_migrations(), "ledger_schema_migrations").apply()

    records_pool = await asyncpg.create_pool(settings.records.storage.postgres.connection_uri)
    async with records_pool.acquire() as conn:
        await PostgresMigrationClient(conn, records_migrations(), "records_schema_migrations").apply()

    service = Service(
        ticks=TicksClient(RedisTickStorage(redis.from_url(settings.ticks.storage.redis.connection_uri, socket_timeout=None))),
        events=EventsClient(RedisEventStorage(redis.from_url(settings.events.storage.redis.connection_uri, socket_timeout=None))),
        ledger=LedgerClient(PostgresLedgerStorage(ledger_pool)),
        records=RecordsClient(PostgresRecordStorage(records_pool)),
    )
    await serve(service)


if __name__ == "__main__":
    main()

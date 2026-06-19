import asyncio

import asyncpg
import redis.asyncio as redis

from .config import Settings
from .events import EventsClient
from .events.storage.redis import RedisEventStorage
from .ledger import LedgerClient
from .ledger.storage.postgres import PostgresLedgerStorage, get_migrations as ledger_migrations
from .records import RecordsClient
from .records.storage.postgres import PostgresRecordStorage, get_migrations as records_migrations
from .server import serve
from .service import Service
from .ticks import TicksClient
from .ticks.storage.redis import RedisTickStorage
from .utils.migrations.postgres import PostgresMigrationClient




def main() -> None:
    asyncio.run(_run(Settings()))


async def _run(settings: Settings) -> None:
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

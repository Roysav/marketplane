import asyncpg


class MigrationTableMismatchError(Exception):
    pass


class PostgresMigrationClient:
    def __init__(
        self,
        conn: asyncpg.Connection,
        migrations: list[tuple[str, str]],
        table: str,
    ) -> None:
        self._conn = conn
        self._migrations = migrations
        self._table = table

    async def apply(self) -> None:
        async with self._conn.transaction():
            await self._conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    name       TEXT        PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            for name, sql in self._migrations:
                inserted = await self._conn.fetchval(
                    f"INSERT INTO {self._table} (name) VALUES ($1) ON CONFLICT DO NOTHING RETURNING name",
                    name,
                )
                if inserted:
                    await self._conn.execute(sql)

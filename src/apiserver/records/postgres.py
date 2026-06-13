from pathlib import Path

import asyncpg

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def get_migrations() -> list[tuple[str, str]]:
    return [(p.stem, p.read_text()) for p in sorted(_MIGRATIONS_DIR.glob("*.sql"))]


class PostgresRecordStorage:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def set(self, key: str, value: bytes, indexes: list[str]) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO records (key, value, indexes) VALUES ($1, $2, $3)
                ON CONFLICT (key) DO UPDATE SET value = excluded.value, indexes = excluded.indexes
            """, key, value, indexes)

    async def get(self, key: str) -> bytes:
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT value FROM records WHERE key = $1", key)
            if result is None:
                raise KeyError(key)
            return bytes(result)

    async def list(self, prefix: str, indices: list[str] | None) -> list[bytes]:
        async with self._pool.acquire() as conn:
            if indices is None:
                rows = await conn.fetch(
                    "SELECT value FROM records WHERE starts_with(key, $1)",
                    prefix,
                )
            else:
                rows = await conn.fetch(
                    "SELECT value FROM records WHERE starts_with(key, $1) AND indexes @> $2",
                    prefix, indices,
                )
            return [bytes(r["value"]) for r in rows]

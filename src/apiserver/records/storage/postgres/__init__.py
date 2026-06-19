from pathlib import Path

import asyncpg

from ..exceptions import KeyAlreadyExists, KeyNotFound, RevisionMismatch
from ....types import Subject

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def get_migrations() -> list[tuple[str, str]]:
    return [(p.stem, p.read_text()) for p in sorted(_MIGRATIONS_DIR.glob("*.sql"))]


class PostgresRecordStorage:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create(self, subject: Subject, value: bytes, indices: list[str]) -> None:
        async with self._pool.acquire() as conn:
            result = await conn.fetchval(
                "INSERT INTO records (key, value, indices, revision) VALUES ($1, $2, $3, 0) ON CONFLICT DO NOTHING RETURNING key",
                subject.key(), value, indices,
            )
            if result is None:
                raise KeyAlreadyExists(subject)

    async def update(self, subject: Subject, value: bytes, indices: list[str], target_revision: int) -> None:
        async with self._pool.acquire() as conn:
            result = await conn.fetchval(
                "UPDATE records SET value = $2, indices = $3, revision = $4 WHERE key = $1 AND revision = $4 - 1 RETURNING key",
                subject.key(), value, indices, target_revision,
            )
            if result is None:
                raise RevisionMismatch(subject)

    async def set(self, subject: Subject, value: bytes, indices: list[str]) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO records (key, value, indices, revision) VALUES ($1, $2, $3, 0) "
                "ON CONFLICT (key) DO UPDATE SET value = excluded.value, indices = excluded.indices, revision = records.revision + 1",
                subject.key(), value, indices,
            )

    async def get(self, subject: Subject) -> bytes:
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT value FROM records WHERE key = $1", subject.key())
            if result is None:
                raise KeyNotFound(subject)
            return bytes(result)

    async def list(self, prefix: str, indices: list[str] | None) -> list[bytes]:
        async with self._pool.acquire() as conn:
            if indices is None:
                rows = await conn.fetch(
                    "SELECT value FROM records WHERE starts_with(key, $1)",
                    prefix,
                )
            elif len(indices) == 0:
                rows = await conn.fetch(
                    "SELECT value FROM records WHERE starts_with(key, $1) AND indices = ARRAY[]::TEXT[]",
                    prefix,
                )
            else:
                rows = await conn.fetch(
                    "SELECT value FROM records WHERE starts_with(key, $1) AND indices @> $2",
                    prefix, indices,
                )
            return [bytes(r["value"]) for r in rows]

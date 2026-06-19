from decimal import Decimal
from pathlib import Path

import asyncpg

from .exceptions import InsufficientBalanceError

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"

def get_migrations() -> list[tuple[str, str]]:
    return [(p.stem, p.read_text()) for p in sorted(_MIGRATIONS_DIR.glob("*.sql"))]


class PostgresLedgerStorage:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def allocate(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: str) -> None:
        if amount <= 0:
            raise ValueError(f"amount must be positive, got {amount}")
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)",
                    f"{from_principal}:{currency}",
                )
                inserted = await conn.fetchval("""
                    INSERT INTO ledger_entries (from_principal, to_principal, currency, amount, subject)
                    SELECT $1, $2, $3, $4, $5
                    WHERE (
                        SELECT COALESCE(SUM(CASE WHEN to_principal = $1 THEN amount ELSE -amount END), 0::NUMERIC)
                        FROM ledger_entries
                        WHERE (from_principal = $1 OR to_principal = $1) AND currency = $3
                    ) >= $4
                    RETURNING 1
                """, from_principal, to_principal, currency, amount, subject)
                if inserted is None:
                    raise InsufficientBalanceError(from_principal, currency, amount)

    async def grant(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: str) -> None:
        if amount <= 0:
            raise ValueError(f"amount must be positive, got {amount}")
        async with self._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO ledger_entries (from_principal, to_principal, currency, amount, subject) VALUES ($1, $2, $3, $4, $5)",
                from_principal, to_principal, currency, amount, subject,
            )

    async def balance(self, principal: str, currency: str) -> Decimal:
        async with self._pool.acquire() as conn:
            return Decimal(str(await conn.fetchval("""
                SELECT COALESCE(SUM(CASE WHEN to_principal = $1 THEN amount ELSE -amount END), 0::NUMERIC)
                FROM ledger_entries
                WHERE (from_principal = $1 OR to_principal = $1) AND currency = $2
            """, principal, currency)))

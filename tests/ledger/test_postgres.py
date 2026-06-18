import asyncio
from decimal import Decimal

import asyncpg
import pytest

from apiserver.ledger.storage.exceptions import InsufficientBalanceError
from apiserver.ledger.storage.postgres import PostgresLedgerStorage
from apiserver.types import Subject

_SUBJECT = Subject(type="instrument", tradespace="ts1", name="AAPL").key()


@pytest.fixture
async def pool(ledger_dsn):
    p = await asyncpg.create_pool(ledger_dsn)
    async with p.acquire() as conn:
        await conn.execute("TRUNCATE ledger_entries RESTART IDENTITY")
    yield p
    await p.close()


@pytest.fixture
def storage(pool: asyncpg.Pool) -> PostgresLedgerStorage:
    return PostgresLedgerStorage(pool)


# --- correctness ---

@pytest.mark.integration
@pytest.mark.asyncio
async def test_grant_bypasses_balance_check(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)
    assert await storage.balance("alice", "USD") == Decimal("100")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_allocate_reduces_balance(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)
    await storage.allocate("alice", "bob", "USD", Decimal("40"), _SUBJECT)
    assert await storage.balance("alice", "USD") == Decimal("60")
    assert await storage.balance("bob", "USD") == Decimal("40")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_allocate_raises_on_insufficient_balance(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("50"), _SUBJECT)
    with pytest.raises(InsufficientBalanceError):
        await storage.allocate("alice", "bob", "USD", Decimal("100"), _SUBJECT)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_allocate_raises_on_zero_balance(storage: PostgresLedgerStorage) -> None:
    with pytest.raises(InsufficientBalanceError):
        await storage.allocate("alice", "bob", "USD", Decimal("1"), _SUBJECT)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_balance_independent_per_currency(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)
    await storage.grant("system", "alice", "EUR", Decimal("200"), _SUBJECT)
    assert await storage.balance("alice", "USD") == Decimal("100")
    assert await storage.balance("alice", "EUR") == Decimal("200")


# --- malicious input ---

@pytest.mark.integration
@pytest.mark.asyncio
async def test_negative_amount_rejected(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)
    with pytest.raises(ValueError):
        await storage.allocate("alice", "bob", "USD", Decimal("-1"), _SUBJECT)
    assert await storage.balance("alice", "USD") == Decimal("100")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_zero_amount_rejected(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)
    with pytest.raises(ValueError):
        await storage.allocate("alice", "bob", "USD", Decimal("0"), _SUBJECT)
    assert await storage.balance("alice", "USD") == Decimal("100")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_negative_grant_rejected(storage: PostgresLedgerStorage) -> None:
    with pytest.raises(ValueError):
        await storage.grant("system", "alice", "USD", Decimal("-500"), _SUBJECT)
    assert await storage.balance("alice", "USD") == Decimal("0")


# --- race conditions ---

@pytest.mark.integration
@pytest.mark.asyncio
async def test_race_condition_only_one_wins(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)

    results = await asyncio.gather(
        *[storage.allocate("alice", "bob", "USD", Decimal("60"), _SUBJECT) for _ in range(5)],
        return_exceptions=True,
    )

    successes = [r for r in results if r is None]
    failures = [r for r in results if isinstance(r, InsufficientBalanceError)]
    assert len(successes) == 1
    assert len(failures) == 4
    assert await storage.balance("alice", "USD") == Decimal("40")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_race_condition_exact_balance_spent_once(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)

    results = await asyncio.gather(
        *[storage.allocate("alice", "bob", "USD", Decimal("100"), _SUBJECT) for _ in range(3)],
        return_exceptions=True,
    )

    successes = [r for r in results if r is None]
    assert len(successes) == 1
    assert await storage.balance("alice", "USD") == Decimal("0")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_race_condition_independent_principals_dont_block(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)
    await storage.grant("system", "bob", "USD", Decimal("100"), _SUBJECT)

    results = await asyncio.gather(
        storage.allocate("alice", "charlie", "USD", Decimal("100"), _SUBJECT),
        storage.allocate("bob", "charlie", "USD", Decimal("100"), _SUBJECT),
        return_exceptions=True,
    )

    assert all(r is None for r in results)
    assert await storage.balance("charlie", "USD") == Decimal("200")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mass_concurrency_exact_spend_count(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("50"), _SUBJECT)

    results = await asyncio.gather(
        *[storage.allocate("alice", "bob", "USD", Decimal("1"), _SUBJECT) for _ in range(100)],
        return_exceptions=True,
    )

    successes = [r for r in results if r is None]
    assert len(successes) == 50
    assert await storage.balance("alice", "USD") == Decimal("0")
    assert await storage.balance("bob", "USD") == Decimal("50")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_grants_accumulate_correctly(storage: PostgresLedgerStorage) -> None:
    results = await asyncio.gather(
        *[storage.grant("system", "alice", "USD", Decimal("10"), _SUBJECT) for _ in range(50)],
        return_exceptions=True,
    )
    assert all(r is None for r in results)
    assert await storage.balance("alice", "USD") == Decimal("500")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_cross_currency_isolation(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("100"), _SUBJECT)
    await storage.grant("system", "alice", "EUR", Decimal("50"), _SUBJECT)

    usd_ops = [storage.allocate("alice", "bob", "USD", Decimal("20"), _SUBJECT) for _ in range(10)]
    eur_ops = [storage.allocate("alice", "bob", "EUR", Decimal("10"), _SUBJECT) for _ in range(10)]
    results = await asyncio.gather(*usd_ops, *eur_ops, return_exceptions=True)

    usd_successes = [r for r in results[:10] if r is None]
    eur_successes = [r for r in results[10:] if r is None]
    assert len(usd_successes) == 5
    assert len(eur_successes) == 5
    assert await storage.balance("alice", "USD") == Decimal("0")
    assert await storage.balance("alice", "EUR") == Decimal("0")


# --- audit trail ---

@pytest.mark.integration
@pytest.mark.asyncio
async def test_ledger_entries_match_successful_allocations(storage: PostgresLedgerStorage, pool: asyncpg.Pool) -> None:
    await storage.grant("system", "alice", "USD", Decimal("50"), _SUBJECT)

    results = await asyncio.gather(
        *[storage.allocate("alice", "bob", "USD", Decimal("10"), _SUBJECT) for _ in range(20)],
        return_exceptions=True,
    )
    successes = len([r for r in results if r is None])

    async with pool.acquire() as conn:
        entry_count = await conn.fetchval(
            "SELECT COUNT(*) FROM ledger_entries WHERE from_principal = 'alice' AND to_principal = 'bob'"
        )
    assert entry_count == successes


@pytest.mark.integration
@pytest.mark.asyncio
async def test_failed_allocate_leaves_no_ledger_entry(storage: PostgresLedgerStorage, pool: asyncpg.Pool) -> None:
    with pytest.raises(InsufficientBalanceError):
        await storage.allocate("alice", "bob", "USD", Decimal("1"), _SUBJECT)

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM ledger_entries")
    assert count == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_balance_never_goes_negative(storage: PostgresLedgerStorage) -> None:
    await storage.grant("system", "alice", "USD", Decimal("10"), _SUBJECT)

    await asyncio.gather(
        *[storage.allocate("alice", "bob", "USD", Decimal("10"), _SUBJECT) for _ in range(20)],
        return_exceptions=True,
    )

    assert await storage.balance("alice", "USD") >= Decimal("0")

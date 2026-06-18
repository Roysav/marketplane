import asyncio

import asyncpg
import pytest

from apiserver.errors import RecordAlreadyExists, RecordNotFound, RecordRevisionConflict
from apiserver.records import Record, RecordMetadata, RecordsClient
from apiserver.records.storage.postgres import PostgresRecordStorage
from apiserver.types import Subject


@pytest.fixture
async def pool(records_dsn):
    p = await asyncpg.create_pool(records_dsn)
    async with p.acquire() as conn:
        await conn.execute("TRUNCATE records CASCADE")
    yield p
    await p.close()


@pytest.fixture
def storage(pool):
    return PostgresRecordStorage(pool)


@pytest.fixture
def client(storage):
    return RecordsClient(storage)


def _subject(type_="instrument", tradespace="ts1", name="AAPL"):
    return Subject(type=type_, tradespace=tradespace, name=name)


def _record(type_="instrument", tradespace="ts1", name="AAPL", labels=None, revision=0):
    return Record(type=type_, tradespace=tradespace, metadata=RecordMetadata(name=name, labels=labels or {}, revision=revision))


# --- correctness ---

@pytest.mark.integration
@pytest.mark.asyncio
async def test_set_and_get(client):
    r = _record()
    await client.create_record(r)
    assert await client.get_record(_subject()) == r


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_missing_raises(client):
    with pytest.raises(RecordNotFound):
        await client.get_record(_subject(name="UNKNOWN"))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_overwrites(client):
    await client.create_record(_record(name="AAPL"))
    r2 = _record(name="AAPL", labels={"env": "prod"}, revision=1)
    await client.update_record(r2)
    assert await client.get_record(_subject()) == r2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_existing_raises(client):
    await client.create_record(_record(name="AAPL"))
    with pytest.raises(RecordAlreadyExists):
        await client.create_record(_record(name="AAPL"))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_wrong_revision_raises(client):
    await client.create_record(_record(name="AAPL"))
    with pytest.raises(RecordRevisionConflict):
        await client.update_record(_record(name="AAPL", revision=2))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_missing_raises(client):
    with pytest.raises(RecordRevisionConflict):
        await client.update_record(_record(name="GHOST", revision=1))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_by_type_and_tradespace(client):
    await client.create_record(_record(name="AAPL"))
    await client.create_record(_record(name="GOOG"))
    await client.create_record(_record(tradespace="ts2", name="MSFT"))

    results = await client.list_records("instrument", tradespace="ts1")
    names = {r.metadata.name for r in results}
    assert names == {"AAPL", "GOOG"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_all_tradespaces(client):
    await client.create_record(_record(tradespace="ts1", name="AAPL"))
    await client.create_record(_record(tradespace="ts2", name="GOOG"))

    results = await client.list_records("instrument", all_tradespaces=True)
    assert len(results) == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_by_label(client):
    await client.create_record(_record(name="AAPL", labels={"env": "prod"}))
    await client.create_record(_record(name="GOOG", labels={"env": "staging"}))
    await client.create_record(_record(name="MSFT", labels={"env": "prod"}))

    results = await client.list_records("instrument", tradespace="ts1", labels={"env": "prod"})
    names = {r.metadata.name for r in results}
    assert names == {"AAPL", "MSFT"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_by_multiple_labels(client):
    await client.create_record(_record(name="AAPL", labels={"env": "prod", "region": "us"}))
    await client.create_record(_record(name="GOOG", labels={"env": "prod", "region": "eu"}))
    await client.create_record(_record(name="MSFT", labels={"env": "prod", "region": "us"}))

    results = await client.list_records("instrument", tradespace="ts1", labels={"env": "prod", "region": "us"})
    names = {r.metadata.name for r in results}
    assert names == {"AAPL", "MSFT"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_empty_labels_returns_empty(client):
    await client.create_record(_record(name="AAPL", labels={"env": "prod"}))
    results = await client.list_records("instrument", tradespace="ts1", labels={})
    assert results == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_no_labels_returns_all(client):
    await client.create_record(_record(name="AAPL", labels={"env": "prod"}))
    await client.create_record(_record(name="GOOG"))
    results = await client.list_records("instrument", tradespace="ts1")
    assert len(results) == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_labels_replaces_old_indexes(client):
    await client.create_record(_record(name="AAPL", labels={"env": "prod"}))
    await client.update_record(_record(name="AAPL", labels={"env": "staging"}, revision=1))

    prod = await client.list_records("instrument", tradespace="ts1", labels={"env": "prod"})
    staging = await client.list_records("instrument", tradespace="ts1", labels={"env": "staging"})
    assert prod == []
    assert len(staging) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_type_isolation(client):
    await client.create_record(_record(type_="instrument", name="AAPL"))
    await client.create_record(_record(type_="portfolio", name="P1"))

    instruments = await client.list_records("instrument", tradespace="ts1")
    portfolios = await client.list_records("portfolio", tradespace="ts1")
    assert len(instruments) == 1
    assert len(portfolios) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_sets_all_stored(client):
    records = [_record(name=f"R{i}") for i in range(20)]
    await asyncio.gather(*[client.create_record(r) for r in records])

    results = await client.list_records("instrument", tradespace="ts1")
    assert len(results) == 20


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_get_after_set(client, pool):
    await asyncio.gather(*[client.create_record(_record(name=f"R{i}")) for i in range(10)])
    results = await asyncio.gather(*[client.get_record(_subject(name=f"R{i}")) for i in range(10)])
    assert len(results) == 10

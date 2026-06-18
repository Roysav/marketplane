import asyncio
from decimal import Decimal

import pytest

from apiserver.errors import InsufficientBalance, TickNotFound
from apiserver.events import Event, EventsClient
from apiserver.events.storage.memory import MemoryEventStorage
from apiserver.ledger import LedgerClient
from apiserver.ledger.storage.memory import MemoryLedgerStorage
from apiserver.records import Record, RecordMetadata, RecordsClient
from apiserver.records.storage.memory import MemoryRecordStorage
from apiserver.service import Service
from apiserver.ticks import TicksClient
from apiserver.ticks.storage.memory import MemoryTickStorage
from apiserver.types import Subject


@pytest.fixture
def events_storage():
    return MemoryEventStorage()


@pytest.fixture
def service(events_storage):
    return Service(
        ticks=TicksClient(MemoryTickStorage()),
        events=EventsClient(events_storage),
        ledger=LedgerClient(MemoryLedgerStorage()),
        records=RecordsClient(MemoryRecordStorage()),
    )


def _subject(type_="instrument", tradespace="ts1", name="AAPL"):
    return Subject(type=type_, tradespace=tradespace, name=name)


def _record(type_="instrument", tradespace="ts1", name="AAPL", labels=None, revision=0):
    return Record(type=type_, tradespace=tradespace, metadata=RecordMetadata(name=name, labels=labels or {}, revision=revision))


# --- ticks ---

@pytest.mark.asyncio
async def test_publish_tick_stores_value(service):
    await service.publish_tick("price.BTC", 50000)
    assert await service.get_tick("price.BTC") == 50000


@pytest.mark.asyncio
async def test_get_tick_missing_raises(service):
    with pytest.raises(TickNotFound):
        await service.get_tick("price.UNKNOWN")


@pytest.mark.asyncio
async def test_subscribe_tick_receives_live(service):
    received = []

    async def watcher():
        async for val in service.subscribe_tick("price.ETH"):
            received.append(val)
            break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await service.publish_tick("price.ETH", 3000)
    await asyncio.wait_for(task, timeout=1.0)

    assert received == [3000]


@pytest.mark.asyncio
async def test_publish_tick_does_not_fire_event(service, events_storage):
    fired = []

    async def watcher():
        async for event in events_storage.watch(""):
            fired.append(event)

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await service.publish_tick("price.BTC", 50000)
    await asyncio.sleep(0.05)
    task.cancel()

    assert fired == []


# --- ledger ---

@pytest.mark.asyncio
async def test_grant_then_balance(service):
    await service.grant("system", "alice", "USD", Decimal("100"), "instrument/ts1/AAPL")
    assert await service.balance("alice", "USD") == Decimal("100")


@pytest.mark.asyncio
async def test_allocate_moves_funds(service):
    await service.grant("system", "alice", "USD", Decimal("100"), "instrument/ts1/AAPL")
    await service.allocate("alice", "bob", "USD", Decimal("40"), "instrument/ts1/AAPL")
    assert await service.balance("alice", "USD") == Decimal("60")
    assert await service.balance("bob", "USD") == Decimal("40")


@pytest.mark.asyncio
async def test_allocate_raises_on_insufficient(service):
    with pytest.raises(InsufficientBalance):
        await service.allocate("alice", "bob", "USD", Decimal("1"), "instrument/ts1/AAPL")


@pytest.mark.asyncio
async def test_ledger_does_not_fire_event(service, events_storage):
    fired = []

    async def watcher():
        async for event in events_storage.watch(""):
            fired.append(event)

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await service.grant("system", "alice", "USD", Decimal("100"), "instrument/ts1/AAPL")
    await service.allocate("alice", "bob", "USD", Decimal("50"), "instrument/ts1/AAPL")
    await asyncio.sleep(0.05)
    task.cancel()

    assert fired == []


# --- records ---

@pytest.mark.asyncio
async def test_create_record_stores_and_retrieves(service):
    r = _record()
    await service.create_record(r)
    assert await service.get_record(_subject()) == r


@pytest.mark.asyncio
async def test_create_record_fires_created_event(service, events_storage):
    received = []

    async def watcher():
        async for event in events_storage.watch("instrument/"):
            received.append(event)
            break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await service.create_record(_record())
    await asyncio.wait_for(task, timeout=1.0)

    assert received[0] == Event(
        action="created",
        subject=Subject(type="instrument", tradespace="ts1", name="AAPL"),
    )


@pytest.mark.asyncio
async def test_update_record_fires_updated_event(service, events_storage):
    received = []

    async def watcher():
        async for event in events_storage.watch("instrument/"):
            received.append(event)
            break

    await service.create_record(_record())
    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await service.update_record(_record(labels={"env": "prod"}, revision=1))
    await asyncio.wait_for(task, timeout=1.0)

    assert received[0].action == "updated"
    assert received[0].subject.name == "AAPL"


@pytest.mark.asyncio
async def test_watch_records_by_tradespace(service, events_storage):
    received = []

    async def watcher():
        async for event in service.watch_records("instrument", tradespace="ts1"):
            received.append(event)
            break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await service.create_record(_record(tradespace="ts2", name="X"))
    await asyncio.sleep(0.05)
    await service.create_record(_record(tradespace="ts1", name="Y"))
    await asyncio.wait_for(task, timeout=1.0)

    assert len(received) == 1
    assert received[0].subject.tradespace == "ts1"


@pytest.mark.asyncio
async def test_watch_records_all_tradespaces(service):
    received = []

    async def watcher():
        async for event in service.watch_records("instrument", all_tradespaces=True):
            received.append(event)
            if len(received) == 3:
                break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    for i in range(3):
        await service.create_record(_record(tradespace=f"ts{i}", name="X"))
    await asyncio.wait_for(task, timeout=1.0)

    assert len(received) == 3


# --- concurrent stress ---

@pytest.mark.asyncio
async def test_concurrent_tick_publishes_all_received(service):
    count = 50
    received = []

    async def watcher():
        async for val in service.subscribe_tick("price.BTC"):
            received.append(val)
            if len(received) == count:
                break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await asyncio.gather(*[service.publish_tick("price.BTC", i) for i in range(count)])
    await asyncio.wait_for(task, timeout=2.0)

    assert len(received) == count


@pytest.mark.asyncio
async def test_concurrent_creates_all_fire_events(service, events_storage):
    count = 20
    received = []

    async def watcher():
        async for event in events_storage.watch("instrument/"):
            received.append(event)
            if len(received) == count:
                break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await asyncio.gather(*[
        service.create_record(_record(name=f"R{i}"))
        for i in range(count)
    ])
    await asyncio.wait_for(task, timeout=2.0)

    assert len(received) == count
    assert all(e.action == "created" for e in received)

import asyncio

import pytest
import redis.asyncio as redis

from apiserver.events import Event
from apiserver.types import Subject
from apiserver.events.storage.redis import RedisEventStorage

@pytest.fixture
async def client(redis_url):
    r = redis.from_url(redis_url)
    await r.flushdb()
    yield r
    await r.aclose()


@pytest.fixture
def storage(client):
    return RedisEventStorage(client)


def _event(type_="instrument", tradespace="ts1", name="AAPL", action="created"):
    return Event(action=action, subject=Subject(type=type_, tradespace=tradespace, name=name))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_watch_receives_matching_event(storage):
    received = []

    async def watcher():
        async for event in storage.watch("instrument/"):
            received.append(event)
            break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0.05)
    e = _event()
    await storage.publish("instrument/ts1/AAPL", e)
    await asyncio.wait_for(task, timeout=2.0)

    assert received == [e]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_watch_ignores_non_matching_prefix(storage):
    received = []

    async def watcher():
        async for event in storage.watch("instrument/"):
            received.append(event)
            break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0.05)
    await storage.publish("portfolio/ts1/P1", _event(type_="portfolio", name="P1"))
    await asyncio.sleep(0.1)
    task.cancel()

    assert received == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_watch_receives_multiple_events(storage):
    count = 4
    received = []

    async def watcher():
        async for event in storage.watch("instrument/ts1/"):
            received.append(event)
            if len(received) == count:
                break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0.05)
    for i in range(count):
        await storage.publish("instrument/ts1/AAPL", _event(action="updated"))
    await asyncio.wait_for(task, timeout=2.0)

    assert len(received) == count
    assert all(e.action == "updated" for e in received)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_watchers_all_receive(storage):
    n = 3
    received = [[] for _ in range(n)]

    async def watcher(i):
        async for event in storage.watch("instrument/"):
            received[i].append(event)
            break

    tasks = [asyncio.create_task(watcher(i)) for i in range(n)]
    await asyncio.sleep(0.05)
    e = _event()
    await storage.publish("instrument/ts1/AAPL", e)
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)

    assert all(r == [e] for r in received)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_publishes_all_received(storage):
    count = 20
    received = []

    async def watcher():
        async for event in storage.watch("instrument/"):
            received.append(event)
            if len(received) == count:
                break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0.05)
    await asyncio.gather(*[
        storage.publish(f"instrument/ts1/R{i}", _event(name=f"R{i}"))
        for i in range(count)
    ])
    await asyncio.wait_for(task, timeout=2.0)

    assert len(received) == count


@pytest.mark.integration
@pytest.mark.asyncio
async def test_prefix_isolation(storage):
    instruments = []
    portfolios = []

    async def watch_instruments():
        async for event in storage.watch("instrument/"):
            instruments.append(event)
            break

    async def watch_portfolios():
        async for event in storage.watch("portfolio/"):
            portfolios.append(event)
            break

    tasks = [asyncio.create_task(watch_instruments()), asyncio.create_task(watch_portfolios())]
    await asyncio.sleep(0.05)
    await storage.publish("instrument/ts1/AAPL", _event())
    await storage.publish("portfolio/ts1/P1", _event(type_="portfolio", name="P1"))
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)

    assert instruments[0].subject.type == "instrument"
    assert portfolios[0].subject.type == "portfolio"

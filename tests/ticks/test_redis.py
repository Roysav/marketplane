import asyncio

import pytest
import redis.asyncio as redis

from apiserver.ticks.storage.exceptions import KeyNotFound
from apiserver.ticks.storage.redis import RedisTickStorage

@pytest.fixture
async def client(redis_url):
    r = redis.from_url(redis_url)
    await r.flushdb()
    yield r
    await r.aclose()


@pytest.fixture
def storage(client):
    return RedisTickStorage(client)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_then_get(storage):
    await storage.publish("price.BTC", "50000")
    assert await storage.get("price.BTC") == "50000"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_overwrites(storage):
    await storage.publish("price.BTC", "50000")
    await storage.publish("price.BTC", "51000")
    assert await storage.get("price.BTC") == "51000"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_missing_raises(storage):
    with pytest.raises(KeyNotFound):
        await storage.get("price.UNKNOWN")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_subscribe_receives_published(storage):
    received = []

    async def watcher():
        async for val in storage.subscribe("price.BTC"):
            received.append(val)
            break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0.05)
    await storage.publish("price.BTC", "50000")
    await asyncio.wait_for(task, timeout=2.0)

    assert received == ["50000"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_subscribe_receives_multiple_updates(storage):
    received = []
    count = 5

    async def watcher():
        async for val in storage.subscribe("price.BTC"):
            received.append(val)
            if len(received) == count:
                break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0.05)
    for i in range(count):
        await storage.publish("price.BTC", str(i))
    await asyncio.wait_for(task, timeout=2.0)

    assert received == [str(i) for i in range(count)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_subscribe_misses_pre_subscription_publishes(storage):
    await storage.publish("price.BTC", "old")

    received = []

    async def watcher():
        async for val in storage.subscribe("price.BTC"):
            received.append(val)
            break

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0.05)
    await storage.publish("price.BTC", "new")
    await asyncio.wait_for(task, timeout=2.0)

    assert received == ["new"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_subscribers_all_receive(storage):
    n = 3
    received = [[] for _ in range(n)]

    async def watcher(i):
        async for val in storage.subscribe("price.BTC"):
            received[i].append(val)
            break

    tasks = [asyncio.create_task(watcher(i)) for i in range(n)]
    await asyncio.sleep(0.05)
    await storage.publish("price.BTC", "42000")
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)

    assert all(r == ["42000"] for r in received)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_independent_keys_dont_cross(storage):
    received_btc = []
    received_eth = []

    async def watch_btc():
        async for val in storage.subscribe("price.BTC"):
            received_btc.append(val)
            break

    async def watch_eth():
        async for val in storage.subscribe("price.ETH"):
            received_eth.append(val)
            break

    tasks = [asyncio.create_task(watch_btc()), asyncio.create_task(watch_eth())]
    await asyncio.sleep(0.05)
    await storage.publish("price.BTC", "50000")
    await storage.publish("price.ETH", "3000")
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)

    assert received_btc == ["50000"]
    assert received_eth == ["3000"]

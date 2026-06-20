import asyncio

import httpx

from controller import Controller
from controllers.polymarket.api import Event, Market
from controllers.polymarket.reconcilers import (
    ASSET_TYPE,
    EVENT_TYPE,
    MARKET_TYPE,
    AssetSubscriber,
    EventImporter,
    EventReconciler,
    MarketReconciler,
)

TRADESPACE = "polymarket"


class FakeAPI:
    def __init__(self, events, markets):
        self._events = events
        self._markets = {m.market_id: m for m in markets}

    async def list_events(self):
        for event in self._events:
            yield event

    async def get_market(self, market_id):
        return self._markets[market_id]


class FakeChannel:
    def __init__(self): self.updates = []
    async def update(self, asset_ids): self.updates.append(set(asset_ids))
    async def run(self): await asyncio.Event().wait()


def _fixtures():
    market = Market(
        market_id="m1", question="q", slug="s", condition_id="0xabc",
        outcomes=["Yes", "No"], clob_token_ids=["tokA", "tokB"], active=True,
    )
    event = Event(event_id="e1", slug="es", title="et", market_ids=["m1"])
    return event, market


async def test_cron_imports_events_then_cascades_to_asset_subscription(client):
    channel = FakeChannel()
    controller = Controller(client, reconnect_backoff=0.01, resync_interval=0.03)
    event, market = _fixtures()
    api = FakeAPI([event], [market])

    AssetSubscriber(controller, channel, tradespace=TRADESPACE)
    MarketReconciler(controller, client, tradespace=TRADESPACE)
    EventReconciler(controller, client, api, tradespace=TRADESPACE)
    EventImporter(controller, client, api, tradespace=TRADESPACE, interval=10.0)

    task = asyncio.create_task(controller.run())
    for _ in range(100):
        await asyncio.sleep(0.02)
        if channel.updates and channel.updates[-1] == {"tokA", "tokB"}:
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert [r.name for r in client.of_type(EVENT_TYPE)] == ["e1"]
    assert [r.name for r in client.of_type(MARKET_TYPE)] == ["m1"]
    assert {r.name for r in client.of_type(ASSET_TYPE)} == {"tokA", "tokB"}
    assert channel.updates[-1] == {"tokA", "tokB"}


class BoomAPI:
    async def list_events(self):
        raise httpx.ConnectError("boom")
        yield

    async def get_market(self, market_id):
        raise httpx.ConnectError("boom")


async def test_import_logs_and_continues_on_api_error(client):
    controller = Controller(client, reconnect_backoff=0.01, resync_interval=10.0)
    importer = EventImporter(controller, client, BoomAPI(), tradespace=TRADESPACE, interval=10.0)
    await importer._import()
    assert client.of_type(EVENT_TYPE) == []

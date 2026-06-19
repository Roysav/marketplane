from typing import Any

import grpc
from grpc.aio import AioRpcError

from controller import Controller, RecordNotification
from controllers.polymarket.api import Event, Market, PolymarketAPI
from controllers.polymarket.channel import Channel
from sdk import MarketplaneClient, Record

TRADESPACE = "polymarket"
EVENT_TYPE = "alphav1/polymarket/Event"
MARKET_TYPE = "alphav1/polymarket/Market"
ASSET_TYPE = "alphav1/polymarket/Asset"


async def _ensure(client: MarketplaneClient, record: Record) -> None:
    try:
        await client.create_record(record)
    except AioRpcError as err:
        if err.code() is not grpc.StatusCode.ALREADY_EXISTS:
            raise


def _market_spec(market: Market) -> dict[str, Any]:
    return {
        "market_id": market.market_id,
        "question": market.question,
        "slug": market.slug,
        "conditionId": market.condition_id,
        "outcomes": market.outcomes,
        "clobTokenIds": market.clob_token_ids,
        "active": market.active,
    }


def _event_spec(event: Event) -> dict[str, Any]:
    return {
        "id": event.event_id,
        "slug": event.slug,
        "title": event.title,
        "market_ids": event.market_ids,
    }


class EventImporter:
    def __init__(self, controller: Controller, client: MarketplaneClient, api: PolymarketAPI, *, interval: float):
        self._client = client
        self._api = api
        controller.on_schedule(interval)(self._import)

    async def _import(self) -> None:
        for event in await self._api.list_events():
            record = Record(type=EVENT_TYPE, tradespace=TRADESPACE, name=event.event_id, spec=_event_spec(event))
            await _ensure(self._client, record)


class EventReconciler:
    def __init__(self, controller: Controller, client: MarketplaneClient, api: PolymarketAPI):
        self._client = client
        self._api = api
        controller.on_existing(EVENT_TYPE, tradespace=TRADESPACE)(self._reconcile)

    async def _reconcile(self, n: RecordNotification) -> None:
        for market_id in n.record.spec["market_ids"]:
            market = await self._api.get_market(market_id)
            record = Record(type=MARKET_TYPE, tradespace=TRADESPACE, name=market.market_id, spec=_market_spec(market))
            await _ensure(self._client, record)


class MarketReconciler:
    def __init__(self, controller: Controller, client: MarketplaneClient):
        self._client = client
        controller.on_existing(MARKET_TYPE, tradespace=TRADESPACE)(self._reconcile)

    async def _reconcile(self, n: RecordNotification) -> None:
        for token_id in n.record.spec["clobTokenIds"]:
            record = Record(type=ASSET_TYPE, tradespace=TRADESPACE, name=token_id, spec={"id": token_id})
            await _ensure(self._client, record)


class AssetSubscriber:
    def __init__(self, controller: Controller, channel: Channel):
        self._channel = channel
        self._asset_ids: dict[tuple[str, str], str] = {}
        self._subscribed: set[str] = set()
        controller.on_existing(ASSET_TYPE, tradespace=TRADESPACE)(self._on_asset)

    async def _on_asset(self, n: RecordNotification) -> None:
        record = n.record
        self._asset_ids[(record.tradespace, record.name)] = record.spec["id"]
        current = set(self._asset_ids.values())
        if current != self._subscribed:
            self._subscribed = current
            await self._channel.update(current)

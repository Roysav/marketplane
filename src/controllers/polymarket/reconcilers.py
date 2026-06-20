import logging
from typing import Any

import httpx
from grpc.aio import AioRpcError

from controller import Controller, RecordNotification
from sdk import MarketplaneClient, Record

from .api import Event, Market, PolymarketAPI
from .channel import Channel

logger = logging.getLogger(__name__)

EVENT_TYPE = "alphav1/polymarket/Event"
MARKET_TYPE = "alphav1/polymarket/Market"
ASSET_TYPE = "alphav1/polymarket/Asset"


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
    def __init__(self, controller: Controller, client: MarketplaneClient, api: PolymarketAPI, *, tradespace: str, interval: float):
        self._client = client
        self._api = api
        self._tradespace = tradespace
        controller.on_schedule(interval)(self._import)

    async def _import(self) -> None:
        try:
            async for event in self._api.list_events():
                record = Record(type=EVENT_TYPE, tradespace=self._tradespace, name=event.event_id, spec=_event_spec(event))
                await self._client.apply_record(record)
        except (httpx.HTTPError, AioRpcError) as err:
            logger.exception("polymarket event import failed", exc_info=err)


class EventReconciler:
    def __init__(self, controller: Controller, client: MarketplaneClient, api: PolymarketAPI, *, tradespace: str):
        self._client = client
        self._api = api
        self._tradespace = tradespace
        controller.on_existing(EVENT_TYPE, tradespace=tradespace)(self._reconcile)

    async def _reconcile(self, n: RecordNotification) -> None:
        for market_id in n.record.spec["market_ids"]:
            market = await self._api.get_market(market_id)
            record = Record(type=MARKET_TYPE, tradespace=self._tradespace, name=market.market_id, spec=_market_spec(market))
            await self._client.apply_record(record)


class MarketReconciler:
    def __init__(self, controller: Controller, client: MarketplaneClient, *, tradespace: str):
        self._client = client
        self._tradespace = tradespace
        controller.on_existing(MARKET_TYPE, tradespace=tradespace)(self._reconcile)

    async def _reconcile(self, n: RecordNotification) -> None:
        for token_id in n.record.spec["clobTokenIds"]:
            record = Record(type=ASSET_TYPE, tradespace=self._tradespace, name=token_id, spec={"id": token_id})
            await self._client.apply_record(record)


class AssetSubscriber:
    def __init__(self, controller: Controller, channel: Channel, *, tradespace: str):
        self._channel = channel
        self._asset_ids: dict[tuple[str, str], str] = {}
        self._subscribed: set[str] = set()
        controller.on_existing(ASSET_TYPE, tradespace=tradespace)(self._on_asset)

    async def _on_asset(self, n: RecordNotification) -> None:
        record = n.record
        self._asset_ids[(record.tradespace, record.name)] = record.spec["id"]
        current = set(self._asset_ids.values())
        if current != self._subscribed:
            self._subscribed = current
            await self._channel.update(current)

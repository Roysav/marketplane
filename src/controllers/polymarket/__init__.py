import asyncio
import json
from collections.abc import Iterable
from typing import Protocol

from websockets.asyncio.client import ClientConnection, connect

from controller import Controller, RecordNotification

MARKET_CHANNEL_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ASSET_TYPE = "alphav1/polymarket/Asset"


class Channel(Protocol):
    async def update(self, asset_ids: Iterable[str]) -> None: ...
    async def run(self) -> None: ...


class MarketChannel:
    def __init__(self, url: str):
        self._url = url
        self._assets: set[str] = set()
        self._ws: ClientConnection | None = None

    async def update(self, asset_ids: Iterable[str]) -> None:
        self._assets = set(asset_ids)
        if self._ws is not None:
            await self._ws.send(self._subscription())

    def _subscription(self) -> str:
        return json.dumps({
            "assets_ids": sorted(self._assets),
            "type": "market",
            "custom_feature_enabled": True,
        })

    async def run(self) -> None:
        async for ws in connect(self._url):
            self._ws = ws
            if self._assets:
                await ws.send(self._subscription())
            async for message in ws:
                print(message)


class Polymarket:
    def __init__(self, controller: Controller, channel: Channel):
        self._controller = controller
        self._channel = channel
        self._asset_ids: dict[tuple[str, str], str] = {}
        self._subscribed: set[str] = set()
        controller.on_existing(ASSET_TYPE)(self._on_asset)

    async def _on_asset(self, n: RecordNotification) -> None:
        record = n.record
        self._asset_ids[(record.tradespace, record.name)] = record.spec["id"]
        current = set(self._asset_ids.values())
        if current != self._subscribed:
            self._subscribed = current
            await self._channel.update(current)

    async def run(self) -> None:
        await asyncio.gather(self._channel.run(), self._controller.run())

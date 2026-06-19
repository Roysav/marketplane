import json
from collections.abc import Awaitable, Callable, Iterable
from typing import Protocol

from websockets.asyncio.client import ClientConnection, connect


class Channel(Protocol):
    async def update(self, asset_ids: Iterable[str]) -> None: ...
    async def run(self) -> None: ...


class MarketChannel:
    def __init__(self, url: str, on_message: Callable[[str], Awaitable[None]]):
        self._url = url
        self._on_message = on_message
        self._assets: set[str] = set()
        self._subscribed: set[str] = set()
        self._ws: ClientConnection | None = None

    async def update(self, asset_ids: Iterable[str]) -> None:
        new = set(asset_ids)
        if new == self._assets:
            return
        self._assets = new
        if self._ws is not None:
            await self._apply()

    async def _apply(self) -> None:
        assert self._ws is not None
        if not self._subscribed:
            if self._assets:
                await self._ws.send(json.dumps({"assets_ids": sorted(self._assets), "type": "market", "custom_feature_enabled": True}))
                self._subscribed = set(self._assets)
            return
        added = self._assets - self._subscribed
        removed = self._subscribed - self._assets
        if added:
            await self._ws.send(json.dumps({"operation": "subscribe", "assets_ids": sorted(added)}))
        if removed:
            await self._ws.send(json.dumps({"operation": "unsubscribe", "assets_ids": sorted(removed)}))
        self._subscribed = set(self._assets)

    async def run(self) -> None:
        async for ws in connect(self._url):
            self._ws = ws
            self._subscribed = set()
            await self._apply()
            async for message in ws:
                await self._on_message(message)
            self._ws = None

import json
from collections.abc import Iterable
from typing import Protocol

from websockets.asyncio.client import ClientConnection, connect


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

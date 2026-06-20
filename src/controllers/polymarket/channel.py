import json
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Protocol

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


class Channel(Protocol):
    async def update(self, asset_ids: Iterable[str]) -> None: ...
    async def run(self) -> None: ...


class MarketChannel:
    def __init__(self, url: str, on_message: Callable[[str], Awaitable[None]], *, max_assets: int, ping_timeout: float):
        self._url = url
        self._on_message = on_message
        self._max_assets = max_assets
        self._ping_timeout = ping_timeout
        self._assets: set[str] = set()
        self._subscribed: set[str] = set()
        self._capped = False
        self._ws: ClientConnection | None = None

    async def update(self, asset_ids: Iterable[str]) -> None:
        new = set(asset_ids)
        if len(new) > self._max_assets:
            if not self._capped:
                logger.warning("asset count %d exceeds max_assets %d; subscribing to a subset", len(new), self._max_assets)
                self._capped = True
            new = set(sorted(new)[: self._max_assets])
        else:
            self._capped = False
        if new == self._assets:
            return
        self._assets = new
        if self._ws is not None:
            try:
                await self._apply()
            except ConnectionClosed:
                pass

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
        async for ws in connect(self._url, ping_timeout=self._ping_timeout):
            self._ws = ws
            self._subscribed = set()
            try:
                await self._apply()
                async for message in ws:
                    try:
                        await self._on_message(message)
                    except Exception as err:
                        err.add_note(f"While processing {message=!r}")
                        logger.exception("market channel message handler failed", exc_info=err)
            except ConnectionClosed as connection_close_err:
                logger.warning("market channel disconnected; reconnecting", exc_info=connection_close_err)
            self._ws = None

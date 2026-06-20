import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


class UserChannel:
    def __init__(self, url: str, api_key: str, api_secret: str, passphrase: str, *, ping_interval: float, ping_timeout: float):
        self._url = url
        self._auth = {"apiKey": api_key, "secret": api_secret, "passphrase": passphrase}
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        async for ws in connect(self._url, ping_timeout=self._ping_timeout):
            await ws.send(json.dumps({"auth": self._auth, "type": "user"}))
            heartbeat = asyncio.create_task(self._heartbeat(ws))
            try:
                async for message in ws:
                    if message == "PONG":
                        continue
                    yield json.loads(message)
            except ConnectionClosed as err:
                logger.warning("user channel disconnected; reconnecting", exc_info=err)
            finally:
                heartbeat.cancel()

    async def _heartbeat(self, ws: ClientConnection) -> None:
        try:
            while True:
                await asyncio.sleep(self._ping_interval)
                await ws.send("PING")
        except ConnectionClosed:
            pass

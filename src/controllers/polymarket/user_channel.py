import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


class UserChannel:
    def __init__(self, url: str, api_key: str, api_secret: str, passphrase: str, *, ping_timeout: float):
        self._url = url
        self._auth = {"apiKey": api_key, "secret": api_secret, "passphrase": passphrase}
        self._ping_timeout = ping_timeout

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        async for ws in connect(self._url, ping_timeout=self._ping_timeout):
            await ws.send(json.dumps({"auth": self._auth, "type": "user"}))
            try:
                async for message in ws:
                    yield json.loads(message)
            except ConnectionClosed as err:
                logger.warning("user channel disconnected; reconnecting", exc_info=err)

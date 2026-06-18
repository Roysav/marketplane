import json
from collections.abc import AsyncIterator
from typing import Any, Protocol

from apiserver.errors import TickNotFound
from apiserver.ticks.storage.exceptions import KeyNotFound


class TickStorage(Protocol):
    async def publish(self, key: str, value: str) -> None: ...
    async def get(self, key: str) -> str: ...
    def subscribe(self, key: str) -> AsyncIterator[str]: ...


class TicksClient:
    def __init__(self, backend: TickStorage) -> None:
        self._backend = backend

    async def publish(self, name: str, value: Any) -> None:
        await self._backend.publish(name, json.dumps(value))

    async def get(self, name: str) -> Any:
        try:
            raw = await self._backend.get(name)
        except KeyNotFound:
            raise TickNotFound(name)
        return json.loads(raw)

    async def subscribe(self, name: str) -> AsyncIterator[Any]:
        async for raw in self._backend.subscribe(name):
            yield json.loads(raw)

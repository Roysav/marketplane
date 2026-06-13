import json
from collections.abc import AsyncIterator
from typing import Any, Protocol


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
        return json.loads(await self._backend.get(name))

    async def subscribe(self, name: str) -> AsyncIterator[Any]:
        async for raw in self._backend.subscribe(name):
            yield json.loads(raw)

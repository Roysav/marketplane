import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator

from .exceptions import KeyNotFound


class MemoryTickStorage:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._subscribers: dict[str, list[asyncio.Queue[str]]] = defaultdict(list)

    async def publish(self, key: str, value: str) -> None:
        self._values[key] = value
        for queue in self._subscribers[key]:
            await queue.put(value)

    async def get(self, key: str) -> str:
        if key not in self._values:
            raise KeyNotFound(key)
        return self._values[key]

    async def subscribe(self, key: str) -> AsyncIterator[str]:
        queue: asyncio.Queue[str] = asyncio.Queue()
        self._subscribers[key].append(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            self._subscribers[key].remove(queue)

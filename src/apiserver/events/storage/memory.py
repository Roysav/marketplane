import asyncio
from collections.abc import AsyncIterator

from apiserver.events import Event


class MemoryEventStorage:
    def __init__(self) -> None:
        self._watchers: list[tuple[str, asyncio.Queue[Event]]] = []

    async def publish(self, stream: str, event: Event) -> None:
        for prefix, queue in self._watchers:
            if stream.startswith(prefix):
                await queue.put(event)

    async def watch(self, prefix: str) -> AsyncIterator[Event]:
        queue: asyncio.Queue[Event] = asyncio.Queue()
        self._watchers.append((prefix, queue))
        try:
            while True:
                yield await queue.get()
        finally:
            self._watchers.remove((prefix, queue))

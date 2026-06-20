import asyncio
import enum
import logging
from collections.abc import Hashable

from ._registry import HandlerRegistry
from ._scheduler import Scheduler
from .notifications import BaseNotification

logger = logging.getLogger(__name__)


class _EOS(enum.Enum):
    TOKEN = enum.auto()


EOS = _EOS.TOKEN

_Item = BaseNotification | _EOS


class Multiplexer:
    def __init__(self, *, scheduler: Scheduler, registry: HandlerRegistry, idle_timeout: float):
        self._scheduler = scheduler
        self._registry = registry
        self._idle_timeout = idle_timeout
        self._streams: dict[Hashable, asyncio.Queue[_Item]] = {}

    async def feed(self, notification: BaseNotification) -> None:
        key = notification.key
        queue = self._streams.get(key)
        if queue is None:
            queue = asyncio.Queue()
            self._streams[key] = queue
            self._scheduler.spawn(self._worker(key), name=f"worker {key!r}")
        queue.put_nowait(notification)

    async def drain(self) -> None:
        for queue in list(self._streams.values()):
            queue.put_nowait(EOS)
        await self._scheduler.wait()

    async def _worker(self, key: Hashable) -> None:
        queue = self._streams[key]
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), self._idle_timeout)
            except asyncio.TimeoutError:
                if queue.empty():
                    del self._streams[key]
                    return
                continue
            if item is EOS:
                del self._streams[key]
                return
            await self._dispatch(item)

    async def _dispatch(self, notification: BaseNotification) -> None:
        for handler in self._registry.iter_handlers(notification):
            try:
                await handler(notification)
            except Exception as err:
                logger.exception("handler failed for %r", notification.key, exc_info=err)

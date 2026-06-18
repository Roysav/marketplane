import asyncio
import enum
import logging
from collections.abc import Hashable

from controller._registry import HandlerRegistry
from controller._scheduler import Scheduler
from controller.notifications import BaseNotification

logger = logging.getLogger(__name__)


class _EOS(enum.Enum):
    TOKEN = enum.auto()


EOS = _EOS.TOKEN

_Item = BaseNotification | _EOS


class Multiplexer:
    def __init__(
        self,
        *,
        scheduler: Scheduler,
        registry: HandlerRegistry,
        idle_timeout: float,
        exit_timeout: float,
    ) -> None:
        self._scheduler = scheduler
        self._registry = registry
        self._idle_timeout = idle_timeout
        self._exit_timeout = exit_timeout
        self._streams: dict[Hashable, asyncio.Queue[_Item]] = {}
        self._lock = asyncio.Lock()

    async def feed(self, notification: BaseNotification) -> None:
        key = notification.key
        async with self._lock:
            queue = self._streams.get(key)
            if queue is None:
                queue = asyncio.Queue()
                self._streams[key] = queue
                await self._scheduler.spawn(self._worker(key), name=f"worker {key!r}")
            queue.put_nowait(notification)

    async def drain(self) -> None:
        async with self._lock:
            for queue in self._streams.values():
                queue.put_nowait(EOS)
        try:
            await asyncio.wait_for(self._scheduler.wait(), timeout=self._exit_timeout)
        except asyncio.TimeoutError:
            logger.warning("Drain timed out with streams still active: %r", list(self._streams))
        await self._scheduler.close()

    async def _worker(self, key: Hashable) -> None:
        queue = self._streams[key]
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=self._idle_timeout)
                except asyncio.TimeoutError:
                    async with self._lock:
                        if queue.empty():
                            del self._streams[key]
                            return
                    continue
                if item is EOS:
                    return
                await self._dispatch(item)
        finally:
            async with self._lock:
                if self._streams.get(key) is queue:
                    del self._streams[key]

    async def _dispatch(self, notification: BaseNotification) -> None:
        for handler in self._registry.iter_handlers(notification):
            try:
                await handler(notification)
            except Exception as err:
                logger.exception(err)

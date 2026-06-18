import asyncio
from collections.abc import Callable, Coroutine
from typing import Any


class Scheduler:
    def __init__(
        self,
        *,
        limit: int | None = None,
        exception_handler: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._closed = False
        self._limit = limit
        self._exception_handler = exception_handler
        self._condition = asyncio.Condition()
        self._pending: asyncio.Queue[Coroutine[Any, Any, Any]] = asyncio.Queue()
        self._running: set[asyncio.Task[Any]] = set()
        self._cleaning: asyncio.Queue[asyncio.Task[Any]] = asyncio.Queue()
        self._spawner = asyncio.create_task(self._task_spawner(), name="scheduler-spawner")
        self._cleaner = asyncio.create_task(self._task_cleaner(), name="scheduler-cleaner")

    def empty(self) -> bool:
        return self._pending.empty() and not self._running

    async def wait(self) -> None:
        async with self._condition:
            await self._condition.wait_for(self.empty)

    async def spawn(self, coro: Coroutine[Any, Any, Any], *, name: str | None = None) -> None:
        if self._closed:
            coro.close()
            raise RuntimeError("Cannot spawn on a closed scheduler.")
        async with self._condition:
            self._pending.put_nowait(coro)
            self._condition.notify_all()
        await asyncio.sleep(0)

    async def close(self) -> None:
        self._closed = True
        for task in self._running:
            task.cancel()
        await self.wait()
        self._spawner.cancel()
        self._cleaner.cancel()
        await asyncio.gather(self._spawner, self._cleaner, return_exceptions=True)

    def _can_spawn(self) -> bool:
        return not self._pending.empty() and (self._limit is None or len(self._running) < self._limit)

    async def _task_spawner(self) -> None:
        while True:
            async with self._condition:
                await self._condition.wait_for(self._can_spawn)
                while self._can_spawn():
                    coro = self._pending.get_nowait()
                    task = asyncio.create_task(coro)
                    task.add_done_callback(self._task_done)
                    self._running.add(task)
                    if self._closed:
                        task.cancel()

    async def _task_cleaner(self) -> None:
        while True:
            await self._cleaning.get()
            async with self._condition:
                self._condition.notify_all()

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self._running.discard(task)
        self._cleaning.put_nowait(task)
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            exc = None
        if exc is not None and self._exception_handler is not None:
            self._exception_handler(exc)

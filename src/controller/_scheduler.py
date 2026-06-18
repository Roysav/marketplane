import asyncio
from collections.abc import Callable, Coroutine
from typing import Any


class Scheduler:
    def __init__(self, *, exception_handler: Callable[[BaseException], None]) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()
        self._exception_handler = exception_handler

    def spawn(self, coro: Coroutine[Any, Any, Any], *, name: str | None = None) -> None:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._on_done)

    async def wait(self) -> None:
        tasks = list(self._tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        await self.wait()

    def _on_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._exception_handler(exc)

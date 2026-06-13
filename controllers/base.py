import asyncio
import math
from abc import ABC, abstractmethod
from typing import Any


class Controller(ABC):
    resync_interval: float = 60.0

    @abstractmethod
    async def list_records(self) -> dict[str, Any]: ...

    @abstractmethod
    async def reconcile(self, records: dict[str, Any]) -> float | None: ...

    async def watch(self, changed: asyncio.Event) -> None:
        await asyncio.sleep(math.inf)

    async def run(self) -> None:
        changed = asyncio.Event()
        watch_task = asyncio.create_task(self.watch(changed))
        while True:
            if watch_task.done():
                watch_task.result()
            changed.clear()
            records = await self.list_records()
            requeue_after = await self.reconcile(records)
            timeout = requeue_after if requeue_after is not None else self.resync_interval
            wait_task = asyncio.create_task(changed.wait())
            await asyncio.wait([wait_task], timeout=timeout)
            wait_task.cancel()


async def run_controllers(*controllers: Controller) -> None:
    async with asyncio.TaskGroup() as tg:
        for ctrl in controllers:
            tg.create_task(ctrl.run(), name=type(ctrl).__name__)

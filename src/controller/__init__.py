import asyncio
from collections.abc import Callable

from sdk import SDK


class _On:
    def __init__(self, controller: "Controller") -> None:
        self._ctrl = controller

    def create(self, type_: str, *, tradespace: str | None = None) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._ctrl._record_handlers.setdefault(type_, []).append(("created", tradespace, fn))
            return fn
        return decorator

    def update(self, type_: str, *, tradespace: str | None = None) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._ctrl._record_handlers.setdefault(type_, []).append(("updated", tradespace, fn))
            return fn
        return decorator

    def tick(self, name: str) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._ctrl._tick_handlers.setdefault(name, []).append(fn)
            return fn
        return decorator


class Controller:
    def __init__(self, sdk: SDK) -> None:
        self._sdk = sdk
        self._record_handlers: dict[str, list[tuple[str, str | None, Callable]]] = {}
        self._tick_handlers: dict[str, list[Callable]] = {}
        self.on = _On(self)

    async def run(self) -> None:
        tasks = [
            *[asyncio.create_task(self._watch_records(type_)) for type_ in self._record_handlers],
            *[asyncio.create_task(self._watch_tick(name)) for name in self._tick_handlers],
        ]
        await asyncio.gather(*tasks)

    async def _watch_records(self, type_: str) -> None:
        async for event in self._sdk.watch_records(type_, all_tradespaces=True):
            for action, tradespace_filter, fn in self._record_handlers.get(type_, []):
                if event.action == action and (tradespace_filter is None or event.tradespace == tradespace_filter):
                    asyncio.create_task(fn(event))

    async def _watch_tick(self, name: str) -> None:
        async for value in self._sdk.subscribe_tick(name):
            for fn in self._tick_handlers.get(name, []):
                asyncio.create_task(fn(value))

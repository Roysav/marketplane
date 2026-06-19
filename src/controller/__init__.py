import asyncio
import functools
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from grpc.aio import AioRpcError
from sdk import MarketplaneClient, Record

from ._multiplexer import Multiplexer
from ._registry import Handler, HandlerRegistry, Selector
from ._scheduler import Scheduler
from .notifications import (
    BaseNotification,
    NotificationType,
    RecordNotification,
    TickNotification,
)

__all__ = [
    "Controller",
    "Handler",
    "BaseNotification",
    "RecordNotification",
    "TickNotification",
    "NotificationType",
]

logger = logging.getLogger(__name__)

_RECORD_ACTIONS = frozenset({
    NotificationType.RECORD_CREATED,
    NotificationType.RECORD_UPDATED,
    NotificationType.RECORD_DELETED,
})


class Controller:
    def __init__(self, client: MarketplaneClient, *, reconnect_backoff: float, resync_interval: float):
        self._client = client
        self._reconnect_backoff = reconnect_backoff
        self._resync_interval = resync_interval
        self._registry = HandlerRegistry()
        self._record_types: set[str] = set()
        self._resync_types: set[str] = set()
        self._tick_names: set[str] = set()
        self._schedules: list[tuple[float, Callable[[], Awaitable[None]]]] = []

    def on_record_event(
        self,
        type_: str,
        *,
        tradespace: str | None = None,
        labels: dict[str, str] | None = None,
        actions: frozenset[NotificationType] = _RECORD_ACTIONS,
    ) -> Callable[[Handler], Handler]:
        selector = Selector(
            types=actions,
            record_type=type_,
            tradespace=tradespace,
            labels=tuple(sorted((labels or {}).items())),
        )

        def decorator(handler: Handler) -> Handler:
            self._registry.register(handler, selector)
            self._record_types.add(type_)
            return handler

        return decorator

    def on_existing(
        self,
        type_: str,
        *,
        tradespace: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> Callable[[Handler], Handler]:
        selector = Selector(
            types=frozenset({
                NotificationType.RECORD_CREATED,
                NotificationType.RECORD_UPDATED,
                NotificationType.RECORD_EXISTING,
            }),
            record_type=type_,
            tradespace=tradespace,
            labels=tuple(sorted((labels or {}).items())),
        )

        def decorator(handler: Handler) -> Handler:
            self._registry.register(handler, selector)
            self._record_types.add(type_)
            self._resync_types.add(type_)
            return handler

        return decorator

    def on_tick(self, name: str) -> Callable[[Handler], Handler]:
        selector = Selector(types=frozenset({NotificationType.TICK_CHANGED}), tick_name=name)

        def decorator(handler: Handler) -> Handler:
            self._registry.register(handler, selector)
            self._tick_names.add(name)
            return handler

        return decorator

    def on_schedule(self, interval: float) -> Callable[[Callable[[], Awaitable[None]]], Callable[[], Awaitable[None]]]:
        def decorator(handler: Callable[[], Awaitable[None]]) -> Callable[[], Awaitable[None]]:
            self._schedules.append((interval, handler))
            return handler

        return decorator

    def _on_worker_error(self, exc: BaseException) -> None:
        logger.error("controller worker failed", exc_info=exc)

    async def run(self) -> None:
        scheduler = Scheduler(exception_handler=self._on_worker_error)
        multiplexer = Multiplexer(scheduler=scheduler, registry=self._registry)
        feeders = [
            asyncio.create_task(
                self._feed(
                    functools.partial(self._client.watch_records, type_, all_tradespaces=True),
                    RecordNotification.from_event,
                    multiplexer,
                    f"watch-records {type_}",
                ),
                name=f"watch-records {type_}",
            )
            for type_ in self._record_types
        ] + [
            asyncio.create_task(
                self._feed(
                    functools.partial(self._client.subscribe_tick, name),
                    functools.partial(TickNotification.from_tick, name),
                    multiplexer,
                    f"subscribe-tick {name}",
                ),
                name=f"subscribe-tick {name}",
            )
            for name in self._tick_names
        ] + [
            asyncio.create_task(
                self._feed(
                    functools.partial(self._list_loop, type_),
                    RecordNotification.from_record,
                    multiplexer,
                    f"resync {type_}",
                ),
                name=f"resync {type_}",
            )
            for type_ in self._resync_types
        ] + [
            asyncio.create_task(self._schedule_loop(interval, handler), name=f"schedule {interval}s")
            for interval, handler in self._schedules
        ]
        try:
            await asyncio.gather(*feeders)
        finally:
            for feeder in feeders:
                feeder.cancel()
            await asyncio.gather(*feeders, return_exceptions=True)
            await multiplexer.drain()

    async def _feed(
        self,
        make_stream: Callable[[], AsyncIterator[Any]],
        parse: Callable[[Any], BaseNotification],
        multiplexer: Multiplexer,
        label: str,
    ) -> None:
        while True:
            try:
                async for item in make_stream():
                    await multiplexer.feed(parse(item))
            except AioRpcError as err:
                err.add_note(f"while handling {label!r}")
                logger.exception(err)
                await asyncio.sleep(self._reconnect_backoff)

    async def _list_loop(self, type_: str) -> AsyncIterator[Record]:
        while True:
            for record in await self._client.list_records(type_, all_tradespaces=True):
                yield record
            await asyncio.sleep(self._resync_interval)

    async def _schedule_loop(self, interval: float, handler: Callable[[], Awaitable[None]]) -> None:
        while True:
            await handler()
            await asyncio.sleep(interval)

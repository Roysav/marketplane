import asyncio
import functools
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any

from grpc.aio import AioRpcError
from sdk import MarketplaneClient

from controller._multiplexer import Multiplexer
from controller._registry import Handler, HandlerRegistry, Selector
from controller._scheduler import Scheduler
from controller.notifications import (
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
    def __init__(
        self,
        client: MarketplaneClient,
        *,
        worker_limit: int | None,
        idle_timeout: float,
        exit_timeout: float,
        reconnect_backoff: float,
    ) -> None:
        self._client = client
        self._worker_limit = worker_limit
        self._idle_timeout = idle_timeout
        self._exit_timeout = exit_timeout
        self._reconnect_backoff = reconnect_backoff
        self._registry = HandlerRegistry()
        self._record_types: set[str] = set()
        self._tick_names: set[str] = set()

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

    def on_tick(self, name: str) -> Callable[[Handler], Handler]:
        selector = Selector(types=frozenset({NotificationType.TICK_CHANGED}), tick_name=name)

        def decorator(handler: Handler) -> Handler:
            self._registry.register(handler, selector)
            self._tick_names.add(name)
            return handler

        return decorator

    async def run(self) -> None:
        scheduler = Scheduler(limit=self._worker_limit)
        multiplexer = Multiplexer(
            scheduler=scheduler,
            registry=self._registry,
            idle_timeout=self._idle_timeout,
            exit_timeout=self._exit_timeout,
        )
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
            except AioRpcError:
                logger.exception("%s failed; reconnecting", label)
                await asyncio.sleep(self._reconnect_backoff)

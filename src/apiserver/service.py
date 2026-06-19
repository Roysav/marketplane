from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

from .events import Event, EventsClient
from .types import Subject
from .ledger import LedgerClient
from .records import Record, RecordsClient
from .ticks import TicksClient


class Service:
    def __init__(
        self,
        ticks: TicksClient,
        events: EventsClient,
        ledger: LedgerClient,
        records: RecordsClient,
    ) -> None:
        self._ticks = ticks
        self._events = events
        self._ledger = ledger
        self._records = records

    async def publish_tick(self, name: str, value: Any) -> None:
        await self._ticks.publish(name, value)

    async def get_tick(self, name: str) -> Any:
        return await self._ticks.get(name)

    def subscribe_tick(self, name: str) -> AsyncIterator[Any]:
        return self._ticks.subscribe(name)

    async def allocate(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: str) -> None:
        await self._ledger.allocate(from_principal, to_principal, currency, amount, subject)

    async def grant(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: str) -> None:
        await self._ledger.grant(from_principal, to_principal, currency, amount, subject)

    async def balance(self, principal: str, currency: str) -> Decimal:
        return await self._ledger.balance(principal, currency)

    async def create_record(self, record: Record) -> None:
        await self._records.create_record(record)
        await self._events.publish("created", record.subject())

    async def update_record(self, record: Record) -> None:
        await self._records.update_record(record)
        await self._events.publish("updated", record.subject())

    async def apply_record(self, record: Record) -> None:
        await self._records.apply_record(record)
        await self._events.publish("updated", record.subject())

    async def get_record(self, subject: Subject) -> Record:
        return await self._records.get_record(subject)

    async def list_records(self, type_: str, tradespace: str | None = None, labels: dict | None = None, *, all_tradespaces: bool = False) -> list[Record]:
        return await self._records.list_records(type_, tradespace, labels, all_tradespaces=all_tradespaces)

    def watch_records(
        self,
        type_: str,
        tradespace: str | None = None,
        *,
        all_tradespaces: bool = False,
    ) -> AsyncIterator[Event]:
        return self._events.watch(type_, tradespace, all_tradespaces=all_tradespaces)

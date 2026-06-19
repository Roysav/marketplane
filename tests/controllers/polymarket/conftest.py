import asyncio

import pytest

from sdk import Record


class FakeClient:
    def __init__(self):
        self.records: dict[tuple[str, str, str], Record] = {}
        self.ticks: list[tuple[str, object]] = []

    async def apply_record(self, record: Record) -> None:
        self.records[(record.type, record.tradespace, record.name)] = record

    async def publish_tick(self, name, value) -> None:
        self.ticks.append((name, value))

    async def watch_records(self, type_, tradespace=None, labels=None, *, all_tradespaces=False):
        if False:
            yield
        await asyncio.Event().wait()

    async def subscribe_tick(self, name):
        if False:
            yield
        await asyncio.Event().wait()

    async def list_records(self, type_, tradespace=None, labels=None, *, all_tradespaces=False):
        return [r for r in self.records.values() if r.type == type_]

    def of_type(self, type_: str) -> list[Record]:
        return [r for r in self.records.values() if r.type == type_]


@pytest.fixture
def client() -> FakeClient:
    return FakeClient()

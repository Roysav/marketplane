import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct, Value
from google.type import decimal_pb2
from marketplane.apiserver.v1 import apiserver_pb2, apiserver_pb2_grpc


@dataclass(frozen=True)
class Record:
    type: str
    tradespace: str
    name: str
    labels: dict[str, str] = field(default_factory=dict)
    spec: dict[str, Any] = field(default_factory=dict)
    revision: int = 0


@dataclass(frozen=True)
class RecordEvent:
    action: str
    type: str
    tradespace: str
    name: str
    record: Record


def _value_to_proto(x: Any) -> Value:
    return json_format.ParseDict(x, Value())


def _value_from_proto(v: Value) -> Any:
    return json.loads(json_format.MessageToJson(v))


def _decimal_to_proto(d: Decimal) -> decimal_pb2.Decimal:
    return decimal_pb2.Decimal(value=str(d))


def _decimal_from_proto(d: decimal_pb2.Decimal) -> Decimal:
    return Decimal(d.value)


def _record_to_proto(r: Record) -> apiserver_pb2.Record:
    spec = Struct()
    spec.update(r.spec)
    return apiserver_pb2.Record(
        type=r.type,
        tradespace=r.tradespace,
        metadata=apiserver_pb2.RecordMetadata(name=r.name, labels=r.labels, revision=r.revision),
        spec=spec,
    )


def _record_from_proto(r: apiserver_pb2.Record) -> Record:
    return Record(
        type=r.type,
        tradespace=r.tradespace,
        name=r.metadata.name,
        labels=dict(r.metadata.labels),
        spec=json_format.MessageToDict(r.spec),
        revision=r.metadata.revision,
    )


OWNER_LABEL = "marketplane.io/owner"
LEASE_LABEL = "marketplane.io/lease"


class MarketplaneClient:
    def __init__(self, grpc_client: apiserver_pb2_grpc.ApiserverServiceStub) -> None:
        self._client = grpc_client

    async def publish_tick(self, name: str, value: Any) -> None:
        await self._client.PublishTick(apiserver_pb2.PublishTickRequest(name=name, value=_value_to_proto(value)))

    async def get_tick(self, name: str) -> Any:
        resp = await self._client.GetTick(apiserver_pb2.GetTickRequest(name=name))
        return _value_from_proto(resp.value)

    async def subscribe_tick(self, name: str) -> AsyncIterator[Any]:
        async for update in self._client.SubscribeTick(apiserver_pb2.SubscribeTickRequest(name=name)):
            yield _value_from_proto(update.value)

    async def grant(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: str) -> None:
        await self._client.Grant(apiserver_pb2.GrantRequest(
            from_principal=from_principal,
            to_principal=to_principal,
            currency=currency,
            amount=_decimal_to_proto(amount),
            subject=subject,
        ))

    async def allocate(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: str) -> None:
        await self._client.Allocate(apiserver_pb2.AllocateRequest(
            from_principal=from_principal,
            to_principal=to_principal,
            currency=currency,
            amount=_decimal_to_proto(amount),
            subject=subject,
        ))

    async def balance(self, principal: str, currency: str) -> Decimal:
        resp = await self._client.Balance(apiserver_pb2.BalanceRequest(principal=principal, currency=currency))
        return _decimal_from_proto(resp.balance)

    async def create_record(self, record: Record) -> None:
        await self._client.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_record_to_proto(record)))

    async def update_record(self, record: Record) -> None:
        await self._client.UpdateRecord(apiserver_pb2.UpdateRecordRequest(record=_record_to_proto(record)))

    @asynccontextmanager
    async def ownership(self, record: Record, *, owner: str, until: float) -> AsyncIterator[Record]:
        claimed = replace(record, labels={**record.labels, OWNER_LABEL: owner, LEASE_LABEL: repr(until)}, revision=record.revision + 1)
        await self.update_record(claimed)
        yield claimed

    async def apply_record(self, record: Record) -> None:
        await self._client.ApplyRecord(apiserver_pb2.ApplyRecordRequest(record=_record_to_proto(record)))

    async def get_record(self, type_: str, tradespace: str, name: str) -> Record:
        resp = await self._client.GetRecord(apiserver_pb2.GetRecordRequest(type=type_, tradespace=tradespace, name=name))
        return _record_from_proto(resp.record)

    async def list_records(
        self,
        type_: str,
        tradespace: str | None = None,
        labels: dict[str, str] | None = None,
        *,
        all_tradespaces: bool = False,
    ) -> list[Record]:
        resp = await self._client.ListRecords(apiserver_pb2.ListRecordsRequest(
            type=type_,
            tradespace=tradespace or "",
            labels=labels or {},
            all_tradespaces=all_tradespaces,
        ))
        return [_record_from_proto(r) for r in resp.records]

    async def watch_records(
        self,
        type_: str,
        tradespace: str | None = None,
        labels: dict[str, str] | None = None,
        *,
        all_tradespaces: bool = False,
    ) -> AsyncIterator[RecordEvent]:
        async for ev in self._client.WatchRecords(apiserver_pb2.WatchRecordsRequest(
            type=type_,
            tradespace=tradespace or "",
            labels=labels or {},
            all_tradespaces=all_tradespaces,
        )):
            yield RecordEvent(action=ev.action, type=ev.type, tradespace=ev.tradespace, name=ev.name, record=_record_from_proto(ev.record))

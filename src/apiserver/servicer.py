import json
from decimal import Decimal
from typing import Any

import grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct, Value
from google.type import decimal_pb2
from marketplane.apiserver.v1 import apiserver_pb2, apiserver_pb2_grpc

from apiserver.ledger import InsufficientBalanceError
from apiserver.records import Record, RecordMetadata
from apiserver.service import Service
from apiserver.types import Subject


def _subject_from_proto(s: apiserver_pb2.Subject) -> Subject:
    return Subject(type=s.type, tradespace=s.tradespace, name=s.name)


def _subject_to_proto(s: Subject) -> apiserver_pb2.Subject:
    return apiserver_pb2.Subject(type=s.type, tradespace=s.tradespace, name=s.name)


def _record_from_proto(r: apiserver_pb2.Record) -> Record:
    return Record(
        type=r.type,
        tradespace=r.tradespace,
        metadata=RecordMetadata(name=r.metadata.name, labels=dict(r.metadata.labels)),
        spec=json_format.MessageToDict(r.spec),
    )


def _record_to_proto(r: Record) -> apiserver_pb2.Record:
    spec = Struct()
    spec.update(r.spec)
    return apiserver_pb2.Record(
        type=r.type,
        tradespace=r.tradespace,
        metadata=apiserver_pb2.RecordMetadata(name=r.metadata.name, labels=r.metadata.labels),
        spec=spec,
    )


def _decimal_from_proto(d: decimal_pb2.Decimal) -> Decimal:
    return Decimal(d.value)


def _decimal_to_proto(d: Decimal) -> decimal_pb2.Decimal:
    return decimal_pb2.Decimal(value=str(d))


def _value_from_proto(v: Value) -> Any:
    return json.loads(json_format.MessageToJson(v))


def _value_to_proto(x: Any) -> Value:
    return json_format.ParseDict(x, Value())


class ApiserverServicer(apiserver_pb2_grpc.ApiserverServiceServicer):
    def __init__(self, service: Service) -> None:
        self._service = service

    # --- ticks ---

    async def PublishTick(self, request: apiserver_pb2.PublishTickRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.PublishTickResponse:
        try:
            await self._service.publish_tick(request.name, _value_from_proto(request.value))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        return apiserver_pb2.PublishTickResponse()

    async def GetTick(self, request: apiserver_pb2.GetTickRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.GetTickResponse:
        try:
            value = await self._service.get_tick(request.name)
        except KeyError:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"tick {request.name!r} not found")
        return apiserver_pb2.GetTickResponse(value=_value_to_proto(value))

    async def SubscribeTick(self, request: apiserver_pb2.SubscribeTickRequest, context: grpc.aio.ServicerContext):
        async for value in self._service.subscribe_tick(request.name):
            yield apiserver_pb2.TickUpdate(value=_value_to_proto(value))

    # --- ledger ---

    async def Allocate(self, request: apiserver_pb2.AllocateRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.AllocateResponse:
        try:
            await self._service.allocate(
                request.from_principal,
                request.to_principal,
                request.currency,
                _decimal_from_proto(request.amount),
                _subject_from_proto(request.subject),
            )
        except InsufficientBalanceError as e:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        return apiserver_pb2.AllocateResponse()

    async def Grant(self, request: apiserver_pb2.GrantRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.GrantResponse:
        try:
            await self._service.grant(
                request.from_principal,
                request.to_principal,
                request.currency,
                _decimal_from_proto(request.amount),
                _subject_from_proto(request.subject),
            )
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        return apiserver_pb2.GrantResponse()

    async def Balance(self, request: apiserver_pb2.BalanceRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.BalanceResponse:
        balance = await self._service.balance(request.principal, request.currency)
        return apiserver_pb2.BalanceResponse(balance=_decimal_to_proto(balance))

    # --- records ---

    async def CreateRecord(self, request: apiserver_pb2.CreateRecordRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.CreateRecordResponse:
        await self._service.create_record(_record_from_proto(request.record))
        return apiserver_pb2.CreateRecordResponse()

    async def UpdateRecord(self, request: apiserver_pb2.UpdateRecordRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.UpdateRecordResponse:
        await self._service.update_record(_record_from_proto(request.record))
        return apiserver_pb2.UpdateRecordResponse()

    async def GetRecord(self, request: apiserver_pb2.GetRecordRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.GetRecordResponse:
        try:
            record = await self._service.get_record(_subject_from_proto(request.subject))
        except KeyError:
            s = request.subject
            await context.abort(grpc.StatusCode.NOT_FOUND, f"record {s.type}/{s.tradespace}/{s.name} not found")
        return apiserver_pb2.GetRecordResponse(record=_record_to_proto(record))

    async def ListRecords(self, request: apiserver_pb2.ListRecordsRequest, context: grpc.aio.ServicerContext) -> apiserver_pb2.ListRecordsResponse:
        try:
            records = await self._service.list_records(
                request.type,
                tradespace=request.tradespace or None,
                labels=dict(request.labels) or None,
                all_tradespaces=request.all_tradespaces,
            )
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        return apiserver_pb2.ListRecordsResponse(records=[_record_to_proto(r) for r in records])

    async def WatchRecords(self, request: apiserver_pb2.WatchRecordsRequest, context: grpc.aio.ServicerContext):
        try:
            stream = self._service.watch_records(
                request.type,
                tradespace=request.tradespace or None,
                all_tradespaces=request.all_tradespaces,
            )
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            return
        async for event in stream:
            yield apiserver_pb2.RecordEvent(action=event.action, subject=_subject_to_proto(event.subject))

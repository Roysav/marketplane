import grpc
import grpc.aio
import pytest
from marketplane.apiserver.v1 import apiserver_pb2_grpc

from apiserver.events import EventsClient
from apiserver.events.storage.memory import MemoryEventStorage
from apiserver.ledger import LedgerClient
from apiserver.ledger.storage.memory import MemoryLedgerStorage
from apiserver.records import RecordsClient
from apiserver.records.storage.memory import MemoryRecordStorage
from apiserver.service import Service
from apiserver.servicer import ApiserverServicer
from apiserver.ticks import TicksClient
from apiserver.ticks.storage.memory import MemoryTickStorage


@pytest.fixture
async def grpc_addr():
    server = grpc.aio.server()
    svc = Service(
        ticks=TicksClient(MemoryTickStorage()),
        events=EventsClient(MemoryEventStorage()),
        ledger=LedgerClient(MemoryLedgerStorage()),
        records=RecordsClient(MemoryRecordStorage()),
    )
    apiserver_pb2_grpc.add_ApiserverServiceServicer_to_server(ApiserverServicer(svc), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    yield f"127.0.0.1:{port}"
    await server.stop(grace=0)


@pytest.fixture
async def stub(grpc_addr):
    async with grpc.aio.insecure_channel(grpc_addr) as channel:
        yield apiserver_pb2_grpc.ApiserverServiceStub(channel)

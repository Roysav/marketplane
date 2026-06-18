import asyncio
from decimal import Decimal

import grpc
import grpc.aio
import pytest
from google.protobuf.struct_pb2 import Value
from google.type import decimal_pb2
from marketplane.apiserver.v1 import apiserver_pb2


def _dec(v: str) -> decimal_pb2.Decimal:
    return decimal_pb2.Decimal(value=v)


def _rec(type_: str, tradespace: str, name: str, labels: dict | None = None) -> apiserver_pb2.Record:
    return apiserver_pb2.Record(
        type=type_,
        tradespace=tradespace,
        metadata=apiserver_pb2.RecordMetadata(name=name, labels=labels or {}),
    )


# --- ticks ---


async def test_publish_and_get_tick(stub):
    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="price.BTC", value=Value(number_value=50000.0)))
    resp = await stub.GetTick(apiserver_pb2.GetTickRequest(name="price.BTC"))
    assert resp.value.number_value == 50000.0


async def test_tick_overwrite(stub):
    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="price.ETH", value=Value(number_value=3000.0)))
    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="price.ETH", value=Value(number_value=3200.0)))
    resp = await stub.GetTick(apiserver_pb2.GetTickRequest(name="price.ETH"))
    assert resp.value.number_value == 3200.0


async def test_tick_string_value(stub):
    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="status.market", value=Value(string_value="open")))
    resp = await stub.GetTick(apiserver_pb2.GetTickRequest(name="status.market"))
    assert resp.value.string_value == "open"


async def test_tick_bool_value(stub):
    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="flag.halt", value=Value(bool_value=True)))
    resp = await stub.GetTick(apiserver_pb2.GetTickRequest(name="flag.halt"))
    assert resp.value.bool_value is True


async def test_get_tick_not_found(stub):
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.GetTick(apiserver_pb2.GetTickRequest(name="nonexistent.tick"))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


async def test_subscribe_tick_receives_update(stub):
    call = stub.SubscribeTick(apiserver_pb2.SubscribeTickRequest(name="price.SOL"))
    received: list[float] = []

    async def _collect():
        try:
            async for update in call:
                received.append(update.value.number_value)
        except (grpc.aio.AioRpcError, asyncio.CancelledError):
            pass

    task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)

    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="price.SOL", value=Value(number_value=150.0)))
    await asyncio.sleep(0.05)

    call.cancel()
    await asyncio.wait_for(task, timeout=1.0)
    assert received == [150.0]


async def test_subscribe_tick_receives_multiple_updates(stub):
    call = stub.SubscribeTick(apiserver_pb2.SubscribeTickRequest(name="price.DOGE"))
    received: list[float] = []

    async def _collect():
        try:
            async for update in call:
                received.append(update.value.number_value)
        except (grpc.aio.AioRpcError, asyncio.CancelledError):
            pass

    task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)

    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="price.DOGE", value=Value(number_value=0.1)))
    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="price.DOGE", value=Value(number_value=0.2)))
    await asyncio.sleep(0.05)

    call.cancel()
    await asyncio.wait_for(task, timeout=1.0)
    assert received == [0.1, 0.2]


async def test_subscribe_tick_only_receives_matching_name(stub):
    call_btc = stub.SubscribeTick(apiserver_pb2.SubscribeTickRequest(name="price.BTC2"))
    received: list[float] = []

    async def _collect():
        try:
            async for update in call_btc:
                received.append(update.value.number_value)
        except (grpc.aio.AioRpcError, asyncio.CancelledError):
            pass

    task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)

    # publish to a different name — should NOT appear in btc stream
    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="price.ETH2", value=Value(number_value=9999.0)))
    await stub.PublishTick(apiserver_pb2.PublishTickRequest(name="price.BTC2", value=Value(number_value=60000.0)))
    await asyncio.sleep(0.05)

    call_btc.cancel()
    await asyncio.wait_for(task, timeout=1.0)
    assert received == [60000.0]


# --- ledger ---


async def test_grant_increases_balance(stub):
    await stub.Grant(apiserver_pb2.GrantRequest(
        from_principal="bank",
        to_principal="alice",
        currency="USD",
        amount=_dec("500.00"),
        subject="account/main/grant-1",
    ))
    resp = await stub.Balance(apiserver_pb2.BalanceRequest(principal="alice", currency="USD"))
    assert Decimal(resp.balance.value) == Decimal("500.00")


async def test_balance_unknown_principal_is_zero(stub):
    resp = await stub.Balance(apiserver_pb2.BalanceRequest(principal="ghost", currency="USD"))
    assert Decimal(resp.balance.value) == Decimal("0")


async def test_allocate_moves_funds(stub):
    await stub.Grant(apiserver_pb2.GrantRequest(
        from_principal="bank",
        to_principal="trader",
        currency="USD",
        amount=_dec("100.00"),
        subject="account/main/fund-trader",
    ))
    await stub.Allocate(apiserver_pb2.AllocateRequest(
        from_principal="trader",
        to_principal="exchange",
        currency="USD",
        amount=_dec("40.00"),
        subject="trade/nyse/T001",
    ))
    trader = await stub.Balance(apiserver_pb2.BalanceRequest(principal="trader", currency="USD"))
    exchange = await stub.Balance(apiserver_pb2.BalanceRequest(principal="exchange", currency="USD"))
    assert Decimal(trader.balance.value) == Decimal("60.00")
    assert Decimal(exchange.balance.value) == Decimal("40.00")


async def test_allocate_insufficient_balance(stub):
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.Allocate(apiserver_pb2.AllocateRequest(
            from_principal="broke",
            to_principal="rich",
            currency="USD",
            amount=_dec("1.00"),
            subject="trade/nyse/doomed",
        ))
    assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION


async def test_multiple_currencies_are_independent(stub):
    await stub.Grant(apiserver_pb2.GrantRequest(
        from_principal="mint",
        to_principal="multi",
        currency="USD",
        amount=_dec("200.00"),
        subject="account/main/fund-usd",
    ))
    await stub.Grant(apiserver_pb2.GrantRequest(
        from_principal="mint",
        to_principal="multi",
        currency="EUR",
        amount=_dec("100.00"),
        subject="account/main/fund-eur",
    ))
    usd = await stub.Balance(apiserver_pb2.BalanceRequest(principal="multi", currency="USD"))
    eur = await stub.Balance(apiserver_pb2.BalanceRequest(principal="multi", currency="EUR"))
    assert Decimal(usd.balance.value) == Decimal("200.00")
    assert Decimal(eur.balance.value) == Decimal("100.00")


# --- records ---


async def test_create_and_get_record(stub):
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("asset", "nyse", "AAPL", {"sector": "tech"})))
    resp = await stub.GetRecord(apiserver_pb2.GetRecordRequest(type="asset", tradespace="nyse", name="AAPL"))
    assert resp.record.type == "asset"
    assert resp.record.tradespace == "nyse"
    assert resp.record.metadata.name == "AAPL"
    assert resp.record.metadata.labels["sector"] == "tech"


async def test_update_record(stub):
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("asset", "nyse", "MSFT", {"tier": "large"})))
    await stub.UpdateRecord(apiserver_pb2.UpdateRecordRequest(record=_rec("asset", "nyse", "MSFT", {"tier": "mega"})))
    resp = await stub.GetRecord(apiserver_pb2.GetRecordRequest(type="asset", tradespace="nyse", name="MSFT"))
    assert resp.record.metadata.labels["tier"] == "mega"


async def test_get_record_not_found(stub):
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.GetRecord(apiserver_pb2.GetRecordRequest(type="asset", tradespace="nyse", name="DOES_NOT_EXIST"))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


async def test_list_records_by_tradespace(stub):
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("stock", "nasdaq", "GOOG")))
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("stock", "nasdaq", "META")))
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("stock", "nyse", "JPM")))
    resp = await stub.ListRecords(apiserver_pb2.ListRecordsRequest(type="stock", tradespace="nasdaq"))
    names = {r.metadata.name for r in resp.records}
    assert {"GOOG", "META"} <= names
    assert "JPM" not in names


async def test_list_records_all_tradespaces(stub):
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("etf", "nyse", "SPY")))
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("etf", "nasdaq", "QQQ")))
    resp = await stub.ListRecords(apiserver_pb2.ListRecordsRequest(type="etf", all_tradespaces=True))
    names = {r.metadata.name for r in resp.records}
    assert {"SPY", "QQQ"} <= names


async def test_list_records_by_label(stub):
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("crypto", "spot", "BTC", {"category": "layer1"})))
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("crypto", "spot", "ETH", {"category": "layer1"})))
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("crypto", "spot", "DOGE", {"category": "meme"})))
    resp = await stub.ListRecords(apiserver_pb2.ListRecordsRequest(
        type="crypto",
        tradespace="spot",
        labels={"category": "layer1"},
    ))
    names = {r.metadata.name for r in resp.records}
    assert names == {"BTC", "ETH"}


async def test_watch_records_create_event(stub):
    events: list[tuple[str, str, apiserver_pb2.Record]] = []
    call = stub.WatchRecords(apiserver_pb2.WatchRecordsRequest(type="bond", tradespace="us-treasury"))

    async def _collect():
        try:
            async for ev in call:
                events.append((ev.action, ev.name, ev.record))
        except (grpc.aio.AioRpcError, asyncio.CancelledError):
            pass

    task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)

    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("bond", "us-treasury", "T-NOTE-10Y")))
    await asyncio.sleep(0.05)

    call.cancel()
    await asyncio.wait_for(task, timeout=1.0)
    assert any(action == "created" and name == "T-NOTE-10Y" for action, name, _ in events)
    record = next(r for _, name, r in events if name == "T-NOTE-10Y")
    assert record.metadata.name == "T-NOTE-10Y"


async def test_watch_records_update_event(stub):
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("bond", "us-treasury", "T-BILL-90D")))

    events: list[str] = []
    call = stub.WatchRecords(apiserver_pb2.WatchRecordsRequest(type="bond", tradespace="us-treasury"))

    async def _collect():
        try:
            async for ev in call:
                events.append(ev.action)
        except (grpc.aio.AioRpcError, asyncio.CancelledError):
            pass

    task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)

    await stub.UpdateRecord(apiserver_pb2.UpdateRecordRequest(record=_rec("bond", "us-treasury", "T-BILL-90D", {"rating": "AAA"})))
    await asyncio.sleep(0.05)

    call.cancel()
    await asyncio.wait_for(task, timeout=1.0)
    assert "updated" in events


async def test_watch_records_all_tradespaces(stub):
    events: list[tuple[str, str]] = []
    call = stub.WatchRecords(apiserver_pb2.WatchRecordsRequest(type="index", all_tradespaces=True))

    async def _collect():
        try:
            async for ev in call:
                events.append((ev.tradespace, ev.name))
        except (grpc.aio.AioRpcError, asyncio.CancelledError):
            pass

    task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)

    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("index", "sp500", "SPX")))
    await stub.CreateRecord(apiserver_pb2.CreateRecordRequest(record=_rec("index", "nasdaq", "NDX")))
    await asyncio.sleep(0.05)

    call.cancel()
    await asyncio.wait_for(task, timeout=1.0)
    assert ("sp500", "SPX") in events
    assert ("nasdaq", "NDX") in events

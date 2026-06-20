import asyncio
import time
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

from controller import Controller, NotificationType, RecordNotification
from controllers.polymarket.orders import SIGNATURE_LABEL, OrderReconciler
from controllers.polymarket.types import (
    ORDER_TYPE,
    OrderPhase,
    OrderSide,
    OrderSpec,
    OrderType,
    PolymarketOrderRecord,
)
from sdk import LEASE_LABEL, OWNER_LABEL, MarketplaneClient, Record


class FakeClob:
    def __init__(self) -> None:
        self.posts = 0
        self.cancels: list[str] = []

    def create_order(self, args):
        return SimpleNamespace(signature="0xsig")

    def post_order(self, order, order_type):
        self.posts += 1
        return {"orderID": f"0xorder{self.posts}"}

    def cancel_orders(self, order_ids):
        self.cancels.extend(order_ids)
        return {"canceled": order_ids}


def _order_record(name: str, *, active: bool = True, labels: dict | None = None) -> Record:
    spec = OrderSpec(type=OrderType.gtc, token="TOK", side=OrderSide.buy, size=Decimal(5), price=Decimal("0.1"), active=active)
    return Record(
        type=ORDER_TYPE,
        tradespace="polymarket",
        name=name,
        labels=labels or {},
        spec=PolymarketOrderRecord(spec=spec).model_dump(by_alias=True, mode="json"),
    )


def _reconciler(client: MarketplaneClient, clob: FakeClob) -> OrderReconciler:
    controller = Controller(client, reconnect_backoff=1.0, resync_interval=60.0, idle_timeout=60.0)
    return OrderReconciler(controller, client, clob, tradespace="polymarket", lease=120.0)


def _note(record: Record) -> RecordNotification:
    return RecordNotification(NotificationType.RECORD_EXISTING, record)


async def _order(client: MarketplaneClient, name: str) -> PolymarketOrderRecord:
    record = await client.get_record(ORDER_TYPE, "polymarket", name)
    return PolymarketOrderRecord.model_validate(record.spec)


async def test_reconcile_places_and_stamps_ownership(stub):
    client = MarketplaneClient(stub)
    clob = FakeClob()
    reconciler = _reconciler(client, clob)
    await client.create_record(_order_record("o1"))

    await reconciler._reconcile(_note(await client.get_record(ORDER_TYPE, "polymarket", "o1")))

    record = await client.get_record(ORDER_TYPE, "polymarket", "o1")
    order = PolymarketOrderRecord.model_validate(record.spec)
    assert order.status.phase is OrderPhase.placed
    assert order.status.polymarket_order_id == "0xorder1"
    assert order.spec.signature == "0xsig"
    assert record.labels[SIGNATURE_LABEL] == "0xsig"
    assert record.labels[OWNER_LABEL] == reconciler._owner
    assert clob.posts == 1


async def test_reconcile_skips_when_held_by_another(stub):
    client = MarketplaneClient(stub)
    clob = FakeClob()
    reconciler = _reconciler(client, clob)
    await client.create_record(_order_record("o2", labels={OWNER_LABEL: "another", LEASE_LABEL: repr(time.time() + 60)}))

    await reconciler._reconcile(_note(await client.get_record(ORDER_TYPE, "polymarket", "o2")))

    assert (await _order(client, "o2")).status.phase is OrderPhase.pending
    assert clob.posts == 0


async def test_reconcile_reclaims_when_lease_expired(stub):
    client = MarketplaneClient(stub)
    clob = FakeClob()
    reconciler = _reconciler(client, clob)
    await client.create_record(_order_record("o3", labels={OWNER_LABEL: "dead", LEASE_LABEL: repr(time.time() - 1)}))

    await reconciler._reconcile(_note(await client.get_record(ORDER_TYPE, "polymarket", "o3")))

    assert (await _order(client, "o3")).status.phase is OrderPhase.placed
    assert clob.posts == 1


async def test_concurrent_reconcilers_one_wins(stub):
    client = MarketplaneClient(stub)
    clob_a, clob_b = FakeClob(), FakeClob()
    a, b = _reconciler(client, clob_a), _reconciler(client, clob_b)
    await client.create_record(_order_record("o4"))
    note = _note(await client.get_record(ORDER_TYPE, "polymarket", "o4"))

    results = await asyncio.gather(a._reconcile(note), b._reconcile(note), return_exceptions=True)

    assert sum(isinstance(r, Exception) for r in results) == 1
    assert clob_a.posts + clob_b.posts == 1
    assert (await _order(client, "o4")).status.phase is OrderPhase.placed


async def test_reconcile_cancels_when_deactivated(stub):
    client = MarketplaneClient(stub)
    clob = FakeClob()
    reconciler = _reconciler(client, clob)
    await client.create_record(_order_record("o5"))
    await reconciler._reconcile(_note(await client.get_record(ORDER_TYPE, "polymarket", "o5")))
    placed = await client.get_record(ORDER_TYPE, "polymarket", "o5")
    order = PolymarketOrderRecord.model_validate(placed.spec)
    order_id = order.status.polymarket_order_id

    deactivated = order.spec.model_copy(update={"active": False})
    await client.update_record(replace(placed, revision=placed.revision + 1, spec=PolymarketOrderRecord(spec=deactivated, status=order.status).model_dump(by_alias=True, mode="json")))
    await reconciler._reconcile(_note(await client.get_record(ORDER_TYPE, "polymarket", "o5")))

    final = await _order(client, "o5")
    assert final.status.phase is OrderPhase.cancelled
    assert clob.cancels == [order_id]


async def test_reconcile_inactive_pending_is_noop(stub):
    client = MarketplaneClient(stub)
    clob = FakeClob()
    reconciler = _reconciler(client, clob)
    await client.create_record(_order_record("o6", active=False))

    await reconciler._reconcile(_note(await client.get_record(ORDER_TYPE, "polymarket", "o6")))

    order = await _order(client, "o6")
    assert order.status.phase is OrderPhase.pending
    assert clob.posts == 0
    assert clob.cancels == []

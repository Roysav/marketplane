from decimal import Decimal

from controller import Controller
from controllers.polymarket.orders import ORDER_ID_LABEL, StatusReconciler
from controllers.polymarket.types import (
    ORDER_TYPE,
    OrderPhase,
    OrderSide,
    OrderSpec,
    OrderStatus,
    OrderType,
    PolymarketOrderRecord,
)
from sdk import MarketplaneClient, Record


class FakeClob:
    def __init__(self, orders: dict | None = None) -> None:
        self._orders = orders or {}

    def get_order(self, order_id: str):
        return self._orders[order_id]


def _placed_record(name: str, order_id: str) -> Record:
    spec = OrderSpec(type=OrderType.gtc, token="TOK", side=OrderSide.buy, size=Decimal(10), price=Decimal("0.1"), active=True)
    status = OrderStatus(phase=OrderPhase.placed, polymarket_order_id=order_id)
    return Record(
        type=ORDER_TYPE,
        tradespace="polymarket",
        name=name,
        labels={ORDER_ID_LABEL: order_id},
        spec=PolymarketOrderRecord(spec=spec, status=status).model_dump(by_alias=True, mode="json"),
    )


def _reconciler(client: MarketplaneClient, clob: FakeClob) -> StatusReconciler:
    controller = Controller(client, reconnect_backoff=1.0, resync_interval=60.0)
    return StatusReconciler(controller, client, clob, None, tradespace="polymarket", interval=60.0)


async def _order(client: MarketplaneClient, name: str) -> PolymarketOrderRecord:
    record = await client.get_record(ORDER_TYPE, "polymarket", name)
    return PolymarketOrderRecord.model_validate(record.spec)


async def test_track_writes_filled_and_status(stub):
    client = MarketplaneClient(stub)
    reconciler = _reconciler(client, FakeClob())
    await client.create_record(_placed_record("o1", "0xORDER"))

    await reconciler._track("0xORDER", Decimal(3), "MATCHED")

    order = await _order(client, "o1")
    assert order.status.filled == Decimal(3)
    assert order.status.api_status == "MATCHED"


async def test_track_is_idempotent_when_unchanged(stub):
    client = MarketplaneClient(stub)
    reconciler = _reconciler(client, FakeClob())
    await client.create_record(_placed_record("o2", "0xB"))
    await reconciler._track("0xB", Decimal(3), "MATCHED")
    before = await client.get_record(ORDER_TYPE, "polymarket", "o2")

    await reconciler._track("0xB", Decimal(3), "MATCHED")

    after = await client.get_record(ORDER_TYPE, "polymarket", "o2")
    assert before.revision == after.revision


async def test_resync_polls_get_order(stub):
    client = MarketplaneClient(stub)
    clob = FakeClob({"0xC": {"size_matched": "7", "status": "MATCHED"}})
    reconciler = _reconciler(client, clob)
    await client.create_record(_placed_record("o3", "0xC"))

    await reconciler._resync()

    order = await _order(client, "o3")
    assert order.status.filled == Decimal(7)
    assert order.status.api_status == "MATCHED"


# verbatim from Polymarket's documented /ws/user "order" event payload
_ORDER_EVENT = {
    "asset_id": "52114319501245915516055106046884209969926127482827954674443846427813813222426",
    "associate_trades": None,
    "event_type": "order",
    "id": "0xff354cd7ca7539dfa9c28d90943ab5779a4eac34b9b37a757d7b32bdfb11790b",
    "market": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
    "order_owner": "9180014b-33c8-9240-a14b-bdca11c0a465",
    "original_size": "10",
    "outcome": "YES",
    "owner": "9180014b-33c8-9240-a14b-bdca11c0a465",
    "price": "0.57",
    "side": "SELL",
    "size_matched": "0",
    "timestamp": "1672290687",
    "type": "PLACEMENT",
}


class FakeChannel:
    def __init__(self, events: list) -> None:
        self._events = events

    async def events(self):
        for event in self._events:
            yield event


async def test_run_parses_documented_user_order_event(stub):
    client = MarketplaneClient(stub)
    controller = Controller(client, reconnect_backoff=1.0, resync_interval=60.0)
    reconciler = StatusReconciler(controller, client, FakeClob(), FakeChannel([_ORDER_EVENT]), tradespace="polymarket", interval=60.0)
    await client.create_record(_placed_record("ows", _ORDER_EVENT["id"]))

    await reconciler.run()

    order = await _order(client, "ows")
    assert order.status.filled == Decimal(0)
    assert order.status.api_status == "PLACEMENT"

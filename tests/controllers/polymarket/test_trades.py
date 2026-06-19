import json

from controllers.polymarket.trades import TradePublisher


class FakeClient:
    def __init__(self):
        self.ticks = []

    async def publish_tick(self, name, value):
        self.ticks.append((name, value))


async def test_publishes_last_trade_price_as_tick():
    client = FakeClient()
    event = {"event_type": "last_trade_price", "asset_id": "tokA", "price": "0.456", "side": "BUY", "size": "10"}
    await TradePublisher(client).on_message(json.dumps(event))
    assert client.ticks == [("alphav1/polymarket/AssetLastTrade/tokA", event)]


async def test_ignores_non_trade_messages():
    client = FakeClient()
    publisher = TradePublisher(client)
    await publisher.on_message(json.dumps({"event_type": "price_change", "asset_id": "tokA"}))
    await publisher.on_message(json.dumps([]))
    assert client.ticks == []


async def test_handles_array_payload():
    client = FakeClient()
    event = {"event_type": "last_trade_price", "asset_id": "tokB", "price": "0.5"}
    await TradePublisher(client).on_message(json.dumps([event]))
    assert client.ticks == [("alphav1/polymarket/AssetLastTrade/tokB", event)]

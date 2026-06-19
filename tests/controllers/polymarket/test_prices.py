import json

from controllers.polymarket.prices import PricePublisher


class FakeClient:
    def __init__(self):
        self.ticks = []

    async def publish_tick(self, name, value):
        self.ticks.append((name, value))


async def test_publishes_each_price_change_entry_as_tick():
    client = FakeClient()
    event = {
        "event_type": "price_change",
        "market": "0xabc",
        "price_changes": [
            {"asset_id": "tokA", "price": "0.46", "best_bid": "0.47", "best_ask": "0.54"},
            {"asset_id": "tokB", "price": "0.54", "best_bid": "0.46", "best_ask": "0.53"},
        ],
    }
    await PricePublisher(client).on_message(json.dumps(event))
    assert client.ticks == [
        ("alphav1/polymarket/AssetPrice/tokA", {"asset_id": "tokA", "price": "0.46", "best_bid": "0.47", "best_ask": "0.54"}),
        ("alphav1/polymarket/AssetPrice/tokB", {"asset_id": "tokB", "price": "0.54", "best_bid": "0.46", "best_ask": "0.53"}),
    ]


async def test_ignores_non_price_change_messages():
    client = FakeClient()
    publisher = PricePublisher(client)
    await publisher.on_message(json.dumps({"event_type": "last_trade_price", "asset_id": "tokA", "price": "0.5"}))
    await publisher.on_message(json.dumps([]))
    assert client.ticks == []


async def test_handles_array_payload():
    client = FakeClient()
    event = {"event_type": "price_change", "price_changes": [{"asset_id": "tokC", "price": "0.9"}]}
    await PricePublisher(client).on_message(json.dumps([event]))
    assert client.ticks == [("alphav1/polymarket/AssetPrice/tokC", {"asset_id": "tokC", "price": "0.9"})]

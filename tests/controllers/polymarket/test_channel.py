import json

from controllers.polymarket.channel import MarketChannel


class FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, message):
        self.sent.append(json.loads(message))


async def _noop(message: str) -> None:
    pass


async def test_update_caps_to_max_assets():
    channel = MarketChannel("ws://x", _noop, max_assets=2)
    await channel.update({"c", "a", "b"})
    assert channel._assets == {"a", "b"}


async def test_apply_sends_initial_subscribe_then_deltas():
    channel = MarketChannel("ws://x", _noop, max_assets=100)
    ws = FakeWS()
    channel._ws = ws

    await channel.update({"a", "b"})
    assert ws.sent == [{"assets_ids": ["a", "b"], "type": "market", "custom_feature_enabled": True}]

    await channel.update({"a", "b", "c"})
    assert ws.sent[-1] == {"operation": "subscribe", "assets_ids": ["c"]}

    await channel.update({"a"})
    assert ws.sent[-1] == {"operation": "unsubscribe", "assets_ids": ["b", "c"]}

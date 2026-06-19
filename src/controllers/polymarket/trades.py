import json
from typing import Any

from sdk import MarketplaneClient

LAST_TRADE_TICK = "alphav1/polymarket/AssetLastTrade"


class TradePublisher:
    def __init__(self, client: MarketplaneClient):
        self._client = client

    async def on_message(self, message: str) -> None:
        payload = json.loads(message)
        events: list[Any] = payload if isinstance(payload, list) else [payload]
        for event in events:
            if event.get("event_type") == "last_trade_price":
                await self._client.publish_tick(f"{LAST_TRADE_TICK}/{event['asset_id']}", event)

import json
from typing import Any

from sdk import MarketplaneClient

ASSET_PRICE_TICK = "alphav1/polymarket/AssetPrice"


class PricePublisher:
    def __init__(self, client: MarketplaneClient):
        self._client = client

    async def on_message(self, message: str) -> None:
        payload = json.loads(message)
        events: list[Any] = payload if isinstance(payload, list) else [payload]
        for event in events:
            if event.get("event_type") == "price_change":
                for change in event["price_changes"]:
                    await self._client.publish_tick(f"{ASSET_PRICE_TICK}/{change['asset_id']}", change)

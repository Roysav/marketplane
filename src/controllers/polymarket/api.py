import asyncio
import json
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class Market:
    market_id: str
    question: str
    slug: str
    condition_id: str
    outcomes: list[str]
    clob_token_ids: list[str]
    active: bool


@dataclass(frozen=True)
class Event:
    event_id: str
    slug: str
    title: str
    market_ids: list[str]


def _market(raw: dict[str, Any]) -> Market:
    return Market(
        market_id=raw["id"],
        question=raw["question"],
        slug=raw["slug"],
        condition_id=raw["conditionId"],
        outcomes=json.loads(raw["outcomes"]),
        clob_token_ids=json.loads(raw["clobTokenIds"]),
        active=raw["active"],
    )


def _event(raw: dict[str, Any]) -> Event:
    return Event(
        event_id=raw["id"],
        slug=raw["slug"],
        title=raw["title"],
        market_ids=[market["id"] for market in raw["markets"] if "clobTokenIds" in market],
    )


class PolymarketAPI:
    def __init__(self, client: httpx.AsyncClient, *, page_size: int, max_concurrency: int):
        self._client = client
        self._page_size = page_size
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def _get(self, url: str, **kwargs: Any) -> httpx.Response:
        async with self._semaphore:
            return await self._client.get(url, **kwargs)

    async def list_events(self) -> list[Event]:
        events: list[Event] = []
        offset = 0
        while True:
            response = await self._get(
                "/events",
                params={"limit": self._page_size, "offset": offset, "closed": "false"},
            )
            if offset > 0 and response.status_code == 422:
                return events
            response.raise_for_status()
            page = response.json()
            events.extend(_event(raw) for raw in page)
            if len(page) < self._page_size:
                return events
            offset += self._page_size

    async def get_market(self, market_id: str) -> Market:
        response = await self._get(f"/markets/{market_id}")
        response.raise_for_status()
        return _market(response.json())

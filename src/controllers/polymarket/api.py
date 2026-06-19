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
    markets: list[Market]


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
        markets=[_market(m) for m in raw["markets"] if "clobTokenIds" in m],
    )


class PolymarketAPI:
    def __init__(self, client: httpx.AsyncClient, *, page_size: int):
        self._client = client
        self._page_size = page_size

    async def list_events(self) -> list[Event]:
        events: list[Event] = []
        offset = 0
        while True:
            response = await self._client.get(
                "/events",
                params={"limit": self._page_size, "offset": offset, "closed": "false"},
            )
            response.raise_for_status()
            page = response.json()
            events.extend(_event(raw) for raw in page)
            if len(page) < self._page_size:
                return events
            offset += self._page_size

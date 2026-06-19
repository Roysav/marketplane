import httpx

from controllers.polymarket.api import PolymarketAPI, _event, _market


async def test_list_events_paginates_and_stops_at_offset_cap():
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        if offset >= 200:
            return httpx.Response(422)
        return httpx.Response(200, json=[{"id": f"e{offset + i}", "slug": "s", "title": "t", "markets": []} for i in range(100)])

    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        events = await PolymarketAPI(client, page_size=100, max_concurrency=4).list_events()

    assert len(events) == 200
    assert events[0].event_id == "e0"
    assert events[-1].event_id == "e199"


def test_event_carries_only_tradeable_market_ids():
    raw = {
        "id": "e1",
        "slug": "es",
        "title": "et",
        "markets": [
            {"id": "m1", "clobTokenIds": '["tokA", "tokB"]'},
            {"id": "m2"},
        ],
    }
    event = _event(raw)
    assert event.event_id == "e1"
    assert event.title == "et"
    assert event.market_ids == ["m1"]


def test_market_parses_json_string_fields():
    raw = {
        "id": "m1",
        "question": "q",
        "slug": "s",
        "conditionId": "0xabc",
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": '["tokA", "tokB"]',
        "active": True,
    }
    market = _market(raw)
    assert market.market_id == "m1"
    assert market.condition_id == "0xabc"
    assert market.outcomes == ["Yes", "No"]
    assert market.clob_token_ids == ["tokA", "tokB"]
    assert market.active is True

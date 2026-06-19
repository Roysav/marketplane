from controllers.polymarket.api import _event, _market


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

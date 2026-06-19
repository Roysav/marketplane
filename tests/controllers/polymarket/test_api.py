from controllers.polymarket.api import _event


def test_parses_event_and_market_json_string_fields():
    raw = {
        "id": "e1",
        "slug": "es",
        "title": "et",
        "markets": [
            {
                "id": "m1",
                "question": "q",
                "slug": "s",
                "conditionId": "0xabc",
                "outcomes": '["Yes", "No"]',
                "clobTokenIds": '["tokA", "tokB"]',
                "active": True,
            }
        ],
    }
    event = _event(raw)
    assert event.event_id == "e1"
    assert event.title == "et"
    market = event.markets[0]
    assert market.market_id == "m1"
    assert market.condition_id == "0xabc"
    assert market.outcomes == ["Yes", "No"]
    assert market.clob_token_ids == ["tokA", "tokB"]
    assert market.active is True

from sdk import Record

from controller._registry import HandlerRegistry, Selector
from controller.notifications import NotificationType, RecordNotification, TickNotification

_ALL_RECORD = frozenset({
    NotificationType.RECORD_CREATED,
    NotificationType.RECORD_UPDATED,
    NotificationType.RECORD_DELETED,
})


def _rec(type_="asset", tradespace="nyse", name="AAPL", labels=None):
    return Record(type=type_, tradespace=tradespace, name=name, labels=labels or {})


async def _noop(_):
    pass


def test_matches_by_type():
    reg = HandlerRegistry()
    reg.register(_noop, Selector(types=frozenset({NotificationType.RECORD_CREATED})))
    created = RecordNotification(NotificationType.RECORD_CREATED, _rec())
    updated = RecordNotification(NotificationType.RECORD_UPDATED, _rec())
    assert list(reg.iter_handlers(created)) == [_noop]
    assert list(reg.iter_handlers(updated)) == []


def test_matches_record_type_and_tradespace():
    reg = HandlerRegistry()
    reg.register(_noop, Selector(types=_ALL_RECORD, record_type="asset", tradespace="nyse"))
    assert list(reg.iter_handlers(RecordNotification(NotificationType.RECORD_CREATED, _rec(tradespace="nyse"))))
    assert not list(reg.iter_handlers(RecordNotification(NotificationType.RECORD_CREATED, _rec(tradespace="nasdaq"))))
    assert not list(reg.iter_handlers(RecordNotification(NotificationType.RECORD_CREATED, _rec(type_="bond"))))


def test_matches_labels_subset():
    reg = HandlerRegistry()
    reg.register(_noop, Selector(types=_ALL_RECORD, labels=(("sector", "tech"),)))
    match = RecordNotification(NotificationType.RECORD_CREATED, _rec(labels={"sector": "tech", "tier": "mega"}))
    miss = RecordNotification(NotificationType.RECORD_CREATED, _rec(labels={"sector": "energy"}))
    none = RecordNotification(NotificationType.RECORD_CREATED, _rec(labels={}))
    assert list(reg.iter_handlers(match)) == [_noop]
    assert list(reg.iter_handlers(miss)) == []
    assert list(reg.iter_handlers(none)) == []


def test_matches_tick_name():
    reg = HandlerRegistry()
    reg.register(_noop, Selector(types=frozenset({NotificationType.TICK_CHANGED}), tick_name="price.BTC"))
    assert list(reg.iter_handlers(TickNotification(NotificationType.TICK_CHANGED, "price.BTC", 1)))
    assert not list(reg.iter_handlers(TickNotification(NotificationType.TICK_CHANGED, "price.ETH", 1)))

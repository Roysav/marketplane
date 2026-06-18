import asyncio

from sdk import Record

from controller._multiplexer import Multiplexer
from controller._registry import Handler, HandlerRegistry, Selector
from controller._scheduler import Scheduler
from controller.notifications import NotificationType, RecordNotification

_ALL_RECORD = frozenset({
    NotificationType.RECORD_CREATED,
    NotificationType.RECORD_UPDATED,
    NotificationType.RECORD_DELETED,
})


def _rec(name="AAPL", revision=0):
    return Record(type="asset", tradespace="nyse", name=name, revision=revision)


def _note(name="AAPL", revision=0):
    return RecordNotification(NotificationType.RECORD_UPDATED, _rec(name=name, revision=revision))


def _build(handler: Handler):
    registry = HandlerRegistry()
    registry.register(handler, Selector(types=_ALL_RECORD, record_type="asset"))
    scheduler = Scheduler(exception_handler=_reraise)
    mux = Multiplexer(scheduler=scheduler, registry=registry)
    return mux, scheduler


def _reraise(exc: BaseException) -> None:
    raise exc


async def test_same_key_serialized():
    log: list[tuple[str, int]] = []

    async def handler(n: RecordNotification) -> None:
        log.append(("start", n.record.revision))
        await asyncio.sleep(0.02)
        log.append(("end", n.record.revision))

    mux, _ = _build(handler)
    for revision in range(3):
        await mux.feed(_note(revision=revision))
    await mux.drain()
    assert log == [("start", 0), ("end", 0), ("start", 1), ("end", 1), ("start", 2), ("end", 2)]


async def test_different_keys_concurrent():
    active = 0
    peak = 0

    async def handler(_: RecordNotification) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.03)
        active -= 1

    mux, _ = _build(handler)
    await mux.feed(_note(name="AAPL"))
    await mux.feed(_note(name="MSFT"))
    await mux.drain()
    assert peak == 2


async def test_drain_processes_backlog():
    seen: list[str] = []

    async def handler(n: RecordNotification) -> None:
        seen.append(n.record.name)

    mux, _ = _build(handler)
    for name in ("A", "B", "C"):
        await mux.feed(_note(name=name))
    await mux.drain()
    assert sorted(seen) == ["A", "B", "C"]

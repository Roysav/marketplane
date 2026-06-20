import asyncio

from sdk import Record, RecordEvent

from controller import Controller, NotificationType


class FakeClient:
    def __init__(self, events=(), ticks=(), records=()):
        self._events = list(events)
        self._ticks = list(ticks)
        self._records = list(records)

    async def watch_records(self, type_, tradespace=None, labels=None, *, all_tradespaces=False):
        for event in self._events:
            if event.type == type_:
                yield event
        await asyncio.Event().wait()

    async def subscribe_tick(self, name):
        for tick_name, value in self._ticks:
            if tick_name == name:
                yield value
        await asyncio.Event().wait()

    async def list_records(self, type_, tradespace=None, labels=None, *, all_tradespaces=False):
        return [r for r in self._records if r.type == type_]


def _event(action, name, revision=0, labels=None):
    record = Record(type="asset", tradespace="nyse", name=name, labels=labels or {}, revision=revision)
    return RecordEvent(action=action, type="asset", tradespace="nyse", name=name, record=record)


def _record(name, labels=None, revision=0):
    return Record(type="asset", tradespace="nyse", name=name, labels=labels or {}, revision=revision)


def _controller(client) -> Controller:
    return Controller(client, reconnect_backoff=0.01, resync_interval=0.05, idle_timeout=60.0)


async def _run_until(ctrl, done, timeout=2.0):
    task = asyncio.create_task(ctrl.run())
    try:
        await asyncio.wait_for(done.wait(), timeout=timeout)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_dispatches_records_in_per_record_order():
    client = FakeClient(events=[
        _event("created", "AAPL", revision=0),
        _event("updated", "AAPL", revision=1),
        _event("created", "MSFT", revision=0),
    ])
    ctrl = _controller(client)
    seen: list[tuple[NotificationType, str]] = []
    done = asyncio.Event()

    @ctrl.on_record_event("asset")
    async def handler(n):
        seen.append((n.type, n.record.name))
        if len(seen) == 3:
            done.set()

    await _run_until(ctrl, done)

    aapl = [t for t, name in seen if name == "AAPL"]
    assert aapl == [NotificationType.RECORD_CREATED, NotificationType.RECORD_UPDATED]
    assert (NotificationType.RECORD_CREATED, "MSFT") in seen


async def test_filters_by_labels():
    client = FakeClient(events=[
        _event("created", "AAPL", labels={"sector": "tech"}),
        _event("created", "XOM", labels={"sector": "energy"}),
    ])
    ctrl = _controller(client)
    seen: list[str] = []
    done = asyncio.Event()

    @ctrl.on_record_event("asset", labels={"sector": "tech"})
    async def handler(n):
        seen.append(n.record.name)
        done.set()

    await _run_until(ctrl, done)
    assert seen == ["AAPL"]


async def test_filters_by_action():
    client = FakeClient(events=[
        _event("created", "AAPL"),
        _event("updated", "AAPL", revision=1),
    ])
    ctrl = _controller(client)
    seen: list[NotificationType] = []
    done = asyncio.Event()

    @ctrl.on_record_event("asset", actions=frozenset({NotificationType.RECORD_UPDATED}))
    async def handler(n):
        seen.append(n.type)
        done.set()

    await _run_until(ctrl, done)
    assert seen == [NotificationType.RECORD_UPDATED]


async def test_dispatches_ticks():
    client = FakeClient(ticks=[("price.BTC", 100.0), ("price.BTC", 101.0)])
    ctrl = _controller(client)
    seen: list[float] = []
    done = asyncio.Event()

    @ctrl.on_tick("price.BTC")
    async def handler(n):
        seen.append(n.value)
        if len(seen) == 2:
            done.set()

    await _run_until(ctrl, done)
    assert seen == [100.0, 101.0]


async def test_on_existing_backfills_from_list():
    client = FakeClient(records=[_record("AAPL"), _record("MSFT")])
    ctrl = _controller(client)
    seen: list[tuple[NotificationType, str]] = []
    done = asyncio.Event()

    @ctrl.on_existing("asset")
    async def handler(n):
        seen.append((n.type, n.record.name))
        if len({name for _, name in seen}) == 2:
            done.set()

    await _run_until(ctrl, done)
    assert (NotificationType.RECORD_EXISTING, "AAPL") in seen
    assert (NotificationType.RECORD_EXISTING, "MSFT") in seen


async def test_on_existing_also_handles_live_events():
    client = FakeClient(events=[_event("created", "AAPL")])
    ctrl = _controller(client)
    seen: list[NotificationType] = []
    done = asyncio.Event()

    @ctrl.on_existing("asset")
    async def handler(n):
        if n.type is NotificationType.RECORD_CREATED:
            seen.append(n.type)
            done.set()

    await _run_until(ctrl, done)
    assert NotificationType.RECORD_CREATED in seen


async def test_on_existing_resyncs_periodically():
    client = FakeClient(records=[_record("AAPL")])
    ctrl = _controller(client)
    count = 0
    done = asyncio.Event()

    @ctrl.on_existing("asset")
    async def handler(n):
        nonlocal count
        count += 1
        if count >= 2:
            done.set()

    await _run_until(ctrl, done)
    assert count >= 2


async def test_on_schedule_fires_periodically():
    ctrl = _controller(FakeClient())
    count = 0
    done = asyncio.Event()

    @ctrl.on_schedule(0.05)
    async def tick():
        nonlocal count
        count += 1
        if count >= 3:
            done.set()

    await _run_until(ctrl, done)
    assert count >= 3

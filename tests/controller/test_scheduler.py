import asyncio

from controller._scheduler import Scheduler


def _reraise(exc: BaseException) -> None:
    raise exc


async def test_runs_spawned_coro():
    sched = Scheduler(exception_handler=_reraise)
    ran = asyncio.Event()

    async def job() -> None:
        ran.set()

    sched.spawn(job())
    await asyncio.wait_for(ran.wait(), timeout=1.0)
    await sched.wait()


async def test_wait_returns_when_all_done():
    sched = Scheduler(exception_handler=_reraise)
    count = 0

    async def job() -> None:
        nonlocal count
        await asyncio.sleep(0.01)
        count += 1

    for _ in range(5):
        sched.spawn(job())
    await sched.wait()
    assert count == 5


async def test_close_cancels_running():
    sched = Scheduler(exception_handler=_reraise)
    completed = False

    async def job() -> None:
        nonlocal completed
        await asyncio.sleep(10)
        completed = True

    sched.spawn(job())
    await asyncio.sleep(0.01)
    await sched.close()
    assert completed is False


async def test_exception_handler_called():
    errors: list[BaseException] = []
    sched = Scheduler(exception_handler=errors.append)

    async def job() -> None:
        raise ValueError("boom")

    sched.spawn(job())
    await sched.wait()
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)

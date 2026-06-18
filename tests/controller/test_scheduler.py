import asyncio

from controller._scheduler import Scheduler


async def test_runs_spawned_coro():
    sched = Scheduler()
    ran = asyncio.Event()

    async def job() -> None:
        ran.set()

    await sched.spawn(job())
    await asyncio.wait_for(ran.wait(), timeout=1.0)
    await sched.close()


async def test_respects_limit():
    sched = Scheduler(limit=2)
    active = 0
    peak = 0

    async def job() -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1

    for _ in range(6):
        await sched.spawn(job())
    await sched.wait()
    await sched.close()
    assert peak == 2


async def test_close_cancels_running():
    sched = Scheduler()
    completed = False

    async def job() -> None:
        nonlocal completed
        await asyncio.sleep(10)
        completed = True

    await sched.spawn(job())
    await asyncio.sleep(0.01)
    await sched.close()
    assert completed is False


async def test_exception_handler_called():
    errors: list[BaseException] = []
    sched = Scheduler(exception_handler=errors.append)

    async def job() -> None:
        raise ValueError("boom")

    await sched.spawn(job())
    await sched.wait()
    await sched.close()
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)

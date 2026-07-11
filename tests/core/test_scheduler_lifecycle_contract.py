from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from hfa_control.scheduler import Scheduler


class _LoopHarness:
    def __init__(self, *, fail_on_gain: Exception | None = None) -> None:
        self.fail_on_gain = fail_on_gain
        self.gained_epochs: list[str] = []
        self.lost_calls = 0
        self.cycle_entries = 0
        self.cycle_entered = asyncio.Event()
        self.release_cycle = asyncio.Event()

    async def on_leadership_gained(self, scheduler_epoch: str) -> None:
        if self.fail_on_gain is not None:
            raise self.fail_on_gain
        self.gained_epochs.append(scheduler_epoch)

    async def on_leadership_lost(self) -> None:
        self.lost_calls += 1

    async def run_cycle(self, max_dispatches: int | None = None) -> int:
        self.cycle_entries += 1
        self.cycle_entered.set()
        await self.release_cycle.wait()
        return 0


def _require_method(instance: Any, name: str):
    method = getattr(instance, name, None)
    assert callable(method), f"Scheduler.{name}() lifecycle method is missing"
    return method


def _running(instance: Any) -> bool:
    for name in ("running", "is_running"):
        value = getattr(instance, name, None)
        if value is not None:
            return bool(value() if callable(value) else value)
    pytest.fail("Scheduler must expose running/is_running state")


def _current_epoch(instance: Any) -> str:
    value = getattr(instance, "current_epoch", None)
    assert value is not None, "Scheduler.current_epoch diagnostic is missing"
    return str(value() if callable(value) else value)


def _cycle_task(instance: Any):
    for name in ("cycle_task", "background_task"):
        value = getattr(instance, name, None)
        if value is not None:
            return value() if callable(value) else value
    return getattr(instance, "_cycle_task", None)


@pytest.mark.asyncio
async def test_start_requires_acquired_non_empty_epoch() -> None:
    scheduler = Scheduler(_LoopHarness())
    start = _require_method(scheduler, "start")

    for invalid in (None, "", "   ", "0", 0):
        with pytest.raises((TypeError, ValueError, RuntimeError)):
            await start(scheduler_epoch=invalid)


@pytest.mark.asyncio
async def test_sequential_and_concurrent_start_create_one_cycle_task() -> None:
    loop = _LoopHarness()
    scheduler = Scheduler(loop)
    start = _require_method(scheduler, "start")
    stop = _require_method(scheduler, "stop")

    await asyncio.gather(
        start(scheduler_epoch="17"),
        start(scheduler_epoch="17"),
        start(scheduler_epoch="17"),
    )
    await asyncio.wait_for(loop.cycle_entered.wait(), timeout=1.0)

    assert loop.gained_epochs == ["17"]
    assert loop.cycle_entries == 1
    assert _running(scheduler) is True
    task = _cycle_task(scheduler)
    assert isinstance(task, asyncio.Task) and not task.done()

    await start(scheduler_epoch="17")
    assert loop.gained_epochs == ["17"]
    assert loop.cycle_entries == 1

    await stop()
    assert _running(scheduler) is False
    assert _current_epoch(scheduler) == ""
    assert loop.lost_calls == 1
    assert task.done()


@pytest.mark.asyncio
async def test_stop_is_restartable_and_close_is_terminal() -> None:
    loop = _LoopHarness()
    scheduler = Scheduler(loop)
    start = _require_method(scheduler, "start")
    stop = _require_method(scheduler, "stop")
    close = _require_method(scheduler, "close")

    await start(scheduler_epoch="21")
    await asyncio.wait_for(loop.cycle_entered.wait(), timeout=1.0)
    await stop()

    loop.cycle_entered = asyncio.Event()
    loop.release_cycle = asyncio.Event()
    await start(scheduler_epoch="22")
    await asyncio.wait_for(loop.cycle_entered.wait(), timeout=1.0)

    assert loop.gained_epochs == ["21", "22"]
    assert _running(scheduler) is True

    await close()
    await close()
    assert _running(scheduler) is False
    assert loop.lost_calls == 2

    with pytest.raises((RuntimeError, ValueError)):
        await start(scheduler_epoch="23")


@pytest.mark.asyncio
async def test_initialisation_failure_is_visible_and_leaves_no_task() -> None:
    loop = _LoopHarness(fail_on_gain=RuntimeError("initialise failed"))
    scheduler = Scheduler(loop)
    start = _require_method(scheduler, "start")

    with pytest.raises(RuntimeError, match="initialise failed"):
        await start(scheduler_epoch="31")

    assert _running(scheduler) is False
    task = _cycle_task(scheduler)
    assert task is None or task.done()
    assert loop.cycle_entries == 0
    assert loop.gained_epochs == []

@pytest.mark.asyncio
async def test_start_stop_close_share_one_lifecycle_lock_without_task_leak() -> None:
    loop = _LoopHarness()
    scheduler = Scheduler(loop)

    start_task = asyncio.create_task(scheduler.start(scheduler_epoch="51"))
    await start_task
    await asyncio.wait_for(loop.cycle_entered.wait(), timeout=1.0)

    stop_task = asyncio.create_task(scheduler.stop())
    close_task = asyncio.create_task(scheduler.close())
    await asyncio.gather(stop_task, close_task)

    assert scheduler.running is False
    assert scheduler.current_epoch == ""
    task = scheduler.cycle_task
    assert task is None or task.done()
    with pytest.raises(RuntimeError):
        await scheduler.start(scheduler_epoch="52")

@pytest.mark.asyncio
async def test_unexpected_cycle_failure_is_visible_and_clears_running_state() -> None:
    class _FailingLoop(_LoopHarness):
        def __init__(self) -> None:
            super().__init__()
            self._config = SimpleNamespace(
                scheduler_loop_max_failures=1,
                scheduler_loop_idle_sleep_ms=0,
                scheduler_loop_error_sleep_ms=0,
            )

        async def run_cycle(self, max_dispatches: int | None = None) -> int:
            self.cycle_entries += 1
            self.cycle_entered.set()
            raise RuntimeError("cycle exploded")

    loop = _FailingLoop()
    scheduler = Scheduler(loop)
    await scheduler.start(scheduler_epoch="61")
    await asyncio.wait_for(loop.cycle_entered.wait(), timeout=1.0)
    task = scheduler.cycle_task
    assert task is not None
    with pytest.raises(RuntimeError, match="cycle exploded"):
        await task

    assert scheduler.running is False
    assert scheduler.current_epoch == ""
    assert isinstance(scheduler.last_failure, RuntimeError)

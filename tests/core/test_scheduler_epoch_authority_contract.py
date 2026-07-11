from __future__ import annotations

from typing import Any

import pytest

from hfa_control.models import ControlPlaneConfig
from hfa_control.scheduler_loop import SchedulerLoop
from hfa_control.service import ControlPlaneService


class _RedisEpochSpy:
    def __init__(self) -> None:
        self.incr_calls: list[str] = []

    async def incr(self, key: str) -> int:
        self.incr_calls.append(key)
        return 999


class _PermitController:
    def __init__(self, redis: Any) -> None:
        self.redis = redis
        self.initialise_calls = 0

    async def initialise(self) -> None:
        self.initialise_calls += 1


class _Fairness:
    def __init__(self) -> None:
        self.reset_calls = 0

    def reset(self) -> None:
        self.reset_calls += 1


class _Leader:
    def __init__(self, *, is_leader: bool, fencing_token: int) -> None:
        self.is_leader = is_leader
        self.fencing_token = fencing_token
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class _SchedulerRecorder:
    def __init__(self, *, fail_on_start: Exception | None = None) -> None:
        self.start_epochs: list[str] = []
        self.stop_calls = 0
        self.close_calls = 0
        self.fail_on_start = fail_on_start

    async def start(self, *, scheduler_epoch: str) -> None:
        if self.fail_on_start is not None:
            raise self.fail_on_start
        self.start_epochs.append(scheduler_epoch)

    async def stop(self) -> None:
        self.stop_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


class _LifecycleRecorder:
    def __init__(self, *, fail_on_start: Exception | None = None) -> None:
        self.start_calls = 0
        self.close_calls = 0
        self.fail_on_start = fail_on_start

    async def start(self) -> None:
        self.start_calls += 1
        if self.fail_on_start is not None:
            raise self.fail_on_start

    async def close(self) -> None:
        self.close_calls += 1


class _Noop:
    async def close(self) -> None:
        return None


def _leadership_reconciler(service: ControlPlaneService):
    for name in (
        "_reconcile_leadership_once",
        "_sync_leader_components",
        "_apply_leadership_state",
    ):
        method = getattr(service, name, None)
        if callable(method):
            return method
    pytest.fail(
        "ControlPlaneService needs one deterministic single-step leadership reconciliation method"
    )


def _bare_service(*, leader: _Leader, scheduler: _SchedulerRecorder) -> ControlPlaneService:
    service = object.__new__(ControlPlaneService)
    service._leader = leader
    service._scheduler = scheduler
    service._recovery = _LifecycleRecorder()
    service._sched_started = False
    service._recovery_started = False
    service._leader_task = None
    service._shards = _Noop()
    service._registry = _Noop()
    service._redis_monitor = _Noop()
    service._config = ControlPlaneConfig(instance_id="cp-sprint-78")
    return service


@pytest.mark.asyncio
async def test_scheduler_loop_accepts_explicit_leader_epoch_without_own_increment() -> None:
    redis = _RedisEpochSpy()
    permit = _PermitController(redis)
    loop = SchedulerLoop(
        redis=redis,
        dispatch_controller=permit,
        snapshot_builder=object(),
        worker_scorer=object(),
        tenant_fairness=_Fairness(),
        tenant_queue=None,
        shards=None,
        lua=None,
        config=ControlPlaneConfig(instance_id="cp-sprint-78"),
        event_store=None,
    )

    try:
        await loop.on_leadership_gained("41")
    except TypeError as exc:
        pytest.fail(f"SchedulerLoop does not accept explicit leader fencing token: {exc}")

    assert loop.current_epoch == "41"
    assert redis.incr_calls == []
    assert permit.initialise_calls == 1

    await loop.on_leadership_lost()
    assert loop.current_epoch == ""


@pytest.mark.asyncio
async def test_control_plane_propagates_fencing_token_and_stops_on_loss() -> None:
    leader = _Leader(is_leader=True, fencing_token=17)
    scheduler = _SchedulerRecorder()
    service = _bare_service(leader=leader, scheduler=scheduler)
    reconcile = _leadership_reconciler(service)

    await reconcile()
    assert scheduler.start_epochs == ["17"]
    assert service._sched_started is True

    leader.is_leader = False
    await reconcile()
    assert scheduler.stop_calls == 1
    assert scheduler.close_calls == 0
    assert service._sched_started is False

    leader.is_leader = True
    leader.fencing_token = 18
    await reconcile()
    assert scheduler.start_epochs == ["17", "18"]


@pytest.mark.asyncio
async def test_zero_fencing_token_fails_closed() -> None:
    leader = _Leader(is_leader=True, fencing_token=0)
    scheduler = _SchedulerRecorder()
    service = _bare_service(leader=leader, scheduler=scheduler)
    reconcile = _leadership_reconciler(service)

    with pytest.raises((RuntimeError, ValueError)):
        await reconcile()
    assert scheduler.start_epochs == []
    assert service._sched_started is False


@pytest.mark.asyncio
async def test_process_shutdown_uses_terminal_scheduler_close() -> None:
    leader = _Leader(is_leader=True, fencing_token=9)
    scheduler = _SchedulerRecorder()
    service = _bare_service(leader=leader, scheduler=scheduler)
    service._sched_started = True

    await service.close()
    assert scheduler.close_calls == 1

@pytest.mark.asyncio
async def test_recovery_does_not_start_when_scheduler_start_fails() -> None:
    leader = _Leader(is_leader=True, fencing_token=27)
    scheduler = _SchedulerRecorder(fail_on_start=RuntimeError("scheduler init failed"))
    service = _bare_service(leader=leader, scheduler=scheduler)
    recovery = _LifecycleRecorder()
    service._recovery = recovery

    with pytest.raises(RuntimeError, match="scheduler init failed"):
        await service._reconcile_leadership_once()

    assert recovery.start_calls == 0
    assert service._sched_started is False
    assert service._recovery_started is False


@pytest.mark.asyncio
async def test_recovery_start_failure_rolls_back_new_scheduler_start() -> None:
    leader = _Leader(is_leader=True, fencing_token=28)
    scheduler = _SchedulerRecorder()
    service = _bare_service(leader=leader, scheduler=scheduler)
    recovery = _LifecycleRecorder(fail_on_start=RuntimeError("recovery init failed"))
    service._recovery = recovery

    with pytest.raises(RuntimeError, match="recovery init failed"):
        await service._reconcile_leadership_once()

    assert scheduler.start_epochs == ["28"]
    assert scheduler.stop_calls == 1
    assert service._sched_started is False
    assert service._recovery_started is False

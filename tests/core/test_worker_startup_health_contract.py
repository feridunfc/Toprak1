from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from redis.exceptions import ResponseError

from hfa_worker.heartbeat import WorkerHeartbeatPublisher
from hfa_worker.main import WorkerService
from hfa_worker.redis_utils import ensure_consumer_group
from hfa_worker.task_executor import TaskExecutionResult


class RedisProbe:
    async def set(self, *args, **kwargs):
        return True

    async def get(self, *args, **kwargs):
        return None

    async def expire(self, *args, **kwargs):
        return 1

    async def hset(self, *args, **kwargs):
        return 1

    async def eval(self, *args, **kwargs):
        return 1


class LegacyExecutorProbe:
    async def execute(self, event):
        return SimpleNamespace(
            status="done",
            payload={},
            cost_cents=0,
            tokens_used=0,
            error="",
        )


class CanonicalExecutorProbe:
    async def execute(self, ctx):
        return TaskExecutionResult(ok=True, output={})


class LifecycleComponent:
    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.events = events
        self.start_calls = 0
        self.close_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        self.events.append(f"{self.name}.start")

    async def close(self) -> None:
        self.close_calls += 1
        self.events.append(f"{self.name}.close")


class DrainProbe:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def start_drain(self, *, reason: str, timeout: float) -> None:
        self.events.append("drain.start")

    def reset(self) -> None:
        self.events.append("drain.reset")


class CrashingConsumer:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._renewer_task = None
        self.close_calls = 0

    @property
    def inflight_count(self) -> int:
        return 0

    @property
    def is_draining(self) -> bool:
        return False

    def stop_pulling(self) -> None:
        return None

    async def start(self) -> None:
        async def crash() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("consumer background crash")

        self._task = asyncio.create_task(crash())

    async def close(self) -> None:
        self.close_calls += 1
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None


def build_service() -> WorkerService:
    return WorkerService(
        RedisProbe(),
        {
            "production": True,
            "worker_id": "worker-health-79",
            "worker_group": "group-health-79",
            "shards": [1],
            "executor": LegacyExecutorProbe(),
            "task_executor": CanonicalExecutorProbe(),
        },
    )


@pytest.mark.asyncio
async def test_production_start_initialises_lua_before_background_components() -> None:
    service = build_service()
    events: list[str] = []

    async def initialise() -> None:
        events.append("dag_lua.initialise")

    service._dag_lua.initialise = initialise
    service._heartbeat = LifecycleComponent("heartbeat", events)
    service._consumer = LifecycleComponent("consumer", events)
    service._drain_manager = DrainProbe(events)

    await service.start()

    assert events == [
        "dag_lua.initialise",
        "heartbeat.start",
        "consumer.start",
    ]

    await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_lua_initialisation_failure_blocks_worker_startup() -> None:
    service = build_service()
    events: list[str] = []

    async def initialise() -> None:
        raise RuntimeError("lua load failed")

    heartbeat = LifecycleComponent("heartbeat", events)
    consumer = LifecycleComponent("consumer", events)

    service._dag_lua.initialise = initialise
    service._heartbeat = heartbeat
    service._consumer = consumer
    service._drain_manager = DrainProbe(events)

    with pytest.raises(RuntimeError, match="lua load failed"):
        await service.start()

    assert heartbeat.start_calls == 0
    assert consumer.start_calls == 0
    assert service.is_ready is False
    assert service.last_failure is not None


@pytest.mark.asyncio
async def test_unexpected_consumer_group_error_is_not_swallowed() -> None:
    class BrokenRedis:
        async def xgroup_create(self, **kwargs):
            raise RuntimeError("redis unavailable")

    with pytest.raises(RuntimeError, match="redis unavailable"):
        await ensure_consumer_group(
            BrokenRedis(),
            "stream-79",
            "group-79",
        )


@pytest.mark.asyncio
async def test_existing_consumer_group_is_tolerated() -> None:
    class ExistingGroupRedis:
        async def xgroup_create(self, **kwargs):
            raise ResponseError(
                "BUSYGROUP Consumer Group name already exists"
            )

    await ensure_consumer_group(
        ExistingGroupRedis(),
        "stream-79",
        "group-79",
    )


@pytest.mark.asyncio
async def test_unexpected_consumer_task_death_clears_readiness_and_surfaces_failure() -> None:
    service = build_service()
    events: list[str] = []
    consumer = CrashingConsumer()

    service._heartbeat = LifecycleComponent("heartbeat", events)
    service._consumer = consumer
    service._drain_manager = DrainProbe(events)

    async def initialise() -> None:
        return None

    service._dag_lua.initialise = initialise

    await service.start()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert service.is_ready is False
    assert isinstance(service.last_failure, RuntimeError)
    assert "consumer background crash" in str(service.last_failure)

    await service.close(drain_timeout=0.0)

@pytest.mark.asyncio
async def test_heartbeat_start_publishes_visibility_before_returning() -> None:
    class VisibilityRedis:
        def __init__(self) -> None:
            self.xadd_calls: list[tuple[tuple, dict]] = []

        async def xadd(self, *args, **kwargs):
            self.xadd_calls.append((args, kwargs))
            return "1-0"

    redis = VisibilityRedis()
    publisher = WorkerHeartbeatPublisher(
        redis=redis,
        worker_id="worker-visible-79",
        worker_group="group-visible-79",
        region="eu-west-1",
        shards=[1],
        capacity=1,
        inflight_fn=lambda: 0,
        is_draining_fn=lambda: False,
        version="79",
        capabilities=["base"],
    )

    try:
        await publisher.start()
        assert len(redis.xadd_calls) == 1
    finally:
        await publisher.close()

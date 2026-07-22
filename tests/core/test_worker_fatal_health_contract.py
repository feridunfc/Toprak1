from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

import hfa_worker.heartbeat as heartbeat_module
import hfa_worker.process_root as process_root
from hfa_worker.heartbeat import WorkerHeartbeatPublisher
from hfa_worker.main import WorkerService
from hfa_worker.task_executor import TaskExecutionResult


class RedisLifecycleProbe:
    async def set(self, *args, **kwargs):
        return True

    async def get(self, *args, **kwargs):
        return None

    async def expire(self, *args, **kwargs):
        return 1

    async def hset(self, *args, **kwargs):
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


class DagLuaProbe:
    async def initialise(self) -> None:
        return None


class ConsumerProbe:
    def __init__(self) -> None:
        self._task = None
        self._renewer_task = None
        self.stop_pulling_calls = 0
        self.close_calls = 0
        self.started = False
        self._draining = False

    @property
    def inflight_count(self) -> int:
        return 0

    @property
    def is_draining(self) -> bool:
        return self._draining

    async def prepare_consumer_groups(self) -> None:
        return None

    async def start(self) -> None:
        self.started = True
        self._draining = False

    def stop_pulling(self) -> None:
        self.stop_pulling_calls += 1
        self._draining = True

    async def close(self) -> None:
        self.close_calls += 1
        self.started = False


class HeartbeatProbe:
    def __init__(self) -> None:
        self.start_calls = 0
        self.close_calls = 0
        self.running = False

    async def start(self) -> None:
        self.start_calls += 1
        self.running = True

    async def close(self) -> None:
        self.close_calls += 1
        self.running = False


class DrainProbe:
    def __init__(self, consumer: ConsumerProbe) -> None:
        self.consumer = consumer

    async def start_drain(
        self,
        *,
        reason: str,
        timeout: float,
    ) -> None:
        self.consumer.stop_pulling()

    def reset(self) -> None:
        return None


class LeaseLossManager:
    def __init__(self) -> None:
        self.renew_calls = 0

    async def claim_shard(self, shard: int, worker_group: str) -> bool:
        return True

    async def renew_shard(self, shard: int, worker_group: str) -> bool:
        self.renew_calls += 1
        return False


async def _wait_until(
    predicate,
    *,
    timeout: float = 0.5,
    interval: float = 0.01,
) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return bool(predicate())


def _build_failing_service():
    redis = RedisLifecycleProbe()
    manager = LeaseLossManager()
    service = WorkerService(
        redis,
        {
            "production": True,
            "worker_id": "worker-fatal-79",
            "worker_group": "group-79",
            "shards": [1],
            "shard_manager": manager,
            "shard_renew_interval": 0.01,
            "executor": LegacyExecutorProbe(),
            "task_executor": CanonicalExecutorProbe(),
        },
    )

    consumer = ConsumerProbe()
    heartbeat = HeartbeatProbe()
    service._dag_lua = DagLuaProbe()
    service._consumer = consumer
    service._heartbeat = heartbeat
    service._drain_manager = DrainProbe(consumer)
    return service, consumer, heartbeat, manager


@pytest.mark.asyncio
async def test_shard_lease_loss_stops_new_consumption() -> None:
    service, consumer, heartbeat, manager = _build_failing_service()
    await service.start()

    try:
        failed = await _wait_until(
            lambda: service.last_failure is not None,
        )
        assert failed is True
        stopped = await _wait_until(
            lambda: consumer.stop_pulling_calls > 0,
        )
        assert stopped is True
        assert consumer.is_draining is True
        assert manager.renew_calls > 0
    finally:
        await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_fatal_worker_failure_stops_heartbeat() -> None:
    service, consumer, heartbeat, _manager = _build_failing_service()
    await service.start()

    try:
        wait_for_failure = getattr(service, "wait_for_failure", None)
        assert callable(wait_for_failure), (
            "WorkerService.wait_for_failure() is required for process-root "
            "fatal health propagation"
        )

        failure = await asyncio.wait_for(
            wait_for_failure(),
            timeout=0.5,
        )
        assert isinstance(failure, BaseException)

        stopped = await _wait_until(
            lambda: heartbeat.close_calls > 0,
        )
        assert stopped is True
        assert heartbeat.running is False
    finally:
        await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_process_root_propagates_fatal_worker_failure(
    monkeypatch,
) -> None:
    events: list[str] = []

    class RedisProbe:
        async def aclose(self) -> None:
            events.append("redis.close")

    redis = RedisProbe()

    class ServiceProbe:
        def __init__(self, redis_arg: Any, config: dict[str, Any]) -> None:
            assert redis_arg is redis
            events.append("service.init")

        async def start(self) -> None:
            events.append("service.start")

        async def wait_for_failure(self):
            await asyncio.sleep(0)
            raise RuntimeError("fatal worker failure")

        async def close(self) -> None:
            events.append("service.close")

    async def wait_forever() -> None:
        events.append("shutdown.wait")
        await asyncio.Event().wait()

    monkeypatch.setattr(process_root, "WorkerService", ServiceProbe)

    with pytest.raises(RuntimeError, match="fatal worker failure"):
        await asyncio.wait_for(
            process_root.run_worker_process(
                redis_factory=lambda _url: redis,
                config={
                    "redis_url": "redis://fatal-root:6379/0",
                    "production": True,
                    "worker_id": "worker-root-fatal-79",
                    "worker_group": "group-79",
                    "shards": [1],
                },
                wait_for_shutdown=wait_forever,
            ),
            timeout=0.5,
        )

    assert "service.close" in events
    assert events[-1] == "redis.close"


@pytest.mark.asyncio
async def test_post_start_heartbeat_failure_becomes_visible(
    monkeypatch,
) -> None:
    class FailingHeartbeatRedis:
        def __init__(self) -> None:
            self.calls = 0

        async def xadd(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return "1-0"
            raise RuntimeError("heartbeat stream unavailable")

    monkeypatch.setattr(
        heartbeat_module,
        "HEARTBEAT_INTERVAL",
        0.01,
    )

    redis = FailingHeartbeatRedis()
    publisher = WorkerHeartbeatPublisher(
        redis=redis,
        worker_id="worker-heartbeat-fatal-79",
        worker_group="group-79",
        region="eu-west-1",
        shards=[1],
        capacity=1,
        inflight_fn=lambda: 0,
        is_draining_fn=lambda: False,
        version="79",
        capabilities=["base"],
    )

    await publisher.start()

    try:
        attempted = await _wait_until(lambda: redis.calls >= 2)
        assert attempted is True

        task = publisher._task
        assert isinstance(task, asyncio.Task)
        visible = await _wait_until(task.done)
        assert visible is True, (
            "Post-start heartbeat publish errors must escape the background "
            "loop so WorkerService can observe the fatal failure"
        )

        with pytest.raises(RuntimeError, match="heartbeat stream unavailable"):
            task.result()
    finally:
        task = publisher._task
        if isinstance(task, asyncio.Task) and not task.done():
            await publisher.close()
        else:
            publisher._task = None

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from hfa.dag.schema import DagRedisKey
from hfa.events.schema import RunRequestedEvent
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.main import WorkerService


class RedisLifecycleProbe:
    def __init__(self) -> None:
        self.task_meta: dict[str, dict[str, str]] = {}
        self.values: dict[str, str] = {}

    async def xgroup_create(self, *args, **kwargs):
        return True

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, *, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def expire(self, key, ttl):
        return 1 if key in self.values else 0

    async def hset(self, *args, **kwargs):
        return 1

    async def eval(self, script, numkeys, *args):
        if numkeys == 1:
            key, worker_group, _ttl = args
        elif numkeys == 2:
            key, _owners_key, worker_group, _ttl, _shard = args
        else:
            raise AssertionError(
                f"Unexpected shard Lua key count: {numkeys}"
            )

        return 1 if self.values.get(key) == worker_group else 0

    async def hgetall(self, key):
        return self.task_meta.get(key, {})

    async def xack(self, *args, **kwargs):
        return 1

    async def xadd(self, *args, **kwargs):
        return "1-0"


class LegacyExecutorProbe:
    async def execute(self, event):
        return SimpleNamespace(
            status="done",
            payload={},
            cost_cents=0,
            tokens_used=0,
            error="",
        )


class CanonicalTaskExecutorProbe:
    async def execute(self, ctx):
        return SimpleNamespace(ok=True, output={}, error="")


class ComponentProbe:
    def __init__(
        self,
        name: str,
        events: list[str],
        *,
        fail_start: bool = False,
    ) -> None:
        self.name = name
        self.events = events
        self.fail_start = fail_start
        self.start_calls = 0
        self.close_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        self.events.append(f"{self.name}.start")
        if self.fail_start:
            raise RuntimeError(f"{self.name} exploded")

    async def close(self) -> None:
        self.close_calls += 1
        self.events.append(f"{self.name}.close")


class DrainProbe:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls = 0

    async def start_drain(self, reason: str = "", timeout: float = 0.0) -> None:
        self.calls += 1
        self.events.append("drain.start")


def _service() -> WorkerService:
    return WorkerService(
        RedisLifecycleProbe(),
        {
            "production": True,
            "worker_id": "worker-lifecycle-79",
            "worker_group": "group-79",
            "shards": [1],
            "executor": LegacyExecutorProbe(),
            "task_executor": CanonicalTaskExecutorProbe(),
        },
    )


def _install_component_probes(
    service: WorkerService,
    *,
    consumer_fail_start: bool = False,
) -> tuple[list[str], ComponentProbe, ComponentProbe, DrainProbe]:
    events: list[str] = []
    consumer = ComponentProbe(
        "consumer",
        events,
        fail_start=consumer_fail_start,
    )
    heartbeat = ComponentProbe("heartbeat", events)
    drain = DrainProbe(events)

    service._consumer = consumer
    service._heartbeat = heartbeat
    service._drain_manager = drain

    dag_lua = getattr(service, "_dag_lua", None)
    if dag_lua is not None:
        async def initialise():
            events.append("dag_lua.initialise")
        dag_lua.initialise = initialise

    return events, consumer, heartbeat, drain


@pytest.mark.asyncio
async def test_worker_start_makes_heartbeat_operational_before_consumption() -> None:
    service = _service()
    events, _, _, _ = _install_component_probes(service)

    await service.start()

    assert events.index("heartbeat.start") < events.index("consumer.start")

    await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_worker_service_start_is_idempotent() -> None:
    service = _service()
    _, consumer, heartbeat, _ = _install_component_probes(service)

    await service.start()
    await service.start()

    assert consumer.start_calls == 1
    assert heartbeat.start_calls == 1

    await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_worker_service_concurrent_start_creates_one_lifecycle() -> None:
    service = _service()
    _, consumer, heartbeat, _ = _install_component_probes(service)

    await asyncio.gather(service.start(), service.start())

    assert consumer.start_calls == 1
    assert heartbeat.start_calls == 1

    await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_worker_service_stop_is_restartable() -> None:
    service = _service()
    _, consumer, heartbeat, _ = _install_component_probes(service)

    stop = getattr(service, "stop", None)
    assert callable(stop), "WorkerService.stop() is required"

    await service.start()
    await stop()
    await service.start()

    assert consumer.start_calls == 2
    assert heartbeat.start_calls == 2

    await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_worker_service_close_is_terminal() -> None:
    service = _service()
    _install_component_probes(service)

    close = getattr(service, "close", None)
    assert callable(close), "WorkerService.close() is required"

    await service.start()
    await close()

    with pytest.raises(RuntimeError, match="closed"):
        await service.start()


@pytest.mark.asyncio
async def test_worker_startup_failure_rolls_back_started_background_components() -> None:
    service = _service()
    events, _, heartbeat, _ = _install_component_probes(
        service,
        consumer_fail_start=True,
    )

    with pytest.raises(RuntimeError, match="consumer exploded"):
        await service.start()

    assert heartbeat.start_calls == 1
    assert heartbeat.close_calls == 1
    assert events == [
        "dag_lua.initialise",
        "heartbeat.start",
        "consumer.start",
        "heartbeat.close",
    ]


@pytest.mark.asyncio
async def test_worker_readiness_tracks_start_stop_lifecycle() -> None:
    service = _service()
    _install_component_probes(service)

    assert getattr(service, "is_ready", False) is False

    await service.start()
    assert getattr(service, "is_ready", False) is True

    stop = getattr(service, "stop", None)
    assert callable(stop)
    await stop()
    assert getattr(service, "is_ready", False) is False


@pytest.mark.asyncio
async def test_worker_startup_failure_is_visible() -> None:
    service = _service()
    _install_component_probes(service, consumer_fail_start=True)

    with pytest.raises(RuntimeError):
        await service.start()

    failure = getattr(service, "last_failure", None)
    assert failure is not None
    assert "consumer exploded" in str(failure)
    assert getattr(service, "is_ready", False) is False


@pytest.mark.asyncio
async def test_worker_consumer_duplicate_start_does_not_replace_live_tasks() -> None:
    redis = RedisLifecycleProbe()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-consumer-79",
        worker_group="group-79",
        shards=[1],
        executor=LegacyExecutorProbe(),
    )

    release = asyncio.Event()

    async def parked():
        await release.wait()

    consumer._main_lifecycle = parked
    consumer._claim_renewer = parked

    captured: list[asyncio.Task] = []
    same_tasks = False
    try:
        await consumer.start()
        first_main = consumer._task
        first_renewer = consumer._renewer_task
        captured.extend([first_main, first_renewer])

        await consumer.start()
        second_main = consumer._task
        second_renewer = consumer._renewer_task
        captured.extend([second_main, second_renewer])

        same_tasks = (
            first_main is second_main
            and first_renewer is second_renewer
        )
    finally:
        release.set()
        unique_tasks = {task for task in captured if task is not None}
        for task in unique_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*unique_tasks, return_exceptions=True)
        consumer._task = None
        consumer._renewer_task = None

    assert same_tasks


@pytest.mark.asyncio
async def test_canonical_active_execution_is_visible_to_drain_inflight_count() -> None:
    redis = RedisLifecycleProbe()
    task_id = "task-active-79"
    run_id = "run-active-79"
    redis.task_meta[DagRedisKey.task_meta(task_id)] = {
        "task_id": task_id,
        "run_id": run_id,
    }

    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingTaskConsumer:
        async def consume_once(self, ctx, *, claimed_at_ms: int):
            entered.set()
            await release.wait()
            return SimpleNamespace(
                claimed=SimpleNamespace(ok=True),
                executed=SimpleNamespace(ok=True),
                completed=SimpleNamespace(completed=True, status="committed"),
                rejected_reason="",
            )

    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-active-79",
        worker_group="group-79",
        shards=[1],
        executor=LegacyExecutorProbe(),
        task_consumer=BlockingTaskConsumer(),
    )
    event = RunRequestedEvent(
        task_id=task_id,
        run_id=run_id,
        tenant_id="tenant-79",
        agent_type="python",
        payload={"prompt": "active"},
        scheduler_epoch="79",
    )

    process_task = asyncio.create_task(
        consumer._process_message_via_task_consumer(
            event,
            "1-active",
            "hfa:stream:shard:1",
            1,
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        observed_inflight = consumer.inflight_count
    finally:
        release.set()
        await asyncio.wait_for(process_task, timeout=1.0)

    assert observed_inflight == 1
    assert consumer.inflight_count == 0

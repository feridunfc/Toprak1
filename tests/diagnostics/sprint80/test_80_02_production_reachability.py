from __future__ import annotations

import inspect
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.scheduler import build_production_scheduler
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.main import WorkerService


class _Executor:
    async def execute(self, event):
        return type(
            "ExecutionResult",
            (),
            {
                "status": "done",
                "payload": {},
                "error": "",
                "cost_cents": 0,
                "tokens_used": 0,
            },
        )()


class _ShardManager:
    pass


class _RoutingRedis:
    def __init__(self) -> None:
        self.xack_calls: list[tuple[str, str, str]] = []
        self.task_meta: dict[str, dict[str, str]] = {}
        self.values: dict[str, str] = {}

    async def get(self, key):
        return self.values.get(key)

    async def hgetall(self, key):
        return self.task_meta.get(key, {})

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.xack_calls.append((stream, group, message_id))
        return 1

    async def delete(self, *args, **kwargs):
        return 1


@dataclass
class _LegacyResult:
    status: str = "done"
    payload: dict | None = None
    cost_cents: int = 0
    tokens_used: int = 0
    error: str = ""


class _LegacyExecutorProbe:
    def __init__(self) -> None:
        self.calls: list[object] = []

    async def execute(self, event):
        self.calls.append(event)
        return _LegacyResult(payload={})


class _RecordingTaskConsumer:
    def __init__(self) -> None:
        self.calls: list[tuple[object, int]] = []

    async def consume_once(self, ctx, *, claimed_at_ms: int):
        self.calls.append((ctx, claimed_at_ms))
        return SimpleNamespace(
            claimed=SimpleNamespace(ok=True, status="task_claimed"),
            executed=SimpleNamespace(ok=True),
            completed=SimpleNamespace(completed=True, status="committed"),
            rejected_reason="",
        )


def _raw_request(*, event_type: str) -> dict[str, str]:
    return {
        "event_type": event_type,
        "task_id": "s80-task-route",
        "run_id": "s80-run-route",
        "tenant_id": "s80-tenant",
        "agent_type": "python",
        "worker_group": "s80-group",
        "shard": "0",
        "priority": "5",
        "payload_json": '{"prompt":"sprint80 routing"}',
        "scheduler_epoch": "80",
        "trace_parent": "s80-trace-parent",
        "trace_state": "s80-trace-state",
    }


def _build_routing_consumer():
    redis = _RoutingRedis()
    task_consumer = _RecordingTaskConsumer()
    legacy_executor = _LegacyExecutorProbe()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="s80-worker",
        worker_group="s80-group",
        shards=[0],
        executor=legacy_executor,
        task_consumer=task_consumer,
    )
    legacy_calls: list[str] = []

    async def should_execute(run_id: str) -> bool:
        legacy_calls.append("should_execute")
        return True

    async def try_claim(*args) -> bool:
        legacy_calls.append("try_claim_and_mark_running")
        return False

    consumer._guard.should_execute = should_execute
    consumer._guard.try_claim_and_mark_running = try_claim
    redis.task_meta[DagRedisKey.task_meta("s80-task-route")] = {
        "task_id": "s80-task-route",
        "run_id": "s80-run-route",
    }
    return consumer, redis, task_consumer, legacy_calls


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_production_worker_composes_taskconsumer(sprint80_redis):
    service = WorkerService(
        sprint80_redis,
        {
            "production": True,
            "worker_id": "s80-worker",
            "worker_group": "s80-group",
            "region": "test",
            "version": "s80",
            "capabilities": ["base"],
            "shards": [0],
            "capacity": 1,
            "executor": _Executor(),
            "shard_manager": _ShardManager(),
            "task_heartbeat_interval_ms": 1000,
            "task_stale_after_ms": 5000,
        },
    )

    assert service._task_consumer is not None
    assert service._consumer._task_consumer is service._task_consumer


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_production_worker_composes_daglua_completion(sprint80_redis):
    service = WorkerService(
        sprint80_redis,
        {
            "production": True,
            "worker_id": "s80-worker",
            "worker_group": "s80-group",
            "executor": _Executor(),
            "shard_manager": _ShardManager(),
            "task_heartbeat_interval_ms": 1000,
            "task_stale_after_ms": 5000,
        },
    )

    assert service._dag_lua is not None
    assert service._task_consumer._completion_manager is service._dag_lua


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.parametrize(
    ("event_type", "bridge_enabled", "expected_path"),
    [
        ("TaskRequested", False, "task_consumer"),
        ("TaskRequested", True, "task_consumer"),
        ("RunRequested", False, "legacy"),
        ("RunRequested", True, "task_consumer"),
    ],
)
async def test_message_type_and_flag_select_observed_execution_path(
    monkeypatch,
    event_type: str,
    bridge_enabled: bool,
    expected_path: str,
):
    if bridge_enabled:
        monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")
    else:
        monkeypatch.delenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", raising=False)

    consumer, redis, task_consumer, legacy_calls = _build_routing_consumer()
    stream = RedisKey.stream_shard(0)
    await consumer._process_message(
        f"s80-{event_type}-{int(bridge_enabled)}",
        _raw_request(event_type=event_type),
        stream,
        0,
    )

    if expected_path == "task_consumer":
        assert len(task_consumer.calls) == 1
        assert legacy_calls == []
        assert redis.xack_calls == [
            (stream, CONSUMER_GROUP, f"s80-{event_type}-{int(bridge_enabled)}")
        ]
    else:
        assert task_consumer.calls == []
        assert legacy_calls == ["should_execute", "try_claim_and_mark_running"]
        assert redis.xack_calls == []


@pytest.mark.sprint80_reality
def test_scheduler_event_store_is_optional_in_production_composition():
    signature = inspect.signature(build_production_scheduler)
    assert signature.parameters["event_store"].default is None


@pytest.mark.asyncio
@pytest.mark.sprint80_reality
@pytest.mark.sprint80_real_redis
async def test_semantic_components_are_not_in_production_worker_composition(sprint80_redis):
    service = WorkerService(
        sprint80_redis,
        {
            "production": True,
            "worker_id": "s80-worker",
            "worker_group": "s80-group",
            "executor": _Executor(),
            "shard_manager": _ShardManager(),
            "task_heartbeat_interval_ms": 1000,
            "task_stale_after_ms": 5000,
        },
    )

    modules = {
        type(component).__module__
        for component in (
            service,
            service._consumer,
            service._task_consumer,
            service._task_claim_manager,
            service._task_heartbeat_manager,
            service._dag_lua,
        )
        if component is not None
    }
    assert all(not module.startswith("hfa_semantic") for module in modules)


@pytest.mark.sprint80_reality
def test_semantic_components_are_not_in_production_scheduler_composition(sprint80_redis):
    config = SimpleNamespace(
        instance_id="s80-control",
        scheduler_reservation_ttl_seconds=30,
        scheduler_loop_idle_sleep_ms=0,
        scheduler_loop_error_sleep_ms=0,
        scheduler_loop_max_failures=2,
        dispatch_tokens_capacity=10,
        dispatch_tokens_refill_per_sec=10,
        dispatch_degraded_refill_per_sec=1,
        dispatch_aimd_enabled=False,
    )
    scheduler = build_production_scheduler(
        redis=sprint80_redis,
        config=config,
        registry=SimpleNamespace(),
        shards=[0],
        event_store=None,
    )
    composition = scheduler.composition
    assert composition is not None
    modules = {
        type(component).__module__
        for component in (
            scheduler,
            composition.scheduler_loop,
            composition.dispatch_controller,
            composition.ready_queue,
            composition.dag_lua,
            composition.dispatch_writer,
            composition.reservation_manager,
            composition.reservation_dispatcher,
        )
        if component is not None
    }
    assert all(not module.startswith("hfa_semantic") for module in modules)

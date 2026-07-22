from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer


class RoutingRedis:
    def __init__(self) -> None:
        self.xack_calls: list[tuple[str, str, str]] = []
        self.xadd_calls: list[tuple[str, dict]] = []
        self.task_meta: dict[str, dict[str, str]] = {}
        self.values: dict[str, str] = {}

    async def get(self, key):
        return self.values.get(key)

    async def hgetall(self, key):
        return self.task_meta.get(key, {})

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.xack_calls.append((stream, group, message_id))
        return 1

    async def xadd(self, stream: str, fields: dict, *args, **kwargs) -> str:
        self.xadd_calls.append((stream, fields))
        return "2-0"

    async def delete(self, *args, **kwargs):
        return 1


@dataclass
class LegacyResult:
    status: str = "done"
    payload: dict | None = None
    cost_cents: int = 0
    tokens_used: int = 0
    error: str = ""


class LegacyExecutorProbe:
    def __init__(self) -> None:
        self.calls: list[object] = []

    async def execute(self, event):
        self.calls.append(event)
        return LegacyResult(payload={})


class RecordingTaskConsumer:
    def __init__(self, result=None) -> None:
        self.calls: list[tuple[object, int]] = []
        self.result = result or SimpleNamespace(
            claimed=SimpleNamespace(ok=True, status="task_claimed"),
            executed=SimpleNamespace(ok=True),
            completed=SimpleNamespace(completed=True, status="committed"),
            rejected_reason="",
        )

    async def consume_once(self, ctx, *, claimed_at_ms: int):
        self.calls.append((ctx, claimed_at_ms))
        return self.result


def _raw_request(
    *,
    event_type: str,
    task_id: str = "task-79",
    run_id: str = "run-79",
    scheduler_epoch: str = "79",
) -> dict[str, str]:
    return {
        "event_type": event_type,
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": "tenant-79",
        "agent_type": "python",
        "worker_group": "group-79",
        "shard": "2",
        "priority": "5",
        "payload_json": '{"prompt":"routing"}',
        "scheduler_epoch": scheduler_epoch,
        "trace_parent": "trace-parent-79",
        "trace_state": "trace-state-79",
    }


def _seed_identity(redis: RoutingRedis, *, task_id: str, run_id: str) -> None:
    redis.task_meta[DagRedisKey.task_meta(task_id)] = {
        "task_id": task_id,
        "run_id": run_id,
    }


def _build_consumer(
    *,
    redis: RoutingRedis,
    task_consumer: RecordingTaskConsumer,
    legacy_executor: LegacyExecutorProbe | None = None,
) -> tuple[WorkerConsumer, list[tuple[str, tuple]]]:
    legacy_executor = legacy_executor or LegacyExecutorProbe()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-79",
        worker_group="group-79",
        shards=[2],
        executor=legacy_executor,
        task_consumer=task_consumer,
    )

    legacy_calls: list[tuple[str, tuple]] = []

    async def should_execute(run_id: str) -> bool:
        legacy_calls.append(("should_execute", (run_id,)))
        return True

    async def try_claim(*args) -> bool:
        legacy_calls.append(("try_claim", args))
        return False

    consumer._guard.should_execute = should_execute
    consumer._guard.try_claim_and_mark_running = try_claim
    return consumer, legacy_calls


@pytest.mark.asyncio
async def test_task_requested_routes_to_canonical_path_without_manual_flag(
    monkeypatch,
) -> None:
    monkeypatch.delenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", raising=False)

    redis = RoutingRedis()
    task_consumer = RecordingTaskConsumer()
    consumer, legacy_calls = _build_consumer(redis=redis, task_consumer=task_consumer)
    _seed_identity(redis, task_id="task-79", run_id="run-79")

    stream = RedisKey.stream_shard(2)
    await consumer._process_message(
        "1-task-requested",
        _raw_request(event_type="TaskRequested"),
        stream,
        2,
    )

    assert legacy_calls == []
    assert len(task_consumer.calls) == 1
    assert redis.xack_calls == [(stream, CONSUMER_GROUP, "1-task-requested")]


@pytest.mark.asyncio
async def test_run_requested_remains_legacy_compatibility_only(monkeypatch) -> None:
    monkeypatch.delenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", raising=False)

    redis = RoutingRedis()
    task_consumer = RecordingTaskConsumer()
    consumer, legacy_calls = _build_consumer(redis=redis, task_consumer=task_consumer)

    await consumer._process_message(
        "1-run-requested",
        _raw_request(event_type="RunRequested"),
        RedisKey.stream_shard(2),
        2,
    )

    assert task_consumer.calls == []
    assert legacy_calls and legacy_calls[0][0] == "should_execute"


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["UnknownRequested", "RunDispatchRequested"])
async def test_noncanonical_event_types_fail_closed(
    monkeypatch,
    event_type: str,
) -> None:
    monkeypatch.delenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", raising=False)

    redis = RoutingRedis()
    task_consumer = RecordingTaskConsumer()
    consumer, legacy_calls = _build_consumer(redis=redis, task_consumer=task_consumer)

    await consumer._process_message(
        f"1-{event_type}",
        _raw_request(event_type=event_type),
        RedisKey.stream_shard(2),
        2,
    )

    assert task_consumer.calls == []
    assert legacy_calls == []
    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_malformed_task_requested_never_falls_back_to_legacy_or_acks(
    monkeypatch,
) -> None:
    monkeypatch.delenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", raising=False)

    redis = RoutingRedis()
    task_consumer = RecordingTaskConsumer()
    consumer, legacy_calls = _build_consumer(redis=redis, task_consumer=task_consumer)

    await consumer._process_message(
        "1-missing-task-id",
        _raw_request(event_type="TaskRequested", task_id=""),
        RedisKey.stream_shard(2),
        2,
    )

    assert task_consumer.calls == []
    assert legacy_calls == []
    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_task_requested_preserves_distinct_task_and_run_identity(
    monkeypatch,
) -> None:
    monkeypatch.delenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", raising=False)

    redis = RoutingRedis()
    task_consumer = RecordingTaskConsumer()
    consumer, legacy_calls = _build_consumer(redis=redis, task_consumer=task_consumer)
    _seed_identity(redis, task_id="task-distinct-79", run_id="run-distinct-79")

    await consumer._process_message(
        "1-distinct",
        _raw_request(
            event_type="TaskRequested",
            task_id="task-distinct-79",
            run_id="run-distinct-79",
        ),
        RedisKey.stream_shard(2),
        2,
    )

    assert legacy_calls == []
    assert len(task_consumer.calls) == 1
    ctx, _ = task_consumer.calls[0]
    assert ctx.task_id == "task-distinct-79"
    assert ctx.run_id == "run-distinct-79"
    assert ctx.task_id != ctx.run_id


@pytest.mark.asyncio
async def test_task_requested_propagates_scheduler_epoch_to_task_context(
    monkeypatch,
) -> None:
    monkeypatch.delenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", raising=False)

    redis = RoutingRedis()
    task_consumer = RecordingTaskConsumer()
    consumer, _ = _build_consumer(redis=redis, task_consumer=task_consumer)
    _seed_identity(redis, task_id="task-epoch-79", run_id="run-epoch-79")

    await consumer._process_message(
        "1-epoch",
        _raw_request(
            event_type="TaskRequested",
            task_id="task-epoch-79",
            run_id="run-epoch-79",
            scheduler_epoch="epoch-79-explicit",
        ),
        RedisKey.stream_shard(2),
        2,
    )

    assert len(task_consumer.calls) == 1
    ctx, _ = task_consumer.calls[0]
    assert ctx.scheduler_epoch == "epoch-79-explicit"

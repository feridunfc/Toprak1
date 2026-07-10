from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.fake_executor import FakeExecutor
from hfa_worker.task_consumer import TaskConsumer
from hfa_control.task_claim import TaskClaimManager


class FakeRedis:
    async def get(self, key):
        return None

    def __init__(self) -> None:
        self.xack_calls: list[tuple[str, str, str]] = []
        self.xadd_calls: list[tuple[str, dict]] = []
        self.task_meta: dict[str, dict[str, str]] = {}

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.xack_calls.append((stream, group, message_id))
        return 1

    async def xadd(self, stream: str, fields: dict) -> str:
        self.xadd_calls.append((stream, fields))
        return "2-0"

    async def hget(self, *args, **kwargs):
        return None

    async def hgetall(self, key):
        return self.task_meta.get(key, {})

    async def hset(self, *args, **kwargs):
        return 1

    async def hincrby(self, *args, **kwargs):
        return 0

    async def delete(self, *args, **kwargs):
        return 1


def seed_canonical_identity(
    redis: FakeRedis,
    *,
    task_id: str,
    run_id: str,
) -> None:
    redis.task_meta[DagRedisKey.task_meta(task_id)] = {
        "run_id": run_id,
    }


class ForbiddenLegacyExecutor(FakeExecutor):
    async def execute(self, run_event):
        raise AssertionError("legacy executor path must not run when TaskConsumer bridge is enabled")


class RecordingTaskConsumer:
    def __init__(self) -> None:
        self.calls: list[tuple[object, int]] = []

    async def consume_once(self, ctx, *, claimed_at_ms: int):
        self.calls.append((ctx, claimed_at_ms))
        return SimpleNamespace(
            claimed=SimpleNamespace(ok=True),
            executed=SimpleNamespace(ok=True),
            completed=SimpleNamespace(completed=True, status="committed"),
            rejected_reason="",
        )


@pytest.mark.asyncio
async def test_worker_consumer_task_consumer_bridge_flag_routes_to_task_consumer(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = FakeRedis()
    task_consumer = RecordingTaskConsumer()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-bridge",
        worker_group="group-bridge",
        shards=[5],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    legacy_calls: list[str] = []

    async def forbidden_should_execute(*args, **kwargs):
        legacy_calls.append("should_execute")
        return True

    async def forbidden_try_claim(*args, **kwargs):
        legacy_calls.append("try_claim")
        return True

    async def forbidden_store_result(*args, **kwargs):
        legacy_calls.append("store_result")

    consumer._guard.should_execute = forbidden_should_execute
    consumer._guard.try_claim_and_mark_running = forbidden_try_claim
    consumer._state.store_result = forbidden_store_result

    event = RunRequestedEvent(
        task_id="task-bridge-1",
        run_id="run-bridge-1",
        tenant_id="tenant-bridge",
        agent_type="agent-bridge",
        payload={"prompt": "bridge"},
        scheduler_epoch="epoch-bridge-1",
        trace_parent="trace-parent-bridge",
        trace_state="trace-state-bridge",
    )
    stream = RedisKey.stream_shard(5)
    seed_canonical_identity(
        redis,
        task_id="task-bridge-1",
        run_id="run-bridge-1",
    )

    await consumer._process_message(
        msg_id="1-bridge",
        data=serialize_event(event),
        stream=stream,
        shard=5,
    )

    assert legacy_calls == []
    assert len(task_consumer.calls) == 1

    ctx, claimed_at_ms = task_consumer.calls[0]
    assert claimed_at_ms > 0
    assert ctx.task_id == "task-bridge-1"
    assert ctx.run_id == "run-bridge-1"
    assert ctx.tenant_id == "tenant-bridge"
    assert ctx.agent_type == "agent-bridge"
    assert ctx.worker_group == "group-bridge"
    assert ctx.worker_instance_id == "worker-bridge"
    assert ctx.shard == 5
    assert ctx.payload == {"prompt": "bridge"}
    assert ctx.scheduler_epoch == "epoch-bridge-1"
    assert ctx.trace_parent == "trace-parent-bridge"
    assert ctx.trace_state == "trace-state-bridge"

    assert redis.xadd_calls == []
    assert redis.xack_calls == [(stream, CONSUMER_GROUP, "1-bridge")]
    assert consumer.inflight_count == 0


@pytest.mark.asyncio
async def test_worker_bridge_blocks_legacy_run_only_identity_before_task_consumer(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = FakeRedis()
    task_consumer = RecordingTaskConsumer()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-legacy-identity",
        worker_group="group-legacy-identity",
        shards=[0],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    event = RunRequestedEvent(
        run_id="run-only-identity-76",
        tenant_id="tenant-a",
        agent_type="agent-a",
        payload={"prompt": "legacy identity"},
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-run-only-identity",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    assert task_consumer.calls == []
    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_bridge_blocks_identity_without_authoritative_task_meta(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = FakeRedis()
    task_consumer = RecordingTaskConsumer()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-missing-meta",
        worker_group="group-missing-meta",
        shards=[0],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    event = RunRequestedEvent(
        task_id="task-missing-meta-76",
        run_id="run-missing-meta-76",
        tenant_id="tenant-a",
        agent_type="agent-a",
        payload={"prompt": "missing authoritative identity"},
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-missing-meta",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    assert task_consumer.calls == []
    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_bridge_blocks_authoritative_run_id_mismatch(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = FakeRedis()
    task_consumer = RecordingTaskConsumer()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-run-mismatch",
        worker_group="group-run-mismatch",
        shards=[0],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    event = RunRequestedEvent(
        task_id="task-run-mismatch-76",
        run_id="message-run-76",
        tenant_id="tenant-a",
        agent_type="agent-a",
        payload={"prompt": "run mismatch"},
    )
    seed_canonical_identity(
        redis,
        task_id="task-run-mismatch-76",
        run_id="authoritative-run-76",
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-run-mismatch",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    assert task_consumer.calls == []
    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_bridge_allows_equal_explicit_task_and_run_ids(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = FakeRedis()
    task_consumer = RecordingTaskConsumer()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-equal-identity",
        worker_group="group-equal-identity",
        shards=[0],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    event = RunRequestedEvent(
        task_id="shared-explicit-identity-76",
        run_id="shared-explicit-identity-76",
        tenant_id="tenant-a",
        agent_type="agent-a",
        payload={"prompt": "equal explicit identity"},
    )
    seed_canonical_identity(
        redis,
        task_id="shared-explicit-identity-76",
        run_id="shared-explicit-identity-76",
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-equal-identity",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    assert len(task_consumer.calls) == 1
    assert redis.xack_calls == [
        (stream, CONSUMER_GROUP, "1-equal-identity")
    ]


@pytest.mark.asyncio
async def test_worker_bridge_noncanonical_identity_never_calls_claim_start(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = FakeRedis()
    claim_manager = AsyncMock(spec=TaskClaimManager)
    task_consumer = TaskConsumer(
        claim_manager=claim_manager,
        executor=ForbiddenLegacyExecutor(),
    )
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-claim-spy",
        worker_group="group-claim-spy",
        shards=[0],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    event = RunRequestedEvent(
        run_id="run-claim-spy-76",
        tenant_id="tenant-claim-spy",
        agent_type="agent-claim-spy",
        payload={"prompt": "must not claim"},
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-claim-spy",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    claim_manager.claim_start.assert_not_awaited()
    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_consumer_task_consumer_bridge_enabled_without_injected_consumer_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-no-bridge",
        worker_group="group-no-bridge",
        shards=[0],
        executor=ForbiddenLegacyExecutor(),
    )

    legacy_calls: list[str] = []

    async def forbidden_should_execute(*args, **kwargs):
        legacy_calls.append("should_execute")
        return True

    async def forbidden_try_claim(*args, **kwargs):
        legacy_calls.append("try_claim")
        return True

    consumer._guard.should_execute = forbidden_should_execute
    consumer._guard.try_claim_and_mark_running = forbidden_try_claim

    event = RunRequestedEvent(
        run_id="run-no-injected-consumer",
        tenant_id="tenant-a",
        agent_type="agent-a",
        payload={"prompt": "no consumer"},
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-no-consumer",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    assert legacy_calls == []
    assert redis.xadd_calls == []
    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_consumer_task_consumer_bridge_flag_off_preserves_legacy_path(monkeypatch) -> None:
    monkeypatch.delenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", raising=False)

    redis = FakeRedis()
    task_consumer = RecordingTaskConsumer()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-legacy",
        worker_group="default",
        shards=[0],
        executor=FakeExecutor(),
        task_consumer=task_consumer,
    )

    calls = {"try_claim": []}

    async def fake_should_execute(run_id: str) -> bool:
        return True

    async def fake_try_claim_and_mark_running(run_id: str, worker_id: str, worker_group: str, shard: int) -> bool:
        calls["try_claim"].append((run_id, worker_id, worker_group, shard))
        return True

    async def fake_store_result(*args, **kwargs) -> None:
        return None

    async def fake_transition_state(*args, **kwargs) -> None:
        return None

    async def fake_mark_completed(*args, **kwargs) -> None:
        return None

    consumer._guard.should_execute = fake_should_execute
    consumer._guard.try_claim_and_mark_running = fake_try_claim_and_mark_running
    consumer._state.store_result = fake_store_result
    consumer._state.transition_state = fake_transition_state
    consumer._state.mark_completed = fake_mark_completed

    event = RunRequestedEvent(
        run_id="run-legacy-still-default",
        tenant_id="tenant-a",
        agent_type="fake",
        payload={"prompt": "legacy"},
    )
    stream = RedisKey.stream_shard(0)

    await consumer._process_message(
        msg_id="1-legacy",
        data=serialize_event(event),
        stream=stream,
        shard=0,
    )

    assert calls["try_claim"] == [("run-legacy-still-default", "worker-legacy", "default", 0)]
    assert task_consumer.calls == []
    assert redis.xack_calls == [(stream, CONSUMER_GROUP, "1-legacy")]


class ClaimFailedTaskConsumer:
    async def consume_once(self, ctx, *, claimed_at_ms: int):
        return SimpleNamespace(
            claimed=SimpleNamespace(ok=False, status="reservation_missing"),
            executed=None,
            completed=None,
            rejected_reason="",
        )


class ExecutionFailedTaskConsumer:
    async def consume_once(self, ctx, *, claimed_at_ms: int):
        return SimpleNamespace(
            claimed=SimpleNamespace(ok=True),
            executed=SimpleNamespace(ok=False),
            completed=None,
            rejected_reason="",
        )


class CompletionRejectedTaskConsumer:
    async def consume_once(self, ctx, *, claimed_at_ms: int):
        return SimpleNamespace(
            claimed=SimpleNamespace(ok=True),
            executed=SimpleNamespace(ok=True),
            completed=SimpleNamespace(completed=False, status="claim_epoch_mismatch"),
            rejected_reason="",
        )


class MissingCompletionResultTaskConsumer:
    async def consume_once(self, ctx, *, claimed_at_ms: int):
        return SimpleNamespace(
            claimed=SimpleNamespace(ok=True),
            executed=SimpleNamespace(ok=True),
            rejected_reason="",
        )


class CrashingTaskConsumer:
    async def consume_once(self, ctx, *, claimed_at_ms: int):
        raise RuntimeError("synthetic task consumer crash")


async def _run_bridge_case(task_consumer) -> FakeRedis:
    redis = FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-ack-policy",
        worker_group="group-ack-policy",
        shards=[0],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    event = RunRequestedEvent(
        task_id="task-ack-policy",
        run_id="run-ack-policy",
        tenant_id="tenant-a",
        agent_type="agent-a",
        payload={"prompt": "ack policy"},
        scheduler_epoch="epoch-ack-policy",
    )

    seed_canonical_identity(
        redis,
        task_id="task-ack-policy",
        run_id="run-ack-policy",
    )

    await consumer._process_message(
        msg_id="1-ack-policy",
        data=serialize_event(event),
        stream=RedisKey.stream_shard(0),
        shard=0,
    )

    return redis


@pytest.mark.asyncio
async def test_worker_consumer_bridge_does_not_ack_when_claim_fails(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = await _run_bridge_case(ClaimFailedTaskConsumer())

    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_consumer_bridge_does_not_ack_when_execution_fails(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = await _run_bridge_case(ExecutionFailedTaskConsumer())

    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_consumer_bridge_does_not_ack_when_completion_is_missing(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = await _run_bridge_case(MissingCompletionResultTaskConsumer())

    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_consumer_bridge_does_not_ack_when_fenced_completion_rejects(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = await _run_bridge_case(CompletionRejectedTaskConsumer())

    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_worker_consumer_bridge_exception_leaves_message_unacked(monkeypatch) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    redis = await _run_bridge_case(CrashingTaskConsumer())

    assert redis.xack_calls == []

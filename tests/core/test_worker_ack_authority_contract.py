from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace

import pytest

from hfa.dag.schema import DagRedisKey
from hfa.events.schema import RunRequestedEvent
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.task_consumer import TaskConsumer


class AckRedis:
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


class LegacyExecutorProbe:
    async def execute(self, event):
        raise AssertionError("legacy executor must not own canonical ACK tests")


class ResultTaskConsumer:
    def __init__(self, result=None, error: BaseException | None = None) -> None:
        self.result = result
        self.error = error
        self.calls = 0

    async def consume_once(self, ctx, *, claimed_at_ms: int):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.result


def _result(
    *,
    claim_ok: bool = True,
    executed_ok: bool = True,
    completion_present: bool = True,
    completion_committed: bool = True,
):
    return SimpleNamespace(
        claimed=SimpleNamespace(
            ok=claim_ok,
            status="task_claimed" if claim_ok else "reservation_missing",
        ),
        executed=(SimpleNamespace(ok=executed_ok) if claim_ok else None),
        completed=(
            SimpleNamespace(
                completed=completion_committed,
                status="committed" if completion_committed else "owner_mismatch",
            )
            if claim_ok and completion_present
            else None
        ),
        rejected_reason="",
    )


def _build(
    result=None,
    *,
    error: BaseException | None = None,
    state: str = "",
    evidence_run_id: str = "run-ack-79",
):
    redis = AckRedis()
    task_id = "task-ack-79"
    run_id = "run-ack-79"
    redis.task_meta[DagRedisKey.task_meta(task_id)] = {
        "task_id": task_id,
        "run_id": evidence_run_id,
    }
    if state:
        redis.values[DagRedisKey.task_state(task_id)] = state

    task_consumer = ResultTaskConsumer(result=result, error=error)
    worker_consumer = WorkerConsumer(
        redis=redis,
        worker_id="worker-ack-79",
        worker_group="group-79",
        shards=[4],
        executor=LegacyExecutorProbe(),
        task_consumer=task_consumer,
    )
    event = RunRequestedEvent(
        task_id=task_id,
        run_id=run_id,
        tenant_id="tenant-79",
        agent_type="python",
        payload={"prompt": "ack"},
        scheduler_epoch="79",
    )
    return redis, task_consumer, worker_consumer, event


@pytest.mark.asyncio
async def test_committed_completion_allows_exactly_one_ack() -> None:
    redis, _, consumer, event = _build(_result())
    stream = "hfa:stream:shard:4"

    await consumer._process_message_via_task_consumer(
        event,
        "1-committed",
        stream,
        4,
    )

    assert redis.xack_calls == [(stream, CONSUMER_GROUP, "1-committed")]


@pytest.mark.asyncio
async def test_claim_rejection_does_not_ack() -> None:
    redis, task_consumer, consumer, event = _build(_result(claim_ok=False))

    await consumer._process_message_via_task_consumer(
        event,
        "1-claim-rejected",
        "hfa:stream:shard:4",
        4,
    )

    assert task_consumer.calls == 1
    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_failed_execution_with_committed_fenced_completion_acks_once() -> None:
    redis, _, consumer, event = _build(
        _result(
            executed_ok=False,
            completion_present=True,
            completion_committed=True,
        )
    )
    stream = "hfa:stream:shard:4"

    await consumer._process_message_via_task_consumer(
        event,
        "1-execution-failed-completed",
        stream,
        4,
    )

    assert redis.xack_calls == [
        (stream, CONSUMER_GROUP, "1-execution-failed-completed")
    ]


@pytest.mark.asyncio
async def test_failed_execution_without_committed_completion_does_not_ack() -> None:
    redis, _, consumer, event = _build(
        _result(
            executed_ok=False,
            completion_present=True,
            completion_committed=False,
        )
    )

    await consumer._process_message_via_task_consumer(
        event,
        "1-execution-failed-uncommitted",
        "hfa:stream:shard:4",
        4,
    )

    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_missing_completion_result_does_not_ack() -> None:
    redis, _, consumer, event = _build(_result(completion_present=False))

    await consumer._process_message_via_task_consumer(
        event,
        "1-no-completion",
        "hfa:stream:shard:4",
        4,
    )

    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_rejected_completion_does_not_ack() -> None:
    redis, _, consumer, event = _build(_result(completion_committed=False))

    await consumer._process_message_via_task_consumer(
        event,
        "1-completion-rejected",
        "hfa:stream:shard:4",
        4,
    )

    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_cancellation_is_not_silently_acked() -> None:
    redis, _, consumer, event = _build(error=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await consumer._process_message_via_task_consumer(
            event,
            "1-cancelled",
            "hfa:stream:shard:4",
            4,
        )

    assert redis.xack_calls == []


@pytest.mark.asyncio
async def test_verified_terminal_duplicate_uses_existing_ack_policy() -> None:
    redis, task_consumer, consumer, event = _build(
        _result(),
        state="done",
        evidence_run_id="run-ack-79",
    )
    stream = "hfa:stream:shard:4"

    await consumer._process_message_via_task_consumer(
        event,
        "1-terminal-duplicate",
        stream,
        4,
    )

    assert task_consumer.calls == 0
    assert redis.xack_calls == [
        (stream, CONSUMER_GROUP, "1-terminal-duplicate")
    ]


@pytest.mark.asyncio
async def test_unverifiable_terminal_duplicate_is_not_acked() -> None:
    redis, task_consumer, consumer, event = _build(
        _result(),
        state="done",
        evidence_run_id="different-run",
    )

    await consumer._process_message_via_task_consumer(
        event,
        "1-terminal-mismatch",
        "hfa:stream:shard:4",
        4,
    )

    assert task_consumer.calls == 0
    assert redis.xack_calls == []


def test_task_consumer_does_not_own_xack() -> None:
    source = inspect.getsource(TaskConsumer)

    assert "xack" not in source
    assert "ack_message" not in source

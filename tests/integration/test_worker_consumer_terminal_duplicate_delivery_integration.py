from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer


class RecordingTaskConsumer:
    def __init__(self) -> None:
        self.calls: list[object] = []

    async def consume_once(self, ctx: object, *, claimed_at_ms: int) -> object:
        self.calls.append((ctx, claimed_at_ms))
        raise AssertionError("terminal duplicate delivery must not enter TaskConsumer.consume_once")


def _event(run_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        run_id=run_id,
        tenant_id="tenant-1",
        agent_type="test",
        payload={"input": "duplicate"},
        trace_parent="",
        trace_state="",
        scheduler_epoch="1",
    )


def _consumer(redis: object, task_consumer: RecordingTaskConsumer) -> WorkerConsumer:
    consumer = object.__new__(WorkerConsumer)
    consumer._redis = redis
    consumer._task_consumer = task_consumer
    consumer._worker_id = "worker-sprint-69-1"
    consumer._worker_group = CONSUMER_GROUP
    return consumer


def _pending_count(summary: object) -> int:
    if isinstance(summary, dict):
        return int(summary.get("pending", 0))
    return int(summary[0])


@pytest.mark.asyncio
async def test_worker_consumer_terminal_duplicate_delivery_suppresses_consume_once_with_real_redis(
    redis_client,
) -> None:
    task_id = f"sprint-69-1-terminal-duplicate-{uuid4().hex}"
    stream = f"hfa:sprint69:stream:{uuid4().hex}"
    consumer_name = "worker-sprint-69-1-consumer"
    msg_id = None

    await redis_client.delete(DagRedisKey.task_state(task_id), stream)
    await redis_client.set(DagRedisKey.task_state(task_id), "done")

    try:
        await redis_client.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
        msg_id = await redis_client.xadd(
            stream,
            {
                "run_id": task_id,
                "tenant_id": "tenant-1",
                "agent_type": "test",
            },
        )
        delivered = await redis_client.xreadgroup(
            CONSUMER_GROUP,
            consumer_name,
            {stream: ">"},
            count=1,
            block=1000,
        )
        assert delivered

        before = _pending_count(await redis_client.xpending(stream, CONSUMER_GROUP))
        assert before == 1

        task_consumer = RecordingTaskConsumer()
        consumer = _consumer(redis_client, task_consumer)

        await consumer._process_message_via_task_consumer(
            _event(task_id),
            msg_id,
            stream,
            shard=0,
        )

        after = _pending_count(await redis_client.xpending(stream, CONSUMER_GROUP))

        assert task_consumer.calls == []
        assert after == before

        state = await redis_client.get(DagRedisKey.task_state(task_id))
        if isinstance(state, bytes):
            state = state.decode("utf-8")
        assert state == "done"
    finally:
        await redis_client.delete(DagRedisKey.task_state(task_id), stream)

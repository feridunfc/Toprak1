
from __future__ import annotations

import json
from typing import Any

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.redis_utils import ensure_consumer_group
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = pytest.mark.asyncio


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if value is None:
        return ""
    return str(value)


async def _pending_count(redis_client, stream: str) -> int:
    pending = await redis_client.xpending(stream, CONSUMER_GROUP)

    if isinstance(pending, dict):
        if "pending" in pending:
            return int(pending.get("pending") or 0)
        if "count" in pending:
            return int(pending.get("count") or 0)

    if isinstance(pending, (list, tuple)) and pending:
        return int(pending[0] or 0)

    return 0


class RuntimeDrillExecutor(TaskExecutor):
    def __init__(self) -> None:
        self.calls: list[TaskContext] = []

    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        self.calls.append(ctx)
        return TaskExecutionResult(
            ok=True,
            output={
                "done": True,
                "task_id": ctx.task_id,
                "run_id": ctx.run_id,
                "agent_type": ctx.agent_type,
            },
        )


class ForbiddenLegacyExecutor:
    async def execute(self, run_event):
        raise AssertionError("legacy WorkerConsumer executor path must not run")


@pytest.mark.integration
async def test_worker_consumer_bridge_real_redis_lua_claim_complete_and_ack(
    redis_client,
    monkeypatch,
) -> None:
    monkeypatch.setenv("HFA_WORKER_TASK_CONSUMER_BRIDGE", "1")

    shard = 0
    stream = RedisKey.stream_shard(shard)
    task_id = "runtime-drill-76-task"
    run_id = "runtime-drill-76-run"
    tenant_id = "tenant-runtime-drill-65"
    worker_id = "worker-runtime-drill-65"
    scheduler_epoch = "epoch-runtime-drill-65"

    await ensure_consumer_group(
        redis_client,
        stream,
        CONSUMER_GROUP,
        start_id="0",
        mkstream=True,
    )

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
        },
    )
    await redis_client.set(
        RedisKey.run_state(run_id),
        "running",
    )

    reservation_mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    reserved = await reservation_mgr.reserve(
        worker_id=worker_id,
        task_id=task_id,
        scheduler_epoch=scheduler_epoch,
        reserved_at_ms=123450,
    )
    assert reserved.ok is True

    dag = DagLua(redis_client)
    claim_mgr = TaskClaimManager(dag)
    task_executor = RuntimeDrillExecutor()
    task_consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=task_executor,
        completion_manager=dag,
    )

    worker_consumer = WorkerConsumer(
        redis=redis_client,
        worker_id=worker_id,
        worker_group="runtime-drill-group",
        shards=[shard],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    legacy_calls: list[str] = []

    async def legacy_should_execute(*args, **kwargs):
        legacy_calls.append("should_execute")
        return True

    async def legacy_try_claim_and_mark_running(*args, **kwargs):
        legacy_calls.append("try_claim_and_mark_running")
        return True

    async def legacy_mark_completed(*args, **kwargs):
        legacy_calls.append("mark_completed")

    async def legacy_release_claim(*args, **kwargs):
        legacy_calls.append("release_claim")

    async def legacy_store_result(*args, **kwargs):
        legacy_calls.append("store_result")

    worker_consumer._guard.should_execute = legacy_should_execute
    worker_consumer._guard.try_claim_and_mark_running = legacy_try_claim_and_mark_running
    worker_consumer._state.mark_completed = legacy_mark_completed
    worker_consumer._state.release_claim = legacy_release_claim
    worker_consumer._state.store_result = legacy_store_result

    event = RunRequestedEvent(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="runtime-drill-agent",
        payload={"prompt": "prove real redis lua bridge path"},
        scheduler_epoch=scheduler_epoch,
        trace_parent="trace-runtime-drill-65",
        trace_state="state-runtime-drill-65",
    )

    added_msg_id = await redis_client.xadd(stream, serialize_event(event))

    messages = await redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=worker_id,
        streams={stream: ">"},
        count=1,
        block=100,
    )
    assert messages, "expected one runtime drill stream message"

    stream_name, entries = messages[0]
    assert _decode(stream_name) == stream
    assert len(entries) == 1

    read_msg_id, data = entries[0]
    assert _decode(read_msg_id) == _decode(added_msg_id)
    assert _decode(data.get(b"task_id") or data.get("task_id")) == task_id
    assert _decode(data.get(b"run_id") or data.get("run_id")) == run_id
    assert task_id != run_id

    assert await _pending_count(redis_client, stream) == 1

    await worker_consumer._process_message(
        msg_id=_decode(read_msg_id),
        data=data,
        stream=stream,
        shard=shard,
    )

    assert legacy_calls == []
    assert len(task_executor.calls) == 1

    ctx = task_executor.calls[0]
    assert ctx.task_id == task_id
    assert ctx.run_id == run_id
    assert ctx.tenant_id == tenant_id
    assert ctx.worker_instance_id == worker_id
    assert ctx.worker_group == "runtime-drill-group"
    assert ctx.scheduler_epoch == scheduler_epoch
    assert ctx.shard == shard
    assert ctx.payload == {"prompt": "prove real redis lua bridge path"}

    assert await _pending_count(redis_client, stream) == 0

    state = _decode(await redis_client.get(DagRedisKey.task_state(task_id)))
    assert state == "done"

    output_raw = await redis_client.get(DagRedisKey.task_output(task_id))
    assert output_raw is not None
    output = json.loads(_decode(output_raw))

    assert output["done"] is True
    assert output["task_id"] == task_id
    assert output["run_id"] == run_id
    assert output["agent_type"] == "runtime-drill-agent"

    meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    normalized_meta = {_decode(k): _decode(v) for k, v in meta.items()}

    assert normalized_meta["run_id"] == run_id
    assert normalized_meta["worker_instance_id"] == worker_id
    assert normalized_meta["scheduler_epoch"] == scheduler_epoch
    assert normalized_meta["claim_epoch"] == "1"

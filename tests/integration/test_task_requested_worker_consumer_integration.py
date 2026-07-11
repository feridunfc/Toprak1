from __future__ import annotations

import json
import time
from typing import Any

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey, DagTaskSeed
from hfa_control.dag_lua import DagLua
from hfa_control.dag_scheduler_bridge import (
    DagReadyQueue,
    DagSchedulerDispatchWriter,
)
from hfa_control.scheduler_reservation_dispatch import (
    SchedulerReservationDispatcher,
)
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import (
    WorkerReservationManager,
)
from hfa_worker.consumer import (
    CONSUMER_GROUP,
    WorkerConsumer,
)
from hfa_worker.redis_utils import ensure_consumer_group
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import (
    TaskExecutionResult,
    TaskExecutor,
)


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if value is None:
        return ""
    return str(value)


def _mapping(raw: dict[Any, Any]) -> dict[str, str]:
    return {
        _decode(key): _decode(value)
        for key, value in raw.items()
    }


async def _pending_count(
    redis_client,
    stream: str,
) -> int:
    pending = await redis_client.xpending(
        stream,
        CONSUMER_GROUP,
    )

    if isinstance(pending, dict):
        if "pending" in pending:
            return int(pending.get("pending") or 0)
        if "count" in pending:
            return int(pending.get("count") or 0)

    if isinstance(pending, (list, tuple)) and pending:
        return int(pending[0] or 0)

    return 0


class RecordingTaskExecutor(TaskExecutor):
    def __init__(self) -> None:
        self.calls: list[TaskContext] = []

    async def execute(
        self,
        ctx: TaskContext,
    ) -> TaskExecutionResult:
        self.calls.append(ctx)

        return TaskExecutionResult(
            ok=True,
            output={
                "accepted_event": "TaskRequested",
                "task_id": ctx.task_id,
                "run_id": ctx.run_id,
                "scheduler_epoch": ctx.scheduler_epoch,
            },
        )


class ForbiddenLegacyExecutor:
    async def execute(self, run_event):
        raise AssertionError(
            "legacy WorkerConsumer executor path must not run"
        )


async def test_task_requested_from_canonical_writer_reaches_worker_consumer(
    redis_client,
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "HFA_WORKER_TASK_CONSUMER_BRIDGE",
        "1",
    )

    shard = 4
    stream = RedisKey.stream_shard(shard)

    task_id = "task-requested-consumer-77"
    run_id = "run-requested-consumer-77"
    tenant_id = "tenant-requested-consumer-77"
    worker_id = "worker-requested-consumer-77"
    worker_group = "worker-group-requested-77"
    scheduler_epoch = "epoch-requested-consumer-77"

    assert task_id != run_id

    await ensure_consumer_group(
        redis_client,
        stream,
        CONSUMER_GROUP,
        start_id="0",
        mkstream=True,
    )

    dag = DagLua(redis_client)
    await dag.initialise()

    seed = DagTaskSeed(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="requested-consumer-agent",
        priority=5,
        admitted_at=float(int(time.time() * 1000)),
        dependency_count=0,
        payload_json=json.dumps(
            {
                "prompt": (
                    "prove TaskRequested reaches "
                    "WorkerConsumer"
                )
            }
        ),
        region="eu-requested-77",
        policy="LEAST_LOADED",
        trace_parent="trace-requested-77",
        trace_state="state-requested-77",
    )

    admitted = await dag.task_admit(seed)

    assert admitted.admitted is True
    assert admitted.ready is True

    reservation_manager = WorkerReservationManager(
        redis_client,
        reservation_ttl_seconds=30,
    )

    writer = DagSchedulerDispatchWriter(
        ready_queue=DagReadyQueue(redis_client),
        dag_lua=dag,
    )

    dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        writer,
    )

    dispatched = await dispatcher.reserve_and_dispatch(
        task_id=task_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        dispatch_payload={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_group": worker_group,
            "shard": shard,
            "region": "eu-requested-77",
        },
        reserved_at_ms=179000,
    )

    assert dispatched.ok is True
    assert dispatched.status == "reserved_and_dispatched"

    messages = await redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=worker_id,
        streams={stream: ">"},
        count=1,
        block=100,
    )

    assert messages, "expected canonical TaskRequested delivery"

    stream_name, entries = messages[0]

    assert _decode(stream_name) == stream
    assert len(entries) == 1

    msg_id, data = entries[0]

    raw = _mapping(data)

    # This is the actual event emitted by
    # task_dispatch_commit.lua.
    assert raw["event_type"] == "TaskRequested"
    assert raw["task_id"] == task_id
    assert raw["run_id"] == run_id
    assert raw["scheduler_epoch"] == scheduler_epoch

    assert await _pending_count(
        redis_client,
        stream,
    ) == 1

    task_executor = RecordingTaskExecutor()

    task_consumer = TaskConsumer(
        claim_manager=TaskClaimManager(dag),
        executor=task_executor,
        completion_manager=dag,
    )

    worker_consumer = WorkerConsumer(
        redis=redis_client,
        worker_id=worker_id,
        worker_group=worker_group,
        shards=[shard],
        executor=ForbiddenLegacyExecutor(),
        task_consumer=task_consumer,
    )

    await worker_consumer._process_message(
        msg_id=_decode(msg_id),
        data=data,
        stream=stream,
        shard=shard,
    )

    assert len(task_executor.calls) == 1

    ctx = task_executor.calls[0]

    assert ctx.task_id == task_id
    assert ctx.run_id == run_id
    assert ctx.tenant_id == tenant_id
    assert ctx.worker_instance_id == worker_id
    assert ctx.worker_group == worker_group
    assert ctx.scheduler_epoch == scheduler_epoch
    assert ctx.shard == shard
    assert ctx.payload == {
        "prompt": (
            "prove TaskRequested reaches "
            "WorkerConsumer"
        )
    }

    assert await _pending_count(
        redis_client,
        stream,
    ) == 0

    state = _decode(
        await redis_client.get(
            DagRedisKey.task_state(task_id)
        )
    )
    assert state == "done"

    output_raw = await redis_client.get(
        DagRedisKey.task_output(task_id)
    )
    assert output_raw is not None

    output = json.loads(_decode(output_raw))

    assert output == {
        "accepted_event": "TaskRequested",
        "task_id": task_id,
        "run_id": run_id,
        "scheduler_epoch": scheduler_epoch,
    }

    meta = _mapping(
        await redis_client.hgetall(
            DagRedisKey.task_meta(task_id)
        )
    )

    assert meta["task_id"] == task_id
    assert meta["run_id"] == run_id
    assert meta["worker_instance_id"] == worker_id
    assert meta["scheduler_epoch"] == scheduler_epoch
    assert meta["claim_epoch"] == "1"

    # Claim consumed the dispatch reservation.
    assert (
        await redis_client.exists(
            DagRedisKey.worker_reservation(worker_id)
        )
        == 0
    )

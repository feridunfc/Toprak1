
from __future__ import annotations

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_control.api.crash_boundary_evidence import read_crash_boundary_evidence
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.redis_utils import ensure_consumer_group
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = pytest.mark.asyncio


TASK_ID = "crash-boundary-product-diagnostic-task"
TENANT_ID = "tenant-crash-boundary-product"
WORKER_ID = "worker-crash-boundary-product"
WORKER_GROUP = "crash-boundary-product-group"
SCHEDULER_EPOCH = "scheduler-epoch-crash-boundary-product"
SHARD = 0


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


class CountingExecutor(TaskExecutor):
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
                "tenant_id": ctx.tenant_id,
            },
        )


async def test_crash_boundary_evidence_detects_terminal_task_with_pending_message(redis_client) -> None:
    stream = RedisKey.stream_shard(SHARD)

    await ensure_consumer_group(
        redis_client,
        stream,
        CONSUMER_GROUP,
        start_id="0",
        mkstream=True,
    )

    await redis_client.set(DagRedisKey.task_state(TASK_ID), "scheduled")

    reservation_mgr = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    reserved = await reservation_mgr.reserve(
        worker_id=WORKER_ID,
        task_id=TASK_ID,
        scheduler_epoch=SCHEDULER_EPOCH,
        reserved_at_ms=123456,
    )
    assert reserved.ok

    event = RunRequestedEvent(
        run_id=TASK_ID,
        tenant_id=TENANT_ID,
        agent_type="crash-boundary-product-agent",
        payload={"prompt": "prove product-placed crash boundary evidence"},
        scheduler_epoch=SCHEDULER_EPOCH,
        trace_parent="trace-crash-boundary-product",
        trace_state="state-crash-boundary-product",
    )

    await redis_client.xadd(stream, serialize_event(event))

    messages = await redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=WORKER_ID,
        streams={stream: ">"},
        count=1,
        block=100,
    )
    assert messages
    assert await _pending_count(redis_client, stream) == 1

    dag = DagLua(redis_client)
    claim_mgr = TaskClaimManager(dag)
    first_executor = CountingExecutor()
    first_consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=first_executor,
        completion_manager=dag,
    )

    ctx = TaskContext(
        task_id=TASK_ID,
        run_id=TASK_ID,
        tenant_id=TENANT_ID,
        agent_type="crash-boundary-product-agent",
        worker_group=WORKER_GROUP,
        worker_instance_id=WORKER_ID,
        payload={"prompt": "prove product-placed crash boundary evidence"},
        trace_parent="trace-crash-boundary-product",
        trace_state="state-crash-boundary-product",
        scheduler_epoch=SCHEDULER_EPOCH,
    )

    first = await first_consumer.consume_once(ctx, claimed_at_ms=123457)

    assert first.claimed is not None
    assert first.claimed.ok
    assert first.claimed.claim_epoch == "1"
    assert first.executed is not None
    assert first.executed.ok
    assert first.completed is not None
    assert first.completed.completed

    # No ACK here. Exact boundary:
    # completion committed, worker stream message still pending.
    assert await _pending_count(redis_client, stream) == 1

    evidence = await read_crash_boundary_evidence(
        redis_client,
        TASK_ID,
        shard=SHARD,
        group=CONSUMER_GROUP,
    )

    assert evidence["task_id"] == TASK_ID
    assert evidence["stream"] == stream
    assert evidence["group"] == CONSUMER_GROUP

    assert evidence["task_evidence"]["found"] is True
    assert evidence["task_evidence"]["state"] == "done"
    assert evidence["task_evidence"]["terminal_state"] == "done"
    assert evidence["task_evidence"]["worker_instance_id"] == WORKER_ID
    assert evidence["task_evidence"]["scheduler_epoch"] == SCHEDULER_EPOCH
    assert evidence["task_evidence"]["claim_epoch"] == "1"
    assert evidence["task_evidence"]["output_found"] is True

    assert evidence["pending"]["count"] == 1
    assert evidence["pending"]["matching_task_message_pending"] is True
    assert len(evidence["pending"]["matching_task_message_ids"]) == 1

    assert evidence["boundary"]["task_terminal"] is True
    assert evidence["boundary"]["task_done"] is True
    assert evidence["boundary"]["terminal_task_with_pending_message"] is True
    assert evidence["boundary"]["ack_missing"] is True
    assert evidence["boundary"]["duplicate_execution_risk_visible"] is True
    assert evidence["boundary"]["operator_action_required"] is True

    assert evidence["safety"]["read_only"] is True
    assert evidence["safety"]["ack_attempted"] is False
    assert evidence["safety"]["reclaim_attempted"] is False
    assert evidence["safety"]["requeue_attempted"] is False
    assert evidence["safety"]["retry_attempted"] is False
    assert evidence["safety"]["runtime_repair_attempted"] is False
    assert evidence["safety"]["production_ready_claim"] is False

    second_executor = CountingExecutor()
    second_consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=second_executor,
        completion_manager=dag,
    )
    second = await second_consumer.consume_once(ctx, claimed_at_ms=123458)

    assert second.claimed is not None
    assert second.claimed.ok is False
    assert second.executed is None
    assert second_executor.calls == []
    assert await _pending_count(redis_client, stream) == 1

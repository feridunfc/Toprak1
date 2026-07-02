
import json

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim import TaskClaimManager
from hfa_control.worker_reservation import WorkerReservationManager
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = pytest.mark.asyncio


class OutputExecutor(TaskExecutor):
    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        return TaskExecutionResult(
            ok=True,
            output={"done": True, "task_id": ctx.task_id},
        )


@pytest.mark.integration
async def test_task_consumer_executes_and_writes_fenced_completion(redis_client):
    task_id = "consumer-complete-63"
    tenant_id = "tenant-a"
    worker_id = "worker-complete-63"
    scheduler_epoch = "epoch-complete-63"

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")

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
    consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=OutputExecutor(),
        completion_manager=dag,
    )

    ctx = TaskContext(
        task_id=task_id,
        run_id="run-complete-63",
        tenant_id=tenant_id,
        agent_type="default",
        worker_group="grp-a",
        worker_instance_id=worker_id,
        payload={"hello": "world"},
        scheduler_epoch=scheduler_epoch,
    )

    result = await consumer.consume_once(ctx, claimed_at_ms=123456)

    assert result.claimed is not None
    assert result.claimed.ok is True
    assert result.claimed.claim_epoch
    assert result.claimed.scheduler_epoch == scheduler_epoch

    assert result.executed is not None
    assert result.executed.ok is True

    assert result.completed is not None
    assert result.completed.completed is True
    assert result.completed.status == "committed"

    state = await redis_client.get(DagRedisKey.task_state(task_id))
    assert state == "done"

    output_raw = await redis_client.get(DagRedisKey.task_output(task_id))
    assert output_raw is not None
    output = json.loads(output_raw)
    assert output["done"] is True
    assert output["task_id"] == task_id

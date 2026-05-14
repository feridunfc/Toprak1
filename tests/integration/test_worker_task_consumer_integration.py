
import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.task_claim import TaskClaimManager
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutor

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_worker_task_consumer_claims_and_executes(redis_client):
    task_id = "consumer-001"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")

    claim_mgr = TaskClaimManager(redis_client)
    executor = TaskExecutor()
    consumer = TaskConsumer(claim_mgr, executor)

    ctx = TaskContext(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        agent_type="default",
        worker_group="grp-a",
        worker_instance_id="worker-i-1",
        payload={"hello": "world"},
    )

    result = await consumer.consume_once(ctx, claimed_at_ms=123456)

    assert result.claimed.ok is True
    assert result.executed is not None
    assert result.executed.ok is True
    assert result.executed.output["task_id"] == task_id

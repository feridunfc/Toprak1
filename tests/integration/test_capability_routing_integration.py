
import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.task_claim import TaskClaimManager
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutor

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_worker_with_required_capabilities_claims_and_executes(redis_client):
    task_id = "cap-001"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")

    claim_mgr = TaskClaimManager(redis_client)
    consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=TaskExecutor(),
        worker_capabilities=["python", "sql"],
    )

    ctx = TaskContext(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        agent_type="python_coder",
        worker_group="grp-a",
        worker_instance_id="worker-1",
        payload={},
        required_capabilities=["python"],
    )

    result = await consumer.consume_once(ctx, claimed_at_ms=123456)

    assert result.rejected_reason == ""
    assert result.claimed is not None
    assert result.claimed.ok is True
    assert result.executed is not None
    assert result.executed.ok is True


@pytest.mark.integration
async def test_worker_missing_capabilities_rejects_before_claim(redis_client):
    task_id = "cap-002"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")

    claim_mgr = TaskClaimManager(redis_client)
    consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=TaskExecutor(),
        worker_capabilities=["frontend"],
    )

    ctx = TaskContext(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        agent_type="python_coder",
        worker_group="grp-a",
        worker_instance_id="worker-2",
        payload={},
        required_capabilities=["python", "sql"],
    )

    result = await consumer.consume_once(ctx, claimed_at_ms=123456)

    assert result.claimed is None
    assert result.executed is None
    assert result.rejected_reason == "missing_capabilities:python,sql"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "scheduled"

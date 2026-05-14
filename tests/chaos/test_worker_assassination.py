
import pytest

from hfa_control.reconciliation_manager import ReconciliationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.chaos
async def test_worker_assassination_mid_execution_reconciles_inflight(redis_client):
    tenant_id = "tenant-assassination"

    # Worker claimed task and inflated running/inflight state
    await redis_client.set(DagRedisKey.tenant_inflight(tenant_id), "1")
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {"task-1": 1000})

    # Simulate crash cleanup path: running task disappears before inflight is corrected
    await redis_client.zrem(DagRedisKey.task_running_zset(tenant_id), "task-1")

    mgr = ReconciliationManager(redis_client)
    report = await mgr.reconcile_tenant(tenant_id)

    assert report.corrected is True
    assert report.actual_inflight == 0
    assert await redis_client.get(DagRedisKey.tenant_inflight(tenant_id)) == "0"

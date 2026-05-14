
import pytest

from hfa_control.reconciliation_manager import ReconciliationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_reconcile_leaked_usage_corrected(redis_client):
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.tenant_inflight(tenant_id), "5")
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {"task-1": 1000, "task-2": 2000})

    mgr = ReconciliationManager(redis_client)
    report = await mgr.reconcile_tenant(tenant_id)

    assert report.corrected is True
    assert report.expected_inflight == 5
    assert report.actual_inflight == 2
    stored = await redis_client.get(DagRedisKey.tenant_inflight(tenant_id))
    assert stored == "2"


@pytest.mark.integration
async def test_reconcile_terminal_but_still_counted_corrected(redis_client):
    tenant_id = "tenant-b"

    await redis_client.set(DagRedisKey.tenant_inflight(tenant_id), "3")

    mgr = ReconciliationManager(redis_client)
    report = await mgr.reconcile_tenant(tenant_id)

    assert report.corrected is True
    assert report.actual_inflight == 0
    stored = await redis_client.get(DagRedisKey.tenant_inflight(tenant_id))
    assert stored == "0"


@pytest.mark.integration
async def test_reconcile_noop_when_clean(redis_client):
    tenant_id = "tenant-c"

    await redis_client.set(DagRedisKey.tenant_inflight(tenant_id), "2")
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {"task-1": 1000, "task-2": 2000})

    mgr = ReconciliationManager(redis_client)
    report = await mgr.reconcile_tenant(tenant_id)

    assert report.corrected is False
    assert report.expected_inflight == 2
    assert report.actual_inflight == 2

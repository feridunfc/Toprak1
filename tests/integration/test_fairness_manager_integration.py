
import pytest

from hfa_control.fairness_manager import FairnessManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_add_runtime_updates_vruntime(redis_client):
    mgr = FairnessManager(redis_client)

    result = await mgr.add_runtime("tenant-a", runtime_ms=100, weight=2.0)

    assert result.old_vruntime == 0.0
    assert result.delta == 50.0
    assert result.new_vruntime == 50.0
    stored = await redis_client.get(DagRedisKey.tenant_vruntime("tenant-a"))
    assert float(stored) == 50.0


@pytest.mark.integration
async def test_choose_lower_vruntime_tenant(redis_client):
    mgr = FairnessManager(redis_client)

    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-a"), "100")
    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-b"), "20")
    await mgr.set_inflight("tenant-a", 0)
    await mgr.set_inflight("tenant-b", 0)

    chosen = await mgr.choose_fairest_tenant(["tenant-a", "tenant-b"])

    assert chosen.tenant_id == "tenant-b"
    assert chosen.vruntime == 20.0


@pytest.mark.integration
async def test_choose_by_inflight_when_vruntime_equal(redis_client):
    mgr = FairnessManager(redis_client)

    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-a"), "10")
    await redis_client.set(DagRedisKey.tenant_vruntime("tenant-b"), "10")
    await mgr.set_inflight("tenant-a", 3)
    await mgr.set_inflight("tenant-b", 1)

    chosen = await mgr.choose_fairest_tenant(["tenant-a", "tenant-b"])

    assert chosen.tenant_id == "tenant-b"
    assert chosen.inflight == 1


import pytest

from hfa.dag.fairness_hardening import FairnessPolicy
from hfa.dag.schema import DagRedisKey
from hfa_control.fairness_hardening_manager import FairnessHardeningManager

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_max_inflight_enforced(redis_client):
    tenant_id = "tenant-a"
    await redis_client.set(DagRedisKey.tenant_inflight(tenant_id), "3")
    await redis_client.set(DagRedisKey.tenant_vruntime(tenant_id), "20")

    mgr = FairnessHardeningManager(
        redis_client,
        FairnessPolicy(max_inflight_per_tenant=3, starvation_recovery_delta=0.0, vruntime_floor=0.0),
    )
    result = await mgr.evaluate_tenant(tenant_id)

    assert result.eligible is False
    assert result.reason == "max_inflight_reached"
    assert result.inflight == 3


@pytest.mark.integration
async def test_penalized_tenant_eventually_recovers(redis_client):
    tenant_id = "tenant-b"
    await redis_client.set(DagRedisKey.tenant_vruntime(tenant_id), "120")

    mgr = FairnessHardeningManager(
        redis_client,
        FairnessPolicy(max_inflight_per_tenant=0, starvation_recovery_delta=30.0, vruntime_floor=10.0),
    )
    first = await mgr.apply_starvation_recovery(tenant_id)
    second = await mgr.apply_starvation_recovery(tenant_id)
    third = await mgr.apply_starvation_recovery(tenant_id)
    fourth = await mgr.apply_starvation_recovery(tenant_id)

    assert first.new_vruntime == 90.0
    assert second.new_vruntime == 60.0
    assert third.new_vruntime == 30.0
    assert fourth.new_vruntime == 10.0

    stored = await redis_client.get(DagRedisKey.tenant_vruntime(tenant_id))
    assert float(stored) == 10.0


@pytest.mark.integration
async def test_fairness_stays_deterministic_under_floor(redis_client):
    tenant_id = "tenant-c"
    await redis_client.set(DagRedisKey.tenant_vruntime(tenant_id), "1")
    await redis_client.set(DagRedisKey.tenant_inflight(tenant_id), "0")

    mgr = FairnessHardeningManager(
        redis_client,
        FairnessPolicy(max_inflight_per_tenant=2, starvation_recovery_delta=5.0, vruntime_floor=10.0),
    )
    result = await mgr.evaluate_tenant(tenant_id)

    assert result.eligible is True
    assert result.reason == "eligible"
    assert result.effective_vruntime == 10.0

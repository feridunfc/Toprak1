
import pytest

from hfa.runtime.backpressure import BackpressurePolicy
from hfa_control.load_shedder import LoadShedder

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_tenant_pending_limit_triggers_rate_limit(redis_client):
    await redis_client.zadd("hfa:dag:tenant:tenant-a:ready", {"task-1": 1, "task-2": 2, "task-3": 3})

    shedder = LoadShedder(redis_client, BackpressurePolicy(max_pending_per_tenant=3))
    decision = await shedder.evaluate(tenant_id="tenant-a", known_tenant_ids=["tenant-a"])

    assert decision.allowed is False
    assert decision.reason == "tenant_rate_limited"
    assert decision.tenant_pending == 3


@pytest.mark.integration
async def test_global_backpressure_triggers(redis_client):
    await redis_client.zadd("hfa:dag:tenant:tenant-a:ready", {"task-1": 1, "task-2": 2})
    await redis_client.zadd("hfa:dag:tenant:tenant-b:ready", {"task-3": 3, "task-4": 4})

    shedder = LoadShedder(redis_client, BackpressurePolicy(global_pending_limit=4))
    decision = await shedder.evaluate(
        tenant_id="tenant-a",
        known_tenant_ids=["tenant-a", "tenant-b"],
    )

    assert decision.allowed is False
    assert decision.reason == "global_backpressure"
    assert decision.global_pending == 4


@pytest.mark.integration
async def test_running_limit_triggers(redis_client):
    await redis_client.zadd("hfa:dag:tenant:tenant-a:running", {"task-r1": 1, "task-r2": 2})

    shedder = LoadShedder(redis_client, BackpressurePolicy(max_running_per_tenant=2))
    decision = await shedder.evaluate(tenant_id="tenant-a", known_tenant_ids=["tenant-a"])

    assert decision.allowed is False
    assert decision.reason == "tenant_running_limit"
    assert decision.tenant_running == 2


@pytest.mark.integration
async def test_allowed_when_under_all_limits(redis_client):
    await redis_client.zadd("hfa:dag:tenant:tenant-a:ready", {"task-1": 1})
    await redis_client.zadd("hfa:dag:tenant:tenant-a:running", {"task-r1": 1})

    shedder = LoadShedder(
        redis_client,
        BackpressurePolicy(
            max_pending_per_tenant=3,
            global_pending_limit=5,
            max_running_per_tenant=3,
        ),
    )
    decision = await shedder.evaluate(tenant_id="tenant-a", known_tenant_ids=["tenant-a"])

    assert decision.allowed is True
    assert decision.reason == "allowed"
    assert decision.tenant_pending == 1
    assert decision.global_pending == 1
    assert decision.tenant_running == 1

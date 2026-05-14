import fakeredis.aioredis as faredis
import pytest

from hfa.runtime.tenant_utils import decrement_tenant_inflight_if_needed
from hfa_control.tenant_registry import TenantRegistry


@pytest.mark.asyncio
class TestSprint14BInflightLifecycle:
    async def test_increment_then_decrement_returns_to_zero(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        assert await registry.get_inflight("tenant-a") == 0
        assert await registry.increment_inflight("tenant-a") == 1
        assert await registry.decrement_inflight("tenant-a") == 0
        assert await registry.get_inflight("tenant-a") == 0

    async def test_multiple_terminal_decrements_never_go_negative(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        await registry.increment_inflight("tenant-a")
        assert await registry.decrement_inflight("tenant-a") == 0
        assert await registry.decrement_inflight("tenant-a") == 0
        assert await registry.get_inflight("tenant-a") == 0

    async def test_tenant_utils_decrements_by_run_id(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        run_id = "tenant-a:run-123"
        tenant_id = "tenant-a"

        await redis.hset(f"hfa:run:meta:{run_id}", "tenant_id", tenant_id)
        await registry.increment_inflight(tenant_id)
        assert await registry.get_inflight(tenant_id) == 1

        await decrement_tenant_inflight_if_needed(redis, run_id)
        assert await registry.get_inflight(tenant_id) == 0

    async def test_tenant_utils_missing_meta_is_noop(self):
        redis = faredis.FakeRedis()

        await decrement_tenant_inflight_if_needed(redis, "tenant-a:missing-run")

    async def test_tenant_utils_floor_at_zero(self):
        redis = faredis.FakeRedis()
        run_id = "tenant-a:run-999"

        await redis.hset(f"hfa:run:meta:{run_id}", "tenant_id", "tenant-a")

        await decrement_tenant_inflight_if_needed(redis, run_id)
        await decrement_tenant_inflight_if_needed(redis, run_id)

        value = await redis.get("hfa:tenant:tenant-a:inflight")
        if value is None:
            assert value is None
        else:
            if isinstance(value, bytes):
                value = value.decode()
            assert int(value) == 0

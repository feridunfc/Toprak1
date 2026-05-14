from __future__ import annotations

import sys
from pathlib import Path

import fakeredis.aioredis as faredis
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-control" / "src"))

from hfa_control.tenant_registry import TenantRegistry


@pytest.mark.asyncio
class TestSprint14TenantRegistry:
    async def test_missing_config_returns_defaults(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        config = await registry.get_config("tenant-missing")

        assert config.tenant_id == "tenant-missing"
        assert config.weight == 1
        assert config.max_inflight_runs is None
        assert config.max_runs_per_second is None

    async def test_update_weight_persists(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        config = await registry.update_weight("tenant-1", 5)
        assert config.weight == 5

        config2 = await registry.get_config("tenant-1")
        assert config2.weight == 5

    async def test_weight_clamped_to_minimum(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        config = await registry.update_weight("tenant-1", 0)
        assert config.weight == 1

    async def test_update_limits_persists(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        config = await registry.update_limits(
            "tenant-1",
            max_inflight_runs=10,
            max_runs_per_second=5.5,
        )

        assert config.max_inflight_runs == 10
        assert config.max_runs_per_second == 5.5

        config2 = await registry.get_config("tenant-1")
        assert config2.max_inflight_runs == 10
        assert config2.max_runs_per_second == 5.5

    async def test_inflight_defaults_to_zero(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        inflight = await registry.get_inflight("tenant-1")
        assert inflight == 0

    async def test_increment_inflight(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        value = await registry.increment_inflight("tenant-1")
        assert value == 1

        value = await registry.increment_inflight("tenant-1")
        assert value == 2

    async def test_decrement_inflight_never_below_zero(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        await registry.increment_inflight("tenant-1")
        await registry.increment_inflight("tenant-1")

        value = await registry.decrement_inflight("tenant-1")
        assert value == 1

        value = await registry.decrement_inflight("tenant-1")
        assert value == 0

        value = await registry.decrement_inflight("tenant-1")
        assert value == 0

    async def test_config_with_malformed_fields_returns_defaults(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        key = "hfa:tenant:tenant-1:config"
        await redis.hset(key, "weight", "not-a-number")
        await redis.hset(key, "max_inflight_runs", "also-not-a-number")
        await redis.hset(key, "max_runs_per_second", "still-not-a-number")

        config = await registry.get_config("tenant-1")

        assert config.weight == 1
        assert config.max_inflight_runs is None
        assert config.max_runs_per_second is None

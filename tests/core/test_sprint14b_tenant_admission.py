from types import SimpleNamespace
from unittest.mock import AsyncMock

import fakeredis.aioredis as faredis
import pytest

from hfa_control.admission import AdmissionController
from hfa_control.exceptions import QuotaExceededError, RateLimitedError
from hfa_control.rate_limit import TenantRateLimiter
from hfa_control.tenant_registry import TenantRegistry


@pytest.mark.asyncio
class TestSprint14BTenantAdmission:
    async def test_inflight_limit_rejects_when_full(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)
        limiter = TenantRateLimiter(redis)

        await registry.update_limits("tenant-a", max_inflight_runs=1)
        await registry.increment_inflight("tenant-a")

        controller = AdmissionController(
            redis=redis,
            config=SimpleNamespace(control_stream="hfa:stream:control"),
            tenant_registry=registry,
            rate_limiter=limiter,
        )
        controller._quota = AsyncMock()
        controller._quota.check_and_increment_runs.return_value = True
        controller._quota.check_rate_limit.return_value = True
        controller._quota.check_and_reserve_budget.return_value = True

        request = SimpleNamespace(
            run_id="tenant-a:run-2",
            tenant_id="tenant-a",
            agent_type="test",
            priority=5,
            payload={},
            estimated_cost_cents=0,
            preferred_region="",
            preferred_placement="LEAST_LOADED",
        )

        with pytest.raises(QuotaExceededError):
            await controller.admit(request)

        state = await redis.get("hfa:run:state:tenant-a:run-2")
        if isinstance(state, bytes):
            state = state.decode()
        assert state == "rejected"

        reason = await redis.hget("hfa:run:meta:tenant-a:run-2", "rejection_reason")
        if isinstance(reason, bytes):
            reason = reason.decode()
        assert reason == "tenant_inflight_limit_exceeded"

        inflight = await registry.get_inflight("tenant-a")
        assert inflight == 1

    async def test_inflight_limit_allows_when_below_limit(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)
        limiter = TenantRateLimiter(redis)

        await registry.update_limits("tenant-a", max_inflight_runs=2)
        await registry.increment_inflight("tenant-a")

        controller = AdmissionController(
            redis=redis,
            config=SimpleNamespace(control_stream="hfa:stream:control"),
            tenant_registry=registry,
            rate_limiter=limiter,
        )

        allowed, reason = await controller._tenant_inflight_allowed("tenant-a")
        assert allowed is True
        assert reason is None

    async def test_missing_limits_preserve_old_behavior(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)

        controller = AdmissionController(
            redis=redis,
            config=SimpleNamespace(control_stream="hfa:stream:control"),
            tenant_registry=registry,
            rate_limiter=None,
        )

        allowed, reason = await controller._tenant_inflight_allowed("tenant-missing")
        assert allowed is True
        assert reason is None

    async def test_rate_limit_rejects_when_exceeded(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)
        limiter = TenantRateLimiter(redis)

        await registry.update_limits("tenant-a", max_runs_per_second=1.0)

        controller = AdmissionController(
            redis=redis,
            config=SimpleNamespace(control_stream="hfa:stream:control"),
            tenant_registry=registry,
            rate_limiter=limiter,
        )

        allowed, reason = await controller._tenant_rate_allowed("tenant-a")
        assert allowed is True
        assert reason is None

        allowed, reason = await controller._tenant_rate_allowed("tenant-a")
        assert allowed is False
        assert reason == "tenant_rate_limit_exceeded"

    async def test_rate_limit_rejection_marks_run_rejected(self):
        redis = faredis.FakeRedis()
        registry = TenantRegistry(redis)
        limiter = TenantRateLimiter(redis)

        await registry.update_limits("tenant-a", max_runs_per_second=1.0)

        controller = AdmissionController(
            redis=redis,
            config=SimpleNamespace(control_stream="hfa:stream:control"),
            tenant_registry=registry,
            rate_limiter=limiter,
        )
        controller._quota = AsyncMock()
        controller._quota.check_and_increment_runs.return_value = True
        controller._quota.check_rate_limit.return_value = True
        controller._quota.check_and_reserve_budget.return_value = True

        request1 = SimpleNamespace(
            run_id="tenant-a:run-1",
            tenant_id="tenant-a",
            agent_type="test",
            priority=5,
            payload={},
            estimated_cost_cents=0,
            preferred_region="",
            preferred_placement="LEAST_LOADED",
        )
        request2 = SimpleNamespace(
            run_id="tenant-a:run-2",
            tenant_id="tenant-a",
            agent_type="test",
            priority=5,
            payload={},
            estimated_cost_cents=0,
            preferred_region="",
            preferred_placement="LEAST_LOADED",
        )

        await controller.admit(request1)

        with pytest.raises(RateLimitedError):
            await controller.admit(request2)

        state = await redis.get("hfa:run:state:tenant-a:run-2")
        if isinstance(state, bytes):
            state = state.decode()
        assert state == "rejected"

        reason = await redis.hget("hfa:run:meta:tenant-a:run-2", "rejection_reason")
        if isinstance(reason, bytes):
            reason = reason.decode()
        assert reason == "tenant_rate_limit_exceeded"

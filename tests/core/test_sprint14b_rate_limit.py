import fakeredis.aioredis as faredis
import pytest

from hfa_control.rate_limit import TenantRateLimiter


@pytest.mark.asyncio
class TestSprint14BRateLimit:
    async def test_rate_limit_disabled_when_none(self):
        redis = faredis.FakeRedis()
        limiter = TenantRateLimiter(redis)

        allowed = await limiter.check_and_consume("tenant-a", None, now=100.0)
        assert allowed is True

    async def test_single_request_allowed_under_limit(self):
        redis = faredis.FakeRedis()
        limiter = TenantRateLimiter(redis)

        allowed = await limiter.check_and_consume("tenant-a", 2, now=100.0)
        assert allowed is True

    async def test_reject_when_limit_reached(self):
        redis = faredis.FakeRedis()
        limiter = TenantRateLimiter(redis)

        assert await limiter.check_and_consume("tenant-a", 1, now=100.0) is True
        assert await limiter.check_and_consume("tenant-a", 1, now=100.1) is False

    async def test_window_expires_old_entries(self):
        redis = faredis.FakeRedis()
        limiter = TenantRateLimiter(redis)

        assert await limiter.check_and_consume("tenant-a", 1, now=100.0) is True
        assert await limiter.check_and_consume("tenant-a", 1, now=101.1) is True

    async def test_tenants_are_isolated(self):
        redis = faredis.FakeRedis()
        limiter = TenantRateLimiter(redis)

        assert await limiter.check_and_consume("tenant-a", 1, now=100.0) is True
        assert await limiter.check_and_consume("tenant-b", 1, now=100.0) is True

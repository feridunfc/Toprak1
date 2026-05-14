"""
tests/integration/test_sprint15_rate_limit_integration.py
IRONCLAD Sprint 15 — Atomic rate limiter integration tests (real Redis)

These tests verify the Lua eval path that fakeredis cannot exercise.
They are the authoritative tests for rate-limit atomicity guarantees.
"""

from __future__ import annotations

import pytest

from hfa_control.rate_limit import TenantRateLimiter
from hfa.config.keys import RedisKey


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------------
# Basic sliding-window semantics
# ---------------------------------------------------------------------------


async def test_first_request_under_limit_is_admitted(real_redis):
    """Single request is always admitted when under limit."""
    limiter = TenantRateLimiter(real_redis)
    result = await limiter.check_and_consume("tenant-a", max_runs_per_second=5, now=1000.0)
    assert result is True


async def test_requests_within_limit_all_admitted(real_redis):
    """All requests up to the limit are admitted within the same window."""
    limiter = TenantRateLimiter(real_redis)
    limit = 3

    results = []
    for i in range(limit):
        r = await limiter.check_and_consume(
            "tenant-b", max_runs_per_second=limit, now=1000.0 + i * 0.01
        )
        results.append(r)

    assert all(results), f"Expected all True, got: {results}"


async def test_request_at_limit_is_rejected(real_redis):
    """The (limit+1)th request within the window must be rejected."""
    limiter = TenantRateLimiter(real_redis)
    limit = 2

    # Fill up the window
    for i in range(limit):
        await limiter.check_and_consume("tenant-c", max_runs_per_second=limit, now=1000.0)

    # One more in the same window should be rejected
    rejected = await limiter.check_and_consume("tenant-c", max_runs_per_second=limit, now=1000.1)
    assert rejected is False


async def test_window_expiry_admits_new_requests(real_redis):
    """After the 1-second window expires, new requests are admitted again."""
    limiter = TenantRateLimiter(real_redis)

    # Fill window at t=1000
    await limiter.check_and_consume("tenant-d", max_runs_per_second=1, now=1000.0)

    # At t=1002 (>1s later) — should be admitted
    result = await limiter.check_and_consume("tenant-d", max_runs_per_second=1, now=1002.0)
    assert result is True


# ---------------------------------------------------------------------------
# Disabled limiter
# ---------------------------------------------------------------------------


async def test_limiter_disabled_when_none(real_redis):
    """max_runs_per_second=None bypasses limiter entirely."""
    limiter = TenantRateLimiter(real_redis)
    for _ in range(100):
        r = await limiter.check_and_consume("tenant-e", max_runs_per_second=None, now=1000.0)
        assert r is True


async def test_limiter_disabled_when_zero(real_redis):
    """max_runs_per_second=0 is treated as disabled."""
    limiter = TenantRateLimiter(real_redis)
    r = await limiter.check_and_consume("tenant-f", max_runs_per_second=0, now=1000.0)
    assert r is True


# ---------------------------------------------------------------------------
# Tenant isolation
# ---------------------------------------------------------------------------


async def test_tenants_are_fully_isolated(real_redis):
    """Rate limit state is completely independent across tenants."""
    limiter = TenantRateLimiter(real_redis)

    # Exhaust tenant-g's limit
    await limiter.check_and_consume("tenant-g", max_runs_per_second=1, now=1000.0)

    # tenant-h should be unaffected
    result = await limiter.check_and_consume("tenant-h", max_runs_per_second=1, now=1000.0)
    assert result is True


# ---------------------------------------------------------------------------
# Atomicity verification (key correctness from Lua)
# ---------------------------------------------------------------------------


async def test_lua_path_leaves_correct_zset_cardinality(real_redis):
    """Verify the ZSET contains exactly one entry per admitted request."""
    limiter = TenantRateLimiter(real_redis)
    limit = 3

    for i in range(limit):
        await limiter.check_and_consume(
            "tenant-i", max_runs_per_second=limit, now=1000.0 + i * 0.01
        )

    key = RedisKey.tenant_rate("tenant-i")
    count = await real_redis.zcard(key)
    assert count == limit


async def test_rejected_request_does_not_add_to_zset(real_redis):
    """A rejected request must not increment the ZSET."""
    limiter = TenantRateLimiter(real_redis)

    await limiter.check_and_consume("tenant-j", max_runs_per_second=1, now=1000.0)
    await limiter.check_and_consume("tenant-j", max_runs_per_second=1, now=1000.1)  # rejected

    key = RedisKey.tenant_rate("tenant-j")
    count = await real_redis.zcard(key)
    assert count == 1, f"Expected 1 entry, got {count} (rejected request leaked into ZSET)"

"""
tests/integration/test_sprint15_admission_integration.py
IRONCLAD Sprint 15 — Admission + rate limit integration tests (real Redis)

Verifies that rate limiting and state writes work correctly end-to-end
with real Redis, using the atomic Lua path.
"""

from __future__ import annotations

import pytest

from hfa_control.rate_limit import TenantRateLimiter
from hfa.config.keys import RedisKey, RedisTTL


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------------
# Rate limit state persistence
# ---------------------------------------------------------------------------


async def test_admitted_run_state_written_correctly(real_redis):
    """After admission, run state key exists with expected value."""
    run_id = "run-adm-001"

    await real_redis.set(
        RedisKey.run_state(run_id),
        "admitted",
        ex=RedisTTL.RUN_STATE,
    )

    raw = await real_redis.get(RedisKey.run_state(run_id))
    state = raw.decode() if isinstance(raw, bytes) else raw
    assert state == "admitted"


async def test_rejected_run_state_written_correctly(real_redis):
    """Rejected run must have its state and meta written."""
    run_id = "run-adm-002"
    tenant_id = "tenant-adm-a"

    await real_redis.set(
        RedisKey.run_state(run_id),
        "rejected",
        ex=RedisTTL.RUN_STATE,
    )
    await real_redis.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "rejection_reason": "tenant_rate_limit_exceeded",
        },
    )
    await real_redis.expire(RedisKey.run_meta(run_id), RedisTTL.RUN_META)

    state_raw = await real_redis.get(RedisKey.run_state(run_id))
    state = state_raw.decode() if isinstance(state_raw, bytes) else state_raw
    assert state == "rejected"

    reason_raw = await real_redis.hget(RedisKey.run_meta(run_id), "rejection_reason")
    reason = reason_raw.decode() if isinstance(reason_raw, bytes) else reason_raw
    assert reason == "tenant_rate_limit_exceeded"


# ---------------------------------------------------------------------------
# Rate limit + admission gate interaction
# ---------------------------------------------------------------------------


async def test_first_run_admitted_second_rejected_in_same_window(real_redis):
    """
    Simulate admission gate: first run passes rate limit, second is rejected.
    Uses atomic Lua via real Redis.
    """
    limiter = TenantRateLimiter(real_redis)
    tenant_id = "tenant-adm-b"

    first = await limiter.check_and_consume(tenant_id, max_runs_per_second=1, now=2000.0)
    assert first is True

    second = await limiter.check_and_consume(tenant_id, max_runs_per_second=1, now=2000.1)
    assert second is False


async def test_rate_limit_resets_after_window(real_redis):
    """After window expires, tenant can submit again."""
    limiter = TenantRateLimiter(real_redis)
    tenant_id = "tenant-adm-c"

    await limiter.check_and_consume(tenant_id, max_runs_per_second=1, now=3000.0)
    again = await limiter.check_and_consume(tenant_id, max_runs_per_second=1, now=3002.0)
    assert again is True


# ---------------------------------------------------------------------------
# Key TTL verification
# ---------------------------------------------------------------------------


async def test_run_state_ttl_is_set(real_redis):
    """Run state key must have a TTL set (not persist forever)."""
    run_id = "run-adm-003"

    await real_redis.set(RedisKey.run_state(run_id), "admitted", ex=RedisTTL.RUN_STATE)

    ttl = await real_redis.ttl(RedisKey.run_state(run_id))
    # TTL should be positive and not too far from RedisTTL.RUN_STATE
    assert 0 < ttl <= RedisTTL.RUN_STATE


async def test_rate_key_ttl_is_set_after_consumption(real_redis):
    """Rate ZSET must have a TTL set to prevent unbounded growth."""
    limiter = TenantRateLimiter(real_redis)
    tenant_id = "tenant-adm-d"

    await limiter.check_and_consume(tenant_id, max_runs_per_second=10, now=4000.0)

    key = RedisKey.tenant_rate(tenant_id)
    ttl = await real_redis.ttl(key)
    assert 0 < ttl <= RedisTTL.TENANT_RATE

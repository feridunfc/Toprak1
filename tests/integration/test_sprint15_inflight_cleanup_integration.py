"""
tests/integration/test_sprint15_inflight_cleanup_integration.py
IRONCLAD Sprint 15 — Tenant inflight cleanup integration tests (real Redis)

Verifies the Lua-backed decrement_tenant_inflight_if_needed() with real Redis.
Tests floor-at-zero guarantee, idempotency, and missing-meta safety.
"""

from __future__ import annotations

import pytest

from hfa.runtime.tenant_utils import decrement_tenant_inflight_if_needed
from hfa.config.keys import RedisKey, RedisTTL


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------------
# Basic decrement
# ---------------------------------------------------------------------------


async def test_decrement_reduces_inflight_by_one(real_redis):
    """Normal decrement reduces counter from 3 to 2."""
    run_id = "run-inflight-001"
    tenant_id = "tenant-inflight-a"

    # Set up run meta
    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"tenant_id": tenant_id})
    # Set inflight to 3
    await real_redis.set(RedisKey.tenant_inflight(tenant_id), 3, ex=RedisTTL.TENANT_INFLIGHT)

    await decrement_tenant_inflight_if_needed(real_redis, run_id)

    raw = await real_redis.get(RedisKey.tenant_inflight(tenant_id))
    assert int(raw) == 2


async def test_decrement_from_one_reaches_zero(real_redis):
    """Decrement from 1 reaches exactly 0, not negative."""
    run_id = "run-inflight-002"
    tenant_id = "tenant-inflight-b"

    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"tenant_id": tenant_id})
    await real_redis.set(RedisKey.tenant_inflight(tenant_id), 1, ex=RedisTTL.TENANT_INFLIGHT)

    await decrement_tenant_inflight_if_needed(real_redis, run_id)

    raw = await real_redis.get(RedisKey.tenant_inflight(tenant_id))
    assert int(raw) == 0


# ---------------------------------------------------------------------------
# Floor-at-zero guarantee
# ---------------------------------------------------------------------------


async def test_repeated_decrement_never_goes_below_zero(real_redis):
    """Calling cleanup multiple times must never produce a negative value."""
    run_id = "run-inflight-003"
    tenant_id = "tenant-inflight-c"

    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"tenant_id": tenant_id})
    await real_redis.set(RedisKey.tenant_inflight(tenant_id), 1, ex=RedisTTL.TENANT_INFLIGHT)

    # Call 5 times — only the first should matter
    for _ in range(5):
        await decrement_tenant_inflight_if_needed(real_redis, run_id)

    raw = await real_redis.get(RedisKey.tenant_inflight(tenant_id))
    assert int(raw) == 0


async def test_decrement_when_already_zero_stays_zero(real_redis):
    """Starting from 0 — cleanup must leave it at 0."""
    run_id = "run-inflight-004"
    tenant_id = "tenant-inflight-d"

    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"tenant_id": tenant_id})
    await real_redis.set(RedisKey.tenant_inflight(tenant_id), 0, ex=RedisTTL.TENANT_INFLIGHT)

    await decrement_tenant_inflight_if_needed(real_redis, run_id)

    raw = await real_redis.get(RedisKey.tenant_inflight(tenant_id))
    assert int(raw) == 0


# ---------------------------------------------------------------------------
# Safety: missing metadata
# ---------------------------------------------------------------------------


async def test_no_crash_when_run_meta_missing(real_redis):
    """Cleanup must not raise when run_meta key does not exist."""
    await decrement_tenant_inflight_if_needed(real_redis, "run-no-meta-999")
    # No exception = pass


async def test_no_crash_when_inflight_key_missing(real_redis):
    """Cleanup must not raise when tenant inflight key has never been set."""
    run_id = "run-inflight-005"
    tenant_id = "tenant-inflight-e"

    await real_redis.hset(RedisKey.run_meta(run_id), mapping={"tenant_id": tenant_id})
    # Intentionally do NOT set the inflight key

    await decrement_tenant_inflight_if_needed(real_redis, run_id)
    # No exception = pass

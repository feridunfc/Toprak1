"""
tests/integration/test_sprint15_evalsha_integration.py
IRONCLAD Sprint 15 — EVALSHA rate limiter integration tests (real Redis)

These tests are the authoritative proof that:
  1. The Lua script loads successfully via SCRIPT LOAD
  2. EVALSHA executes the script correctly
  3. NOSCRIPT recovery works after a simulated script flush
  4. Atomicity holds — no TOCTOU leakage under concurrent calls
"""

from __future__ import annotations

import asyncio
import pytest
from pathlib import Path

from hfa_control.rate_limit import TenantRateLimiter

# from hfa.lua.loader import LuaScriptLoader
from hfa.config.keys import RedisKey


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_LUA_PATH = (
    Path(__file__).parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua" / "rate_limit.lua"
)


# ---------------------------------------------------------------------------
# EVALSHA lifecycle
# ---------------------------------------------------------------------------


async def test_initialise_loads_sha(real_redis):
    """SCRIPT LOAD returns a non-empty SHA on real Redis."""
    limiter = TenantRateLimiter(real_redis)
    await limiter.initialise()
    assert limiter._loader is not None
    assert limiter._loader.sha is not None
    assert len(limiter._loader.sha) == 40  # SHA1 is always 40 hex chars


async def test_evalsha_admits_first_request(real_redis):
    """First request via EVALSHA is admitted."""
    limiter = TenantRateLimiter(real_redis)
    await limiter.initialise()
    result = await limiter.check_and_consume("tenant-eval-a", max_runs_per_second=5, now=1000.0)
    assert result is True


async def test_evalsha_rejects_at_limit(real_redis):
    """EVALSHA path correctly rejects when limit is reached."""
    limiter = TenantRateLimiter(real_redis)
    await limiter.initialise()

    assert await limiter.check_and_consume("tenant-eval-b", max_runs_per_second=2, now=1000.0)
    assert await limiter.check_and_consume("tenant-eval-b", max_runs_per_second=2, now=1000.1)
    rejected = await limiter.check_and_consume("tenant-eval-b", max_runs_per_second=2, now=1000.2)
    assert rejected is False


async def test_evalsha_window_reset(real_redis):
    """After window expires, EVALSHA admits again."""
    limiter = TenantRateLimiter(real_redis)
    await limiter.initialise()

    await limiter.check_and_consume("tenant-eval-c", max_runs_per_second=1, now=2000.0)
    result = await limiter.check_and_consume("tenant-eval-c", max_runs_per_second=1, now=2002.0)
    assert result is True


# ---------------------------------------------------------------------------
# NOSCRIPT recovery
# ---------------------------------------------------------------------------


async def test_noscript_recovery_after_script_flush(real_redis):
    """After SCRIPT FLUSH, the loader reloads and EVALSHA succeeds."""
    limiter = TenantRateLimiter(real_redis)
    await limiter.initialise()
    sha_before = limiter._loader.sha

    # Simulate Redis restart by flushing the script cache
    await real_redis.script_flush()

    # The next call should trigger NOSCRIPT → reload → retry
    result = await limiter.check_and_consume("tenant-noscript", max_runs_per_second=5, now=3000.0)
    assert result is True
    sha_after = limiter._loader.sha

    # SHA should be the same (deterministic based on script body)
    assert sha_before == sha_after


# ---------------------------------------------------------------------------
# Atomicity proof — concurrent calls must not exceed limit
# ---------------------------------------------------------------------------


async def test_concurrent_calls_never_exceed_limit(real_redis):
    """
    100 concurrent admission calls with limit=10.
    Exactly 10 must be admitted — no more, no less.

    This is the proof of atomicity. A non-atomic implementation would
    admit > 10 because multiple coroutines read "count < 10" before any write.
    """
    limiter = TenantRateLimiter(real_redis)
    await limiter.initialise()

    tenant = "tenant-concurrent"
    limit = 10
    total = 100
    fixed_now = 5000.0

    async def one_call(i: int) -> bool:
        # Vary now slightly so members are unique but all within the window
        now = fixed_now + (i * 0.0001)
        return await limiter.check_and_consume(tenant, max_runs_per_second=limit, now=now)

    results = await asyncio.gather(*[one_call(i) for i in range(total)])

    admitted = sum(1 for r in results if r)
    rejected = sum(1 for r in results if not r)

    assert admitted == limit, (
        f"Expected exactly {limit} admitted, got {admitted} "
        f"(rejected={rejected}) — atomicity violated!"
    )
    assert admitted + rejected == total


# ---------------------------------------------------------------------------
# ZSET cardinality after EVALSHA
# ---------------------------------------------------------------------------


async def test_zset_has_correct_cardinality_after_evalsha(real_redis):
    """ZSET contains exactly N entries after N admitted calls."""
    limiter = TenantRateLimiter(real_redis)
    await limiter.initialise()

    tenant = "tenant-card"
    limit = 5
    for i in range(limit):
        await limiter.check_and_consume(tenant, max_runs_per_second=limit, now=6000.0 + i * 0.01)

    key = RedisKey.tenant_rate(tenant)
    count = await real_redis.zcard(key)
    assert count == limit


async def test_rejected_call_does_not_add_to_zset(real_redis):
    """A rejected call via EVALSHA must not add an entry to the ZSET."""
    limiter = TenantRateLimiter(real_redis)
    await limiter.initialise()

    tenant = "tenant-nocard"
    await limiter.check_and_consume(tenant, max_runs_per_second=1, now=7000.0)
    await limiter.check_and_consume(tenant, max_runs_per_second=1, now=7000.1)  # rejected

    key = RedisKey.tenant_rate(tenant)
    count = await real_redis.zcard(key)
    assert count == 1, f"Rejected call leaked into ZSET — count={count}"

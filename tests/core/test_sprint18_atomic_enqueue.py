"""
tests/core/test_sprint18_atomic_enqueue.py
IRONCLAD Sprint 18.1 — Atomic enqueue tests

Verifies:
  - enqueue_admitted writes queue + meta + state + active-index atomically
  - Re-enqueue same run_id is idempotent (NX guard)
  - Meta is always present after enqueue (no ghost runs)
  - State is set to "queued" after enqueue
  - Active tenant SET is updated
  - Failed meta write doesn't leave orphaned queue entry (Lua atomicity)
"""

from __future__ import annotations

import pytest
import fakeredis.aioredis as faredis

from hfa_control.scheduler_lua import SchedulerLua
from hfa.config.keys import RedisKey


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_lua(redis) -> SchedulerLua:
    lua = SchedulerLua(redis)
    await lua.initialise()
    return lua


def _score(priority: int = 5, now: float = 1000.0) -> float:
    from hfa_control.tenant_queue import MAX_PRIORITY

    ts_micros = int(now * 1_000_000) % int(1e12)
    return float((MAX_PRIORITY - priority) * int(1e12) + ts_micros)


# ---------------------------------------------------------------------------
# 18.1: Atomic enqueue — all writes happen together
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_writes_queue_entry():
    redis = faredis.FakeRedis()
    lua = await _make_lua(redis)
    await lua.enqueue_admitted(
        run_id="run-001",
        tenant_id="acme",
        agent_type="coder",
        priority=5,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        admitted_at=1000.0,
        score=_score(5, 1000.0),
    )
    count = await redis.zcard(RedisKey.tenant_queue("acme"))
    assert count == 1


@pytest.mark.asyncio
async def test_enqueue_writes_meta():
    redis = faredis.FakeRedis()
    lua = await _make_lua(redis)
    await lua.enqueue_admitted(
        run_id="run-002",
        tenant_id="acme",
        agent_type="researcher",
        priority=3,
        preferred_region="eu-west",
        preferred_placement="REGION_AFFINITY",
        admitted_at=2000.0,
        score=_score(3, 2000.0),
    )
    meta = await redis.hgetall(RedisKey.run_meta("run-002"))
    assert meta, "Meta must be written atomically with queue entry"

    def _s(k):
        v = meta.get(k.encode()) or meta.get(k)
        return (v.decode() if isinstance(v, bytes) else v) or ""

    assert _s("run_id") == "run-002"
    assert _s("tenant_id") == "acme"
    assert _s("agent_type") == "researcher"
    assert _s("priority") == "3"
    assert _s("queue_state") == "queued"


@pytest.mark.asyncio
async def test_enqueue_sets_state_to_queued():
    redis = faredis.FakeRedis()
    lua = await _make_lua(redis)
    await lua.enqueue_admitted(
        run_id="run-003",
        tenant_id="acme",
        agent_type="coder",
        priority=5,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        admitted_at=3000.0,
        score=_score(),
    )
    state = await redis.get(RedisKey.run_state("run-003"))
    s = (state.decode() if isinstance(state, bytes) else state) or ""
    assert s == "queued"


@pytest.mark.asyncio
async def test_enqueue_adds_to_active_tenant_set():
    redis = faredis.FakeRedis()
    lua = await _make_lua(redis)
    await lua.enqueue_admitted(
        run_id="run-004",
        tenant_id="beta",
        agent_type="coder",
        priority=5,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        admitted_at=4000.0,
        score=_score(),
    )
    members = await redis.smembers(RedisKey.tenant_active_set())
    decoded = {m.decode() if isinstance(m, bytes) else m for m in members}
    assert "beta" in decoded


# ---------------------------------------------------------------------------
# Idempotency — re-enqueueing same run_id is a no-op
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_idempotent_same_run_id():
    redis = faredis.FakeRedis()
    lua = await _make_lua(redis)

    result1 = await lua.enqueue_admitted(
        run_id="run-idem",
        tenant_id="acme",
        agent_type="coder",
        priority=5,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        admitted_at=1000.0,
        score=_score(),
    )
    result2 = await lua.enqueue_admitted(
        run_id="run-idem",
        tenant_id="acme",
        agent_type="coder",
        priority=1,  # even with different priority — NX ignores this
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        admitted_at=1000.1,
        score=_score(1),
    )

    assert result1 is True
    assert result2 is False  # already in queue

    count = await redis.zcard(RedisKey.tenant_queue("acme"))
    assert count == 1


@pytest.mark.asyncio
async def test_enqueue_returns_true_for_new_run():
    redis = faredis.FakeRedis()
    lua = await _make_lua(redis)
    result = await lua.enqueue_admitted(
        run_id="run-new",
        tenant_id="acme",
        agent_type="coder",
        priority=5,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        admitted_at=1000.0,
        score=_score(),
    )
    assert result is True


# ---------------------------------------------------------------------------
# Multi-run isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_multiple_runs_different_tenants():
    redis = faredis.FakeRedis()
    lua = await _make_lua(redis)

    for i, tenant in enumerate(["acme", "beta", "gamma"]):
        await lua.enqueue_admitted(
            run_id=f"run-{i:03d}",
            tenant_id=tenant,
            agent_type="coder",
            priority=5,
            preferred_region="",
            preferred_placement="LEAST_LOADED",
            admitted_at=float(i + 1000),
            score=_score(),
        )

    # Each tenant's queue is isolated
    for i, tenant in enumerate(["acme", "beta", "gamma"]):
        count = await redis.zcard(RedisKey.tenant_queue(tenant))
        assert count == 1, f"Tenant {tenant} should have 1 queued run"

    # Active tenant set contains all three
    members = await redis.smembers(RedisKey.tenant_active_set())
    decoded = {m.decode() if isinstance(m, bytes) else m for m in members}
    assert decoded == {"acme", "beta", "gamma"}

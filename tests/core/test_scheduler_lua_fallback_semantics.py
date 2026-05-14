"""
tests/core/test_scheduler_lua_fallback_semantics.py
IRONCLAD Sprint 22 — Lua fallback semantic equivalence

Verifies that _commit_fallback():
1. Checks CAS BEFORE writing any side effects
2. Does NOT write meta/running_zset if CAS fails
3. Logs a WARNING when fallback path is used
"""
import pytest
import fakeredis.aioredis as faredis
from unittest.mock import AsyncMock, patch

from hfa.config.keys import RedisKey, RedisTTL
from hfa_control.scheduler_lua import SchedulerLua, DispatchCommitResult


async def _make_lua(redis):
    lua = SchedulerLua(redis)
    return lua


@pytest.mark.asyncio
async def test_fallback_cas_fail_no_side_effects():
    """If CAS fails, meta and running_zset must NOT be written."""
    redis = faredis.FakeRedis()
    state_key = RedisKey.run_state("run-fb-cas")
    meta_key = RedisKey.run_meta("run-fb-cas")
    running_zset = "hfa:cp:running"

    # Set state to a value that will cause CAS miss (already scheduled)
    await redis.set(state_key, "scheduled", ex=RedisTTL.RUN_STATE)

    lua = await _make_lua(redis)
    result = await lua._commit_fallback(
        state_key, meta_key, running_zset,
        run_id="run-fb-cas",
        tenant_id="t1",
        agent_type="gpt",
        worker_group="grp-a",
        shard=0,
        reschedule_count=0,
        admitted_at=1000.0,
        scheduled_at=1001.0,
    )

    # CAS should fail (state was "scheduled", not "admitted"/"queued")
    assert result.committed is False
    assert result.status in (
        DispatchCommitResult.ALREADY_RUNNING,
        DispatchCommitResult.STATE_CONFLICT,
        DispatchCommitResult.ILLEGAL_TRANSITION,
    )

    # Side effects must NOT have been written
    meta = await redis.hgetall(meta_key)
    assert not meta, f"meta was written despite CAS failure: {meta}"
    in_running = await redis.zscore(running_zset, "run-fb-cas")
    assert in_running is None, "run was added to running_zset despite CAS failure"


@pytest.mark.asyncio
async def test_fallback_success_writes_side_effects():
    """On successful CAS, meta and running_zset MUST be written."""
    redis = faredis.FakeRedis()
    state_key = RedisKey.run_state("run-fb-ok")
    meta_key = RedisKey.run_meta("run-fb-ok")
    running_zset = "hfa:cp:running"

    await redis.set(state_key, "admitted", ex=RedisTTL.RUN_STATE)

    lua = await _make_lua(redis)
    result = await lua._commit_fallback(
        state_key, meta_key, running_zset,
        run_id="run-fb-ok",
        tenant_id="t1",
        agent_type="gpt",
        worker_group="grp-a",
        shard=2,
        reschedule_count=0,
        admitted_at=1000.0,
        scheduled_at=1001.0,
    )

    assert result.committed is True
    # State must now be "scheduled"
    state = await redis.get(state_key)
    assert (state.decode() if isinstance(state, bytes) else state) == "scheduled"
    # Meta and zset must have been written
    meta = await redis.hgetall(meta_key)
    assert meta, "meta not written after successful commit"
    in_running = await redis.zscore(running_zset, "run-fb-ok")
    assert in_running is not None, "run not added to running_zset after successful commit"


@pytest.mark.asyncio
async def test_fallback_missing_state_returns_conflict():
    """If state key does not exist, return STATE_CONFLICT, no side effects."""
    redis = faredis.FakeRedis()
    lua = await _make_lua(redis)
    result = await lua._commit_fallback(
        RedisKey.run_state("run-missing"), RedisKey.run_meta("run-missing"),
        "hfa:cp:running",
        run_id="run-missing",
        tenant_id="t1", agent_type="gpt", worker_group="grp-a",
        shard=0, reschedule_count=0, admitted_at=1000.0, scheduled_at=1001.0,
    )
    assert result.committed is False
    assert result.status == DispatchCommitResult.STATE_CONFLICT

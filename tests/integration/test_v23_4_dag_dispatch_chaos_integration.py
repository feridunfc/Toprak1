from __future__ import annotations

import asyncio
import time

import pytest

from hfa.config.keys import RedisKey
from hfa_control.scheduler_lua import SchedulerLua

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _lua(redis_client) -> SchedulerLua:
    lua = SchedulerLua(redis_client)
    maybe = getattr(lua, "initialise", None) or getattr(lua, "initialize", None)
    if callable(maybe):
        result = maybe()
        if hasattr(result, "__await__"):
            await result
    return lua


async def _dispatch(
    lua: SchedulerLua,
    *,
    run_id: str,
    tenant_id: str = "tenant-chaos",
    agent_type: str = "default",
    worker_group: str = "grp-a",
    shard: int = 0,
    admitted_at: int | None = None,
    running_zset: str | None = None,
    priority: int = 5,
    payload: dict | None = None,
    trace_parent: str | None = None,
    trace_state: str | None = None,
    policy: str = "LEAST_LOADED",
    region: str = "",
    control_stream: str | None = None,
    shard_stream: str | None = None,
):
    now_ms = admitted_at or int(time.time() * 1000)
    return await lua.dispatch_commit_detailed(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type=agent_type,
        worker_group=worker_group,
        shard=shard,
        admitted_at=now_ms,
        running_zset=running_zset or RedisKey.cp_running(),
        reschedule_count=0,
        priority=priority,
        payload=payload or {"kind": "chaos"},
        trace_parent=trace_parent or "00-chaos-parent-01",
        trace_state=trace_state or "chaos=1",
        policy=policy,
        region=region,
        control_stream=control_stream or RedisKey.stream_control(),
        shard_stream=shard_stream or RedisKey.stream_shard(shard),
    )


@pytest.mark.integration
async def test_duplicate_task_dispatch_race(redis_client):
    """
    Two concurrent dispatch attempts for the same ready run.
    Exactly one may commit; the other must return a non-committing status.
    No duplicate stream emission is allowed.
    """
    run_id = "dag-chaos-race-001"
    state_key = RedisKey.run_state(run_id)
    running_zset = RedisKey.cp_running()
    control_stream = RedisKey.stream_control()
    shard_stream = RedisKey.stream_shard(0)

    await redis_client.set(state_key, "admitted")

    lua1 = await _lua(redis_client)
    lua2 = await _lua(redis_client)

    async def one(lua):
        return await _dispatch(
            lua,
            run_id=run_id,
            running_zset=running_zset,
            control_stream=control_stream,
            shard_stream=shard_stream,
        )

    r1, r2 = await asyncio.gather(one(lua1), one(lua2))
    results = [r1, r2]
    committed = [r for r in results if r.committed]
    rejected = [r for r in results if not r.committed]

    assert len(committed) == 1, f"expected exactly one commit, got {results!r}"
    assert len(rejected) == 1

    state = await redis_client.get(state_key)
    running = await redis_client.zcard(running_zset)
    control_events = await redis_client.xrange(control_stream, "-", "+", 20)
    shard_events = await redis_client.xrange(shard_stream, "-", "+", 20)

    assert state == "scheduled"
    assert running == 1
    assert sum(1 for _, f in control_events if f.get("run_id") == run_id) == 1
    assert sum(1 for _, f in shard_events if f.get("run_id") == run_id) == 1


@pytest.mark.integration
async def test_script_flush_recovery_for_dispatch_commit(redis_client):
    """
    After Redis SCRIPT FLUSH, the next dispatch call must trigger NOSCRIPT recovery,
    reload the Lua script, and still commit atomically.
    """
    run_id = "dag-chaos-flush-001"
    state_key = RedisKey.run_state(run_id)
    running_zset = RedisKey.cp_running()

    await redis_client.set(state_key, "admitted")

    lua = await _lua(redis_client)
    await redis_client.script_flush()

    result = await _dispatch(lua, run_id=run_id, running_zset=running_zset)

    assert result.committed is True
    assert await redis_client.get(state_key) == "scheduled"
    assert await redis_client.zscore(running_zset, run_id) is not None


@pytest.mark.integration
async def test_partial_commit_impossible_on_rejected_state(redis_client):
    """
    If current state is incompatible, the dispatch must leave no partial side effects:
    - state unchanged
    - no running zset member
    - no control event
    - no shard event
    """
    run_id = "dag-chaos-partial-001"
    state_key = RedisKey.run_state(run_id)
    meta_key = RedisKey.run_meta(run_id)
    running_zset = RedisKey.cp_running()
    control_stream = RedisKey.stream_control()
    shard_stream = RedisKey.stream_shard(0)

    await redis_client.set(state_key, "running")

    lua = await _lua(redis_client)
    result = await _dispatch(
        lua,
        run_id=run_id,
        running_zset=running_zset,
        control_stream=control_stream,
        shard_stream=shard_stream,
    )

    assert result.committed is False
    assert await redis_client.get(state_key) == "running"
    assert await redis_client.zscore(running_zset, run_id) is None
    assert await redis_client.hgetall(meta_key) == {}
    control_events = await redis_client.xrange(control_stream, "-", "+", 20)
    shard_events = await redis_client.xrange(shard_stream, "-", "+", 20)
    assert not any(f.get("run_id") == run_id for _, f in control_events)
    assert not any(f.get("run_id") == run_id for _, f in shard_events)


@pytest.mark.integration
async def test_double_dispatch_idempotent_after_success(redis_client):
    """
    First dispatch commits, second sequential attempt must not create duplicates.
    """
    run_id = "dag-chaos-idem-001"
    state_key = RedisKey.run_state(run_id)
    running_zset = RedisKey.cp_running()
    control_stream = RedisKey.stream_control()
    shard_stream = RedisKey.stream_shard(0)

    await redis_client.set(state_key, "admitted")
    lua = await _lua(redis_client)

    first = await _dispatch(
        lua,
        run_id=run_id,
        running_zset=running_zset,
        control_stream=control_stream,
        shard_stream=shard_stream,
    )
    second = await _dispatch(
        lua,
        run_id=run_id,
        running_zset=running_zset,
        control_stream=control_stream,
        shard_stream=shard_stream,
    )

    assert first.committed is True
    assert second.committed is False

    assert await redis_client.zcard(running_zset) == 1
    control_events = await redis_client.xrange(control_stream, "-", "+", 20)
    shard_events = await redis_client.xrange(shard_stream, "-", "+", 20)

    assert sum(1 for _, f in control_events if f.get("run_id") == run_id) == 1
    assert sum(1 for _, f in shard_events if f.get("run_id") == run_id) == 1


@pytest.mark.integration
async def test_dispatch_commit_preserves_trace_fields_exactly(redis_client):
    """
    Trace fields must survive the Lua path exactly, without silent mutation.
    """
    run_id = "dag-chaos-trace-001"
    trace_parent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
    trace_state = "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"

    await redis_client.set(RedisKey.run_state(run_id), "admitted")
    lua = await _lua(redis_client)

    result = await _dispatch(
        lua,
        run_id=run_id,
        trace_parent=trace_parent,
        trace_state=trace_state,
    )

    assert result.committed is True

    meta = await redis_client.hgetall(RedisKey.run_meta(run_id))
    control_events = await redis_client.xrange(RedisKey.stream_control(), "-", "+", 20)
    shard_events = await redis_client.xrange(RedisKey.stream_shard(0), "-", "+", 20)

    assert meta.get("trace_parent") == trace_parent
    assert meta.get("trace_state") == trace_state

    control = next(f for _, f in control_events if f.get("run_id") == run_id)
    shard = next(f for _, f in shard_events if f.get("run_id") == run_id)

    assert control.get("trace_parent") == trace_parent
    assert control.get("trace_state") == trace_state
    assert shard.get("trace_parent") == trace_parent
    assert shard.get("trace_state") == trace_state

from __future__ import annotations

import os
import time

import pytest

from hfa.config.keys import RedisKey
from hfa.state import TransitionResult, transition_state
from hfa_control.scheduler_lua import SchedulerLua
from hfa_control.scheduler_reasons import STATE_CONFLICT

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _strict_enabled() -> bool:
    return os.getenv("HFA_STRICT_CAS_MODE") == "1"


async def _new_scheduler_lua(redis_client) -> SchedulerLua:
    lua = SchedulerLua(redis_client)
    maybe = getattr(lua, "initialise", None) or getattr(lua, "initialize", None)
    if callable(maybe):
        result = maybe()
        if hasattr(result, "__await__"):
            await result
    return lua


@pytest.mark.integration
async def test_atomic_dispatch_god_script_execution(redis_client):
    assert _strict_enabled()

    run_id = "run-atomic-001"
    now_ms = int(time.time() * 1000)

    state_key = RedisKey.run_state(run_id)
    meta_key = RedisKey.run_meta(run_id)
    running_key = RedisKey.cp_running()
    control_stream = RedisKey.stream_control()
    shard_stream = RedisKey.stream_shard(0)

    await redis_client.set(state_key, "admitted")

    lua = await _new_scheduler_lua(redis_client)

    result = await lua.dispatch_commit_detailed(
        run_id=run_id,
        tenant_id="tenant-a",
        agent_type="default",
        worker_group="grp-a",
        shard=0,
        admitted_at=now_ms,
        running_zset=running_key,
    )

    assert result.committed is True

    state = await redis_client.get(state_key)
    meta = await redis_client.hgetall(meta_key)
    running = await redis_client.zscore(running_key, run_id)

    control_events = await redis_client.xrange(control_stream, "-", "+", 10)
    shard_events = await redis_client.xrange(shard_stream, "-", "+", 10)

    assert state == "scheduled"
    assert meta["worker_group"] == "grp-a"
    assert running is not None
    assert any(e[1].get("run_id") == run_id for e in control_events)
    assert any(e[1].get("run_id") == run_id for e in shard_events)


@pytest.mark.integration
async def test_strict_mode_hard_fails_on_illegal_transition(redis_client):
    assert _strict_enabled()

    run_id = "run-illegal-001"
    state_key = RedisKey.run_state(run_id)

    await redis_client.set(state_key, "running")

    result = await transition_state(
        redis_client,
        run_id,
        "admitted",
        state_key=state_key,
    )

    assert isinstance(result, TransitionResult)
    assert result.ok is False
    assert result.reason in {"illegal_transition", STATE_CONFLICT}


@pytest.mark.integration
async def test_zero_silent_drop_when_dispatch_rejected(redis_client):
    assert _strict_enabled()

    run_id = "run-no-drop-001"
    now_ms = int(time.time() * 1000)

    state_key = RedisKey.run_state(run_id)
    meta_key = RedisKey.run_meta(run_id)
    running_key = RedisKey.cp_running()
    control_stream = RedisKey.stream_control()
    shard_stream = RedisKey.stream_shard(0)

    await redis_client.set(state_key, "running")  # CAS miss

    lua = await _new_scheduler_lua(redis_client)

    result = await lua.dispatch_commit_detailed(
        run_id=run_id,
        tenant_id="tenant-a",
        agent_type="default",
        worker_group="grp-a",
        shard=0,
        admitted_at=now_ms,
        running_zset=running_key,
    )

    assert result.committed is False

    state = await redis_client.get(state_key)
    meta = await redis_client.hgetall(meta_key)
    running = await redis_client.zscore(running_key, run_id)

    control_events = await redis_client.xrange(control_stream, "-", "+", 20)
    shard_events = await redis_client.xrange(shard_stream, "-", "+", 20)

    assert state == "running"
    assert meta == {}
    assert running is None
    assert not any(e[1].get("run_id") == run_id for e in control_events)
    assert not any(e[1].get("run_id") == run_id for e in shard_events)

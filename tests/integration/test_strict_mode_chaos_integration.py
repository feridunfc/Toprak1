from __future__ import annotations

import time

import pytest
import redis.asyncio as redis_async

from hfa.config.keys import RedisKey, RedisTTL
from hfa_control.models import ControlPlaneConfig
from hfa_control.recovery import RecoveryService
from hfa_control.scheduler_lua import DispatchCommitResult, SchedulerLua

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

REDIS_URL = "redis://127.0.0.1:6389/0"


async def _lua(redis_client):
    lua = SchedulerLua(redis_client)
    await lua.initialise()
    return lua


@pytest.mark.integration
async def test_scheduler_lua_survives_client_reconnect():
    client_a = redis_async.from_url(REDIS_URL, decode_responses=True)
    try:
        await client_a.flushdb()
        await client_a.set(RedisKey.run_state("reconnect-001"), "admitted")
        lua_a = await _lua(client_a)
        first = await lua_a.dispatch_commit_detailed(
            run_id="reconnect-001",
            tenant_id="tenant-reconnect",
            agent_type="default",
            worker_group="grp-a",
            shard=0,
            admitted_at=time.time(),
            running_zset=RedisKey.cp_running(),
        )
        assert first.committed is True
    finally:
        await client_a.aclose()

    client_b = redis_async.from_url(REDIS_URL, decode_responses=True)
    try:
        await client_b.set(RedisKey.run_state("reconnect-002"), "admitted")
        lua_b = await _lua(client_b)
        second = await lua_b.dispatch_commit_detailed(
            run_id="reconnect-002",
            tenant_id="tenant-reconnect",
            agent_type="default",
            worker_group="grp-a",
            shard=0,
            admitted_at=time.time(),
            running_zset=RedisKey.cp_running(),
        )
        assert second.committed is True
    finally:
        await client_b.aclose()


@pytest.mark.integration
async def test_duplicate_dispatch_attempt_returns_already_running_under_strict_mode(redis_client):
    run_id = "dup-chaos-001"
    await redis_client.set(RedisKey.run_state(run_id), "admitted")
    lua = await _lua(redis_client)

    first = await lua.dispatch_commit_detailed(
        run_id=run_id,
        tenant_id="tenant-dup",
        agent_type="default",
        worker_group="grp-a",
        shard=0,
        admitted_at=time.time(),
        running_zset=RedisKey.cp_running(),
    )
    second = await lua.dispatch_commit_detailed(
        run_id=run_id,
        tenant_id="tenant-dup",
        agent_type="default",
        worker_group="grp-a",
        shard=0,
        admitted_at=time.time(),
        running_zset=RedisKey.cp_running(),
    )

    assert first.status == DispatchCommitResult.SUCCESS
    assert second.status == DispatchCommitResult.ALREADY_RUNNING


@pytest.mark.integration
async def test_recovery_reschedule_after_script_flush_under_strict_mode(redis_client):
    config = ControlPlaneConfig(strict_cas_mode=True, stale_run_timeout=1.0)
    svc = RecoveryService(redis_client, config)
    run_id = "stale-strict-001"
    tenant_id = "tenant-stale"

    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "tenant_id": tenant_id,
            "agent_type": "default",
            "worker_group": "grp-a",
            "reschedule_count": "0",
        },
    )
    await redis_client.expire(RedisKey.run_meta(run_id), RedisTTL.RUN_META)
    await redis_client.set(RedisKey.run_state(run_id), "running", ex=RedisTTL.RUN_STATE)
    await redis_client.zadd(config.running_zset, {run_id: time.time() - 100})

    # Force NOSCRIPT on the next state-machine Lua call.
    await redis_client.script_flush()

    result = await svc._reschedule(
        run_id,
        tenant_id,
        agent_type="default",
        prev_worker="grp-a",
        reschedule_count=0,
    )
    assert result == "rescheduled"

    state = await redis_client.get(RedisKey.run_state(run_id))
    assert state == "rescheduled"

    events = [e for e in await redis_client.xrange(config.control_stream, "-", "+", 50) if e[1].get("run_id") == run_id]
    event_types = {e[1].get("event_type") for e in events}
    assert "RunRescheduled" in event_types
    assert "RunAdmitted" in event_types

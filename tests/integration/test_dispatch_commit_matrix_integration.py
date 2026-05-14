from __future__ import annotations

import json
import time

import pytest

from hfa.config.keys import RedisKey
from hfa_control.scheduler_lua import DispatchCommitResult, SchedulerLua

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _lua(redis_client) -> SchedulerLua:
    lua = SchedulerLua(redis_client)
    await lua.initialise()
    return lua


async def _commit(redis_client, run_id: str, *, state: str, shard: int = 0, **kwargs):
    state_key = RedisKey.run_state(run_id)
    if state is None:
        await redis_client.delete(state_key)
    else:
        await redis_client.set(state_key, state)
    lua = await _lua(redis_client)
    return await lua.dispatch_commit_detailed(
        run_id=run_id,
        tenant_id=kwargs.get("tenant_id", "tenant-matrix"),
        agent_type=kwargs.get("agent_type", "default"),
        worker_group=kwargs.get("worker_group", "grp-a"),
        shard=shard,
        admitted_at=kwargs.get("admitted_at", time.time()),
        running_zset=kwargs.get("running_zset", RedisKey.cp_running()),
        priority=kwargs.get("priority", 7),
        payload=kwargs.get("payload", {"hello": "world"}),
        trace_parent=kwargs.get("trace_parent", "00-abc-xyz-01"),
        trace_state=kwargs.get("trace_state", "vendor=test"),
        policy=kwargs.get("policy", "LEAST_LOADED"),
        region=kwargs.get("region", "eu-west-1"),
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("initial_state", "expected_status", "should_commit"),
    [
        ("admitted", DispatchCommitResult.SUCCESS, True),
        ("queued", DispatchCommitResult.SUCCESS, True),
        ("scheduled", DispatchCommitResult.ALREADY_RUNNING, False),
        ("running", DispatchCommitResult.ALREADY_RUNNING, False),
        ("done", DispatchCommitResult.ILLEGAL_TRANSITION, False),
        ("failed", DispatchCommitResult.ILLEGAL_TRANSITION, False),
        ("rejected", DispatchCommitResult.ILLEGAL_TRANSITION, False),
        ("dead_lettered", DispatchCommitResult.ILLEGAL_TRANSITION, False),
        (None, DispatchCommitResult.STATE_CONFLICT, False),
    ],
)
async def test_dispatch_commit_result_matrix(redis_client, initial_state, expected_status, should_commit):
    run_id = f"matrix-{initial_state or 'missing'}"
    result = await _commit(redis_client, run_id, state=initial_state)
    assert result.status == expected_status
    assert result.committed is should_commit


@pytest.mark.integration
async def test_stream_payload_fields_are_exact(redis_client):
    run_id = "stream-fields-001"
    running_zset = RedisKey.cp_running()
    result = await _commit(
        redis_client,
        run_id,
        state="admitted",
        tenant_id="tenant-stream",
        agent_type="agent-x",
        worker_group="grp-z",
        shard=3,
        admitted_at=1711111111.125,
        running_zset=running_zset,
        priority=9,
        payload={"job": "demo", "count": 2},
        trace_parent="00-4bf92f3577b34da6a3ce929d0e0e4736-00aa00aa00aa00aa-01",
        trace_state="rojo=00f067aa0ba902b7",
        policy="STRICT_AFFINITY",
        region="eu-central-1",
    )
    assert result.committed is True

    control_events = await redis_client.xrange(RedisKey.stream_control(), "-", "+", 10)
    shard_events = await redis_client.xrange(RedisKey.stream_shard(3), "-", "+", 10)
    assert len(control_events) == 1
    assert len(shard_events) == 1

    control = control_events[0][1]
    shard = shard_events[0][1]

    assert control["event_type"] == "RunScheduled"
    assert control["run_id"] == run_id
    assert control["tenant_id"] == "tenant-stream"
    assert control["agent_type"] == "agent-x"
    assert control["worker_group"] == "grp-z"
    assert control["shard"] == "3"
    assert control["region"] == "eu-central-1"
    assert control["policy"] == "STRICT_AFFINITY"
    assert control["trace_parent"] == "00-4bf92f3577b34da6a3ce929d0e0e4736-00aa00aa00aa00aa-01"
    assert control["trace_state"] == "rojo=00f067aa0ba902b7"

    assert shard["event_type"] == "RunRequested"
    assert shard["run_id"] == run_id
    assert shard["tenant_id"] == "tenant-stream"
    assert shard["agent_type"] == "agent-x"
    assert shard["priority"] == "9"
    assert json.loads(shard["payload"]) == {"job": "demo", "count": 2}
    assert shard["trace_parent"] == "00-4bf92f3577b34da6a3ce929d0e0e4736-00aa00aa00aa00aa-01"
    assert shard["trace_state"] == "rojo=00f067aa0ba902b7"


@pytest.mark.integration
async def test_duplicate_dispatch_attempt_is_non_committing_and_non_duplicating(redis_client):
    run_id = "dup-dispatch-001"
    running_zset = RedisKey.cp_running()

    first = await _commit(redis_client, run_id, state="admitted", running_zset=running_zset)
    second = await _commit(redis_client, run_id, state="scheduled", running_zset=running_zset)

    assert first.committed is True
    assert second.committed is False
    assert second.status == DispatchCommitResult.ALREADY_RUNNING

    control_events = [e for e in await redis_client.xrange(RedisKey.stream_control(), "-", "+", 100) if e[1].get("run_id") == run_id]
    shard_events = [e for e in await redis_client.xrange(RedisKey.stream_shard(0), "-", "+", 100) if e[1].get("run_id") == run_id]
    assert len(control_events) == 1
    assert len(shard_events) == 1


@pytest.mark.integration
async def test_dispatch_commit_loader_recovers_after_script_flush(redis_client):
    run_id_a = "flush-a"
    run_id_b = "flush-b"
    running_zset = RedisKey.cp_running()
    lua = await _lua(redis_client)

    await redis_client.set(RedisKey.run_state(run_id_a), "admitted")
    first = await lua.dispatch_commit_detailed(
        run_id=run_id_a,
        tenant_id="tenant-a",
        agent_type="default",
        worker_group="grp-a",
        shard=0,
        admitted_at=time.time(),
        running_zset=running_zset,
    )
    assert first.committed is True

    await redis_client.script_flush()

    await redis_client.set(RedisKey.run_state(run_id_b), "admitted")
    second = await lua.dispatch_commit_detailed(
        run_id=run_id_b,
        tenant_id="tenant-a",
        agent_type="default",
        worker_group="grp-a",
        shard=0,
        admitted_at=time.time(),
        running_zset=running_zset,
    )
    assert second.committed is True

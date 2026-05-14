from __future__ import annotations

import time

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey, DagTaskSeed, DagTaskDispatchInput
from hfa_control.dag_lua import DagLua

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _dag(redis_client) -> DagLua:
    lua = DagLua(redis_client)
    await lua.initialise()
    return lua


async def _seed_root(redis_client, *, task_id: str = 'task-root-001', run_id: str = 'run-dag-001', tenant_id: str = 'tenant-dag'):
    lua = await _dag(redis_client)
    seed = DagTaskSeed(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type='default',
        priority=5,
        admitted_at=time.time(),
        payload_json='{"hello":"dag"}',
        dependency_count=0,
    )
    admitted = await lua.task_admit(seed)
    assert admitted.admitted is True
    return lua, seed


@pytest.mark.integration
async def test_task_dispatch_commit_ready_to_scheduled(redis_client):
    lua, seed = await _seed_root(redis_client)
    dispatch = DagTaskDispatchInput(
        task_id=seed.task_id,
        run_id=seed.run_id,
        tenant_id=seed.tenant_id,
        agent_type=seed.agent_type,
        worker_group='grp-a',
        shard=0,
        priority=seed.priority,
        admitted_at=seed.admitted_at,
        scheduled_at=time.time(),
        running_zset=DagRedisKey.task_running_zset(seed.tenant_id),
        control_stream=RedisKey.stream_control(),
        shard_stream=RedisKey.stream_shard(0),
        region='eu-west',
        policy='LEAST_LOADED',
        payload_json=seed.payload_json,
    )
    result = await lua.task_dispatch_commit(dispatch)
    assert result.committed is True

    state = await redis_client.get(DagRedisKey.task_state(seed.task_id))
    meta = await redis_client.hgetall(DagRedisKey.task_meta(seed.task_id))
    running = await redis_client.zscore(DagRedisKey.task_running_zset(seed.tenant_id), seed.task_id)
    control_events = await redis_client.xrange(RedisKey.stream_control(), '-', '+', 10)
    shard_events = await redis_client.xrange(RedisKey.stream_shard(0), '-', '+', 10)

    assert state == 'scheduled'
    assert meta['worker_group'] == 'grp-a'
    assert meta['shard'] == '0'
    assert running is not None
    assert any(e[1].get('task_id') == seed.task_id and e[1].get('event_type') == 'TaskScheduled' for e in control_events)
    assert any(e[1].get('task_id') == seed.task_id and e[1].get('event_type') == 'TaskRequested' for e in shard_events)


@pytest.mark.integration
async def test_task_dispatch_commit_duplicate_is_non_committing(redis_client):
    lua, seed = await _seed_root(redis_client, task_id='task-root-dup')
    dispatch = DagTaskDispatchInput(
        task_id=seed.task_id,
        run_id=seed.run_id,
        tenant_id=seed.tenant_id,
        agent_type=seed.agent_type,
        worker_group='grp-a',
        shard=0,
        priority=seed.priority,
        admitted_at=seed.admitted_at,
        scheduled_at=time.time(),
        running_zset=DagRedisKey.task_running_zset(seed.tenant_id),
        control_stream=RedisKey.stream_control(),
        shard_stream=RedisKey.stream_shard(0),
        payload_json=seed.payload_json,
    )
    first = await lua.task_dispatch_commit(dispatch)
    second = await lua.task_dispatch_commit(dispatch)
    assert first.committed is True
    assert second.committed is False
    assert second.status == 'already_running'


@pytest.mark.integration
async def test_task_dispatch_commit_illegal_terminal_state(redis_client):
    lua, seed = await _seed_root(redis_client, task_id='task-root-terminal')
    await redis_client.set(DagRedisKey.task_state(seed.task_id), 'done')
    dispatch = DagTaskDispatchInput(
        task_id=seed.task_id,
        run_id=seed.run_id,
        tenant_id=seed.tenant_id,
        agent_type=seed.agent_type,
        worker_group='grp-a',
        shard=0,
        priority=seed.priority,
        admitted_at=seed.admitted_at,
        scheduled_at=time.time(),
        running_zset=DagRedisKey.task_running_zset(seed.tenant_id),
        control_stream=RedisKey.stream_control(),
        shard_stream=RedisKey.stream_shard(0),
    )
    result = await lua.task_dispatch_commit(dispatch)
    assert result.committed is False
    assert result.status == 'illegal_transition'

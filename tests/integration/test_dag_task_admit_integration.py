
from __future__ import annotations

import pytest

from hfa.dag.schema import DagRedisKey, DagTaskSeed
from hfa_control.dag_lua import DagLua

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.mark.integration
async def test_task_admit_root_emits_ready(redis_client):
    dag = DagLua(redis_client)
    await dag.initialise()

    task = DagTaskSeed(
        task_id='task-root-001',
        tenant_id='tenant-dag',
        run_id='run-dag-001',
        agent_type='default',
        priority=5,
        admitted_at=1000.0,
        dependency_count=0,
        child_task_ids=('task-child-001',),
    )
    result = await dag.task_admit(task)
    assert result.admitted is True
    assert result.ready is True

    state = await redis_client.get(DagRedisKey.task_state(task.task_id))
    ready_score = await redis_client.zscore(DagRedisKey.task_ready_queue(task.tenant_id), task.task_id)
    remaining = await redis_client.get(DagRedisKey.task_remaining_deps(task.task_id))
    children = await redis_client.smembers(DagRedisKey.task_children(task.task_id))

    assert state == 'ready'
    assert ready_score is not None
    assert str(remaining) == '0'
    assert 'task-child-001' in children


@pytest.mark.integration
async def test_task_admit_waiting_does_not_emit_ready(redis_client):
    dag = DagLua(redis_client)
    task = DagTaskSeed(
        task_id='task-wait-001',
        tenant_id='tenant-dag',
        run_id='run-dag-001',
        agent_type='default',
        priority=5,
        admitted_at=1000.0,
        dependency_count=2,
    )
    result = await dag.task_admit(task)
    assert result.admitted is True
    assert result.ready is False
    assert await redis_client.get(DagRedisKey.task_state(task.task_id)) == 'pending'
    assert await redis_client.zscore(DagRedisKey.task_ready_queue(task.tenant_id), task.task_id) is None


@pytest.mark.integration
async def test_task_admit_is_idempotent(redis_client):
    dag = DagLua(redis_client)
    task = DagTaskSeed(
        task_id='task-idem-001',
        tenant_id='tenant-dag',
        run_id='run-dag-001',
        agent_type='default',
        priority=5,
        admitted_at=1000.0,
    )
    first = await dag.task_admit(task)
    second = await dag.task_admit(task)
    assert first.status == 'seeded_root'
    assert second.status == 'already_exists'

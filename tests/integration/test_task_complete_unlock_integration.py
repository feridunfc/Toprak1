from __future__ import annotations

import time

import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _lua(redis_client) -> DagLua:
    lua = DagLua(redis_client)
    await lua.initialise()
    return lua


@pytest.mark.integration
async def test_task_complete_done_unlocks_direct_child(redis_client):
    tenant_id = "tenant-dag"
    run_id = "run-dag-001"
    parent = "task-parent"
    child = "task-child"

    await redis_client.set(DagRedisKey.task_state(parent), "running")
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.sadd(DagRedisKey.task_children(parent), child)
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)

    lua = await _lua(redis_client)
    result = await lua.task_complete(
        task_id=parent,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=int(time.time() * 1000),
    )

    assert result.completed is True
    assert result.unlocked_count == 1
    assert await redis_client.get(DagRedisKey.task_state(child)) == "ready"
    assert await redis_client.zcard(DagRedisKey.tenant_ready_queue(tenant_id)) == 1


@pytest.mark.integration
async def test_task_complete_failed_does_not_unlock_children(redis_client):
    tenant_id = "tenant-dag"
    run_id = "run-dag-002"
    parent = "task-parent-f"
    child = "task-child-f"

    await redis_client.set(DagRedisKey.task_state(parent), "running")
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.sadd(DagRedisKey.task_children(parent), child)
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)

    lua = await _lua(redis_client)
    result = await lua.task_complete(
        task_id=parent,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="failed",
        finished_at_ms=int(time.time() * 1000),
        reason_code="agent_error",
    )

    assert result.completed is True
    assert result.unlocked_count == 0
    assert await redis_client.get(DagRedisKey.task_state(child)) == "pending"
    assert await redis_client.zcard(DagRedisKey.tenant_ready_queue(tenant_id)) == 0


@pytest.mark.integration
async def test_task_complete_is_idempotent(redis_client):
    tenant_id = "tenant-dag"
    run_id = "run-dag-003"
    parent = "task-parent-i"
    child = "task-child-i"

    await redis_client.set(DagRedisKey.task_state(parent), "running")
    await redis_client.set(DagRedisKey.task_state(child), "pending")
    await redis_client.sadd(DagRedisKey.task_children(parent), child)
    await redis_client.set(DagRedisKey.task_remaining_deps(child), 1)

    lua = await _lua(redis_client)
    first = await lua.task_complete(
        task_id=parent,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=int(time.time() * 1000),
    )
    second = await lua.task_complete(
        task_id=parent,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=int(time.time() * 1000),
    )

    assert first.completed is True
    assert second.status == "already_terminal"
    assert await redis_client.zcard(DagRedisKey.tenant_ready_queue(tenant_id)) == 1

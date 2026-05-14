
import time
import pytest

from hfa.dag.reasons import TASK_OWNER_MISMATCH
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua

pytestmark = pytest.mark.asyncio


async def _lua(redis_client) -> DagLua:
    lua = DagLua(redis_client)
    await lua.initialise()
    return lua


@pytest.mark.integration
async def test_correct_owner_can_complete(redis_client):
    task_id = "complete-owner-001"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"worker_instance_id": "worker-1"})

    lua = await _lua(redis_client)
    result = await lua.task_complete(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=int(time.time() * 1000),
        worker_instance_id="worker-1",
    )

    assert result.completed is True
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "done"


@pytest.mark.integration
async def test_wrong_owner_rejected(redis_client):
    task_id = "complete-owner-002"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"worker_instance_id": "worker-1"})

    lua = await _lua(redis_client)
    result = await lua.task_complete(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=int(time.time() * 1000),
        worker_instance_id="worker-2",
    )

    assert result.completed is False
    assert result.status == TASK_OWNER_MISMATCH
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"


@pytest.mark.integration
async def test_late_completion_after_requeue_rejected(redis_client):
    task_id = "complete-owner-003"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"worker_instance_id": "worker-b"})

    lua = await _lua(redis_client)
    result = await lua.task_complete(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=int(time.time() * 1000),
        worker_instance_id="worker-a",
    )

    assert result.completed is False
    assert result.status == TASK_OWNER_MISMATCH
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"

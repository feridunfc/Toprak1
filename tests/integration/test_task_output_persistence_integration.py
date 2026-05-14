
import time
import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua

pytestmark = pytest.mark.asyncio

async def _lua(redis_client) -> DagLua:
    lua = DagLua(redis_client)
    await lua.initialise()
    return lua

@pytest.mark.integration
async def test_successful_task_saves_output(redis_client):
    task_id = "output-001"
    tenant_id = "tenant-a"
    output = '{"code":"print(1)","status":"ok"}'
    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"worker_instance_id": "worker-1"})
    lua = await _lua(redis_client)
    result = await lua.task_complete(
        task_id=task_id, run_id="run-1", tenant_id=tenant_id, terminal_state="done",
        finished_at_ms=int(time.time() * 1000), worker_instance_id="worker-1", output_data=output,
    )
    assert result.completed is True
    saved = await redis_client.get(DagRedisKey.task_output(task_id))
    assert saved == output

@pytest.mark.integration
async def test_failed_task_does_not_save_output(redis_client):
    task_id = "output-002"
    tenant_id = "tenant-a"
    output = '{"error":"boom"}'
    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"worker_instance_id": "worker-1"})
    lua = await _lua(redis_client)
    result = await lua.task_complete(
        task_id=task_id, run_id="run-1", tenant_id=tenant_id, terminal_state="failed",
        finished_at_ms=int(time.time() * 1000), worker_instance_id="worker-1", output_data=output,
    )
    assert result.completed is True
    saved = await redis_client.get(DagRedisKey.task_output(task_id))
    assert saved is None

@pytest.mark.integration
async def test_output_ttl_matches_dag(redis_client):
    task_id = "output-003"
    tenant_id = "tenant-a"
    output = '{"result":42}'
    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"worker_instance_id": "worker-1"})
    lua = await _lua(redis_client)
    result = await lua.task_complete(
        task_id=task_id, run_id="run-1", tenant_id=tenant_id, terminal_state="done",
        finished_at_ms=int(time.time() * 1000), worker_instance_id="worker-1", output_data=output,
    )
    assert result.completed is True
    ttl = await redis_client.ttl(DagRedisKey.task_output(task_id))
    assert ttl is not None
    assert ttl > 0


import pytest

from hfa_control.task_recovery import TaskHeartbeatManager, TaskRecoveryManager
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_record_heartbeat_updates_meta(redis_client):
    task_id = "hb-001"
    tenant_id = "tenant-a"
    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    mgr = TaskHeartbeatManager(redis_client)

    result = await mgr.record_heartbeat(task_id=task_id, tenant_id=tenant_id, worker_id="worker-1", now_ms=123456)

    assert result.ok is True
    meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    assert meta["heartbeat_owner"] == "worker-1"
    assert meta["heartbeat_at_ms"] == "123456"


@pytest.mark.integration
async def test_requeue_stale_task_moves_running_to_ready(redis_client):
    task_id = "rq-001"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"heartbeat_at_ms": "1000", "requeue_count": "0"})
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1000})

    mgr = TaskRecoveryManager(redis_client, HeartbeatPolicy(stale_after_ms=100, max_requeue_count=3))
    await mgr.initialise()
    result = await mgr.requeue_stale_task(task_id=task_id, tenant_id=tenant_id, now_ms=5000)

    assert result.status == "TASK_REQUEUED"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "ready"
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) is None
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), task_id) is not None


@pytest.mark.integration
async def test_requeue_stale_task_exhausts_to_failed(redis_client):
    task_id = "rq-002"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"heartbeat_at_ms": "1000", "requeue_count": "3"})
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1000})

    mgr = TaskRecoveryManager(redis_client, HeartbeatPolicy(stale_after_ms=100, max_requeue_count=3))
    await mgr.initialise()
    result = await mgr.requeue_stale_task(task_id=task_id, tenant_id=tenant_id, now_ms=5000)

    assert result.status == "TASK_RETRY_EXHAUSTED"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "failed"

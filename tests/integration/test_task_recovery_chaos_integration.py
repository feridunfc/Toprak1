
import pytest

from hfa_control.task_recovery import TaskHeartbeatManager, TaskRecoveryManager
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.reasons import TASK_HEARTBEAT_OWNER_MISMATCH
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_fresh_heartbeat_not_marked_stale(redis_client):
    task_id = "chaos-fresh-001"
    tenant_id = "tenant-chaos"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"heartbeat_at_ms": "4900"})
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 4900})

    mgr = TaskRecoveryManager(redis_client, HeartbeatPolicy(stale_after_ms=200, max_requeue_count=3))
    stale = await mgr.find_stale_tasks(tenant_id=tenant_id, now_ms=5000)

    assert stale == []


@pytest.mark.integration
async def test_owner_mismatch_heartbeat_is_rejected(redis_client):
    task_id = "chaos-owner-001"
    tenant_id = "tenant-chaos"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={
        "heartbeat_owner": "worker-a",
        "heartbeat_at_ms": "1000",
    })

    mgr = TaskHeartbeatManager(redis_client)
    result = await mgr.record_heartbeat(
        task_id=task_id,
        tenant_id=tenant_id,
        worker_id="worker-b",
        now_ms=2000,
    )

    assert result.ok is False
    assert result.status == TASK_HEARTBEAT_OWNER_MISMATCH


@pytest.mark.integration
async def test_requeue_twice_is_idempotent_after_first_requeue(redis_client):
    task_id = "chaos-rq-001"
    tenant_id = "tenant-chaos"

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"heartbeat_at_ms": "1000", "requeue_count": "0"})
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1000})

    mgr = TaskRecoveryManager(redis_client, HeartbeatPolicy(stale_after_ms=100, max_requeue_count=3))
    await mgr.initialise()

    first = await mgr.requeue_stale_task(task_id=task_id, tenant_id=tenant_id, now_ms=5000)
    second = await mgr.requeue_stale_task(task_id=task_id, tenant_id=tenant_id, now_ms=6000)

    assert first.status == "TASK_REQUEUED"
    assert second.status == "TASK_ALREADY_REQUEUED"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "ready"


@pytest.mark.integration
async def test_terminal_task_not_requeued(redis_client):
    task_id = "chaos-terminal-001"
    tenant_id = "tenant-chaos"

    await redis_client.set(DagRedisKey.task_state(task_id), "failed")
    await redis_client.hset(DagRedisKey.task_meta(task_id), mapping={"heartbeat_at_ms": "1000", "requeue_count": "0"})

    mgr = TaskRecoveryManager(redis_client, HeartbeatPolicy(stale_after_ms=100, max_requeue_count=3))
    await mgr.initialise()
    result = await mgr.requeue_stale_task(task_id=task_id, tenant_id=tenant_id, now_ms=5000)

    assert result.status == "TASK_TERMINAL"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "failed"

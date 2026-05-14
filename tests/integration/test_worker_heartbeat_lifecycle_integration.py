
import asyncio
import pytest

from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.schema import DagRedisKey
from hfa_control.task_claim import TaskClaimManager
from hfa_control.task_recovery import TaskHeartbeatManager, TaskRecoveryManager
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

pytestmark = pytest.mark.asyncio


class SlowExecutor(TaskExecutor):
    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        await asyncio.sleep(0.15)
        return TaskExecutionResult(ok=True, output={"done": True, "task_id": ctx.task_id})


@pytest.mark.integration
async def test_running_task_gets_heartbeat_updates(redis_client):
    task_id = "hb-loop-001"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")

    claim_mgr = TaskClaimManager(redis_client)
    hb_mgr = TaskHeartbeatManager(redis_client)
    consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=SlowExecutor(),
        heartbeat_manager=hb_mgr,
        heartbeat_interval_ms=30,
    )

    ctx = TaskContext(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        agent_type="default",
        worker_group="grp-a",
        worker_instance_id="worker-1",
        payload={},
    )

    task = asyncio.create_task(consumer.consume_once(ctx, claimed_at_ms=1000))
    await asyncio.sleep(0.08)

    meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    assert meta.get("heartbeat_owner") == "worker-1"
    assert int(meta.get("heartbeat_at_ms", "0")) > 0

    result = await task
    assert result.claimed.ok is True
    assert result.executed is not None
    assert result.executed.ok is True


@pytest.mark.integration
async def test_long_running_task_not_marked_stale_while_heartbeating(redis_client):
    task_id = "hb-loop-002"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")

    claim_mgr = TaskClaimManager(redis_client)
    hb_mgr = TaskHeartbeatManager(redis_client)
    recovery_mgr = TaskRecoveryManager(redis_client, HeartbeatPolicy(stale_after_ms=500, heartbeat_interval_ms=50))
    consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=SlowExecutor(),
        heartbeat_manager=hb_mgr,
        heartbeat_interval_ms=50,
    )

    ctx = TaskContext(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        agent_type="default",
        worker_group="grp-a",
        worker_instance_id="worker-1",
        payload={},
    )

    running_task = asyncio.create_task(consumer.consume_once(ctx, claimed_at_ms=1000))
    await asyncio.sleep(0.12)
    stale = await recovery_mgr.find_stale_tasks(tenant_id=tenant_id, now_ms=1300)
    assert stale == []

    await running_task


@pytest.mark.integration
async def test_heartbeat_stops_after_execution_finishes(redis_client):
    task_id = "hb-loop-003"
    tenant_id = "tenant-a"

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")

    claim_mgr = TaskClaimManager(redis_client)
    hb_mgr = TaskHeartbeatManager(redis_client)
    consumer = TaskConsumer(
        claim_manager=claim_mgr,
        executor=SlowExecutor(),
        heartbeat_manager=hb_mgr,
        heartbeat_interval_ms=30,
    )

    ctx = TaskContext(
        task_id=task_id,
        run_id="run-1",
        tenant_id=tenant_id,
        agent_type="default",
        worker_group="grp-a",
        worker_instance_id="worker-1",
        payload={},
    )

    await consumer.consume_once(ctx, claimed_at_ms=1000)
    meta_before = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    hb_before = int(meta_before.get("heartbeat_at_ms", "0"))

    await asyncio.sleep(0.12)
    meta_after = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    hb_after = int(meta_after.get("heartbeat_at_ms", "0"))

    assert hb_after == hb_before

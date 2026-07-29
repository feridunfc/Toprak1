from __future__ import annotations

import asyncio

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim import TaskClaimManager
from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


class _ImmediateExecutor:
    def __init__(self, *, ok: bool = True) -> None:
        self.ok = ok
        self.calls = 0

    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        self.calls += 1
        return TaskExecutionResult(ok=self.ok, output={"task_id": ctx.task_id})


class _BlockingExecutor:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        self.calls += 1
        self.started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class _TerminalizeBeforeCompletion:
    def __init__(self, redis_client, dag: DagLua, run_id: str) -> None:
        self.redis = redis_client
        self.dag = dag
        self.run_id = run_id
        self.calls = 0

    async def task_complete(self, **kwargs):
        self.calls += 1
        await self.redis.set(RedisKey.run_state(self.run_id), "done")
        return await self.dag.task_complete(**kwargs)


def _ctx(*, task_id: str, run_id: str, tenant_id: str, worker_id: str) -> TaskContext:
    return TaskContext(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="agent",
        worker_group="workers",
        worker_instance_id=worker_id,
        payload={},
        scheduler_epoch="sched-e2e",
    )


async def _seed(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    run_state: str,
) -> None:
    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "scheduler_epoch": "sched-e2e",
        },
    )
    await redis_client.zadd(DagRedisKey.task_scheduled_zset(tenant_id), {task_id: 1})
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        mapping={
            "worker_id": worker_id,
            "task_id": task_id,
            "scheduler_epoch": "sched-e2e",
        },
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(task_id),
        mapping={
            "worker_id": worker_id,
            "task_id": task_id,
            "scheduler_epoch": "sched-e2e",
        },
    )
    await redis_client.set(RedisKey.run_state(run_id), run_state)


async def _components(redis_client):
    dag = DagLua(redis_client)
    await dag.initialise()
    return dag, TaskClaimManager(dag), TaskHeartbeatManager(redis_client)


async def test_terminal_run_before_claim_never_executes(redis_client) -> None:
    task_id = "task-e2e-before-claim"
    run_id = "run-e2e-before-claim"
    tenant_id = "tenant-e2e-before-claim"
    worker_id = "worker-e2e-before-claim"
    await _seed(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        run_state="done",
    )
    dag, claim_manager, heartbeat = await _components(redis_client)
    executor = _ImmediateExecutor()
    consumer = TaskConsumer(
        claim_manager,
        executor,
        heartbeat_manager=heartbeat,
        completion_manager=dag,
        heartbeat_interval_ms=10,
    )

    result = await consumer.consume_once(
        _ctx(task_id=task_id, run_id=run_id, tenant_id=tenant_id, worker_id=worker_id),
        claimed_at_ms=100,
    )

    assert result.claimed is not None
    assert result.claimed.status == "run_truth_terminal_conflict"
    assert result.executed is None
    assert result.completed is None
    assert executor.calls == 0
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "scheduled"


async def test_terminal_run_during_execution_cancels_executor(redis_client) -> None:
    task_id = "task-e2e-during-execution"
    run_id = "run-e2e-during-execution"
    tenant_id = "tenant-e2e-during-execution"
    worker_id = "worker-e2e-during-execution"
    await _seed(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        run_state="running",
    )
    dag, claim_manager, heartbeat = await _components(redis_client)
    executor = _BlockingExecutor()
    consumer = TaskConsumer(
        claim_manager,
        executor,
        heartbeat_manager=heartbeat,
        completion_manager=dag,
        heartbeat_interval_ms=10,
    )
    task = asyncio.create_task(
        consumer.consume_once(
            _ctx(task_id=task_id, run_id=run_id, tenant_id=tenant_id, worker_id=worker_id),
            claimed_at_ms=200,
        )
    )
    await asyncio.wait_for(executor.started.wait(), timeout=2)
    await redis_client.set(RedisKey.run_state(run_id), "done")

    with pytest.raises(RuntimeError, match="ownership lost"):
        await asyncio.wait_for(task, timeout=3)

    assert executor.calls == 1
    assert executor.cancelled.is_set()
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"
    assert await redis_client.exists(DagRedisKey.task_output(task_id)) == 0


async def test_terminal_run_after_execution_blocks_completion(redis_client) -> None:
    task_id = "task-e2e-before-completion"
    run_id = "run-e2e-before-completion"
    tenant_id = "tenant-e2e-before-completion"
    worker_id = "worker-e2e-before-completion"
    await _seed(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        run_state="running",
    )
    dag, claim_manager, _heartbeat = await _components(redis_client)
    executor = _ImmediateExecutor()
    completion = _TerminalizeBeforeCompletion(redis_client, dag, run_id)
    consumer = TaskConsumer(
        claim_manager,
        executor,
        completion_manager=completion,
    )

    result = await consumer.consume_once(
        _ctx(task_id=task_id, run_id=run_id, tenant_id=tenant_id, worker_id=worker_id),
        claimed_at_ms=300,
    )

    assert result.claimed is not None and result.claimed.ok
    assert result.executed is not None and result.executed.ok
    assert result.completed is not None
    assert result.completed.completed is False
    assert result.completed.status == "run_truth_terminal_conflict"
    assert completion.calls == 1
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"
    assert await redis_client.exists(DagRedisKey.task_output(task_id)) == 0


async def test_nonterminal_run_completes_normally(redis_client) -> None:
    task_id = "task-e2e-success"
    run_id = "run-e2e-success"
    tenant_id = "tenant-e2e-success"
    worker_id = "worker-e2e-success"
    await _seed(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        run_state="running",
    )
    dag, claim_manager, _heartbeat = await _components(redis_client)
    executor = _ImmediateExecutor()
    consumer = TaskConsumer(
        claim_manager,
        executor,
        completion_manager=dag,
    )

    result = await consumer.consume_once(
        _ctx(task_id=task_id, run_id=run_id, tenant_id=tenant_id, worker_id=worker_id),
        claimed_at_ms=400,
    )

    assert result.claimed is not None and result.claimed.ok
    assert result.executed is not None and result.executed.ok
    assert result.completed is not None and result.completed.completed
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "done"
    assert await redis_client.exists(DagRedisKey.task_output(task_id)) == 1

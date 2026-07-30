from __future__ import annotations

from types import SimpleNamespace

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa.events.codec import serialize_event
from hfa.events.schema import RunRequestedEvent
from hfa_control.dag_lua import DagLua
from hfa_control.models import ControlPlaneConfig
from hfa_control.recovery import RecoveryService
from hfa_control.service import ControlPlaneService
from hfa_control.task_claim import TaskClaimManager
from hfa_control.task_recovery import TaskRecoveryManager
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.idempotency import IdempotencyGuard
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutionResult


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


TERMINAL_TASK_STATES = {"done", "failed", "cancelled", "dead_lettered", "rejected", "blocked_by_failure"}


def _decode(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _decode_mapping(raw: dict) -> dict[str, str]:
    return {_decode(key): _decode(value) for key, value in (raw or {}).items()}


async def _conflict_rows(redis_client) -> list[dict[str, str]]:
    key = RedisKey.runtime_truth_conflict_stream()
    if _decode(await redis_client.type(key)) != "stream":
        return []
    rows = await redis_client.xrange(key)
    return [_decode_mapping(fields) for _entry_id, fields in rows]


async def _seed_truth_pair(
    redis_client,
    *,
    run_id: str,
    task_id: str,
    tenant_id: str,
    run_state: str | None,
    task_state: str | None,
    task_meta: bool = True,
    run_meta: bool = True,
) -> None:
    if run_state is not None:
        await redis_client.set(RedisKey.run_state(run_id), run_state)
    if run_meta:
        await redis_client.hset(
            RedisKey.run_meta(run_id),
            mapping={
                "run_id": run_id,
                "tenant_id": tenant_id,
                "agent_type": "agent",
                "worker_group": "workers",
                "shard": "0",
                "reschedule_count": "0",
                "admitted_at": "1",
                "state": run_state or "",
            },
        )
    await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
    if task_state is not None:
        await redis_client.set(DagRedisKey.task_state(task_id), task_state)
    if task_meta:
        await redis_client.hset(
            DagRedisKey.task_meta(task_id),
            mapping={
                "task_id": task_id,
                "run_id": run_id,
                "tenant_id": tenant_id,
                "scheduler_epoch": "s82-4",
                "last_heartbeat_at_ms": "1",
            },
        )


class _Executor:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        self.calls += 1
        return TaskExecutionResult(ok=True, output={"task_id": ctx.task_id})


async def _pending_count(redis_client, stream: str) -> int:
    pending = await redis_client.xpending(stream, CONSUMER_GROUP)
    if isinstance(pending, dict):
        return int(pending.get("pending", pending.get(b"pending", 0)) or 0)
    if isinstance(pending, (tuple, list)) and pending:
        return int(pending[0])
    return int(pending or 0)


async def _worker_bridge(redis_client, *, worker_id: str, executor: _Executor) -> WorkerConsumer:
    dag = DagLua(redis_client)
    await dag.initialise()
    task_consumer = TaskConsumer(
        TaskClaimManager(dag),
        executor,
        completion_manager=dag,
    )
    bridge = WorkerConsumer(
        redis_client,
        worker_id,
        "workers",
        [0],
        executor,
        task_consumer=task_consumer,
    )
    await bridge.prepare_consumer_groups()
    return bridge


async def _enqueue_pending(
    redis_client,
    *,
    worker_id: str,
    task_id: str,
    run_id: str,
    tenant_id: str,
) -> tuple[str, str, dict]:
    stream = RedisKey.stream_shard(0)
    await redis_client.xadd(
        stream,
        serialize_event(
            RunRequestedEvent(
                event_type="TaskRequested",
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                agent_type="agent",
                payload={},
                scheduler_epoch="s82-4",
            )
        ),
    )
    messages = await redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=worker_id,
        streams={stream: ">"},
        count=1,
    )
    assert messages
    _stream, entries = messages[0]
    assert len(entries) == 1
    message_id, raw = entries[0]
    return stream, _decode(message_id), raw


async def _seed_worker_reservation(
    redis_client,
    *,
    worker_id: str,
    task_id: str,
    tenant_id: str,
) -> None:
    await redis_client.zadd(DagRedisKey.task_scheduled_zset(tenant_id), {task_id: 1})
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        mapping={
            "worker_id": worker_id,
            "task_id": task_id,
            "scheduler_epoch": "s82-4",
        },
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(task_id),
        mapping={
            "worker_id": worker_id,
            "task_id": task_id,
            "scheduler_epoch": "s82-4",
        },
    )


async def test_observation_01_api_run_done_task_running(redis_client) -> None:
    run_id = "s82-4-api-done-run"
    task_id = "s82-4-api-done-task"
    tenant_id = "s82-4-api"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="done",
        task_state="running",
    )

    result = await ControlPlaneService(
        redis_client, ControlPlaneConfig(instance_id="s82-4-api-done")
    ).get_run_state(run_id)

    assert result["state"] == "done"
    assert result["truth_conflict"] is True
    assert any(
        row["detail_code"] == "run_terminal_task_nonterminal"
        for row in result["truth_conflicts"]
    )


async def test_observation_02_api_run_running_task_done(redis_client) -> None:
    run_id = "s82-4-api-running-run"
    task_id = "s82-4-api-running-task"
    tenant_id = "s82-4-api"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="running",
        task_state="done",
    )

    result = await ControlPlaneService(
        redis_client, ControlPlaneConfig(instance_id="s82-4-api-running")
    ).get_run_state(run_id)

    assert result["state"] == "running"
    assert result["truth_conflict"] is True
    assert any(
        row["detail_code"] == "task_terminal_run_nonterminal"
        for row in result["truth_conflicts"]
    )


async def test_observation_03_run_recovery_missing_run_task_terminal(redis_client) -> None:
    run_id = "s82-4-run-missing"
    task_id = "s82-4-run-missing-task"
    tenant_id = "s82-4-recovery"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state=None,
        task_state="done",
    )
    config = ControlPlaneConfig(instance_id="s82-4-run-missing", stale_run_timeout=1)
    await redis_client.zadd(config.running_zset, {run_id: 1})

    result = await RecoveryService(redis_client, config)._handle_stale(run_id)

    assert result == "conflict"
    assert await redis_client.exists(RedisKey.run_state(run_id)) == 0
    assert await redis_client.zscore(config.running_zset, run_id) == 1.0
    rows = await _conflict_rows(redis_client)
    assert len(rows) == 1
    assert rows[0]["operation"] == "RUN_RECOVERY"
    assert rows[0]["conflict_type"] == "run_truth_missing"


async def test_observation_04_run_recovery_terminal_run_task_running(redis_client) -> None:
    run_id = "s82-4-run-terminal"
    task_id = "s82-4-run-terminal-task"
    tenant_id = "s82-4-recovery"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="done",
        task_state="running",
    )
    config = ControlPlaneConfig(instance_id="s82-4-run-terminal", stale_run_timeout=1)
    await redis_client.zadd(config.running_zset, {run_id: 1})

    result = await RecoveryService(redis_client, config)._handle_stale(run_id)

    assert result == "conflict"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "done"
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert await redis_client.zscore(config.running_zset, run_id) == 1.0
    rows = await _conflict_rows(redis_client)
    assert len(rows) == 1
    assert rows[0]["operation"] == "RUN_RECOVERY"
    assert rows[0]["conflict_type"] == "run_truth_terminal_conflict"


async def test_observation_05_task_recovery_missing_task_run_terminal(redis_client) -> None:
    run_id = "s82-4-task-missing-run"
    task_id = "s82-4-task-missing"
    tenant_id = "s82-4-task-recovery"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="done",
        task_state=None,
    )
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1})

    result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=100_000,
        ready_score=100_000,
    )

    assert result.status == "task_truth_missing"
    assert await redis_client.exists(DagRedisKey.task_state(task_id)) == 0
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) == 1.0
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), task_id) is None
    rows = await _conflict_rows(redis_client)
    assert len(rows) == 1
    assert rows[0]["operation"] == "TASK_REQUEUE"
    assert rows[0]["observed_run_state"] == "done"


async def test_observation_06_worker_nonterminal_task_terminal_run_blocks_claim(redis_client) -> None:
    run_id = "s82-4-worker-run-terminal"
    task_id = "s82-4-worker-task-scheduled"
    tenant_id = "s82-4-worker"
    worker_id = "s82-4-worker-a"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="done",
        task_state="scheduled",
    )
    await _seed_worker_reservation(
        redis_client,
        worker_id=worker_id,
        task_id=task_id,
        tenant_id=tenant_id,
    )
    executor = _Executor()
    bridge = await _worker_bridge(redis_client, worker_id=worker_id, executor=executor)
    stream, message_id, raw = await _enqueue_pending(
        redis_client,
        worker_id=worker_id,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
    )

    await bridge._process_message(message_id, raw, stream, 0)

    assert executor.calls == 0
    assert await _pending_count(redis_client, stream) == 1
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "scheduled"
    rows = await _conflict_rows(redis_client)
    assert len(rows) == 1
    assert rows[0]["operation"] == "TASK_CLAIM"
    assert rows[0]["conflict_type"] == "run_truth_terminal_conflict"


async def test_observation_07_worker_terminal_task_nonterminal_run_transport_cleanup(redis_client) -> None:
    run_id = "s82-4-worker-task-terminal-run"
    task_id = "s82-4-worker-task-terminal"
    tenant_id = "s82-4-worker-terminal"
    worker_id = "s82-4-worker-b"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="running",
        task_state="done",
    )
    executor = _Executor()
    bridge = await _worker_bridge(redis_client, worker_id=worker_id, executor=executor)
    stream, message_id, raw = await _enqueue_pending(
        redis_client,
        worker_id=worker_id,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
    )

    await bridge._process_message(message_id, raw, stream, 0)
    read_model = await ControlPlaneService(
        redis_client, ControlPlaneConfig(instance_id="s82-4-worker-terminal")
    ).get_run_state(run_id)

    assert executor.calls == 0
    assert await _pending_count(redis_client, stream) == 0
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "done"
    assert read_model["state"] == "running"
    assert any(
        row["detail_code"] == "task_terminal_run_nonterminal"
        for row in read_model["truth_conflicts"]
    )


async def test_observation_08_legacy_worker_terminal_run_suppresses_execution(redis_client) -> None:
    run_id = "s82-4-legacy-run-terminal"
    task_id = "s82-4-legacy-task-running"
    tenant_id = "s82-4-legacy"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="done",
        task_state="running",
    )
    before_state = _decode(await redis_client.get(RedisKey.run_state(run_id)))
    before_task = _decode(await redis_client.get(DagRedisKey.task_state(task_id)))

    should_execute = await IdempotencyGuard(redis_client).should_execute(run_id)

    assert should_execute is False
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == before_state
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == before_task
    assert await _conflict_rows(redis_client) == []


async def test_observation_09_scheduler_terminal_run_ready_task_blocks_dispatch(redis_client) -> None:
    run_id = "s82-4-scheduler-run-terminal"
    task_id = "s82-4-scheduler-task-ready"
    tenant_id = "s82-4-scheduler"
    await _seed_truth_pair(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="done",
        task_state="ready",
    )
    await redis_client.zadd(DagRedisKey.tenant_ready_queue(tenant_id), {task_id: 1})
    dag = DagLua(redis_client)
    await dag.initialise()
    dispatch = SimpleNamespace(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="agent",
        worker_group="workers",
        shard=0,
        priority=0,
        admitted_at=1,
        scheduled_at=100,
        scheduler_epoch="s82-4",
        payload_json="{}",
    )

    result = await dag.task_dispatch_commit(dispatch)

    assert result.committed is False
    assert result.status == "run_truth_terminal_conflict"
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "ready"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), task_id) == 1.0
    assert await redis_client.exists(RedisKey.stream_control()) == 0
    assert await redis_client.exists(RedisKey.stream_shard(0)) == 0
    rows = await _conflict_rows(redis_client)
    assert len(rows) == 1
    assert rows[0]["operation"] == "TASK_DISPATCH"
    assert rows[0]["conflict_type"] == "run_truth_terminal_conflict"

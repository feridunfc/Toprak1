from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.run_termination import RunTerminationCoordinator
from hfa_control.run_terminal_event_evidence import ensure_terminal_event_index
from hfa_worker.consumer import CONSUMER_GROUP
from hfa_worker.run_finalizing_runtime import (
    RunFinalizingTaskConsumer,
    RunFinalizingWorkerConsumer,
)


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


class _UnusedExecutor:
    async def execute(self, _ctx):
        raise AssertionError("terminal duplicate recovery must not execute")


async def _dag(redis_client) -> RunTerminationCoordinator:
    await ensure_terminal_event_index(redis_client)
    dag = DagLua(redis_client)
    await dag.initialise()
    coordinator = RunTerminationCoordinator(redis_client, dag, enabled=True)
    await coordinator.initialise()
    return coordinator


async def _seed_run(
    redis_client,
    *,
    run_id: str,
    tenant_id: str,
    tasks: dict[str, str],
    run_state: str = "running",
) -> None:
    await redis_client.set(RedisKey.run_state(run_id), run_state)
    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "state": run_state,
            "worker_group": "workers",
            "shard": "0",
        },
    )
    await redis_client.zadd(RedisKey.cp_running(), {run_id: 100})
    for task_id, state in tasks.items():
        await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
        await redis_client.set(DagRedisKey.task_state(task_id), state)
        await redis_client.hset(
            DagRedisKey.task_meta(task_id),
            mapping={
                "task_id": task_id,
                "run_id": run_id,
                "tenant_id": tenant_id,
            },
        )


async def _result_events(redis_client, run_id: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for _entry_id, fields in await redis_client.xrange(RedisKey.stream_results()):
        decoded = {
            (key.decode() if isinstance(key, bytes) else str(key)): (
                value.decode() if isinstance(value, bytes) else str(value)
            )
            for key, value in fields.items()
        }
        if decoded.get("run_id") == run_id:
            rows.append(decoded)
    return rows


async def _pending_count(redis_client, stream: str) -> int:
    pending = await redis_client.xpending(stream, CONSUMER_GROUP)
    if isinstance(pending, dict):
        return int(pending.get("pending", pending.get(b"pending", 0)) or 0)
    if isinstance(pending, (tuple, list)) and pending:
        return int(pending[0] or 0)
    return int(pending or 0)


async def _conflict_count(redis_client) -> int:
    raw = await redis_client.hget(
        RedisKey.runtime_truth_conflict_index(),
        "__runtime_truth_conflict_count",
    )
    return int(raw or 0)


async def test_single_done_task_finalizes_run_and_result_event(redis_client) -> None:
    run_id = "run-finalize-single-done"
    tenant_id = "tenant-finalize"
    task_id = "task-finalize-single-done"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={task_id: "done"},
    )
    dag = await _dag(redis_client)

    result = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=task_id,
        finalized_at_ms=1000,
        worker_instance_id="worker-finalize",
        trigger_terminal_state="done",
    )

    assert result.finalized is True
    assert result.status == "finalized"
    assert result.final_state == "done"
    assert result.task_count == 1
    assert result.done_count == 1
    assert result.failed_count == 0
    assert result.ack_allowed is True
    assert await redis_client.get(RedisKey.run_state(run_id)) == "done"
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) is None

    meta = await redis_client.hgetall(RedisKey.run_meta(run_id))
    stored = await redis_client.hgetall(RedisKey.run_result(run_id))
    assert meta["state"] == "done"
    assert meta["finalization_operation"] == "RUN_TERMINATE"
    assert stored["status"] == "done"
    assert stored["finalization_source"] == "terminal_task_aggregate"
    assert json.loads(stored["payload"])["task_count"] == 1
    events = await _result_events(redis_client, run_id)
    assert len(events) == 1
    assert events[0]["event_type"] == "RunCompleted"
    assert events[0]["finalization_operation"] == "RUN_TERMINATE"


async def test_any_failed_terminal_task_finalizes_run_failed(redis_client) -> None:
    run_id = "run-finalize-failed"
    tenant_id = "tenant-finalize-failed"
    tasks = {
        "task-finalize-done": "done",
        "task-finalize-failed": "failed",
        "task-finalize-skipped": "skipped",
    }
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks=tasks,
    )
    dag = await _dag(redis_client)

    result = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id="task-finalize-failed",
        finalized_at_ms=2000,
        worker_instance_id="worker-finalize",
        trigger_terminal_state="failed",
    )

    assert result.finalized is True
    assert result.final_state == "failed"
    assert result.task_count == 3
    assert result.done_count == 1
    assert result.failed_count == 1
    assert result.skipped_count == 1
    assert await redis_client.get(RedisKey.run_state(run_id)) == "failed"
    stored = await redis_client.hgetall(RedisKey.run_result(run_id))
    assert stored["status"] == "failed"
    assert stored["error"] == "aggregate_task_failure"
    events = await _result_events(redis_client, run_id)
    assert len(events) == 1
    assert events[0]["event_type"] == "RunFailed"


async def test_nonterminal_sibling_returns_not_ready_without_run_mutation(redis_client) -> None:
    run_id = "run-finalize-not-ready"
    tenant_id = "tenant-finalize-not-ready"
    trigger = "task-finalize-terminal"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={trigger: "done", "task-finalize-running": "running"},
    )
    before_meta = await redis_client.hgetall(RedisKey.run_meta(run_id))
    before_score = await redis_client.zscore(RedisKey.cp_running(), run_id)
    before_events = await redis_client.xlen(RedisKey.stream_results())
    dag = await _dag(redis_client)

    result = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=trigger,
        finalized_at_ms=3000,
        trigger_terminal_state="done",
    )

    assert result.finalized is False
    assert result.status == "not_ready"
    assert result.ack_allowed is True
    assert result.task_count == 2
    assert await redis_client.get(RedisKey.run_state(run_id)) == "running"
    assert await redis_client.hgetall(RedisKey.run_meta(run_id)) == before_meta
    assert await redis_client.exists(RedisKey.run_result(run_id)) == 0
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == before_score
    assert await redis_client.xlen(RedisKey.stream_results()) == before_events
    assert await _conflict_count(redis_client) == 0


async def test_last_terminal_task_converges_after_prior_not_ready(redis_client) -> None:
    run_id = "run-finalize-last"
    tenant_id = "tenant-finalize-last"
    first = "task-finalize-first"
    last = "task-finalize-last"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={first: "done", last: "running"},
    )
    dag = await _dag(redis_client)

    first_result = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=first,
        finalized_at_ms=4000,
        trigger_terminal_state="done",
    )
    assert first_result.status == "not_ready"

    await redis_client.set(DagRedisKey.task_state(last), "done")
    last_result = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=last,
        finalized_at_ms=5000,
        trigger_terminal_state="done",
    )
    assert last_result.status == "finalized"
    assert last_result.final_state == "done"
    assert last_result.done_count == 2
    assert await redis_client.get(RedisKey.run_state(run_id)) == "done"


async def test_duplicate_run_termination_is_idempotent_and_emits_one_event(redis_client) -> None:
    run_id = "run-finalize-duplicate"
    tenant_id = "tenant-finalize-duplicate"
    task_id = "task-finalize-duplicate"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={task_id: "done"},
    )
    dag = await _dag(redis_client)

    first = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=task_id,
        finalized_at_ms=6000,
        trigger_terminal_state="done",
    )
    second = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=task_id,
        finalized_at_ms=7000,
        trigger_terminal_state="done",
    )

    assert first.status == "finalized"
    assert second.status == "already_finalized"
    assert second.already_finalized is True
    assert second.ack_allowed is True
    assert len(await _result_events(redis_client, run_id)) == 1
    stored = await redis_client.hgetall(RedisKey.run_result(run_id))
    assert stored["finalized_at_ms"] == "6000"


async def test_missing_task_meta_blocks_run_mutation_and_records_conflict(redis_client) -> None:
    run_id = "run-finalize-missing-meta"
    tenant_id = "tenant-finalize-missing-meta"
    task_id = "task-finalize-missing-meta"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={task_id: "done"},
    )
    await redis_client.delete(DagRedisKey.task_meta(task_id))
    before_meta = await redis_client.hgetall(RedisKey.run_meta(run_id))
    before_score = await redis_client.zscore(RedisKey.cp_running(), run_id)
    dag = await _dag(redis_client)

    result = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=task_id,
        finalized_at_ms=8000,
        trigger_terminal_state="done",
    )

    assert result.finalized is False
    assert result.status == "task_truth_corruption_conflict"
    assert result.ack_allowed is False
    assert await redis_client.get(RedisKey.run_state(run_id)) == "running"
    assert await redis_client.hgetall(RedisKey.run_meta(run_id)) == before_meta
    assert await redis_client.exists(RedisKey.run_result(run_id)) == 0
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == before_score
    assert await _conflict_count(redis_client) == 1
    rows = await redis_client.xrange(RedisKey.runtime_truth_conflict_stream())
    assert rows[0][1]["operation"] == "RUN_TERMINATE"
    assert rows[0][1]["detail_code"] == f"task_meta_missing:{task_id}"


async def test_wrong_type_result_stream_has_no_partial_run_mutation(redis_client) -> None:
    run_id = "run-finalize-wrong-stream"
    tenant_id = "tenant-finalize-wrong-stream"
    task_id = "task-finalize-wrong-stream"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={task_id: "done"},
    )
    await redis_client.set(RedisKey.stream_results(), "wrong-type")
    before_meta = await redis_client.hgetall(RedisKey.run_meta(run_id))
    before_score = await redis_client.zscore(RedisKey.cp_running(), run_id)
    dag = await _dag(redis_client)

    result = await dag.finalize_run_from_tasks(
        run_id=run_id,
        tenant_id=tenant_id,
        trigger_task_id=task_id,
        finalized_at_ms=9000,
        trigger_terminal_state="done",
    )

    assert result.status == "run_truth_corruption_conflict"
    assert result.ack_allowed is False
    assert await redis_client.get(RedisKey.run_state(run_id)) == "running"
    assert await redis_client.hgetall(RedisKey.run_meta(run_id)) == before_meta
    assert await redis_client.exists(RedisKey.run_result(run_id)) == 0
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == before_score
    assert await redis_client.get(RedisKey.stream_results()) == "wrong-type"


async def test_task_complete_coordinates_run_termination_when_binding_enabled(redis_client) -> None:
    run_id = "run-task-complete-finalize"
    tenant_id = "tenant-task-complete-finalize"
    task_id = "task-task-complete-finalize"
    worker_id = "worker-task-complete-finalize"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={task_id: "running"},
    )
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "worker_instance_id": worker_id,
            "scheduler_epoch": "sched-finalize",
            "claim_epoch": "1",
        },
    )
    await redis_client.zadd(
        DagRedisKey.task_running_zset(tenant_id),
        {task_id: 100},
    )
    dag = await _dag(redis_client)

    result = await dag.task_complete(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=10000,
        worker_instance_id=worker_id,
        output_data=json.dumps({"ok": True}),
        expected_scheduler_epoch="sched-finalize",
        expected_claim_epoch="1",
    )

    assert result.task_committed is True
    assert result.completed is True
    assert result.ack_allowed is True
    assert result.run_termination is not None
    assert result.run_termination.status == "finalized"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "done"
    assert await redis_client.get(RedisKey.run_state(run_id)) == "done"


async def test_task_complete_not_ready_remains_ack_safe(redis_client) -> None:
    run_id = "run-task-complete-not-ready"
    tenant_id = "tenant-task-complete-not-ready"
    task_id = "task-task-complete-not-ready"
    sibling_id = "task-task-complete-sibling"
    worker_id = "worker-task-complete-not-ready"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={task_id: "running", sibling_id: "pending"},
    )
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "worker_instance_id": worker_id,
            "scheduler_epoch": "sched-not-ready",
            "claim_epoch": "1",
        },
    )
    await redis_client.zadd(
        DagRedisKey.task_running_zset(tenant_id),
        {task_id: 100},
    )
    dag = await _dag(redis_client)

    result = await dag.task_complete(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=11000,
        worker_instance_id=worker_id,
        output_data="{}",
        expected_scheduler_epoch="sched-not-ready",
        expected_claim_epoch="1",
    )

    assert result.task_committed is True
    assert result.completed is True
    assert result.ack_allowed is True
    assert result.run_termination is not None
    assert result.run_termination.status == "not_ready"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "done"
    assert await redis_client.get(RedisKey.run_state(run_id)) == "running"


async def test_terminal_duplicate_recovery_finalizes_then_acks(redis_client) -> None:
    run_id = "run-terminal-duplicate-finalize"
    tenant_id = "tenant-terminal-duplicate-finalize"
    task_id = "task-terminal-duplicate-finalize"
    worker_id = "worker-terminal-duplicate-finalize"
    stream = "hfa:sprint83:terminal-duplicate"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={task_id: "done"},
    )
    dag = await _dag(redis_client)
    task_consumer = RunFinalizingTaskConsumer(
        object(),
        _UnusedExecutor(),
        completion_manager=dag,
    )
    consumer = object.__new__(RunFinalizingWorkerConsumer)
    consumer._redis = redis_client
    consumer._task_consumer = task_consumer
    consumer._worker_id = worker_id
    consumer._worker_group = "workers"

    await redis_client.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
    message_id = await redis_client.xadd(
        stream,
        {
            "event_type": "TaskRequested",
            "run_id": run_id,
            "task_id": task_id,
            "tenant_id": tenant_id,
            "agent_type": "test",
            "scheduler_epoch": "sched-duplicate",
        },
    )
    delivered = await redis_client.xreadgroup(
        CONSUMER_GROUP,
        worker_id,
        {stream: ">"},
        count=1,
        block=1000,
    )
    assert delivered

    event = SimpleNamespace(
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        agent_type="test",
        payload={},
        trace_parent="",
        trace_state="",
        scheduler_epoch="sched-duplicate",
    )
    await consumer._process_message_via_task_consumer(
        event,
        message_id,
        stream,
        shard=0,
    )

    assert await _pending_count(redis_client, stream) == 0
    assert await redis_client.get(RedisKey.run_state(run_id)) == "done"
    assert len(await _result_events(redis_client, run_id)) == 1


async def test_terminal_duplicate_finalization_conflict_stays_pending(redis_client) -> None:
    run_id = "run-terminal-duplicate-conflict"
    tenant_id = "tenant-terminal-duplicate-conflict"
    task_id = "task-terminal-duplicate-conflict"
    worker_id = "worker-terminal-duplicate-conflict"
    stream = "hfa:sprint83:terminal-duplicate-conflict"
    await _seed_run(
        redis_client,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks={task_id: "done"},
    )
    await redis_client.delete(DagRedisKey.task_meta(task_id))
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={"task_id": task_id, "run_id": run_id, "tenant_id": tenant_id},
    )
    rogue_id = "task-terminal-duplicate-rogue"
    await redis_client.sadd(DagRedisKey.run_tasks(run_id), rogue_id)
    await redis_client.set(DagRedisKey.task_state(rogue_id), "done")

    dag = await _dag(redis_client)
    task_consumer = RunFinalizingTaskConsumer(
        object(),
        _UnusedExecutor(),
        completion_manager=dag,
    )
    consumer = object.__new__(RunFinalizingWorkerConsumer)
    consumer._redis = redis_client
    consumer._task_consumer = task_consumer
    consumer._worker_id = worker_id
    consumer._worker_group = "workers"

    await redis_client.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
    message_id = await redis_client.xadd(
        stream,
        {
            "event_type": "TaskRequested",
            "run_id": run_id,
            "task_id": task_id,
            "tenant_id": tenant_id,
            "agent_type": "test",
        },
    )
    delivered = await redis_client.xreadgroup(
        CONSUMER_GROUP,
        worker_id,
        {stream: ">"},
        count=1,
        block=1000,
    )
    assert delivered

    event = SimpleNamespace(
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        agent_type="test",
        payload={},
        trace_parent="",
        trace_state="",
        scheduler_epoch="",
    )
    await consumer._process_message_via_task_consumer(
        event,
        message_id,
        stream,
        shard=0,
    )

    assert await _pending_count(redis_client, stream) == 1
    assert await redis_client.get(RedisKey.run_state(run_id)) == "running"
    assert await _conflict_count(redis_client) == 1

from __future__ import annotations

import json

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_recovery import TaskHeartbeatManager


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _dag(redis_client) -> DagLua:
    dag = DagLua(redis_client)
    await dag.initialise()
    return dag


async def _seed_scheduled(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    scheduler_epoch: str,
    run_state: str | None = "running",
) -> None:
    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "scheduler_epoch": scheduler_epoch,
        },
    )
    await redis_client.zadd(DagRedisKey.task_scheduled_zset(tenant_id), {task_id: 10})
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        mapping={
            "worker_id": worker_id,
            "task_id": task_id,
            "scheduler_epoch": scheduler_epoch,
        },
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(task_id),
        mapping={
            "worker_id": worker_id,
            "task_id": task_id,
            "scheduler_epoch": scheduler_epoch,
        },
    )
    if run_state is not None:
        await redis_client.set(RedisKey.run_state(run_id), run_state)


async def _seed_running(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    scheduler_epoch: str = "sched-1",
    claim_epoch: str = "1",
    run_state: str | None = "running",
) -> None:
    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": claim_epoch,
            "last_heartbeat_at_ms": "100",
        },
    )
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 100})
    if run_state is not None:
        await redis_client.set(RedisKey.run_state(run_id), run_state)


async def _truth_rows(redis_client) -> list[dict[str, str]]:
    rows = await redis_client.xrange(RedisKey.runtime_truth_conflict_stream())
    return [fields for _entry_id, fields in rows]


async def _truth_count(redis_client) -> int:
    raw = await redis_client.hget(
        RedisKey.runtime_truth_conflict_index(),
        "__runtime_truth_conflict_count",
    )
    return int(raw or 0)


@pytest.mark.parametrize(
    "run_state",
    ["admitted", "queued", "scheduled", "running", "rescheduled"],
)
async def test_each_nonterminal_run_state_allows_claim(redis_client, run_state: str) -> None:
    task_id = f"task-claim-{run_state}"
    run_id = f"run-claim-{run_state}"
    tenant_id = "tenant-claim"
    worker_id = "worker-claim"
    scheduler_epoch = "sched-claim"
    await _seed_scheduled(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        run_state=run_state,
    )
    dag = await _dag(redis_client)

    result = await dag.task_claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=200,
        scheduler_epoch=scheduler_epoch,
    )

    assert result.ok is True
    assert result.status == "task_claimed"
    assert result.claim_epoch == "1"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"
    assert await redis_client.zscore(DagRedisKey.task_scheduled_zset(tenant_id), task_id) is None
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) == 200.0
    assert await redis_client.exists(DagRedisKey.worker_reservation(worker_id)) == 0
    assert await redis_client.exists(DagRedisKey.task_reservation_owner(task_id)) == 0
    assert await _truth_count(redis_client) == 0


@pytest.mark.parametrize("run_state", ["done", "failed", "rejected", "dead_lettered"])
async def test_terminal_run_blocks_claim_without_consuming_reservation(
    redis_client, run_state: str
) -> None:
    task_id = f"task-terminal-claim-{run_state}"
    run_id = f"run-terminal-claim-{run_state}"
    tenant_id = "tenant-terminal-claim"
    worker_id = "worker-terminal-claim"
    scheduler_epoch = "sched-terminal"
    await _seed_scheduled(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        run_state=run_state,
    )
    before_meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    dag = await _dag(redis_client)

    result = await dag.task_claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=300,
        scheduler_epoch=scheduler_epoch,
    )

    assert result.ok is False
    assert result.status == "run_truth_terminal_conflict"
    assert result.claim_epoch == ""
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "scheduled"
    assert await redis_client.hgetall(DagRedisKey.task_meta(task_id)) == before_meta
    assert await redis_client.zscore(DagRedisKey.task_scheduled_zset(tenant_id), task_id) == 10.0
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) is None
    assert await redis_client.exists(DagRedisKey.worker_reservation(worker_id)) == 1
    assert await redis_client.exists(DagRedisKey.task_reservation_owner(task_id)) == 1
    assert await _truth_count(redis_client) == 1
    rows = await _truth_rows(redis_client)
    assert rows[0]["operation"] == "TASK_CLAIM"


async def test_terminal_run_blocks_heartbeat_without_refresh(redis_client) -> None:
    task_id = "task-terminal-heartbeat"
    run_id = "run-terminal-heartbeat"
    tenant_id = "tenant-heartbeat"
    worker_id = "worker-heartbeat"
    await _seed_running(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        run_state="done",
    )
    before_meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    before_score = await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id)
    manager = TaskHeartbeatManager(redis_client)

    result = await manager.record_heartbeat(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        claim_epoch="1",
        now_ms=900,
    )

    assert result.ok is False
    assert result.status == "run_truth_terminal_conflict"
    assert await redis_client.hgetall(DagRedisKey.task_meta(task_id)) == before_meta
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) == before_score
    assert await _truth_count(redis_client) == 1
    assert (await _truth_rows(redis_client))[0]["operation"] == "TASK_HEARTBEAT"


@pytest.mark.parametrize(
    "terminal_state,expected_operation",
    [("done", "TASK_COMPLETE"), ("failed", "TASK_FAIL")],
)
async def test_terminal_run_blocks_completion_and_child_effects(
    redis_client, terminal_state: str, expected_operation: str
) -> None:
    task_id = f"task-terminal-{terminal_state}"
    child_id = f"child-terminal-{terminal_state}"
    run_id = f"run-terminal-{terminal_state}"
    tenant_id = "tenant-complete"
    worker_id = "worker-complete"
    await _seed_running(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        run_state="done",
    )
    await redis_client.sadd(DagRedisKey.task_children(task_id), child_id)
    await redis_client.set(DagRedisKey.task_state(child_id), "pending")
    await redis_client.set(DagRedisKey.task_remaining_deps(child_id), 1)
    before_meta = await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    dag = await _dag(redis_client)

    result = await dag.task_complete(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state=terminal_state,
        finished_at_ms=1000,
        worker_instance_id=worker_id,
        output_data=json.dumps({"ok": True}),
        expected_scheduler_epoch="sched-1",
        expected_claim_epoch="1",
    )

    assert result.completed is False
    assert result.status == "run_truth_terminal_conflict"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"
    assert await redis_client.hgetall(DagRedisKey.task_meta(task_id)) == before_meta
    assert await redis_client.exists(DagRedisKey.task_output(task_id)) == 0
    assert await redis_client.get(DagRedisKey.task_state(child_id)) == "pending"
    assert await redis_client.get(DagRedisKey.task_remaining_deps(child_id)) == "1"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), child_id) is None
    assert await _truth_count(redis_client) == 1
    assert (await _truth_rows(redis_client))[0]["operation"] == expected_operation


async def test_wrong_explicit_run_id_writes_no_conflict(redis_client) -> None:
    task_id = "task-wrong-run"
    run_id = "run-authoritative"
    wrong_run_id = "run-wrong"
    tenant_id = "tenant-wrong-run"
    worker_id = "worker-wrong-run"
    await _seed_scheduled(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch="sched-wrong",
        run_state="running",
    )
    await redis_client.set(RedisKey.run_state(wrong_run_id), "done")
    dag = await _dag(redis_client)

    result = await dag.task_claim_start(
        task_id=task_id,
        run_id=wrong_run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=1100,
        scheduler_epoch="sched-wrong",
    )

    assert result.status == "identity_run_id_mismatch"
    assert await _truth_count(redis_client) == 0
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "scheduled"


async def test_same_operation_retry_deduplicates_but_operations_do_not_collide(
    redis_client,
) -> None:
    task_id = "task-cross-operation"
    run_id = "run-cross-operation"
    tenant_id = "tenant-cross-operation"
    worker_id = "worker-cross-operation"
    await _seed_scheduled(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch="sched-cross",
        run_state=None,
    )
    dag = await _dag(redis_client)

    first = await dag.task_claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=1200,
        scheduler_epoch="sched-cross",
    )
    second = await dag.task_claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=1300,
        scheduler_epoch="sched-cross",
    )
    assert first.status == second.status == "run_truth_missing"
    assert await _truth_count(redis_client) == 1

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "worker_instance_id": worker_id,
            "claim_epoch": "1",
            "scheduler_epoch": "sched-cross",
        },
    )
    heartbeat = TaskHeartbeatManager(redis_client)
    heartbeat_result = await heartbeat.record_heartbeat(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        claim_epoch="1",
        now_ms=1400,
    )
    assert heartbeat_result.status == "run_truth_missing"

    completion_result = await dag.task_complete(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=1500,
        worker_instance_id=worker_id,
        expected_scheduler_epoch="sched-cross",
        expected_claim_epoch="1",
    )
    assert completion_result.status == "run_truth_missing"
    assert await _truth_count(redis_client) == 3
    operations = {row["operation"] for row in await _truth_rows(redis_client)}
    assert operations == {"TASK_CLAIM", "TASK_HEARTBEAT", "TASK_COMPLETE"}


async def test_corrupt_conflict_store_blocks_claim_without_mutation(redis_client) -> None:
    task_id = "task-corrupt-store"
    run_id = "run-corrupt-store"
    tenant_id = "tenant-corrupt-store"
    worker_id = "worker-corrupt-store"
    await _seed_scheduled(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch="sched-corrupt",
        run_state=None,
    )
    await redis_client.set(RedisKey.runtime_truth_conflict_index(), "wrong-type")
    dag = await _dag(redis_client)

    result = await dag.task_claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=1600,
        scheduler_epoch="sched-corrupt",
    )

    assert result.status == "truth_conflict_evidence_store_unavailable"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "scheduled"
    assert await redis_client.exists(DagRedisKey.worker_reservation(worker_id)) == 1

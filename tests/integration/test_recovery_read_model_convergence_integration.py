from __future__ import annotations

import json

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.schema import DagRedisKey
from hfa_control.models import ControlPlaneConfig
from hfa_control.recovery import RecoveryService
from hfa_control.service import ControlPlaneService
from hfa_control.task_recovery import TaskRecoveryManager


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _decode(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value) if value is not None else ""


def _decode_mapping(raw: dict) -> dict[str, str]:
    return {_decode(key): _decode(value) for key, value in (raw or {}).items()}


async def _truth_count(redis_client) -> int:
    raw = await redis_client.hget(
        RedisKey.runtime_truth_conflict_index(),
        "__runtime_truth_conflict_count",
    )
    return int(raw or 0)


async def _truth_rows(redis_client) -> list[dict[str, str]]:
    rows = await redis_client.xrange(RedisKey.runtime_truth_conflict_stream())
    return [_decode_mapping(fields) for _entry_id, fields in rows]


async def _seed_task(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    task_state: str = "running",
    run_state: str | None = "running",
    requeue_count: int = 0,
) -> None:
    await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
    await redis_client.set(DagRedisKey.task_state(task_id), task_state)
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": "worker-1",
            "scheduler_epoch": "sched-1",
            "claim_epoch": "4",
            "last_heartbeat_at_ms": "100",
            "requeue_count": str(requeue_count),
        },
    )
    await redis_client.zadd(
        DagRedisKey.task_running_zset(tenant_id),
        {task_id: 100},
    )
    if run_state is not None:
        await redis_client.set(RedisKey.run_state(run_id), run_state)


async def _seed_run_recovery(
    redis_client,
    *,
    run_id: str,
    task_id: str,
    tenant_id: str = "tenant-recovery",
    run_state: str = "running",
    task_state: str = "running",
    reschedule_count: int = 0,
) -> None:
    await _seed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        task_state=task_state,
        run_state=run_state,
    )
    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "fake",
            "worker_group": "workers",
            "state": run_state,
            "reschedule_count": str(reschedule_count),
            "admitted_at": "1",
        },
    )
    await redis_client.zadd(RedisKey.cp_running(), {run_id: 1})


async def _task_snapshot(redis_client, *, task_id: str, tenant_id: str) -> dict:
    return {
        "state": _decode(await redis_client.get(DagRedisKey.task_state(task_id))),
        "meta": _decode_mapping(
            await redis_client.hgetall(DagRedisKey.task_meta(task_id))
        ),
        "ready": await redis_client.zscore(
            DagRedisKey.tenant_ready_queue(tenant_id), task_id
        ),
        "running": await redis_client.zscore(
            DagRedisKey.task_running_zset(tenant_id), task_id
        ),
        "completion_len": await redis_client.xlen(
            DagRedisKey.completion_stream(tenant_id)
        ),
    }


@pytest.mark.parametrize(
    "run_state",
    ["admitted", "queued", "scheduled", "running", "rescheduled"],
)
async def test_each_nonterminal_run_truth_allows_atomic_task_requeue(
    redis_client, run_state: str
) -> None:
    task_id = f"task-requeue-{run_state}"
    run_id = f"run-requeue-{run_state}"
    tenant_id = "tenant-requeue"
    await _seed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        run_state=run_state,
    )
    manager = TaskRecoveryManager(
        redis_client,
        HeartbeatPolicy(max_requeue_count=3),
    )

    result = await manager.requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=200,
        ready_score=200,
    )

    assert result.ok is True
    assert result.status == "TASK_REQUEUED"
    assert result.requeue_count == 1
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "ready"
    meta = _decode_mapping(await redis_client.hgetall(DagRedisKey.task_meta(task_id)))
    assert meta["requeue_count"] == "1"
    assert meta["worker_instance_id"] == ""
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) is None
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), task_id) == 200.0
    assert await redis_client.xlen(DagRedisKey.completion_stream(tenant_id)) == 1
    assert await _truth_count(redis_client) == 0


@pytest.mark.parametrize("run_state", ["done", "failed", "rejected", "dead_lettered"])
async def test_terminal_run_blocks_requeue_with_zero_lifecycle_mutation(
    redis_client, run_state: str
) -> None:
    task_id = f"task-terminal-requeue-{run_state}"
    run_id = f"run-terminal-requeue-{run_state}"
    tenant_id = "tenant-terminal-requeue"
    await _seed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        run_state=run_state,
    )
    before = await _task_snapshot(redis_client, task_id=task_id, tenant_id=tenant_id)
    manager = TaskRecoveryManager(redis_client)

    result = await manager.requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=300,
    )

    assert result.ok is False
    assert result.status == "run_truth_terminal_conflict"
    assert await _task_snapshot(redis_client, task_id=task_id, tenant_id=tenant_id) == before
    assert await _truth_count(redis_client) == 1
    row = (await _truth_rows(redis_client))[0]
    assert row["operation"] == "TASK_REQUEUE"
    assert row["run_id"] == run_id
    assert row["task_id"] == task_id
    assert row["observed_run_state"] == run_state


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("missing", "run_truth_missing"),
        ("unknown", "run_truth_corruption_conflict"),
        ("wrong_type", "run_truth_corruption_conflict"),
    ],
)
async def test_missing_unknown_and_wrong_type_run_truth_fail_closed(
    redis_client, mode: str, expected: str
) -> None:
    task_id = f"task-run-{mode}"
    run_id = f"run-{mode}"
    tenant_id = "tenant-run-truth"
    await _seed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        run_state=None,
    )
    if mode == "unknown":
        await redis_client.set(RedisKey.run_state(run_id), "mystery")
    elif mode == "wrong_type":
        await redis_client.hset(RedisKey.run_state(run_id), mapping={"state": "running"})
    before = await _task_snapshot(redis_client, task_id=task_id, tenant_id=tenant_id)

    result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=400,
    )

    assert result.status == expected
    assert await _task_snapshot(redis_client, task_id=task_id, tenant_id=tenant_id) == before
    assert await _truth_count(redis_client) == 1


async def test_wrong_explicit_run_id_writes_no_conflict_against_wrong_run(redis_client) -> None:
    task_id = "task-wrong-explicit-run"
    run_id = "run-correct"
    wrong_run_id = "run-wrong"
    tenant_id = "tenant-wrong-explicit"
    await _seed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        run_state="running",
    )
    await redis_client.set(RedisKey.run_state(wrong_run_id), "done")

    result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=wrong_run_id,
        tenant_id=tenant_id,
        now_ms=500,
    )

    assert result.status == "identity_run_id_mismatch"
    assert await _truth_count(redis_client) == 0
    assert await redis_client.xlen(RedisKey.runtime_truth_conflict_stream()) == 0


async def test_missing_task_state_is_durable_candidate_and_deduplicates(redis_client) -> None:
    task_id = "task-missing-state-requeue"
    run_id = "run-missing-state-requeue"
    tenant_id = "tenant-missing-state"
    await _seed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        run_state="running",
    )
    await redis_client.delete(DagRedisKey.task_state(task_id))
    manager = TaskRecoveryManager(redis_client)

    first = await manager.requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=600,
    )
    second = await manager.requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=700,
    )

    assert first.status == second.status == "task_truth_missing"
    assert await _truth_count(redis_client) == 1
    assert await redis_client.xlen(RedisKey.runtime_truth_conflict_stream()) == 1


async def test_missing_task_meta_requires_exact_run_membership_before_evidence(redis_client) -> None:
    task_id = "task-missing-meta-requeue"
    run_id = "run-missing-meta-requeue"
    tenant_id = "tenant-missing-meta"
    await _seed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        run_state="running",
    )
    await redis_client.delete(DagRedisKey.task_meta(task_id))

    result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=800,
    )

    assert result.status == "task_truth_corruption_conflict"
    row = (await _truth_rows(redis_client))[0]
    assert row["detail_code"] == "task_meta_missing"
    assert row["run_id"] == run_id


async def test_conflict_store_corruption_blocks_requeue_without_mutation(redis_client) -> None:
    task_id = "task-store-corrupt-requeue"
    run_id = "run-store-corrupt-requeue"
    tenant_id = "tenant-store-corrupt"
    await _seed_task(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        run_state="done",
    )
    await redis_client.set(RedisKey.runtime_truth_conflict_index(), "wrong-type")
    before = await _task_snapshot(redis_client, task_id=task_id, tenant_id=tenant_id)

    result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=900,
    )

    assert result.status == "truth_conflict_evidence_store_unavailable"
    assert await _task_snapshot(redis_client, task_id=task_id, tenant_id=tenant_id) == before


async def test_run_recovery_reschedules_only_after_all_task_truth_is_valid(redis_client) -> None:
    run_id = "run-recovery-success"
    task_id = "task-recovery-success"
    await _seed_run_recovery(redis_client, run_id=run_id, task_id=task_id)
    service = RecoveryService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-recovery", max_reschedule_attempts=3),
    )

    result = await service._commit_recovery(
        run_id=run_id,
        expected_reschedule_count=0,
        requested_action="RESCHEDULE",
        reason_code="stale_running",
        now_ms=1000,
        running_score=1000,
    )

    assert result.committed is True
    assert result.status == "RUN_RESCHEDULED"
    assert result.reschedule_count == 1
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "rescheduled"
    meta = _decode_mapping(await redis_client.hgetall(RedisKey.run_meta(run_id)))
    assert meta["state"] == "rescheduled"
    assert meta["reschedule_count"] == "1"
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == 1000.0
    assert await _truth_count(redis_client) == 0


async def test_terminal_run_in_running_projection_is_not_silently_cleaned(redis_client) -> None:
    run_id = "run-terminal-projection"
    task_id = "task-terminal-projection"
    await _seed_run_recovery(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        run_state="done",
        task_state="done",
    )
    service = RecoveryService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-terminal-projection"),
    )

    stale = await service._find_stale_runs()
    result = await service._commit_recovery(
        run_id=run_id,
        expected_reschedule_count=0,
        requested_action="RESCHEDULE",
        reason_code="stale_running",
        now_ms=1100,
    )

    assert run_id in stale
    assert result.status == "run_truth_terminal_conflict"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "done"
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == 1.0
    assert await _truth_count(redis_client) == 1


async def test_nonterminal_run_with_terminal_task_blocks_run_recovery(redis_client) -> None:
    run_id = "run-task-terminal-conflict"
    task_id = "task-terminal-conflict"
    await _seed_run_recovery(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        run_state="running",
        task_state="done",
    )
    service = RecoveryService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-task-terminal"),
    )

    result = await service._commit_recovery(
        run_id=run_id,
        expected_reschedule_count=0,
        requested_action="RESCHEDULE",
        reason_code="stale_running",
        now_ms=1200,
    )

    assert result.status == "task_truth_terminal_conflict"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == 1.0
    assert await _truth_count(redis_client) == 1


async def test_auto_dead_letter_is_blocked_until_explicit_task_terminalization(redis_client) -> None:
    run_id = "run-dead-letter-guard"
    task_id = "task-dead-letter-guard"
    await _seed_run_recovery(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        reschedule_count=3,
    )
    service = RecoveryService(
        redis_client,
        ControlPlaneConfig(
            instance_id="cp-dead-letter-guard",
            max_reschedule_attempts=3,
        ),
    )

    result = await service._commit_recovery(
        run_id=run_id,
        expected_reschedule_count=3,
        requested_action="DEAD_LETTER",
        reason_code="max_reschedule_exceeded",
        now_ms=1300,
    )

    assert result.status == "task_truth_terminal_conflict"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == 1.0
    row = (await _truth_rows(redis_client))[0]
    assert row["detail_code"] == "dead_letter_requires_explicit_task_terminalization"


async def test_run_recovery_count_race_is_fail_closed_without_evidence_noise(redis_client) -> None:
    run_id = "run-count-race"
    task_id = "task-count-race"
    await _seed_run_recovery(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        reschedule_count=1,
    )
    service = RecoveryService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-count-race"),
    )

    result = await service._commit_recovery(
        run_id=run_id,
        expected_reschedule_count=0,
        requested_action="RESCHEDULE",
        reason_code="stale_running",
        now_ms=1400,
    )

    assert result.status == "RUN_RECOVERY_COUNT_CONFLICT"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    assert await _truth_count(redis_client) == 0


async def test_run_state_read_model_preserves_run_scope_and_reports_conflict(redis_client) -> None:
    run_id = "run-read-model-conflict"
    task_id = "task-read-model-conflict"
    tenant_id = "tenant-read-model"
    await _seed_run_recovery(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="done",
        task_state="running",
    )
    service = ControlPlaneService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-read-model"),
    )

    result = await service.get_run_state(run_id)

    assert result["state"] == "done"
    assert result["tenant_id"] == tenant_id
    assert result["task_count"] == 1
    assert result["task_truth"][0]["state"] == "running"
    assert result["truth_conflict"] is True
    assert result["truth_status"] == "conflict"
    assert any(
        row["detail_code"] == "run_terminal_task_nonterminal"
        for row in result["truth_conflicts"]
    )


async def test_run_state_read_model_handles_wrong_types_without_raw_redis_error(redis_client) -> None:
    run_id = "run-read-model-wrong-type"
    await redis_client.hset(RedisKey.run_state(run_id), mapping={"state": "running"})
    await redis_client.set(RedisKey.run_meta(run_id), "wrong-type")
    await redis_client.set(DagRedisKey.run_tasks(run_id), "wrong-type")
    service = ControlPlaneService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-read-wrong-type"),
    )

    result = await service.get_run_state(run_id)

    assert result["state"] == "unknown"
    assert result["truth_conflict"] is True
    details = {row["detail_code"] for row in result["truth_conflicts"]}
    assert "run_state_key_type_mismatch" in details
    assert "run_meta_type_mismatch" in details
    assert "run_task_index_type_mismatch" in details

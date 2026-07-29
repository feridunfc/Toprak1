from __future__ import annotations

import pytest

from hfa.config.keys import RedisKey
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


async def _seed(
    redis_client,
    *,
    run_id: str,
    task_id: str,
    tenant_id: str,
    run_state: str,
    task_state: str,
) -> None:
    await redis_client.set(RedisKey.run_state(run_id), run_state)
    await redis_client.hset(
        RedisKey.run_meta(run_id),
        mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "fake",
            "worker_group": "workers",
            "state": run_state,
            "reschedule_count": "0",
            "admitted_at": "1",
        },
    )
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
            "claim_epoch": "1",
            "last_heartbeat_at_ms": "100",
            "requeue_count": "0",
        },
    )
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1})
    await redis_client.zadd(RedisKey.cp_running(), {run_id: 1})


async def _truth_rows(redis_client) -> list[dict[str, str]]:
    rows = await redis_client.xrange(RedisKey.runtime_truth_conflict_stream())
    return [_decode_mapping(fields) for _entry_id, fields in rows]


async def _operations(redis_client) -> list[str]:
    return [row.get("operation", "") for row in await _truth_rows(redis_client)]


async def test_stale_task_then_run_recovery_converges_without_truth_conflict(
    redis_client,
) -> None:
    run_id = "run-convergence-success"
    task_id = "task-convergence-success"
    tenant_id = "tenant-convergence-success"
    await _seed(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="running",
        task_state="running",
    )

    task_result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=200,
        ready_score=200,
    )
    recovery = RecoveryService(
        redis_client,
        ControlPlaneConfig(
            instance_id="cp-convergence-success",
            running_zset=RedisKey.cp_running(),
            max_reschedule_attempts=3,
        ),
    )
    run_result = await recovery._handle_stale(run_id)
    read_model = await ControlPlaneService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-convergence-read"),
    ).get_run_state(run_id)

    assert task_result.status == "TASK_REQUEUED"
    assert run_result == "rescheduled"
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "ready"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "rescheduled"
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) is None
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(tenant_id), task_id) == 200.0
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) is not None
    assert await redis_client.xlen(RedisKey.stream_control()) == 2
    assert await redis_client.xlen(RedisKey.runtime_truth_conflict_stream()) == 0
    assert read_model["state"] == "rescheduled"
    assert read_model["task_truth"][0]["state"] == "ready"
    assert read_model["truth_conflict"] is False


async def test_terminal_run_contradiction_blocks_both_recovery_callers_and_events(
    redis_client,
) -> None:
    run_id = "run-convergence-terminal"
    task_id = "task-convergence-terminal"
    tenant_id = "tenant-convergence-terminal"
    await _seed(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="done",
        task_state="running",
    )

    task_result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=300,
    )
    recovery = RecoveryService(
        redis_client,
        ControlPlaneConfig(
            instance_id="cp-convergence-terminal",
            running_zset=RedisKey.cp_running(),
        ),
    )
    run_result = await recovery._handle_stale(run_id)
    read_model = await ControlPlaneService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-convergence-terminal-read"),
    ).get_run_state(run_id)

    assert task_result.status == "run_truth_terminal_conflict"
    assert run_result == "conflict"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "done"
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == 1.0
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) == 1.0
    assert await redis_client.xlen(RedisKey.stream_control()) == 0
    assert sorted(await _operations(redis_client)) == ["RUN_RECOVERY", "TASK_REQUEUE"]
    assert read_model["state"] == "done"
    assert read_model["truth_conflict"] is True
    assert any(
        item["detail_code"] == "run_terminal_task_nonterminal"
        for item in read_model["truth_conflicts"]
    )


async def test_wrong_type_ready_queue_blocks_task_requeue_before_mutation(
    redis_client,
) -> None:
    run_id = "run-ready-wrong-type"
    task_id = "task-ready-wrong-type"
    tenant_id = "tenant-ready-wrong-type"
    await _seed(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="running",
        task_state="running",
    )
    ready_key = DagRedisKey.tenant_ready_queue(tenant_id)
    await redis_client.set(ready_key, "wrong-type")
    before_meta = _decode_mapping(
        await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    )

    result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=400,
        ready_score=400,
    )

    assert result.status == "task_truth_corruption_conflict"
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert _decode_mapping(await redis_client.hgetall(DagRedisKey.task_meta(task_id))) == before_meta
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) == 1.0
    assert _decode(await redis_client.get(ready_key)) == "wrong-type"
    assert (await _truth_rows(redis_client))[0]["detail_code"] == "ready_queue_type_mismatch"


async def test_wrong_type_completion_stream_blocks_task_requeue_before_mutation(
    redis_client,
) -> None:
    run_id = "run-completion-wrong-type"
    task_id = "task-completion-wrong-type"
    tenant_id = "tenant-completion-wrong-type"
    await _seed(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="running",
        task_state="running",
    )
    completion_key = DagRedisKey.completion_stream(tenant_id)
    await redis_client.set(completion_key, "wrong-type")
    before_meta = _decode_mapping(
        await redis_client.hgetall(DagRedisKey.task_meta(task_id))
    )

    result = await TaskRecoveryManager(redis_client).requeue_stale_task(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        now_ms=500,
    )

    assert result.status == "task_truth_corruption_conflict"
    assert _decode(await redis_client.get(DagRedisKey.task_state(task_id))) == "running"
    assert _decode_mapping(await redis_client.hgetall(DagRedisKey.task_meta(task_id))) == before_meta
    assert await redis_client.zscore(DagRedisKey.task_running_zset(tenant_id), task_id) == 1.0
    assert _decode(await redis_client.get(completion_key)) == "wrong-type"
    assert (await _truth_rows(redis_client))[0]["detail_code"] == "completion_stream_type_mismatch"


async def test_wrong_type_control_stream_blocks_run_recovery_before_mutation(
    redis_client,
) -> None:
    run_id = "run-control-wrong-type"
    task_id = "task-control-wrong-type"
    tenant_id = "tenant-control-wrong-type"
    await _seed(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="running",
        task_state="running",
    )
    control_key = RedisKey.stream_control()
    await redis_client.set(control_key, "wrong-type")
    before_meta = _decode_mapping(await redis_client.hgetall(RedisKey.run_meta(run_id)))
    recovery = RecoveryService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-control-wrong-type"),
    )

    result = await recovery._commit_recovery(
        run_id=run_id,
        expected_reschedule_count=0,
        requested_action="RESCHEDULE",
        reason_code="stale_running",
        now_ms=600,
        running_score=600,
    )

    assert result.status == "run_truth_corruption_conflict"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    assert _decode_mapping(await redis_client.hgetall(RedisKey.run_meta(run_id))) == before_meta
    assert await redis_client.zscore(RedisKey.cp_running(), run_id) == 1.0
    assert _decode(await redis_client.get(control_key)) == "wrong-type"
    assert (await _truth_rows(redis_client))[0]["detail_code"] == "control_stream_type_mismatch"


async def test_wrong_type_running_projection_blocks_run_recovery_before_mutation(
    redis_client,
) -> None:
    run_id = "run-projection-wrong-type"
    task_id = "task-projection-wrong-type"
    tenant_id = "tenant-projection-wrong-type"
    await _seed(
        redis_client,
        run_id=run_id,
        task_id=task_id,
        tenant_id=tenant_id,
        run_state="running",
        task_state="running",
    )
    running_key = RedisKey.cp_running()
    await redis_client.delete(running_key)
    await redis_client.set(running_key, "wrong-type")
    before_meta = _decode_mapping(await redis_client.hgetall(RedisKey.run_meta(run_id)))
    recovery = RecoveryService(
        redis_client,
        ControlPlaneConfig(instance_id="cp-projection-wrong-type"),
    )

    result = await recovery._commit_recovery(
        run_id=run_id,
        expected_reschedule_count=0,
        requested_action="RESCHEDULE",
        reason_code="stale_running",
        now_ms=700,
        running_score=700,
    )

    assert result.status == "run_truth_corruption_conflict"
    assert _decode(await redis_client.get(RedisKey.run_state(run_id))) == "running"
    assert _decode_mapping(await redis_client.hgetall(RedisKey.run_meta(run_id))) == before_meta
    assert _decode(await redis_client.get(running_key)) == "wrong-type"
    assert (await _truth_rows(redis_client))[0]["detail_code"] == "running_projection_type_mismatch"

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import (
    DagRedisKey,
    DagTaskSeed,
)
from hfa_control.dag_lua import DagLua


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if value is None:
        return ""
    return str(value)


def _mapping(raw: dict[Any, Any]) -> dict[str, str]:
    return {
        _text(key): _text(value)
        for key, value in raw.items()
    }


async def _dag(redis_client) -> DagLua:
    dag = DagLua(redis_client)
    await dag.initialise()
    return dag


async def _seed_ready(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str = "tenant-77",
):
    dag = await _dag(redis_client)

    seed = DagTaskSeed(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="default",
        priority=5,
        admitted_at=float(int(time.time() * 1000)),
        payload_json='{"sprint":77}',
        dependency_count=0,
        trace_parent="trace-parent-77",
        trace_state="trace-state-77",
        region="eu-77",
        policy="LEAST_LOADED",
    )

    admitted = await dag.task_admit(seed)

    assert admitted.admitted is True
    assert admitted.ready is True

    return dag, seed


def _dispatch(
    seed: DagTaskSeed,
    *,
    run_id: str | None = None,
    scheduler_epoch: str = "epoch-77",
):
    scheduled_at = float(int(time.time() * 1000))

    return SimpleNamespace(
        task_id=seed.task_id,
        run_id=seed.run_id if run_id is None else run_id,
        tenant_id=seed.tenant_id,
        agent_type=seed.agent_type,
        worker_group="worker-group-77",
        shard=0,
        priority=seed.priority,
        admitted_at=seed.admitted_at,
        scheduled_at=scheduled_at,
        scheduled_zset=DagRedisKey.task_scheduled_zset(
            seed.tenant_id
        ),
        running_zset=DagRedisKey.task_running_zset(
            seed.tenant_id
        ),
        control_stream=RedisKey.stream_control(),
        shard_stream=RedisKey.stream_shard(0),
        payload_json=seed.payload_json,
        trace_parent=seed.trace_parent,
        trace_state=seed.trace_state,
        region=seed.region,
        policy=seed.policy,
        scheduler_epoch=scheduler_epoch,
    )


async def _snapshot(
    redis_client,
    *,
    task_id: str,
    tenant_id: str,
) -> dict[str, Any]:
    return {
        "state": _text(
            await redis_client.get(
                DagRedisKey.task_state(task_id)
            )
        ),
        "ready_score": await redis_client.zscore(
            DagRedisKey.task_ready_queue(tenant_id),
            task_id,
        ),
        "scheduled_score": await redis_client.zscore(
            DagRedisKey.task_scheduled_zset(tenant_id),
            task_id,
        ),
        "running_score": await redis_client.zscore(
            DagRedisKey.task_running_zset(tenant_id),
            task_id,
        ),
        "meta": _mapping(
            await redis_client.hgetall(
                DagRedisKey.task_meta(task_id)
            )
        ),
        "control_len": await redis_client.xlen(
            RedisKey.stream_control()
        ),
        "shard_len": await redis_client.xlen(
            RedisKey.stream_shard(0)
        ),
    }


async def _latest_fields(
    redis_client,
    stream: str,
) -> dict[str, str]:
    entries = await redis_client.xrevrange(
        stream,
        max="+",
        min="-",
        count=1,
    )

    assert entries

    return _mapping(entries[0][1])


async def test_canonical_dispatch_propagates_identity_and_epoch_without_running_write(
    redis_client,
) -> None:
    dag, seed = await _seed_ready(
        redis_client,
        task_id="task-distinct-77",
        run_id="run-distinct-77",
    )

    result = await dag.task_dispatch_commit(
        _dispatch(
            seed,
            scheduler_epoch="scheduler-epoch-77",
        )
    )

    assert result.committed is True
    assert result.status == "committed"

    state = _text(
        await redis_client.get(
            DagRedisKey.task_state(seed.task_id)
        )
    )
    meta = _mapping(
        await redis_client.hgetall(
            DagRedisKey.task_meta(seed.task_id)
        )
    )

    ready_score = await redis_client.zscore(
        DagRedisKey.task_ready_queue(seed.tenant_id),
        seed.task_id,
    )
    scheduled_score = await redis_client.zscore(
        DagRedisKey.task_scheduled_zset(seed.tenant_id),
        seed.task_id,
    )
    running_score = await redis_client.zscore(
        DagRedisKey.task_running_zset(seed.tenant_id),
        seed.task_id,
    )

    control = await _latest_fields(
        redis_client,
        RedisKey.stream_control(),
    )
    shard = await _latest_fields(
        redis_client,
        RedisKey.stream_shard(0),
    )

    assert state == "scheduled"
    assert ready_score is None
    assert scheduled_score is not None

    # A task becomes running only after task_claim_start.
    assert running_score is None

    assert meta["task_id"] == seed.task_id
    assert meta["run_id"] == seed.run_id
    assert meta["scheduler_epoch"] == "scheduler-epoch-77"

    assert control["event_type"] == "TaskScheduled"
    assert control["task_id"] == seed.task_id
    assert control["run_id"] == seed.run_id
    assert control["scheduler_epoch"] == "scheduler-epoch-77"

    assert shard["event_type"] == "TaskRequested"
    assert shard["task_id"] == seed.task_id
    assert shard["run_id"] == seed.run_id
    assert shard["scheduler_epoch"] == "scheduler-epoch-77"


@pytest.mark.parametrize(
    ("case_name", "expected_status"),
    [
        ("missing_task_meta", "missing_task_meta"),
        (
            "identity_task_id_missing",
            "identity_task_id_missing",
        ),
        (
            "identity_task_id_mismatch",
            "identity_task_id_mismatch",
        ),
        (
            "identity_run_id_missing",
            "identity_run_id_missing",
        ),
        (
            "identity_run_id_mismatch",
            "identity_run_id_mismatch",
        ),
    ],
)
async def test_identity_failure_precedes_every_dispatch_mutation(
    redis_client,
    case_name: str,
    expected_status: str,
) -> None:
    task_id = f"task-{case_name}-77"
    run_id = f"run-{case_name}-77"

    dag, seed = await _seed_ready(
        redis_client,
        task_id=task_id,
        run_id=run_id,
    )

    meta_key = DagRedisKey.task_meta(task_id)

    if case_name == "missing_task_meta":
        await redis_client.delete(meta_key)
    elif case_name == "identity_task_id_missing":
        await redis_client.hdel(meta_key, "task_id")
    elif case_name == "identity_task_id_mismatch":
        await redis_client.hset(
            meta_key,
            "task_id",
            "different-authoritative-task-77",
        )
    elif case_name == "identity_run_id_missing":
        await redis_client.hdel(meta_key, "run_id")
    elif case_name == "identity_run_id_mismatch":
        await redis_client.hset(
            meta_key,
            "run_id",
            "different-authoritative-run-77",
        )
    else:
        raise AssertionError(
            f"unknown test case: {case_name}"
        )

    before = await _snapshot(
        redis_client,
        task_id=task_id,
        tenant_id=seed.tenant_id,
    )

    result = await dag.task_dispatch_commit(
        _dispatch(
            seed,
            scheduler_epoch="scheduler-epoch-77",
        )
    )

    after = await _snapshot(
        redis_client,
        task_id=task_id,
        tenant_id=seed.tenant_id,
    )

    assert result.committed is False
    assert result.status == expected_status

    # Identity rejection must happen before the first mutation.
    assert after == before

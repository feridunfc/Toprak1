from __future__ import annotations

import time
from typing import Any

import pytest

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey, DagTaskSeed
from hfa_control.dag_lua import DagLua
from hfa_control.dag_scheduler_bridge import (
    DagReadyQueue,
    DagSchedulerDispatchWriter,
)
from hfa_control.scheduler_reservation_dispatch import (
    SchedulerReservationDispatcher,
)
from hfa_control.worker_reservation import (
    WorkerReservationManager,
)


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
    tenant_id: str = "tenant-writer-77",
) -> tuple[DagLua, DagTaskSeed]:
    dag = await _dag(redis_client)

    seed = DagTaskSeed(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="default",
        worker_group="seed-group-77",
        priority=5,
        admitted_at=float(int(time.time() * 1000)),
        dependency_count=0,
        payload_json='{"source":"sprint-77"}',
        region="seed-region-77",
        policy="LEAST_LOADED",
        trace_parent="trace-parent-77",
        trace_state="trace-state-77",
    )

    result = await dag.task_admit(seed)

    assert result.admitted is True
    assert result.ready is True
    await redis_client.set(
        RedisKey.run_state(run_id),
        "admitted",
    )

    return dag, seed


def _dispatcher(
    redis_client,
    *,
    dag: DagLua,
) -> SchedulerReservationDispatcher:
    ready_queue = DagReadyQueue(redis_client)
    writer = DagSchedulerDispatchWriter(
        ready_queue=ready_queue,
        dag_lua=dag,
    )

    reservation_manager = WorkerReservationManager(
        redis_client,
        reservation_ttl_seconds=30,
        scheduler_id="scheduler-writer-77",
    )

    return SchedulerReservationDispatcher(
        reservation_manager,
        writer,
    )


async def _latest_fields(
    redis_client,
    stream: str,
) -> dict[str, str]:
    rows = await redis_client.xrevrange(
        stream,
        max="+",
        min="-",
        count=1,
    )

    assert rows

    return _mapping(rows[0][1])


async def test_scheduler_dispatcher_commits_through_canonical_dag_writer(
    redis_client,
) -> None:
    dag, seed = await _seed_ready(
        redis_client,
        task_id="task-writer-distinct-77",
        run_id="run-writer-distinct-77",
    )

    dispatcher = _dispatcher(
        redis_client,
        dag=dag,
    )

    result = await dispatcher.reserve_and_dispatch(
        task_id=seed.task_id,
        worker_id="worker-writer-77",
        scheduler_epoch="epoch-writer-77",
        dispatch_payload={
            "task_id": seed.task_id,
            "run_id": seed.run_id,
            "tenant_id": seed.tenant_id,
            "worker_group": "dispatch-group-77",
            "shard": 2,
            "region": "dispatch-region-77",
        },
        reserved_at_ms=177000,
    )

    assert result.ok is True
    assert result.status == "reserved_and_dispatched"
    assert result.task_id == seed.task_id
    assert result.run_id == seed.run_id
    assert result.scheduler_epoch == "epoch-writer-77"
    assert result.committed_state == "scheduled"

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
    reservation = _mapping(
        await redis_client.hgetall(
            DagRedisKey.worker_reservation(
                "worker-writer-77"
            )
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
        RedisKey.stream_shard(2),
    )

    assert state == "scheduled"
    assert ready_score is None
    assert scheduled_score is not None
    assert running_score is None

    assert meta["task_id"] == seed.task_id
    assert meta["run_id"] == seed.run_id
    assert meta["worker_group"] == "dispatch-group-77"
    assert meta["shard"] == "2"
    assert meta["dispatch_region"] == "dispatch-region-77"
    assert meta["scheduler_epoch"] == "epoch-writer-77"

    assert reservation["task_id"] == seed.task_id
    assert reservation["scheduler_epoch"] == "epoch-writer-77"

    assert control["event_type"] == "TaskScheduled"
    assert control["task_id"] == seed.task_id
    assert control["run_id"] == seed.run_id
    assert control["scheduler_epoch"] == "epoch-writer-77"

    assert shard["event_type"] == "TaskRequested"
    assert shard["task_id"] == seed.task_id
    assert shard["run_id"] == seed.run_id
    assert shard["scheduler_epoch"] == "epoch-writer-77"


async def test_real_writer_identity_rejection_releases_reservation(
    redis_client,
) -> None:
    dag, seed = await _seed_ready(
        redis_client,
        task_id="task-writer-reject-77",
        run_id="run-writer-reject-77",
    )

    # Corrupt authoritative metadata after admission.
    await redis_client.hset(
        DagRedisKey.task_meta(seed.task_id),
        "run_id",
        "different-authoritative-run-77",
    )

    dispatcher = _dispatcher(
        redis_client,
        dag=dag,
    )

    control_len_before = await redis_client.xlen(
        RedisKey.stream_control()
    )
    shard_len_before = await redis_client.xlen(
        RedisKey.stream_shard(3)
    )

    result = await dispatcher.reserve_and_dispatch(
        task_id=seed.task_id,
        worker_id="worker-reject-77",
        scheduler_epoch="epoch-reject-77",
        dispatch_payload={
            "task_id": seed.task_id,
            "run_id": seed.run_id,
            "tenant_id": seed.tenant_id,
            "worker_group": "reject-group-77",
            "shard": 3,
            "region": "reject-region-77",
        },
        reserved_at_ms=178000,
    )

    assert result.ok is False
    assert result.status == "identity_run_id_mismatch"
    assert result.reason == "different-authoritative-run-77"

    # Structured writer failure must release the worker reservation.
    assert (
        await redis_client.exists(
            DagRedisKey.worker_reservation(
                "worker-reject-77"
            )
        )
        == 0
    )

    # Lua identity rejection precedes every dispatch mutation.
    assert (
        _text(
            await redis_client.get(
                DagRedisKey.task_state(seed.task_id)
            )
        )
        == "ready"
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_ready_queue(seed.tenant_id),
            seed.task_id,
        )
        is not None
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_scheduled_zset(seed.tenant_id),
            seed.task_id,
        )
        is None
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_running_zset(seed.tenant_id),
            seed.task_id,
        )
        is None
    )

    assert (
        await redis_client.xlen(
            RedisKey.stream_control()
        )
        == control_len_before
    )
    assert (
        await redis_client.xlen(
            RedisKey.stream_shard(3)
        )
        == shard_len_before
    )

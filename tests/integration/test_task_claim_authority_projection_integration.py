from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from hfa.authority import AggregateType, CanonicalAggregateIdentity
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim_authority import (
    TaskClaimCanonicalProjectionInput,
)
from hfa_control.worker_reservation import WorkerReservationManager

pytestmark = pytest.mark.asyncio


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _operation_ids(
    *,
    task_id: str,
    run_id: str,
    attempt: int,
) -> tuple[str, str]:
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=run_id,
        task_id=task_id,
    )
    dispatch = (
        f"task-dispatch:v1:{identity.sha256}:attempt:{attempt}"
    )
    claim = (
        f"task-claim:v1:{identity.sha256}:attempt:{attempt}"
    )
    return dispatch, claim


async def _seed_canonical_dispatch(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    scheduler_epoch: str,
    attempt: int = 2,
    dispatch_revision: int = 4,
) -> TaskClaimCanonicalProjectionInput:
    dispatch_operation_id, claim_operation_id = _operation_ids(
        task_id=task_id,
        run_id=run_id,
        attempt=attempt,
    )
    dispatch_transition_id = f"dispatch-transition:{task_id}"
    dispatch_record_hash = _sha256(
        f"dispatch-record:{task_id}"
    )
    dispatch_command_hash = _sha256(
        f"dispatch-command:{task_id}"
    )

    await redis_client.set(
        DagRedisKey.task_state(task_id),
        "scheduled",
    )
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "scheduler_epoch": scheduler_epoch,
            "dispatch_attempt": str(attempt),
            "dispatch_worker_id": worker_id,
            "claim_epoch": "0",
            "canonical_transition_id": dispatch_transition_id,
            "canonical_record_hash": dispatch_record_hash,
            "canonical_command_hash": dispatch_command_hash,
            "canonical_revision": str(dispatch_revision),
            "canonical_operation_id": dispatch_operation_id,
        },
    )
    await redis_client.set(
        RedisKey.run_state(run_id),
        "running",
    )
    await redis_client.zadd(
        DagRedisKey.task_scheduled_zset(tenant_id),
        {task_id: 100.0},
    )

    reservation = WorkerReservationManager(
        redis_client,
        reservation_ttl_seconds=30,
    )
    reserved = await reservation.reserve(
        worker_id=worker_id,
        task_id=task_id,
        scheduler_epoch=scheduler_epoch,
        reserved_at_ms=90,
    )
    assert reserved.ok is True

    return TaskClaimCanonicalProjectionInput(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        claimed_at_ms=100,
        dispatch_attempt=attempt,
        dispatch_revision=dispatch_revision,
        previous_claim_epoch=0,
        dispatch_transition_id=dispatch_transition_id,
        dispatch_record_hash=dispatch_record_hash,
        dispatch_command_hash=dispatch_command_hash,
        dispatch_operation_id=dispatch_operation_id,
        canonical_transition_id=f"claim-transition:{task_id}",
        canonical_record_hash=_sha256(
            f"claim-record:{task_id}"
        ),
        canonical_command_hash=_sha256(
            f"claim-command:{task_id}"
        ),
        canonical_revision=dispatch_revision + 1,
        canonical_operation_id=claim_operation_id,
        claim_epoch=1,
    )


@pytest.mark.integration
async def test_canonical_claim_projection_is_exact_and_duplicate_safe(
    redis_client,
):
    task_id = "canonical-claim-projection-1"
    run_id = "run-canonical-claim-projection-1"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "scheduler-epoch-a"

    projection = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    dag = DagLua(redis_client)

    first = await dag.task_claim_canonical_projection(projection)

    assert first.ok is True
    assert first.status == "task_claimed"
    assert first.claim_epoch == "1"
    assert (
        await redis_client.get(DagRedisKey.task_state(task_id))
        == "running"
    )

    meta = await redis_client.hgetall(
        DagRedisKey.task_meta(task_id)
    )
    assert meta["claim_epoch"] == "1"
    assert (
        meta["canonical_transition_id"]
        == projection.canonical_transition_id
    )
    assert (
        meta["canonical_operation_id"]
        == projection.canonical_operation_id
    )
    assert (
        meta["claim_canonical_transition_id"]
        == projection.canonical_transition_id
    )
    assert (
        meta["claim_canonical_operation_id"]
        == projection.canonical_operation_id
    )
    assert (
        meta["dispatch_canonical_transition_id"]
        == projection.dispatch_transition_id
    )
    assert (
        meta["dispatch_canonical_operation_id"]
        == projection.dispatch_operation_id
    )
    assert (
        await redis_client.exists(
            DagRedisKey.worker_reservation(worker_id)
        )
        == 0
    )
    assert (
        await redis_client.exists(
            DagRedisKey.task_reservation_owner(task_id)
        )
        == 0
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_scheduled_zset(tenant_id),
            task_id,
        )
        is None
    )
    running_score = await redis_client.zscore(
        DagRedisKey.task_running_zset(tenant_id),
        task_id,
    )
    assert running_score is not None

    before_meta = dict(meta)
    duplicate = await dag.task_claim_canonical_projection(
        projection
    )

    assert duplicate.ok is False
    assert (
        duplicate.status
        == "canonical_claim_already_projected"
    )
    assert duplicate.claim_epoch == "1"
    assert (
        await redis_client.hgetall(
            DagRedisKey.task_meta(task_id)
        )
        == before_meta
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_running_zset(tenant_id),
            task_id,
        )
        == running_score
    )


@pytest.mark.integration
async def test_canonical_claim_projection_rejects_dispatch_proof_mismatch(
    redis_client,
):
    task_id = "canonical-claim-projection-2"
    run_id = "run-canonical-claim-projection-2"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "scheduler-epoch-a"

    projection = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    invalid = replace(
        projection,
        dispatch_record_hash=_sha256(
            "wrong-dispatch-record"
        ),
    )
    before_meta = await redis_client.hgetall(
        DagRedisKey.task_meta(task_id)
    )

    result = await DagLua(
        redis_client
    ).task_claim_canonical_projection(invalid)

    assert result.ok is False
    assert result.status == "canonical_projection_conflict"
    assert (
        await redis_client.get(DagRedisKey.task_state(task_id))
        == "scheduled"
    )
    assert (
        await redis_client.hgetall(
            DagRedisKey.task_meta(task_id)
        )
        == before_meta
    )
    assert (
        await redis_client.exists(
            DagRedisKey.worker_reservation(worker_id)
        )
        == 1
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_scheduled_zset(tenant_id),
            task_id,
        )
        is not None
    )
    assert (
        await redis_client.zscore(
            DagRedisKey.task_running_zset(tenant_id),
            task_id,
        )
        is None
    )


@pytest.mark.integration
async def test_canonical_claim_duplicate_with_changed_proof_conflicts(
    redis_client,
):
    task_id = "canonical-claim-projection-3"
    run_id = "run-canonical-claim-projection-3"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "scheduler-epoch-a"

    projection = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    dag = DagLua(redis_client)
    first = await dag.task_claim_canonical_projection(projection)
    assert first.ok is True

    before_meta = await redis_client.hgetall(
        DagRedisKey.task_meta(task_id)
    )
    changed = replace(
        projection,
        canonical_record_hash=_sha256(
            "changed-claim-record"
        ),
    )

    result = await dag.task_claim_canonical_projection(changed)

    assert result.ok is False
    assert result.status == "canonical_projection_conflict"
    assert (
        await redis_client.hgetall(
            DagRedisKey.task_meta(task_id)
        )
        == before_meta
    )
    assert (
        await redis_client.get(DagRedisKey.task_state(task_id))
        == "running"
    )


@pytest.mark.integration
async def test_canonical_claim_terminal_duplicate_keeps_claim_proof(
    redis_client,
):
    task_id = "canonical-claim-projection-terminal"
    run_id = "run-canonical-claim-projection-terminal"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "scheduler-epoch-a"

    projection = await _seed_canonical_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    dag = DagLua(redis_client)
    first = await dag.task_claim_canonical_projection(projection)
    assert first.ok is True

    await redis_client.set(
        DagRedisKey.task_state(task_id),
        "done",
    )
    await redis_client.set(
        RedisKey.run_state(run_id),
        "done",
    )

    terminal_transition_id = f"complete-transition:{task_id}"
    terminal_operation_id = f"task-complete:test:{task_id}"
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "canonical_transition_id": terminal_transition_id,
            "canonical_record_hash": _sha256(
                f"complete-record:{task_id}"
            ),
            "canonical_command_hash": _sha256(
                f"complete-command:{task_id}"
            ),
            "canonical_revision": str(
                projection.canonical_revision + 1
            ),
            "canonical_operation_id": terminal_operation_id,
        },
    )

    before_meta = await redis_client.hgetall(
        DagRedisKey.task_meta(task_id)
    )
    assert (
        before_meta["canonical_transition_id"]
        == terminal_transition_id
    )
    assert (
        before_meta["claim_canonical_transition_id"]
        == projection.canonical_transition_id
    )
    assert (
        before_meta["claim_canonical_operation_id"]
        == projection.canonical_operation_id
    )

    duplicate = await dag.task_claim_canonical_projection(
        projection
    )

    assert duplicate.ok is False
    assert (
        duplicate.status
        == "canonical_claim_already_projected"
    )
    assert duplicate.claim_epoch == "1"
    after_meta = await redis_client.hgetall(
        DagRedisKey.task_meta(task_id)
    )
    assert after_meta == before_meta
    assert (
        after_meta["canonical_transition_id"]
        == terminal_transition_id
    )
    assert (
        after_meta["canonical_operation_id"]
        == terminal_operation_id
    )
    assert (
        after_meta["claim_canonical_transition_id"]
        == projection.canonical_transition_id
    )
    assert (
        after_meta["claim_canonical_operation_id"]
        == projection.canonical_operation_id
    )


@pytest.mark.integration
async def test_canonical_claim_invalid_hash_is_rejected_before_lua(
    redis_client,
):
    projection = await _seed_canonical_dispatch(
        redis_client,
        task_id="canonical-claim-projection-invalid-hash",
        run_id="run-canonical-claim-projection-invalid-hash",
        tenant_id="tenant-a",
        worker_id="worker-a",
        scheduler_epoch="scheduler-epoch-a",
    )
    before_meta = await redis_client.hgetall(
        DagRedisKey.task_meta(projection.task_id)
    )

    with pytest.raises(
        ValueError,
        match="canonical_record_hash must be a lowercase SHA-256",
    ):
        await DagLua(
            redis_client
        ).task_claim_canonical_projection(
            replace(
                projection,
                canonical_record_hash="not-a-sha256",
            )
        )

    assert (
        await redis_client.get(
            DagRedisKey.task_state(projection.task_id)
        )
        == "scheduled"
    )
    assert (
        await redis_client.hgetall(
            DagRedisKey.task_meta(projection.task_id)
        )
        == before_meta
    )

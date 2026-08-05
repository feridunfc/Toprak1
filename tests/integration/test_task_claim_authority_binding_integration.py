from __future__ import annotations

from unittest.mock import Mock

import pytest

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisCanonicalAuthorityStore,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim import TaskClaimManager
from hfa_control.task_claim_authority import (
    TASK_CLAIM_DUPLICATE_STATUS,
    TASK_CLAIM_PROJECTION_PENDING_STATUS,
    TASK_CLAIM_PROJECTED_STATUS,
)

pytestmark = pytest.mark.asyncio


def _context(command: AuthorityCommand, writer: str) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=writer,
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=(
            command.aggregate_identity.sha256
        ),
        fence_required=(command.operation_type is not OperationType.TASK_ADMIT),
        fence_valid=True,
    )


async def _commit(
    store: RedisCanonicalAuthorityStore,
    command: AuthorityCommand,
    *,
    revision: int,
    state: str | None,
    committed_at_ms: int,
):
    evaluation = evaluate_authority_commit(
        context=_context(command, f"test/{command.operation_type.value}"),
        command=command,
        current_revision=revision,
        current_state=state,
        receipt_probe=None,
        committed_at_ms=committed_at_ms,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    result = await store.commit(evaluation.commit_plan)
    assert result.status is RedisAuthorityCommitStatus.COMMITTED
    return evaluation.commit_plan.record


async def _seed_dispatch(
    redis_client,
    *,
    task_id: str,
    run_id: str,
    tenant_id: str,
    worker_id: str,
    scheduler_epoch: str,
):
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=run_id,
        task_id=task_id,
    )
    admit = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_ADMIT,
        operation_id=f"task-admit:v1:{identity.sha256}",
        expected_revision=0,
        intended_previous_state=None,
        intended_next_state="ready",
        authoritative_payload={"task_id": task_id, "run_id": run_id},
        authoritative_metadata_changes={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
        },
        requested_child_effects={},
        requested_projection_intents=(
            {
                "kind": "READY_QUEUE_IF_READY",
                "tenant_id": tenant_id,
                "task_id": task_id,
            },
        ),
    )
    await _commit(
        store,
        admit,
        revision=0,
        state=None,
        committed_at_ms=100,
    )
    dispatch_metadata = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "dispatch_attempt": 1,
        "scheduled_at_ms": 200,
    }
    dispatch = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:1",
        expected_revision=1,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload=dispatch_metadata,
        authoritative_metadata_changes=dispatch_metadata,
        requested_child_effects={
            "worker_reservation": {
                "worker_id": worker_id,
                "task_id": task_id,
                "scheduler_epoch": scheduler_epoch,
            }
        },
        requested_projection_intents=(
            {"kind": "CONTROL_NOTIFICATION", "stream": "control"},
            {"kind": "TASK_REQUEST_MESSAGE", "stream": "shard"},
        ),
    )
    dispatch_record = await _commit(
        store,
        dispatch,
        revision=1,
        state="ready",
        committed_at_ms=200,
    )

    await redis_client.set(DagRedisKey.task_state(task_id), "scheduled")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "scheduler_epoch": scheduler_epoch,
            "dispatch_attempt": "1",
            "dispatch_worker_id": worker_id,
            "claim_epoch": "0",
            "canonical_transition_id": dispatch_record.transition_id,
            "canonical_record_hash": dispatch_record.canonical_record_hash,
            "canonical_command_hash": dispatch_record.canonical_command_hash,
            "canonical_revision": str(dispatch_record.to_revision),
            "canonical_operation_id": dispatch_record.operation_id,
        },
    )
    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.zadd(
        DagRedisKey.task_scheduled_zset(tenant_id),
        {task_id: 200.0},
    )
    reservation = {
        "worker_id": worker_id,
        "task_id": task_id,
        "scheduler_epoch": scheduler_epoch,
        "reserved_at_ms": "190",
        "scheduler_id": "scheduler-1",
    }
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        mapping=reservation,
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(task_id),
        mapping=reservation,
    )
    await redis_client.expire(
        DagRedisKey.worker_reservation(worker_id), 60
    )
    await redis_client.expire(
        DagRedisKey.task_reservation_owner(task_id), 60
    )
    dag = DagLua(redis_client)
    await dag.initialise()
    return store, dag, identity


def _manager(dag, store):
    return TaskClaimManager(
        dag,
        object(),
        canonical_task_claim_binding=True,
        canonical_task_admit_binding=True,
        canonical_task_dispatch_binding=True,
        canonical_authority_store=store,
    )


@pytest.mark.integration
async def test_first_claim_and_exact_duplicate_are_canonical_and_event_safe(
    redis_client,
    monkeypatch,
):
    task_id = "claim-binding-1"
    run_id = "run-claim-binding-1"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "epoch-a"
    store, dag, identity = await _seed_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    manager = _manager(dag, store)

    first = await manager.claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=300,
        scheduler_epoch=scheduler_epoch,
    )
    assert first.ok is True
    assert first.status == TASK_CLAIM_PROJECTED_STATUS
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 3
    assert snapshot.state == "running"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"
    assert await redis_client.hget(
        DagRedisKey.task_meta(task_id), "claim_epoch"
    ) == "1"
    event.assert_called_once()

    duplicate = await manager.claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=999,
        scheduler_epoch=scheduler_epoch,
    )
    assert duplicate.ok is False
    assert duplicate.status == TASK_CLAIM_DUPLICATE_STATUS
    after = await store.get_aggregate_snapshot(identity)
    assert after is not None
    assert after.revision == 3
    assert await redis_client.hget(
        DagRedisKey.task_meta(task_id), "claim_epoch"
    ) == "1"
    event.assert_called_once()


@pytest.mark.integration
async def test_projection_failure_preserves_reservation_and_retry_projects_once(
    redis_client,
    monkeypatch,
):
    task_id = "claim-binding-pending"
    run_id = "run-claim-binding-pending"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "epoch-a"
    store, dag, identity = await _seed_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    original_projection = dag.task_claim_canonical_projection

    async def fail_projection(_projection):
        raise RuntimeError("simulated projection failure")

    dag.task_claim_canonical_projection = fail_projection
    manager = _manager(dag, store)
    pending = await manager.claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=300,
        scheduler_epoch=scheduler_epoch,
    )
    assert pending.status == TASK_CLAIM_PROJECTION_PENDING_STATUS
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 3
    assert snapshot.state == "running"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "scheduled"
    assert await redis_client.exists(
        DagRedisKey.worker_reservation(worker_id)
    ) == 1
    assert await redis_client.exists(
        DagRedisKey.task_reservation_owner(task_id)
    ) == 1
    event.assert_not_called()

    dag.task_claim_canonical_projection = original_projection
    projected = await manager.claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=999,
        scheduler_epoch=scheduler_epoch,
    )
    assert projected.ok is True
    assert projected.status == TASK_CLAIM_PROJECTED_STATUS
    after = await store.get_aggregate_snapshot(identity)
    assert after is not None
    assert after.revision == 3
    event.assert_called_once()


@pytest.mark.integration
async def test_conflicting_reservation_fails_before_claim_commit(
    redis_client,
):
    task_id = "claim-binding-conflict"
    run_id = "run-claim-binding-conflict"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "epoch-a"
    store, dag, identity = await _seed_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        "scheduler_epoch",
        "wrong-epoch",
    )
    result = await _manager(dag, store).claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=300,
        scheduler_epoch=scheduler_epoch,
    )
    assert result.ok is False
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 2
    assert snapshot.state == "scheduled"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "scheduled"


@pytest.mark.integration
async def test_terminal_head_allows_exact_claim_replay_but_not_execution(
    redis_client,
    monkeypatch,
):
    task_id = "claim-binding-terminal"
    run_id = "run-claim-binding-terminal"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "epoch-a"
    store, dag, identity = await _seed_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    manager = _manager(dag, store)
    first = await manager.claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=300,
        scheduler_epoch=scheduler_epoch,
    )
    assert first.ok is True

    complete = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_COMPLETE,
        operation_id=f"task-complete:test:{identity.sha256}",
        expected_revision=3,
        intended_previous_state="running",
        intended_next_state="done",
        authoritative_payload={"task_id": task_id},
        authoritative_metadata_changes={"task_id": task_id},
        requested_child_effects={},
        requested_projection_intents=(
            {"kind": "OUTPUT_PROJECTION"},
            {"kind": "DEPENDENCY_FANOUT_INTENT"},
        ),
    )
    await _commit(
        store,
        complete,
        revision=3,
        state="running",
        committed_at_ms=400,
    )
    await redis_client.set(DagRedisKey.task_state(task_id), "done")
    await redis_client.set(RedisKey.run_state(run_id), "done")

    duplicate = await manager.claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=999,
        scheduler_epoch=scheduler_epoch,
    )
    assert duplicate.ok is False
    assert duplicate.status == TASK_CLAIM_DUPLICATE_STATUS
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 4
    assert snapshot.state == "done"
    event.assert_called_once()


@pytest.mark.integration
async def test_default_disabled_path_does_not_advance_canonical_claim(
    redis_client,
):
    task_id = "claim-binding-legacy-default"
    run_id = "run-claim-binding-legacy-default"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "epoch-a"
    store, dag, identity = await _seed_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    result = await TaskClaimManager(dag, None).claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=300,
        scheduler_epoch=scheduler_epoch,
    )
    assert result.ok is True
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 2
    assert snapshot.state == "scheduled"
    assert await redis_client.get(DagRedisKey.task_state(task_id)) == "running"


@pytest.mark.integration
async def test_dispatch_worker_mismatch_fails_before_claim_commit(
    redis_client,
):
    task_id = "claim-binding-worker-mismatch"
    run_id = "run-claim-binding-worker-mismatch"
    tenant_id = "tenant-a"
    scheduler_epoch = "epoch-a"
    store, dag, identity = await _seed_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id="worker-a",
        scheduler_epoch=scheduler_epoch,
    )
    result = await _manager(dag, store).claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id="worker-b",
        claimed_at_ms=300,
        scheduler_epoch=scheduler_epoch,
    )
    assert result.ok is False
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 2
    assert snapshot.state == "scheduled"


@pytest.mark.integration
async def test_changed_projected_claim_proof_fails_closed_without_execution(
    redis_client,
    monkeypatch,
):
    task_id = "claim-binding-changed-proof"
    run_id = "run-claim-binding-changed-proof"
    tenant_id = "tenant-a"
    worker_id = "worker-a"
    scheduler_epoch = "epoch-a"
    store, dag, identity = await _seed_dispatch(
        redis_client,
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_id=worker_id,
        scheduler_epoch=scheduler_epoch,
    )
    event = Mock()
    monkeypatch.setattr("hfa_control.task_claim.emit_event_background", event)
    manager = _manager(dag, store)
    first = await manager.claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=300,
        scheduler_epoch=scheduler_epoch,
    )
    assert first.ok is True
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        "claim_canonical_record_hash",
        "0" * 64,
    )
    changed = await manager.claim_start(
        task_id=task_id,
        run_id=run_id,
        tenant_id=tenant_id,
        worker_instance_id=worker_id,
        claimed_at_ms=999,
        scheduler_epoch=scheduler_epoch,
    )
    assert changed.ok is False
    assert changed.status == "canonical_projection_conflict"
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None
    assert snapshot.revision == 3
    assert snapshot.state == "running"
    event.assert_called_once()

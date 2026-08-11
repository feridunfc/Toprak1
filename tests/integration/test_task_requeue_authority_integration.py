from __future__ import annotations

import asyncio

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
from hfa_control.task_claim_authority import TaskClaimAuthorityBinding
from hfa_control.task_requeue_authority import (
    TASK_REQUEUE_DUPLICATE_STATUS,
    TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
    TASK_REQUEUE_PROJECTED_STATUS,
    TaskRequeueAuthorityBinding,
    TaskRequeueAuthorityError,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return "" if value is None else str(value)


def _context(command: AuthorityCommand) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id=f"test/{command.operation_type.value}",
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False,
        fence_valid=True,
    )


async def _commit(
    store: RedisCanonicalAuthorityStore,
    command: AuthorityCommand,
    *,
    revision: int,
    state: str | None,
    at_ms: int,
):
    evaluation = evaluate_authority_commit(
        context=_context(command),
        command=command,
        current_revision=revision,
        current_state=state,
        receipt_probe=None,
        committed_at_ms=at_ms,
        correlation_id=None,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    result = await store.commit(evaluation.commit_plan)
    assert result.status is RedisAuthorityCommitStatus.COMMITTED
    return evaluation.commit_plan.record


async def _seed_claimed_task(
    redis_client,
    *,
    suffix: str,
    claim_epoch: int = 1,
    requeue_count: int | None = None,
):
    task_id = f"s84-9-task-{suffix}"
    run_id = f"s84-9-run-{suffix}"
    tenant_id = f"s84-9-tenant-{suffix}"
    worker_id = f"s84-9-worker-{suffix}"
    scheduler_epoch = "7"
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    identity = CanonicalAggregateIdentity(AggregateType.TASK, run_id, task_id)

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
            {"kind": "READY_QUEUE_IF_READY", "task_id": task_id, "tenant_id": tenant_id},
        ),
    )
    await _commit(store, admit, revision=0, state=None, at_ms=100)

    dispatch_attempt = claim_epoch
    dispatch_metadata = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "dispatch_attempt": dispatch_attempt,
    }
    dispatch = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:{dispatch_attempt}",
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
    dispatch_record = await _commit(store, dispatch, revision=1, state="ready", at_ms=200)

    claim_metadata = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_instance_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "claimed_at_ms": 300,
        "dispatch_attempt": dispatch_attempt,
        "previous_claim_epoch": claim_epoch - 1,
        "claim_epoch": claim_epoch,
        "dispatch_transition_id": dispatch_record.transition_id,
        "dispatch_record_hash": dispatch_record.canonical_record_hash,
        "dispatch_command_hash": dispatch_record.canonical_command_hash,
        "dispatch_operation_id": dispatch_record.operation_id,
        "dispatch_revision": dispatch_record.to_revision,
    }
    claim = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_CLAIM,
        operation_id=f"task-claim:v1:{identity.sha256}:attempt:{dispatch_attempt}",
        expected_revision=2,
        intended_previous_state="scheduled",
        intended_next_state="running",
        authoritative_payload=claim_metadata,
        authoritative_metadata_changes=claim_metadata,
        requested_child_effects={
            "worker_reservation": {
                "action": "CONSUME",
                "task_id": task_id,
                "worker_id": worker_id,
                "scheduler_epoch": scheduler_epoch,
            }
        },
        requested_projection_intents=(
            {"kind": "RUNNING_SET", "tenant_id": tenant_id, "task_id": task_id},
        ),
        causation_id=dispatch_record.transition_id,
    )
    claim_record = await _commit(store, claim, revision=2, state="scheduled", at_ms=300)

    projected_requeues = claim_epoch - 1 if requeue_count is None else requeue_count
    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": str(claim_epoch),
            "requeue_count": str(projected_requeues),
            "heartbeat_at_ms": "1000",
            "last_heartbeat_at_ms": "1000",
            "canonical_transition_id": claim_record.transition_id,
            "canonical_record_hash": claim_record.canonical_record_hash,
            "canonical_command_hash": claim_record.canonical_command_hash,
            "canonical_revision": str(claim_record.to_revision),
            "canonical_operation_id": claim_record.operation_id,
            "claim_canonical_transition_id": claim_record.transition_id,
            "claim_canonical_record_hash": claim_record.canonical_record_hash,
            "claim_canonical_command_hash": claim_record.canonical_command_hash,
            "claim_canonical_revision": str(claim_record.to_revision),
            "claim_canonical_operation_id": claim_record.operation_id,
            "dispatch_attempt": str(dispatch_attempt),
        },
    )
    await redis_client.set(RedisKey.run_state(run_id), "running")
    await redis_client.sadd(DagRedisKey.run_tasks(run_id), task_id)
    await redis_client.zadd(DagRedisKey.task_running_zset(tenant_id), {task_id: 1000.0})
    return {
        "store": store,
        "identity": identity,
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "claim_record": claim_record,
        "claim_epoch": claim_epoch,
    }


async def _requeue(binding: TaskRequeueAuthorityBinding, seeded: dict, *, reason: str = "TASK_STALE_DETECTED", at_ms: int = 5000):
    return await binding.requeue(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        observed_at_ms=at_ms,
        max_requeue_count=3,
        reason_code=reason,
        worker_instance_id=seeded["worker_id"],
        scheduler_epoch=seeded["scheduler_epoch"],
        claim_epoch=seeded["claim_epoch"],
    )


async def test_canonical_requeue_commits_receipt_before_projection_and_delivery(redis_client):
    seeded = await _seed_claimed_task(redis_client, suffix="authority")
    binding = TaskRequeueAuthorityBinding(redis_client)
    result = await _requeue(binding, seeded)

    assert result.status == TASK_REQUEUE_PROJECTED_STATUS
    snapshot = await seeded["store"].get_aggregate_snapshot(seeded["identity"])
    assert snapshot is not None and snapshot.revision == 4 and snapshot.state == "ready"
    probe = await seeded["store"].load_receipt_probe(seeded["identity"], result.canonical_operation_id)
    assert probe is not None and probe.canonical_store_record is not None
    assert await redis_client.get(DagRedisKey.task_state(seeded["task_id"])) in {"ready", b"ready"}
    assert _text(await redis_client.hget(DagRedisKey.task_meta(seeded["task_id"]), "requeue_count")) == "1"
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1


async def test_exact_replay_returns_same_revision_and_no_duplicate_delivery(redis_client):
    seeded = await _seed_claimed_task(redis_client, suffix="exact")
    binding = TaskRequeueAuthorityBinding(redis_client)
    first = await _requeue(binding, seeded, at_ms=5000)
    second = await _requeue(binding, seeded, at_ms=9000)

    assert second.status == TASK_REQUEUE_DUPLICATE_STATUS
    assert second.exact_retry is True
    assert second.canonical_operation_id == first.canonical_operation_id
    assert second.canonical_command_hash == first.canonical_command_hash
    assert second.canonical_revision == first.canonical_revision == 4
    assert _text(await redis_client.hget(DagRedisKey.task_meta(seeded["task_id"]), "requeue_count")) == "1"
    assert await redis_client.zscore(DagRedisKey.tenant_ready_queue(seeded["tenant_id"]), seeded["task_id"]) == 5000.0
    events = await redis_client.xrange(DagRedisKey.completion_stream(seeded["tenant_id"]))
    assert len(events) == 1
    assert _text(events[0][1].get(b"at_ms") or events[0][1].get("at_ms")) == "5000"


async def test_divergent_duplicate_reason_fails_closed(redis_client):
    seeded = await _seed_claimed_task(redis_client, suffix="divergent")
    binding = TaskRequeueAuthorityBinding(redis_client)
    first = await _requeue(binding, seeded)
    with pytest.raises(TaskRequeueAuthorityError) as exc:
        await _requeue(binding, seeded, reason="DIFFERENT_REASON", at_ms=9000)
    assert exc.value.status == RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT.value
    snapshot = await seeded["store"].get_aggregate_snapshot(seeded["identity"])
    assert snapshot is not None and snapshot.revision == first.canonical_revision == 4
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1


async def test_terminal_canonical_head_rejects_late_requeue(redis_client):
    seeded = await _seed_claimed_task(redis_client, suffix="terminal")
    identity = seeded["identity"]
    fail = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_FAIL,
        operation_id=f"task-terminal:v1:{identity.sha256}:claim:1",
        expected_revision=3,
        intended_previous_state="running",
        intended_next_state="failed",
        authoritative_payload={"reason_code": "completed_elsewhere"},
        authoritative_metadata_changes={"reason_code": "completed_elsewhere"},
        requested_child_effects={},
        requested_projection_intents=(
            {"kind": "DEPENDENCY_FAILURE_FANOUT_INTENT"},
        ),
        causation_id=seeded["claim_record"].transition_id,
    )
    await _commit(seeded["store"], fail, revision=3, state="running", at_ms=4000)
    await redis_client.set(DagRedisKey.task_state(seeded["task_id"]), "failed")

    binding = TaskRequeueAuthorityBinding(redis_client)
    with pytest.raises(TaskRequeueAuthorityError) as exc:
        await _requeue(binding, seeded)
    assert exc.value.status == TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS
    snapshot = await seeded["store"].get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.state == "failed" and snapshot.revision == 4


async def test_two_recovery_callers_create_one_canonical_requeue(redis_client):
    seeded = await _seed_claimed_task(redis_client, suffix="race")
    first = TaskRequeueAuthorityBinding(redis_client)
    second = TaskRequeueAuthorityBinding(redis_client)
    a, b = await asyncio.gather(_requeue(first, seeded), _requeue(second, seeded))
    assert {a.status, b.status} <= {TASK_REQUEUE_PROJECTED_STATUS, TASK_REQUEUE_DUPLICATE_STATUS}
    snapshot = await seeded["store"].get_aggregate_snapshot(seeded["identity"])
    assert snapshot is not None and snapshot.revision == 4
    assert a.canonical_operation_id == b.canonical_operation_id == snapshot.operation_id
    assert await redis_client.xlen(DagRedisKey.completion_stream(seeded["tenant_id"])) == 1


async def test_mutable_retry_count_must_match_canonical_dispatch_attempt(redis_client):
    seeded = await _seed_claimed_task(redis_client, suffix="count-conflict", requeue_count=9)
    binding = TaskRequeueAuthorityBinding(redis_client)
    with pytest.raises(TaskRequeueAuthorityError) as exc:
        await _requeue(binding, seeded)
    assert exc.value.status == TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS
    snapshot = await seeded["store"].get_aggregate_snapshot(seeded["identity"])
    assert snapshot is not None and snapshot.revision == 3 and snapshot.state == "running"


async def test_requeued_task_can_enter_existing_task_claim_authority_path(redis_client):
    seeded = await _seed_claimed_task(redis_client, suffix="next-claim")
    requeue = TaskRequeueAuthorityBinding(redis_client)
    result = await _requeue(requeue, seeded)
    assert result.canonical_revision == 4

    identity = seeded["identity"]
    worker_id = "s84-9-worker-next-claim-b"
    scheduler_epoch = "9"
    dispatch_metadata = {
        "task_id": seeded["task_id"],
        "run_id": seeded["run_id"],
        "tenant_id": seeded["tenant_id"],
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "dispatch_attempt": 2,
    }
    dispatch = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:2",
        expected_revision=4,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload=dispatch_metadata,
        authoritative_metadata_changes=dispatch_metadata,
        requested_child_effects={
            "worker_reservation": {
                "worker_id": worker_id,
                "task_id": seeded["task_id"],
                "scheduler_epoch": scheduler_epoch,
            }
        },
        requested_projection_intents=(
            {"kind": "CONTROL_NOTIFICATION", "stream": "control"},
            {"kind": "TASK_REQUEST_MESSAGE", "stream": "shard"},
        ),
        causation_id=result.canonical_transition_id,
    )
    record = await _commit(seeded["store"], dispatch, revision=4, state="ready", at_ms=6000)
    meta_key = DagRedisKey.task_meta(seeded["task_id"])
    await redis_client.set(DagRedisKey.task_state(seeded["task_id"]), "scheduled")
    await redis_client.hset(
        meta_key,
        mapping={
            "scheduler_epoch": scheduler_epoch,
            "dispatch_worker_id": worker_id,
            "dispatch_attempt": "2",
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_command_hash": record.canonical_command_hash,
            "canonical_revision": str(record.to_revision),
            "canonical_operation_id": record.operation_id,
        },
    )
    await redis_client.hset(
        DagRedisKey.worker_reservation(worker_id),
        mapping={"task_id": seeded["task_id"], "worker_id": worker_id, "scheduler_epoch": scheduler_epoch},
    )
    await redis_client.hset(
        DagRedisKey.task_reservation_owner(seeded["task_id"]),
        mapping={"task_id": seeded["task_id"], "worker_id": worker_id, "scheduler_epoch": scheduler_epoch},
    )

    claim = TaskClaimAuthorityBinding(redis_client)
    prepared = await claim.prepare_claim(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        worker_instance_id=worker_id,
        scheduler_epoch=scheduler_epoch,
        claimed_at_ms=7000,
    )
    assert prepared.projection.canonical_revision == 6
    assert prepared.projection.claim_epoch == 2
    assert prepared.projection.dispatch_attempt == 2


async def test_corrupt_durable_requeue_receipt_fails_closed(redis_client):
    seeded = await _seed_claimed_task(redis_client, suffix="receipt-corrupt")
    binding = TaskRequeueAuthorityBinding(redis_client)
    first = await _requeue(binding, seeded)
    assert first.canonical_revision == 4

    identity = seeded["identity"]
    store = binding.store
    assert store is not None
    keyspace = store.keyspace(identity.sha256)
    field = keyspace.operation_field(first.canonical_operation_id)
    await redis_client.hset(keyspace.receipts, field, "{not-valid-authority-envelope")

    with pytest.raises(TaskRequeueAuthorityError) as excinfo:
        await _requeue(TaskRequeueAuthorityBinding(redis_client), seeded)
    assert excinfo.value.status == "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    snapshot = await store.get_aggregate_snapshot(identity)
    assert snapshot is not None and snapshot.revision == 4

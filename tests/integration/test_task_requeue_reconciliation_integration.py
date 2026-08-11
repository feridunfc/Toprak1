from __future__ import annotations

import asyncio
from typing import Any

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
from hfa_control.reconciliation import (
    ReconciliationCheckClass,
    ReconciliationReason,
    ReconciliationRedisReader,
    ReconciliationSeverity,
    ReconciliationStatus,
    RedisCanonicalReconciliationReader,
    TaskRequeueReconciler,
)
from hfa_control.task_requeue_authority import TaskRequeueAuthorityBinding

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _text(value: Any) -> str:
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


async def _seed_claimed_task(redis_client, *, suffix: str):
    task_id = f"s85-0a-task-{suffix}"
    run_id = f"s85-0a-run-{suffix}"
    tenant_id = f"s85-0a-tenant-{suffix}"
    worker_id = f"s85-0a-worker-{suffix}"
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

    dispatch_metadata = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "dispatch_attempt": 1,
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
    dispatch_record = await _commit(store, dispatch, revision=1, state="ready", at_ms=200)

    claim_metadata = {
        "task_id": task_id,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "worker_instance_id": worker_id,
        "scheduler_epoch": scheduler_epoch,
        "claimed_at_ms": 300,
        "dispatch_attempt": 1,
        "previous_claim_epoch": 0,
        "claim_epoch": 1,
        "dispatch_transition_id": dispatch_record.transition_id,
        "dispatch_record_hash": dispatch_record.canonical_record_hash,
        "dispatch_command_hash": dispatch_record.canonical_command_hash,
        "dispatch_operation_id": dispatch_record.operation_id,
        "dispatch_revision": dispatch_record.to_revision,
    }
    claim = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_CLAIM,
        operation_id=f"task-claim:v1:{identity.sha256}:attempt:1",
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

    await redis_client.set(DagRedisKey.task_state(task_id), "running")
    await redis_client.hset(
        DagRedisKey.task_meta(task_id),
        mapping={
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": "1",
            "requeue_count": "0",
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
            "dispatch_attempt": "1",
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
    }


async def _seed_consistent_requeue(redis_client, *, suffix: str):
    seeded = await _seed_claimed_task(redis_client, suffix=suffix)
    binding = TaskRequeueAuthorityBinding(redis_client)
    result = await binding.requeue(
        task_id=seeded["task_id"],
        run_id=seeded["run_id"],
        tenant_id=seeded["tenant_id"],
        observed_at_ms=5000,
        max_requeue_count=3,
        reason_code="TASK_STALE_DETECTED",
        worker_instance_id=seeded["worker_id"],
        scheduler_epoch=seeded["scheduler_epoch"],
        claim_epoch=1,
    )
    seeded["requeue"] = result
    return seeded


def _reconciler(redis_client, *, canonical_reader=None, runtime_reader=None):
    return TaskRequeueReconciler(
        canonical_reader=canonical_reader or RedisCanonicalReconciliationReader(redis_client),
        runtime_reader=runtime_reader or ReconciliationRedisReader(redis_client),
        clock_ms=lambda: 9000,
    )


def _find(findings, reason: ReconciliationReason):
    return [item for item in findings if item.reason_code is reason]


async def _reconcile(redis_client, seeded, **kwargs):
    return await _reconciler(redis_client, **kwargs).reconcile(
        task_id=seeded["task_id"], run_id=seeded["run_id"]
    )


async def test_A_fully_consistent_current_requeue_and_durable_effect(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="A")
    findings = await _reconcile(redis_client, seeded)
    assert len(findings) == 2
    assert {f.check_class for f in findings} == {
        ReconciliationCheckClass.CURRENT_PROJECTION,
        ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT,
    }
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}


async def test_B_task_state_mismatch_is_critical_drift(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="B")
    await redis_client.set(DagRedisKey.task_state(seeded["task_id"]), "running")
    findings = await _reconcile(redis_client, seeded)
    matches = _find(findings, ReconciliationReason.TASK_STATE_MISMATCH)
    assert len(matches) == 1
    assert matches[0].status is ReconciliationStatus.DRIFT
    assert matches[0].severity is ReconciliationSeverity.CRITICAL


async def test_C_ready_queue_missing_is_drift(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="C")
    await redis_client.zrem(DagRedisKey.tenant_ready_queue(seeded["tenant_id"]), seeded["task_id"])
    findings = await _reconcile(redis_client, seeded)
    assert _find(findings, ReconciliationReason.READY_QUEUE_MEMBERSHIP_MISSING)


async def test_D_stale_running_membership_is_drift(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="D")
    await redis_client.zadd(DagRedisKey.task_running_zset(seeded["tenant_id"]), {seeded["task_id"]: 9999.0})
    findings = await _reconcile(redis_client, seeded)
    assert _find(findings, ReconciliationReason.RUNNING_INDEX_MEMBERSHIP_PRESENT)


async def test_E_generic_canonical_projection_proof_mismatch_is_drift(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="E")
    await redis_client.hset(DagRedisKey.task_meta(seeded["task_id"]), "canonical_transition_id", "wrong")
    findings = await _reconcile(redis_client, seeded)
    matches = _find(findings, ReconciliationReason.CANONICAL_PROOF_MISMATCH)
    assert matches and all(x.status is ReconciliationStatus.DRIFT for x in matches)


async def test_F_requeue_specific_projection_proof_mismatch_is_drift(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="F")
    await redis_client.hset(DagRedisKey.task_meta(seeded["task_id"]), "requeue_canonical_transition_id", "wrong")
    findings = await _reconcile(redis_client, seeded)
    assert _find(findings, ReconciliationReason.REQUEUE_PROOF_MISMATCH)


async def test_G_proven_wrong_ready_queue_type_is_critical_schema_drift(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="G")
    key = DagRedisKey.tenant_ready_queue(seeded["tenant_id"])
    await redis_client.delete(key)
    await redis_client.hset(key, mapping={"wrong": "type"})
    findings = await _reconcile(redis_client, seeded)
    matches = _find(findings, ReconciliationReason.PROJECTION_SCHEMA_MISMATCH)
    assert matches
    assert all(x.status is ReconciliationStatus.DRIFT for x in matches)
    assert all(x.severity is ReconciliationSeverity.CRITICAL for x in matches)


async def test_H_canonical_record_corruption_blocks_projection_comparison(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="H")
    operation_id = seeded["requeue"].canonical_operation_id
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    await redis_client.hset(
        keyspace.operation_records,
        keyspace.operation_field(operation_id),
        "corrupt-envelope",
    )
    findings = await _reconcile(redis_client, seeded)
    assert {f.status for f in findings} == {ReconciliationStatus.BLOCKED_EVIDENCE}
    assert {f.reason_code for f in findings} == {ReconciliationReason.CANONICAL_RECORD_CORRUPTION}


class _CanonicalSecondReadBarrier:
    def __init__(self, inner):
        self.inner = inner
        self.calls = 0
        self.second_started = asyncio.Event()
        self.release_second = asyncio.Event()

    async def read_task_requeue_head(self, *, task_id: str, run_id: str):
        self.calls += 1
        if self.calls == 2:
            self.second_started.set()
            await self.release_second.wait()
        return await self.inner.read_task_requeue_head(task_id=task_id, run_id=run_id)


async def test_I_canonical_change_during_observation_is_blocked(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="I")
    barrier = _CanonicalSecondReadBarrier(RedisCanonicalReconciliationReader(redis_client))
    task = asyncio.create_task(_reconcile(redis_client, seeded, canonical_reader=barrier))
    await barrier.second_started.wait()

    identity = seeded["identity"]
    metadata = {
        "task_id": seeded["task_id"],
        "run_id": seeded["run_id"],
        "tenant_id": seeded["tenant_id"],
        "worker_id": "worker-I-next",
        "scheduler_epoch": "9",
        "dispatch_attempt": 2,
    }
    command = AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=f"task-dispatch:v1:{identity.sha256}:attempt:2",
        expected_revision=4,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={
            "worker_reservation": {
                "worker_id": "worker-I-next",
                "task_id": seeded["task_id"],
                "scheduler_epoch": "9",
            }
        },
        requested_projection_intents=(
            {"kind": "CONTROL_NOTIFICATION", "stream": "control"},
            {"kind": "TASK_REQUEST_MESSAGE", "stream": "shard"},
        ),
    )
    await _commit(seeded["store"], command, revision=4, state="ready", at_ms=7000)
    barrier.release_second.set()
    findings = await task
    assert {f.status for f in findings} == {ReconciliationStatus.BLOCKED_EVIDENCE}
    assert {f.reason_code for f in findings} == {ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION}


class _RuntimeSecondReadBarrier:
    def __init__(self, inner):
        self.inner = inner
        self.calls = 0
        self.second_started = asyncio.Event()
        self.release_second = asyncio.Event()

    async def read_task_requeue_projection(self, *, task_id: str, tenant_id: str):
        self.calls += 1
        if self.calls == 2:
            self.second_started.set()
            await self.release_second.wait()
        return await self.inner.read_task_requeue_projection(task_id=task_id, tenant_id=tenant_id)


async def test_J_projection_change_during_observation_is_blocked(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="J")
    barrier = _RuntimeSecondReadBarrier(ReconciliationRedisReader(redis_client))
    task = asyncio.create_task(_reconcile(redis_client, seeded, runtime_reader=barrier))
    await barrier.second_started.wait()
    await redis_client.set(DagRedisKey.task_state(seeded["task_id"]), "running")
    barrier.release_second.set()
    findings = await task
    assert {f.status for f in findings} == {ReconciliationStatus.BLOCKED_EVIDENCE}
    assert {f.reason_code for f in findings} == {ReconciliationReason.PROJECTION_OBSERVATION_CHANGED}


async def test_K_missing_durable_delivery_proof_is_historical_drift(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="K")
    await redis_client.hdel(
        DagRedisKey.task_meta(seeded["task_id"]),
        "requeue_notification_operation_id",
    )
    findings = await _reconcile(redis_client, seeded)
    matches = _find(findings, ReconciliationReason.REQUEUE_DELIVERY_PROOF_MISSING)
    assert len(matches) == 1
    assert matches[0].check_class is ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT
    assert matches[0].status is ReconciliationStatus.DRIFT


async def test_L_trimmed_raw_stream_does_not_override_valid_durable_proof(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="L")
    await redis_client.delete(DagRedisKey.completion_stream(seeded["tenant_id"]))
    findings = await _reconcile(redis_client, seeded)
    historical = [
        f for f in findings
        if f.check_class is ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT
    ]
    assert len(historical) == 1
    assert historical[0].status is ReconciliationStatus.CONSISTENT
    assert historical[0].reason_code is ReconciliationReason.CONSISTENT


def _normalize(value: Any):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, dict):
        return tuple(sorted((_normalize(k), _normalize(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_normalize(v) for v in value)
    if isinstance(value, set):
        return tuple(sorted(_normalize(v) for v in value))
    return value


async def _keyspace_snapshot(redis_client):
    keys = []
    async for raw_key in redis_client.scan_iter(match="*"):
        keys.append(raw_key)
    result = []
    for raw_key in sorted(keys, key=_text):
        key = _text(raw_key)
        kind = _text(await redis_client.type(raw_key))
        if kind == "string":
            value = await redis_client.get(raw_key)
        elif kind == "hash":
            value = await redis_client.hgetall(raw_key)
        elif kind == "set":
            value = await redis_client.smembers(raw_key)
        elif kind == "zset":
            value = await redis_client.zrange(raw_key, 0, -1, withscores=True)
        elif kind == "stream":
            value = await redis_client.xrange(raw_key)
        elif kind == "list":
            value = await redis_client.lrange(raw_key, 0, -1)
        else:
            value = None
        result.append((key, kind, _normalize(value)))
    return tuple(result)


async def test_M_reconciliation_produces_exact_zero_keyspace_mutation(redis_client):
    seeded = await _seed_consistent_requeue(redis_client, suffix="M")
    before = await _keyspace_snapshot(redis_client)
    findings = await _reconcile(redis_client, seeded)
    after = await _keyspace_snapshot(redis_client)
    assert before == after
    assert all(f.mutation_attempted is False for f in findings)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}

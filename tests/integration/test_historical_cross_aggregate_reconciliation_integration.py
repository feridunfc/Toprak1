from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Any

import pytest

from hfa.authority import (
    AggregateType,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    RedisAuthorityCommitStatus,
    RedisCanonicalAuthorityStore,
    canonical_json_bytes,
    evaluate_authority_commit,
)
from hfa.authority import canonical_transition as _authority_core
from hfa.authority.redis_persistence import (
    RedisAuthorityCorruptionError,
    RedisAuthorityHistoryIncompleteError,
    RedisAuthorityObservationChangedError,
)
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationManager,
    RESERVATION_STATE_FINALIZED,
    RESERVATION_STATE_RESERVED,
    RESERVATION_STATE_SETTLED,
)
from hfa_control.reconciliation import (
    HistoricalCanonicalReconciliationReader,
    HistoricalCrossAggregateReconciler,
    ReadOnlyResourceReceiptReader,
    ReadOnlyTerminalProofReader,
    ReconciliationReason,
    ReconciliationStatus,
)
from hfa_control.run_create_authority import (
    WRITER_ID as RUN_CREATE_WRITER_ID,
    RunCreateAuthorityInput,
    build_run_create_command,
    resource_reservation_from_run_create_record,
)
from hfa_control.run_terminate_authority import (
    WRITER_ID as RUN_TERMINATE_WRITER_ID,
    TerminalAggregateProof,
    TerminalAggregateProofManager,
    TerminalTaskEvidence,
    build_run_terminate_command,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _context(command):
    writer_id = (
        RUN_CREATE_WRITER_ID
        if command.operation_type.value == "RUN_CREATE"
        else RUN_TERMINATE_WRITER_ID
        if command.operation_type.value == "RUN_TERMINATE"
        else f"test/{command.operation_type.value}"
    )
    return AuthorityEntryContext(
        authenticated_writer_id=writer_id,
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False,
        fence_valid=True,
    )


async def _commit(store, command, *, revision: int, state: str | None, at_ms: int):
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


async def _store(redis_client):
    store = RedisCanonicalAuthorityStore(redis_client)
    await store.initialise()
    return store


def _run_create_input(run_id: str, tenant_id: str = "tenant-85c") -> RunCreateAuthorityInput:
    return RunCreateAuthorityInput(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type="agent",
        priority=1,
        payload={"sprint": "85.0C"},
        estimated_cost_cents=7,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        created_at_ms=1000,
        control_stream="hfa:stream:control",
    )


def _terminal_proof(run_id: str, tenant_id: str = "tenant-85c") -> TerminalAggregateProof:
    tasks = (
        TerminalTaskEvidence(task_id="task-01", state="done"),
        TerminalTaskEvidence(task_id="task-02", state="blocked_by_failure"),
        TerminalTaskEvidence(task_id="task-03", state="skipped"),
    )
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "tasks": [{"task_id": row.task_id, "state": row.state} for row in tasks],
        "task_count": 3,
        "done_count": 1,
        "failed_count": 1,
        "skipped_count": 1,
        "final_state": "failed",
    }
    payload_json = canonical_json_bytes(payload).decode("utf-8")
    return TerminalAggregateProof(
        schema_version=1,
        run_id=run_id,
        tenant_id=tenant_id,
        tasks=tasks,
        task_count=3,
        done_count=1,
        failed_count=1,
        skipped_count=1,
        final_state="failed",
        proof_sha256=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        proof_payload_json=payload_json,
        finalized_at_ms=2000,
        worker_instance_id="worker-85c",
        trigger_task_id="task-01",
        trigger_terminal_state="done",
        canonical_expected_revision=1,
        canonical_previous_state="pending",
    )


async def _write_terminal_proof(redis_client, proof: TerminalAggregateProof) -> None:
    await redis_client.hset(
        TerminalAggregateProofManager.proof_key(proof.run_id),
        mapping={
            "schema_version": str(proof.schema_version),
            "run_id": proof.run_id,
            "tenant_id": proof.tenant_id,
            "proof_sha256": proof.proof_sha256,
            "proof_payload_json": proof.proof_payload_json,
            "task_count": str(proof.task_count),
            "done_count": str(proof.done_count),
            "failed_count": str(proof.failed_count),
            "skipped_count": str(proof.skipped_count),
            "final_state": proof.final_state,
            "finalized_at_ms": str(proof.finalized_at_ms),
            "worker_instance_id": proof.worker_instance_id,
            "trigger_task_id": proof.trigger_task_id,
            "trigger_terminal_state": proof.trigger_terminal_state,
            "canonical_expected_revision": str(proof.canonical_expected_revision),
            "canonical_previous_state": proof.canonical_previous_state,
        },
    )


async def _seed_run(redis_client, suffix: str, *, resource_state: str | None = None, terminate: bool = False):
    run_id = f"s85c-run-{suffix}"
    tenant_id = f"s85c-tenant-{suffix}"
    store = await _store(redis_client)
    value = _run_create_input(run_id, tenant_id)
    create_command = build_run_create_command(value)
    reservation = None
    resources = None
    if resource_state is not None:
        resources = AdmissionResourceReservationManager(redis_client)
        reservation = resource_reservation_from_run_create_record(
            # The reservation reconstruction needs the canonical RUN_CREATE shape;
            # evaluate a deterministic local record before the durable commit.
            evaluate_authority_commit(
                context=_context(create_command),
                command=create_command,
                current_revision=0,
                current_state=None,
                receipt_probe=None,
                committed_at_ms=1000,
                correlation_id=None,
            ).commit_plan.record
        )
        result = await resources.reserve_once(
            reservation,
            concurrent_run_limit=None,
            budget_limit_cents=None,
            tenant_inflight_limit=None,
            now_ms=900,
        )
        assert result.state == RESERVATION_STATE_RESERVED
    create = await _commit(store, create_command, revision=0, state=None, at_ms=1000)
    if resources is not None and resource_state in {RESERVATION_STATE_FINALIZED, RESERVATION_STATE_SETTLED}:
        result = await resources.finalize_once(reservation, now_ms=1100)
        assert result.state == RESERVATION_STATE_FINALIZED
    elif resources is not None and resource_state == "RELEASED":
        result = await resources.release_once(reservation, now_ms=1100)
        assert result.state == "RELEASED"

    proof = None
    terminate_record = None
    if terminate:
        proof = _terminal_proof(run_id, tenant_id)
        await _write_terminal_proof(redis_client, proof)
        terminate_record = await _commit(
            store,
            build_run_terminate_command(proof),
            revision=1,
            state="pending",
            at_ms=2000,
        )
        if resources is not None and resource_state == RESERVATION_STATE_SETTLED:
            from hfa.governance.admission_resource_reservation import AdmissionResourceSettlementInput
            settlement = AdmissionResourceSettlementInput(
                run_create_operation_id=reservation.operation_id,
                run_id=reservation.run_id,
                tenant_id=reservation.tenant_id,
                estimated_cost_cents=reservation.estimated_cost_cents,
                run_create_reservation_proof_sha256=reservation.proof_sha256,
                run_terminate_operation_id=terminate_record.operation_id,
                terminal_proof_sha256=proof.proof_sha256,
                canonical_transition_id=terminate_record.transition_id,
                canonical_record_hash=terminate_record.canonical_record_hash,
                canonical_command_hash=terminate_record.canonical_command_hash,
                canonical_revision=terminate_record.to_revision,
                final_state=terminate_record.next_state,
            )
            settled = await resources.settle_once(settlement, now_ms=2100)
            assert settled.state == RESERVATION_STATE_SETTLED
    return {
        "store": store,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "identity": CanonicalAggregateIdentity(AggregateType.RUN, run_id, None),
        "create": create,
        "reservation": reservation,
        "resources": resources,
        "proof": proof,
        "terminate": terminate_record,
    }


async def _logical_snapshot(redis_client) -> tuple[tuple[str, str, Any], ...]:
    rows = []
    async for key in redis_client.scan_iter(match="*"):
        kind = await redis_client.type(key)
        if kind == "string":
            value = await redis_client.get(key)
        elif kind == "hash":
            value = tuple(sorted((await redis_client.hgetall(key)).items()))
        elif kind == "set":
            value = tuple(sorted(await redis_client.smembers(key)))
        elif kind == "zset":
            value = tuple(await redis_client.zrange(key, 0, -1, withscores=True))
        elif kind == "list":
            value = tuple(await redis_client.lrange(key, 0, -1))
        elif kind == "stream":
            value = tuple(await redis_client.xrange(key, min="-", max="+"))
        else:
            value = f"UNSUPPORTED:{kind}"
        rows.append((str(key), str(kind), value))
    return tuple(sorted(rows, key=lambda row: row[0]))


def _storage_envelope(payload: dict[str, Any]) -> str:
    payload_text = canonical_json_bytes(payload).decode("utf-8")
    return json.dumps(
        {
            "payload": payload_text,
            "storage_sha1": hashlib.sha1(payload_text.encode("utf-8")).hexdigest(),
        },
        separators=(",", ":"),
        sort_keys=True,
    )


async def _rewrite_operation_revision(real_redis, seeded, *, from_revision: int, to_revision: int) -> None:
    """Keep cardinality fixed while creating a valid individual record at another revision."""
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    record = seeded["terminate"]
    field = keyspace.operation_field(record.operation_id)
    raw_record = json.loads(await real_redis.hget(keyspace.operation_records, field))
    record_payload = json.loads(raw_record["payload"])
    old_transition = record_payload["transition_id"]
    new_transition = _authority_core._transition_id(
        seeded["identity"].sha256, to_revision, record.operation_id
    )
    record_payload["from_revision"] = from_revision
    record_payload["to_revision"] = to_revision
    record_payload["transition_id"] = new_transition
    hash_payload = dict(record_payload)
    hash_payload.pop("canonical_record_hash", None)
    record_payload["canonical_record_hash"] = _authority_core.canonical_json_sha256(hash_payload)

    raw_receipt = json.loads(await real_redis.hget(keyspace.receipts, field))
    receipt_payload = json.loads(raw_receipt["payload"])
    receipt_payload["aggregate_revision"] = to_revision
    receipt_payload["transition_id"] = new_transition
    receipt_payload["canonical_record_hash"] = record_payload["canonical_record_hash"]

    raw_index = json.loads(await real_redis.hget(keyspace.transition_indexes, old_transition))
    index_payload = json.loads(raw_index["payload"])
    index_payload["aggregate_revision"] = to_revision
    index_payload["transition_id"] = new_transition
    index_payload["canonical_record_hash"] = record_payload["canonical_record_hash"]

    await real_redis.hset(keyspace.operation_records, field, _storage_envelope(record_payload))
    await real_redis.hset(keyspace.receipts, field, _storage_envelope(receipt_payload))
    await real_redis.hdel(keyspace.transition_indexes, old_transition)
    await real_redis.hset(keyspace.transition_indexes, new_transition, _storage_envelope(index_payload))


async def test_85_0c_clean_single_revision_history(real_redis):
    seeded = await _seed_run(real_redis, "single")
    history = await seeded["store"].load_aggregate_history(seeded["identity"])
    assert history is not None
    assert [row.revision for row in history.operations] == [1]


async def test_85_0c_clean_multi_revision_history(real_redis):
    seeded = await _seed_run(real_redis, "multi", terminate=True)
    history = await seeded["store"].load_aggregate_history(seeded["identity"])
    assert history is not None
    assert [row.revision for row in history.operations] == [1, 2]


@pytest.mark.parametrize("member", ["receipt", "record", "index"])
async def test_85_0c_missing_history_member_fails_closed(real_redis, member):
    seeded = await _seed_run(real_redis, f"missing-{member}")
    store = seeded["store"]
    keyspace = store.keyspace(seeded["identity"].sha256)
    record = seeded["create"]
    if member == "receipt":
        await real_redis.hdel(keyspace.receipts, keyspace.operation_field(record.operation_id))
    elif member == "record":
        await real_redis.hdel(keyspace.operation_records, keyspace.operation_field(record.operation_id))
    else:
        await real_redis.hdel(keyspace.transition_indexes, record.transition_id)
    with pytest.raises(RedisAuthorityHistoryIncompleteError):
        await store.load_aggregate_history(seeded["identity"])


async def test_85_0c_wrong_history_redis_type_blocks(real_redis):
    seeded = await _seed_run(real_redis, "wrong-type")
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    await real_redis.delete(keyspace.receipts)
    await real_redis.set(keyspace.receipts, "wrong-type")
    with pytest.raises(RedisAuthorityCorruptionError):
        await seeded["store"].load_aggregate_history(seeded["identity"])


async def test_85_0c_corrupt_history_envelope_blocks(real_redis):
    seeded = await _seed_run(real_redis, "bad-envelope")
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    field = keyspace.operation_field(seeded["create"].operation_id)
    await real_redis.hset(keyspace.operation_records, field, "not-an-envelope")
    with pytest.raises(RedisAuthorityCorruptionError):
        await seeded["store"].load_aggregate_history(seeded["identity"])


async def test_85_0c_operation_digest_field_mismatch_blocks(real_redis):
    seeded = await _seed_run(real_redis, "digest")
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    field = keyspace.operation_field(seeded["create"].operation_id)
    raw = await real_redis.hget(keyspace.operation_records, field)
    await real_redis.hdel(keyspace.operation_records, field)
    await real_redis.hset(keyspace.operation_records, "f" * 64, raw)
    with pytest.raises(RedisAuthorityCorruptionError):
        await seeded["store"].load_aggregate_history(seeded["identity"])


async def test_85_0c_out_of_range_revision_is_canonical_corruption(real_redis):
    seeded = await _seed_run(real_redis, "hole", terminate=True)
    await _rewrite_operation_revision(real_redis, seeded, from_revision=2, to_revision=3)
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    assert int(await real_redis.hget(keyspace.aggregate, "revision")) == 2
    assert await real_redis.hlen(keyspace.operation_records) == 2
    assert await real_redis.hlen(keyspace.receipts) == 2
    assert await real_redis.hlen(keyspace.transition_indexes) == 2
    with pytest.raises(
        RedisAuthorityCorruptionError,
        match="canonical historical revision sequence is contradictory",
    ):
        await seeded["store"].load_aggregate_history(seeded["identity"])


async def test_85_0c_extra_historical_member_blocks(real_redis):
    seeded = await _seed_run(real_redis, "extra-member", terminate=True)
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    first = seeded["create"]
    field = keyspace.operation_field(first.operation_id)
    await real_redis.hset(keyspace.operation_records, "e" * 64, await real_redis.hget(keyspace.operation_records, field))
    await real_redis.hset(keyspace.receipts, "e" * 64, await real_redis.hget(keyspace.receipts, field))
    await real_redis.hset(keyspace.transition_indexes, "extra-transition", await real_redis.hget(keyspace.transition_indexes, first.transition_id))
    with pytest.raises(RedisAuthorityCorruptionError):
        await seeded["store"].load_aggregate_history(seeded["identity"])


async def test_85_0c_duplicate_revision_reaches_uniqueness_validator(real_redis):
    seeded = await _seed_run(real_redis, "duplicate-revision", terminate=True)
    # Cardinality remains exactly head.revision == 2. The validator must reach
    # revision uniqueness rather than fail first on extra-member cardinality.
    await _rewrite_operation_revision(real_redis, seeded, from_revision=0, to_revision=1)
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    assert await real_redis.hlen(keyspace.operation_records) == 2
    assert await real_redis.hlen(keyspace.receipts) == 2
    assert await real_redis.hlen(keyspace.transition_indexes) == 2
    with pytest.raises(RedisAuthorityCorruptionError, match="duplicate canonical historical revision"):
        await seeded["store"].load_aggregate_history(seeded["identity"])


async def test_85_0c_head_history_mismatch_blocks(real_redis):
    seeded = await _seed_run(real_redis, "head-mismatch")
    keyspace = seeded["store"].keyspace(seeded["identity"].sha256)
    await real_redis.hset(keyspace.aggregate, "state", "wrong-state")
    with pytest.raises(RedisAuthorityCorruptionError):
        await seeded["store"].load_aggregate_history(seeded["identity"])


async def test_85_0c_head_change_during_enumeration_blocks(real_redis, monkeypatch):
    seeded = await _seed_run(real_redis, "head-race")
    store = seeded["store"]
    original = store.get_aggregate_snapshot
    first = await original(seeded["identity"])
    changed = replace(first, updated_at_ms=first.updated_at_ms + 1)
    values = iter((first, changed))

    async def fake_snapshot(identity):
        return next(values)

    monkeypatch.setattr(store, "get_aggregate_snapshot", fake_snapshot)
    with pytest.raises(RedisAuthorityObservationChangedError):
        await store.load_aggregate_history(seeded["identity"])


async def test_85_0c_history_reader_zero_logical_mutation(real_redis):
    seeded = await _seed_run(real_redis, "zero-mutation", terminate=True)
    before = await _logical_snapshot(real_redis)
    history = await seeded["store"].load_aggregate_history(seeded["identity"])
    after = await _logical_snapshot(real_redis)
    assert history is not None
    assert after == before


async def test_85_0c_exact_terminal_proof_binding_consistent_and_zero_mutation(real_redis):
    seeded = await _seed_run(real_redis, "proof", terminate=True)
    reconciler = HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        terminal_proof_reader=ReadOnlyTerminalProofReader(real_redis),
    )
    before = await _logical_snapshot(real_redis)
    findings = await reconciler.reconcile_run_terminate_proof(run_id=seeded["run_id"])
    after = await _logical_snapshot(real_redis)
    assert {item.status for item in findings} == {ReconciliationStatus.CONSISTENT}
    assert after == before


@pytest.mark.parametrize(
    ("resource_state", "expected_status", "expected_reason"),
    [
        (RESERVATION_STATE_RESERVED, ReconciliationStatus.BLOCKED_EVIDENCE, ReconciliationReason.RESOURCE_FINALIZATION_PENDING),
        (RESERVATION_STATE_FINALIZED, ReconciliationStatus.CONSISTENT, ReconciliationReason.CONSISTENT),
        (RESERVATION_STATE_SETTLED, ReconciliationStatus.CONSISTENT, ReconciliationReason.CONSISTENT),
        ("RELEASED", ReconciliationStatus.DRIFT, ReconciliationReason.RESOURCE_PROOF_MISMATCH),
    ],
)
async def test_85_0c_run_create_resource_states(real_redis, resource_state, expected_status, expected_reason):
    seeded = await _seed_run(
        real_redis,
        f"resource-{resource_state.lower()}",
        resource_state=resource_state,
        terminate=resource_state == RESERVATION_STATE_SETTLED,
    )
    reconciler = HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        resource_reader=ReadOnlyResourceReceiptReader(real_redis),
    )
    findings = await reconciler.reconcile_run_create_resource(run_id=seeded["run_id"])
    assert findings[0].status is expected_status
    assert findings[0].reason_code is expected_reason


@pytest.mark.parametrize("field", ["tenant_id", "proof_sha256"])
async def test_85_0c_missing_required_resource_immutable_field_blocks(real_redis, field):
    seeded = await _seed_run(real_redis, f"resource-missing-{field}", resource_state=RESERVATION_STATE_FINALIZED)
    key = seeded["resources"].reservation_receipt_key(seeded["reservation"].operation_id)
    await real_redis.hdel(key, field)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        resource_reader=ReadOnlyResourceReceiptReader(real_redis),
    ).reconcile_run_create_resource(run_id=seeded["run_id"])
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE


@pytest.mark.parametrize(
    ("field", "value"),
    [("tenant_id", "wrong-tenant"), ("proof_sha256", "b" * 64)],
)
async def test_85_0c_present_but_wrong_resource_immutable_field_drifts(real_redis, field, value):
    seeded = await _seed_run(real_redis, f"resource-wrong-{field}", resource_state=RESERVATION_STATE_FINALIZED)
    key = seeded["resources"].reservation_receipt_key(seeded["reservation"].operation_id)
    await real_redis.hset(key, field, value)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        resource_reader=ReadOnlyResourceReceiptReader(real_redis),
    ).reconcile_run_create_resource(run_id=seeded["run_id"])
    assert findings[0].status is ReconciliationStatus.DRIFT
    assert findings[0].reason_code is ReconciliationReason.RESOURCE_PROOF_MISMATCH


async def test_85_0c_exact_settlement_binding_consistent(real_redis):
    seeded = await _seed_run(real_redis, "settled", resource_state=RESERVATION_STATE_SETTLED, terminate=True)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        terminal_proof_reader=ReadOnlyTerminalProofReader(real_redis),
        resource_reader=ReadOnlyResourceReceiptReader(real_redis),
    ).reconcile_run_terminate_settlement(run_id=seeded["run_id"])
    assert findings[0].status is ReconciliationStatus.CONSISTENT


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_terminate_operation_id", "run-terminate:v1:" + "b" * 64),
        ("terminal_proof_sha256", "b" * 64),
        ("canonical_record_hash", "b" * 64),
    ],
)
async def test_85_0c_settlement_positive_contradiction_drifts(real_redis, field, value):
    seeded = await _seed_run(real_redis, f"settlement-{field}", resource_state=RESERVATION_STATE_SETTLED, terminate=True)
    key = seeded["resources"].reservation_receipt_key(seeded["reservation"].operation_id)
    await real_redis.hset(key, field, value)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        terminal_proof_reader=ReadOnlyTerminalProofReader(real_redis),
        resource_reader=ReadOnlyResourceReceiptReader(real_redis),
    ).reconcile_run_terminate_settlement(run_id=seeded["run_id"])
    assert findings[0].status is ReconciliationStatus.DRIFT
    assert findings[0].reason_code is ReconciliationReason.RESOURCE_PROOF_MISMATCH


async def test_85_0c_corrupt_resource_receipt_blocks(real_redis):
    seeded = await _seed_run(real_redis, "resource-corrupt", resource_state=RESERVATION_STATE_SETTLED, terminate=True)
    key = seeded["resources"].reservation_receipt_key(seeded["reservation"].operation_id)
    await real_redis.hset(key, "canonical_revision", "not-an-integer")
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        terminal_proof_reader=ReadOnlyTerminalProofReader(real_redis),
        resource_reader=ReadOnlyResourceReceiptReader(real_redis),
    ).reconcile_run_terminate_settlement(run_id=seeded["run_id"])
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE


async def test_85_0c_terminal_run_without_settlement_blocks_when_applicability_unresolved(real_redis):
    seeded = await _seed_run(real_redis, "settlement-absent", resource_state=RESERVATION_STATE_FINALIZED, terminate=True)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        terminal_proof_reader=ReadOnlyTerminalProofReader(real_redis),
        resource_reader=ReadOnlyResourceReceiptReader(real_redis),
    ).reconcile_run_terminate_settlement(run_id=seeded["run_id"])
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE

async def test_85_0c_r1_1_all_cross_aggregate_observers_zero_logical_mutation(real_redis):
    seeded = await _seed_run(
        real_redis,
        "all-zero-mutation",
        resource_state=RESERVATION_STATE_SETTLED,
        terminate=True,
    )
    reconciler = HistoricalCrossAggregateReconciler(
        canonical_reader=HistoricalCanonicalReconciliationReader(real_redis),
        terminal_proof_reader=ReadOnlyTerminalProofReader(real_redis),
        resource_reader=ReadOnlyResourceReceiptReader(real_redis),
    )
    before = await _logical_snapshot(real_redis)
    create_findings = await reconciler.reconcile_run_create_resource(run_id=seeded["run_id"])
    proof_findings = await reconciler.reconcile_run_terminate_proof(run_id=seeded["run_id"])
    settlement_findings = await reconciler.reconcile_run_terminate_settlement(run_id=seeded["run_id"])
    after = await _logical_snapshot(real_redis)
    assert create_findings[0].status is ReconciliationStatus.CONSISTENT
    assert proof_findings[0].status is ReconciliationStatus.CONSISTENT
    assert settlement_findings[0].status is ReconciliationStatus.CONSISTENT
    assert after == before

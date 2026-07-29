from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
import redis.asyncio as redis_async

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    CanonicalStoreDecision,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisAuthorityKeyspace,
    RedisAuthorityPersistenceError,
    RedisCanonicalAuthorityStore,
    classify_canonical_store_write,
    evaluate_authority_commit,
)

REDIS_URL_ENV = "SPRINT81_2_REDIS_URL"


def _identity(*, run_id: str = "run-81-2", task_id: str = "task-81-2") -> CanonicalAggregateIdentity:
    return CanonicalAggregateIdentity(AggregateType.TASK, run_id=run_id, task_id=task_id)


def _context(identity: CanonicalAggregateIdentity, *operations: OperationType) -> AuthorityEntryContext:
    return AuthorityEntryContext(
        authenticated_writer_id="sprint81.2-test-writer",
        allowed_operations=frozenset(operations),
        target_aggregate_identity_sha256=identity.sha256,
    )


def _accepted_plan(
    command: AuthorityCommand,
    *,
    current_revision: int,
    current_state: str | None,
    committed_at_ms: int,
):
    evaluation = evaluate_authority_commit(
        context=_context(command.aggregate_identity, command.operation_type),
        command=command,
        current_revision=current_revision,
        current_state=current_state,
        receipt_probe=None,
        committed_at_ms=committed_at_ms,
    )
    assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    return evaluation.commit_plan


def _admit_command(
    *,
    operation_id: str = "op-admit",
    payload_version: int = 1,
    identity: CanonicalAggregateIdentity | None = None,
) -> AuthorityCommand:
    identity = identity or _identity()
    return AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_ADMIT,
        operation_id=operation_id,
        expected_revision=0,
        intended_previous_state=None,
        intended_next_state="ready",
        authoritative_payload={"version": payload_version},
        authoritative_metadata_changes={"tenant_id": "tenant-a"},
        requested_child_effects=[],
        requested_projection_intents=({"kind": "READY_QUEUE_IF_READY"},),
        causation_id="cause-admit",
    )


def _dispatch_command(
    *,
    operation_id: str,
    identity: CanonicalAggregateIdentity | None = None,
) -> AuthorityCommand:
    return AuthorityCommand(
        aggregate_identity=identity or _identity(),
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=operation_id,
        expected_revision=1,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload={"shard": 3},
        authoritative_metadata_changes={"scheduler_epoch": "epoch-7"},
        requested_child_effects=[],
        requested_projection_intents=(
            {"kind": "CONTROL_NOTIFICATION"},
            {"kind": "TASK_REQUEST_MESSAGE"},
        ),
        causation_id="cause-dispatch",
    )


@pytest_asyncio.fixture
async def redis_client():
    url = os.getenv(REDIS_URL_ENV)
    if not url:
        pytest.skip(f"{REDIS_URL_ENV} is required for real Redis authority tests")
    client = redis_async.Redis.from_url(url, decode_responses=True)
    await client.ping()
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


@pytest_asyncio.fixture
async def store(redis_client):
    value = RedisCanonicalAuthorityStore(redis_client, namespace="hfa:test:authority:v1")
    await value.initialise()
    return value


def _envelope(payload: str) -> str:
    return json.dumps(
        {"payload": payload, "storage_sha1": hashlib.sha1(payload.encode("utf-8")).hexdigest()},
        separators=(",", ":"),
        sort_keys=True,
    )


async def _stored_payload(redis_client, key: str, field: str) -> tuple[dict[str, Any], dict[str, Any]]:
    raw = await redis_client.hget(key, field)
    assert raw is not None
    env = json.loads(raw)
    return env, json.loads(env["payload"])


async def _replace_payload(redis_client, key: str, field: str, payload: dict[str, Any], *, refresh_digest: bool) -> None:
    raw = await redis_client.hget(key, field)
    assert raw is not None
    env = json.loads(raw)
    text = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    env["payload"] = text
    if refresh_digest:
        env["storage_sha1"] = hashlib.sha1(text.encode("utf-8")).hexdigest()
    await redis_client.hset(key, field, json.dumps(env, separators=(",", ":"), sort_keys=True))


async def _assert_no_new_lifecycle(redis_client, keyspace, *, revision: int, log_len: int, outbox_len: int) -> None:
    snapshot = await redis_client.hget(keyspace.aggregate, "revision")
    assert snapshot == str(revision)
    assert await redis_client.xlen(keyspace.transition_log) == log_len
    assert await redis_client.xlen(keyspace.outbox) == outbox_len


def test_keyspace_uses_one_cluster_hash_tag_and_fixed_proof_hashes() -> None:
    keyspace = RedisAuthorityKeyspace(_identity().sha256)
    keys = keyspace.commit_keys()
    assert len(keys) == 8
    assert all(keyspace.hash_tag in key for key in keys)
    assert len({key.split("{")[1].split("}")[0] for key in keys}) == 1
    assert keyspace.operation_field("op-1") != keyspace.operation_field("op-2")
    assert keyspace.transition_indexes.endswith(":transition-indexes")
    assert keyspace.receipts.endswith(":receipts")
    assert keyspace.operation_records.endswith(":operation-records")
    assert keyspace.conflict_index.endswith(":conflict-index")


def test_lua_script_contains_fixed_proof_history_and_conflict_gates() -> None:
    script = Path(__file__).resolve().parents[2] / "src" / "hfa" / "lua" / "canonical_authority_commit.lua"
    source = script.read_text(encoding="utf-8")
    assert "Receipt-first idempotency" in source
    assert 'redis.call("HGET", KEYS[2], pre_record.transition_id)' in source
    assert "storage_integrity" in source
    assert "aggregate_head_proof_missing" in source
    assert "transition_log_tail_mismatch" in source
    assert "outbox_tail_mismatch" in source
    assert 'redis.call("HSETNX", KEYS[7]' in source
    assert "detail_code=stable_detail_code" in source
    assert "length_prefix(stable_detail_code)" in source
    assert "HFA_ALLOW" not in source


@pytest.mark.asyncio
async def test_atomic_commit_persists_one_revision_record_receipt_intents_and_no_ttl(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=1_000)
    result = await store.commit(plan)
    assert result.status is RedisAuthorityCommitStatus.COMMITTED
    assert result.transition_id == plan.record.transition_id
    assert result.aggregate_revision == 1

    snapshot = await store.get_aggregate_snapshot(command.aggregate_identity)
    assert snapshot is not None
    assert snapshot.revision == 1
    assert snapshot.state == "ready"
    assert snapshot.transition_id == plan.record.transition_id
    assert snapshot.operation_id == command.operation_id

    keyspace = store.keyspace(command.aggregate_identity.sha256)
    field = keyspace.operation_field(command.operation_id)
    assert await redis_client.hexists(keyspace.transition_indexes, plan.record.transition_id) == 1
    assert await redis_client.hexists(keyspace.operation_records, field) == 1
    assert await redis_client.hexists(keyspace.receipts, field) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    for key in keyspace.commit_keys():
        if await redis_client.exists(key):
            assert await redis_client.pttl(key) == -1


@pytest.mark.asyncio
async def test_persisted_receipt_probe_round_trips_and_is_fully_validated(store) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=2_000)
    assert (await store.commit(plan)).committed
    probe = await store.load_receipt_probe(command.aggregate_identity, command.operation_id)
    assert probe is not None
    duplicate = evaluate_authority_commit(
        context=_context(command.aggregate_identity, OperationType.TASK_ADMIT),
        command=command,
        current_revision=1,
        current_state="ready",
        receipt_probe=probe,
        committed_at_ms=2_001,
    )
    assert duplicate.decision.code is AuthorityDecisionCode.ALREADY_APPLIED
    assert duplicate.commit_plan is None


@pytest.mark.asyncio
async def test_load_receipt_probe_rejects_receipt_record_mismatch(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=2_100)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    field = keyspace.operation_field(command.operation_id)
    _, receipt = await _stored_payload(redis_client, keyspace.receipts, field)
    receipt["operation_type"] = OperationType.TASK_DISPATCH.value
    await _replace_payload(redis_client, keyspace.receipts, field, receipt, refresh_digest=True)
    with pytest.raises(RedisAuthorityPersistenceError, match="stored receipt proof mismatch"):
        await store.load_receipt_probe(command.aggregate_identity, command.operation_id)


@pytest.mark.asyncio
async def test_load_receipt_probe_rejects_wrong_lookup_operation_id(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=2_200)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    original = keyspace.operation_field(command.operation_id)
    wrong = keyspace.operation_field("wrong-operation")
    await redis_client.hset(keyspace.receipts, wrong, await redis_client.hget(keyspace.receipts, original))
    await redis_client.hset(keyspace.operation_records, wrong, await redis_client.hget(keyspace.operation_records, original))
    with pytest.raises(RedisAuthorityPersistenceError, match="stored receipt proof mismatch"):
        await store.load_receipt_probe(command.aggregate_identity, "wrong-operation")


@pytest.mark.asyncio
async def test_load_receipt_probe_rejects_wrong_lookup_aggregate(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=2_300)
    assert (await store.commit(plan)).committed
    source = store.keyspace(command.aggregate_identity.sha256)
    other_identity = _identity(run_id="other-run", task_id="other-task")
    target = store.keyspace(other_identity.sha256)
    field = source.operation_field(command.operation_id)
    await redis_client.hset(target.receipts, field, await redis_client.hget(source.receipts, field))
    await redis_client.hset(target.operation_records, field, await redis_client.hget(source.operation_records, field))
    await redis_client.hset(
        target.transition_indexes,
        plan.record.transition_id,
        await redis_client.hget(source.transition_indexes, plan.record.transition_id),
    )
    with pytest.raises(RedisAuthorityPersistenceError, match="stored receipt proof mismatch"):
        await store.load_receipt_probe(other_identity, command.operation_id)


@pytest.mark.asyncio
async def test_same_plan_is_idempotent_without_extra_lifecycle_or_conflict_entries(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=3_000)
    assert (await store.commit(plan)).status is RedisAuthorityCommitStatus.COMMITTED
    assert (await store.commit(plan)).status is RedisAuthorityCommitStatus.ALREADY_APPLIED
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    assert await redis_client.xlen(keyspace.conflicts) == 0


@pytest.mark.asyncio
async def test_idempotency_conflict_is_durable_deduplicated_and_revision_neutral(store, redis_client) -> None:
    first = _admit_command(payload_version=1)
    first_plan = _accepted_plan(first, current_revision=0, current_state=None, committed_at_ms=4_000)
    assert (await store.commit(first_plan)).committed
    conflicting = _admit_command(payload_version=2)
    conflict_plan = _accepted_plan(conflicting, current_revision=0, current_state=None, committed_at_ms=4_001)
    one = await store.commit(conflict_plan)
    two = await store.commit(conflict_plan)
    assert one.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    assert two.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    keyspace = store.keyspace(first.aggregate_identity.sha256)
    assert await redis_client.hlen(keyspace.conflict_index) == 2
    assert await redis_client.xlen(keyspace.conflicts) == 1
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
async def test_changed_transition_with_missing_old_index_is_corruption_before_idempotency(store, redis_client) -> None:
    admit = _admit_command(operation_id="shared-op")
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=4_100)
    assert (await store.commit(admit_plan)).committed
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    await redis_client.hdel(keyspace.transition_indexes, admit_plan.record.transition_id)
    changed = _dispatch_command(operation_id="shared-op")
    changed_plan = _accepted_plan(changed, current_revision=1, current_state="ready", committed_at_ms=4_101)
    result = await store.commit(changed_plan)
    repeated = await store.commit(changed_plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert repeated.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "stored_proof_canonical_validation_failed"
    assert await redis_client.hlen(keyspace.conflict_index) == 2
    assert await redis_client.xlen(keyspace.conflicts) == 1
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
async def test_changed_transition_with_corrupted_old_index_is_corruption(store, redis_client) -> None:
    admit = _admit_command(operation_id="shared-corrupted-op")
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=4_150)
    assert (await store.commit(admit_plan)).committed
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    _, payload = await _stored_payload(redis_client, keyspace.transition_indexes, admit_plan.record.transition_id)
    payload["canonical_record_hash"] = "0" * 64
    await _replace_payload(redis_client, keyspace.transition_indexes, admit_plan.record.transition_id, payload, refresh_digest=True)
    changed = _dispatch_command(operation_id="shared-corrupted-op")
    changed_plan = _accepted_plan(changed, current_revision=1, current_state="ready", committed_at_ms=4_151)
    result = await store.commit(changed_plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert await redis_client.xlen(keyspace.conflicts) == 1
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("writer_id", "tampered-writer"),
        ("authoritative_metadata_changes", {"tenant_id": "tampered"}),
        ("child_effects", [{"kind": "tampered"}]),
        ("durable_projection_intents", [{"kind": "TAMPERED"}]),
    ],
)
async def test_tampered_stored_record_payload_is_corruption(store, redis_client, field, replacement) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=4_200)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    op_field = keyspace.operation_field(command.operation_id)
    _, payload = await _stored_payload(redis_client, keyspace.operation_records, op_field)
    payload[field] = replacement
    # Preserve the old storage digest to model Redis payload corruption.
    await _replace_payload(redis_client, keyspace.operation_records, op_field, payload, refresh_digest=False)
    result = await store.commit(plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert await redis_client.xlen(keyspace.conflicts) == 1
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
async def test_transition_index_extra_field_is_corruption(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=4_300)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    _, payload = await _stored_payload(redis_client, keyspace.transition_indexes, plan.record.transition_id)
    payload["extra"] = "tampered"
    await _replace_payload(redis_client, keyspace.transition_indexes, plan.record.transition_id, payload, refresh_digest=True)
    result = await store.commit(plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
async def test_concurrent_second_transition_fails_strict_revision_cas(store, redis_client) -> None:
    admit = _admit_command()
    assert (await store.commit(_accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=5_000))).committed
    first = _accepted_plan(_dispatch_command(operation_id="dispatch-a"), current_revision=1, current_state="ready", committed_at_ms=5_001)
    second = _accepted_plan(_dispatch_command(operation_id="dispatch-b"), current_revision=1, current_state="ready", committed_at_ms=5_002)
    assert (await store.commit(first)).status is RedisAuthorityCommitStatus.COMMITTED
    assert (await store.commit(second)).status is RedisAuthorityCommitStatus.STALE_REVISION_CONFLICT
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=2, log_len=2, outbox_len=2)


@pytest.mark.asyncio
async def test_aggregate_exists_conflict_writes_one_durable_record_without_revision(store, redis_client) -> None:
    first = _admit_command(operation_id="create-1")
    assert (await store.commit(_accepted_plan(first, current_revision=0, current_state=None, committed_at_ms=5_100))).committed
    second = _admit_command(operation_id="create-2")
    second_plan = _accepted_plan(second, current_revision=0, current_state=None, committed_at_ms=5_101)
    assert (await store.commit(second_plan)).status is RedisAuthorityCommitStatus.AGGREGATE_ALREADY_EXISTS_CONFLICT
    assert (await store.commit(second_plan)).status is RedisAuthorityCommitStatus.AGGREGATE_ALREADY_EXISTS_CONFLICT
    keyspace = store.keyspace(first.aggregate_identity.sha256)
    assert await redis_client.hlen(keyspace.conflict_index) == 2
    assert await redis_client.xlen(keyspace.conflicts) == 1
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["index", "record", "receipt", "log", "outbox"])
async def test_missing_previous_head_proof_blocks_next_revision(store, redis_client, missing: str) -> None:
    admit = _admit_command()
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=6_000)
    assert (await store.commit(admit_plan)).committed
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    op_field = keyspace.operation_field(admit.operation_id)
    if missing == "index":
        await redis_client.hdel(keyspace.transition_indexes, admit_plan.record.transition_id)
    elif missing == "record":
        await redis_client.hdel(keyspace.operation_records, op_field)
    elif missing == "receipt":
        await redis_client.hdel(keyspace.receipts, op_field)
    elif missing == "log":
        await redis_client.delete(keyspace.transition_log)
    else:
        await redis_client.delete(keyspace.outbox)
    dispatch = _dispatch_command(operation_id=f"dispatch-after-{missing}")
    dispatch_plan = _accepted_plan(dispatch, current_revision=1, current_state="ready", committed_at_ms=6_001)
    result = await store.commit(dispatch_plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert await redis_client.xlen(keyspace.conflicts) == 1
    assert await redis_client.hget(keyspace.aggregate, "revision") == "1"
    assert await redis_client.hexists(keyspace.operation_records, keyspace.operation_field(dispatch.operation_id)) == 0


@pytest.mark.asyncio
async def test_aggregate_head_stream_tail_mismatch_blocks_next_revision(store, redis_client) -> None:
    admit = _admit_command()
    assert (await store.commit(_accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=6_100))).committed
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    await redis_client.xadd(keyspace.transition_log, {"aggregate_revision": "999", "transition_id": "tampered"})
    dispatch = _dispatch_command(operation_id="dispatch-after-tail-tamper")
    plan = _accepted_plan(dispatch, current_revision=1, current_state="ready", committed_at_ms=6_101)
    result = await store.commit(plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "transition_log_tail_mismatch"
    assert await redis_client.hget(keyspace.aggregate, "revision") == "1"
    assert await redis_client.xlen(keyspace.outbox) == 1


@pytest.mark.asyncio
async def test_outbox_tail_mismatch_blocks_next_revision(store, redis_client) -> None:
    admit = _admit_command()
    assert (await store.commit(_accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=6_150))).committed
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    await redis_client.xadd(keyspace.outbox, {"aggregate_revision": "999", "transition_id": "tampered"})
    dispatch = _dispatch_command(operation_id="dispatch-after-outbox-tail-tamper")
    plan = _accepted_plan(dispatch, current_revision=1, current_state="ready", committed_at_ms=6_151)
    result = await store.commit(plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "outbox_tail_mismatch"
    assert await redis_client.hget(keyspace.aggregate, "revision") == "1"
    assert await redis_client.xlen(keyspace.transition_log) == 1


@pytest.mark.asyncio
async def test_wrong_redis_key_type_fails_closed_and_records_conflict(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=7_000)
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.set(keyspace.aggregate, "wrong-type")
    result = await store.commit(plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert await redis_client.hlen(keyspace.transition_indexes) == 0
    assert await redis_client.hlen(keyspace.operation_records) == 0
    assert await redis_client.hlen(keyspace.receipts) == 0
    assert await redis_client.xlen(keyspace.transition_log) == 0
    assert await redis_client.xlen(keyspace.outbox) == 0
    assert await redis_client.xlen(keyspace.conflicts) == 1


@pytest.mark.asyncio
async def test_partial_aggregate_hash_is_not_treated_as_revision_zero(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=7_100)
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.hset(keyspace.aggregate, mapping={"unrelated": "value"})
    result = await store.commit(plan)
    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "aggregate_revision_missing"
    assert await redis_client.xlen(keyspace.conflicts) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("revision", "-1", "snapshot revision"),
        ("revision", str(2**53), "snapshot revision"),
        ("state_is_null", "2", "state_is_null"),
        ("canonical_record_hash", "bad", "canonical_record_hash"),
        ("transition_id", "", "transition_id"),
    ],
)
async def test_snapshot_read_rejects_invalid_domain_values(store, redis_client, field, value, message) -> None:
    command = _admit_command()
    assert (await store.commit(_accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=7_200))).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.hset(keyspace.aggregate, field, value)
    with pytest.raises(RedisAuthorityPersistenceError, match=message):
        await store.get_aggregate_snapshot(command.aggregate_identity)


# Sprint 81.2 final re-review regressions


def _claim_command(
    *,
    operation_id: str,
    identity: CanonicalAggregateIdentity | None = None,
) -> AuthorityCommand:
    return AuthorityCommand(
        aggregate_identity=identity or _identity(),
        operation_type=OperationType.TASK_CLAIM,
        operation_id=operation_id,
        expected_revision=2,
        intended_previous_state="scheduled",
        intended_next_state="running",
        authoritative_payload={"worker_id": "worker-81-2"},
        authoritative_metadata_changes={"claim_epoch": "epoch-81-2"},
        requested_child_effects=[],
        requested_projection_intents=({"kind": "RUNNING_SET"},),
        causation_id="cause-claim",
    )


@pytest.mark.asyncio
async def test_redis_duplicate_outcome_matches_canonical_store_classifier(
    store,
    redis_client,
) -> None:
    command = _admit_command(operation_id="concurrent-same-command")
    first_plan = _accepted_plan(
        command,
        current_revision=0,
        current_state=None,
        committed_at_ms=10_000,
    )
    second_plan = _accepted_plan(
        command,
        current_revision=0,
        current_state=None,
        committed_at_ms=10_001,
    )
    assert first_plan.record.canonical_command_hash == second_plan.record.canonical_command_hash
    assert first_plan.record.canonical_record_hash != second_plan.record.canonical_record_hash
    assert (
        classify_canonical_store_write(first_plan.record, second_plan.record)
        is CanonicalStoreDecision.CANONICAL_RECORD_CORRUPTION_CONFLICT
    )

    first = await store.commit(first_plan)
    second = await store.commit(second_plan)

    assert first.status is RedisAuthorityCommitStatus.COMMITTED
    assert second.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert second.detail == "exact_duplicate_payload_mismatch"
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    assert await redis_client.hlen(keyspace.transition_indexes) == 1
    assert await redis_client.hlen(keyspace.receipts) == 1
    assert await redis_client.hlen(keyspace.operation_records) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    assert await redis_client.xlen(keyspace.conflicts) == 1
    assert (await store.get_aggregate_snapshot(command.aggregate_identity)).revision == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("broken_key", "expected_detail"),
    [
        ("conflict_index", "conflict_index_type_mismatch"),
        ("conflicts", "conflict_stream_type_mismatch"),
    ],
)
async def test_conflict_store_wrong_type_has_explicit_fail_closed_result(
    store,
    redis_client,
    broken_key: str,
    expected_detail: str,
) -> None:
    command = _admit_command(operation_id=f"wrong-{broken_key}")
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=10_100)
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.set(getattr(keyspace, broken_key), "wrong-type")

    result = await store.commit(plan)

    assert result.status is RedisAuthorityCommitStatus.CONFLICT_EVIDENCE_STORE_UNAVAILABLE
    assert result.detail == expected_detail
    assert await redis_client.exists(keyspace.aggregate) == 0
    assert await redis_client.exists(keyspace.transition_indexes) == 0
    assert await redis_client.exists(keyspace.receipts) == 0
    assert await redis_client.exists(keyspace.operation_records) == 0
    assert await redis_client.exists(keyspace.transition_log) == 0
    assert await redis_client.exists(keyspace.outbox) == 0


@pytest.mark.asyncio
async def test_operator_audit_notes_are_separate_from_authority_conflict_pair(
    store,
    redis_client,
) -> None:
    identity = _identity()
    await store.record_conflict(
        identity,
        conflict_type="OPERATOR_NOTE",
        operation_id="operator-note",
        observed_at_ms=10_200,
        detail={"reason": "manual audit"},
    )
    keyspace = store.keyspace(identity.sha256)
    assert await redis_client.xlen(keyspace.operator_audits) == 1
    assert await redis_client.exists(keyspace.conflict_index) == 0
    assert await redis_client.exists(keyspace.conflicts) == 0


async def _commit_two_revisions(store):
    admit = _admit_command(operation_id="history-admit")
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=10_400)
    assert (await store.commit(admit_plan)).committed
    dispatch = _dispatch_command(operation_id="history-dispatch")
    dispatch_plan = _accepted_plan(
        dispatch,
        current_revision=1,
        current_state="ready",
        committed_at_ms=10_401,
    )
    assert (await store.commit(dispatch_plan)).committed
    return admit, admit_plan, dispatch, dispatch_plan


@pytest.mark.asyncio
async def test_old_revision_operation_record_deletion_blocks_next_commit(store, redis_client) -> None:
    admit, _, _, _ = await _commit_two_revisions(store)
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    await redis_client.hdel(keyspace.operation_records, keyspace.operation_field(admit.operation_id))
    claim = _claim_command(operation_id="history-claim-after-record-delete")
    claim_plan = _accepted_plan(
        claim,
        current_revision=2,
        current_state="scheduled",
        committed_at_ms=10_402,
    )

    result = await store.commit(claim_plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "historical_cardinality_mismatch"
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=2, log_len=2, outbox_len=2)
    assert await redis_client.hexists(
        keyspace.operation_records,
        keyspace.operation_field(claim.operation_id),
    ) == 0


@pytest.mark.asyncio
async def test_old_transition_log_entry_deletion_blocks_next_commit(store, redis_client) -> None:
    admit, _, _, _ = await _commit_two_revisions(store)
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    rows = await redis_client.xrange(keyspace.transition_log)
    assert len(rows) == 2
    assert await redis_client.xdel(keyspace.transition_log, rows[0][0]) == 1
    claim = _claim_command(operation_id="history-claim-after-log-delete")
    claim_plan = _accepted_plan(
        claim,
        current_revision=2,
        current_state="scheduled",
        committed_at_ms=10_403,
    )

    result = await store.commit(claim_plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "historical_cardinality_mismatch"
    assert (await store.get_aggregate_snapshot(admit.aggregate_identity)).revision == 2
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 2


@pytest.mark.asyncio
async def test_old_outbox_entry_deletion_blocks_next_commit(store, redis_client) -> None:
    admit, _, _, _ = await _commit_two_revisions(store)
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    rows = await redis_client.xrange(keyspace.outbox)
    assert len(rows) == 2
    assert await redis_client.xdel(keyspace.outbox, rows[0][0]) == 1
    claim = _claim_command(operation_id="history-claim-after-outbox-delete")
    claim_plan = _accepted_plan(
        claim,
        current_revision=2,
        current_state="scheduled",
        committed_at_ms=10_404,
    )

    result = await store.commit(claim_plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "historical_cardinality_mismatch"
    assert (await store.get_aggregate_snapshot(admit.aggregate_identity)).revision == 2
    assert await redis_client.xlen(keyspace.transition_log) == 2
    assert await redis_client.xlen(keyspace.outbox) == 1

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tamper_kind",
    [
        "writer_id",
        "authoritative_metadata_changes",
        "child_effects",
        "durable_projection_intents",
        "canonical_aggregate_identity",
        "transition_id",
    ],
)
async def test_refreshed_storage_digest_cannot_hide_canonical_record_tamper(
    store,
    redis_client,
    tamper_kind: str,
) -> None:
    command = _admit_command(operation_id=f"canonical-tamper-{tamper_kind}")
    plan = _accepted_plan(
        command,
        current_revision=0,
        current_state=None,
        committed_at_ms=11_000,
    )
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    field = keyspace.operation_field(command.operation_id)
    _, record = await _stored_payload(redis_client, keyspace.operation_records, field)
    if tamper_kind == "writer_id":
        record["writer_id"] = "tampered-writer"
    elif tamper_kind == "authoritative_metadata_changes":
        record["authoritative_metadata_changes"] = {"tenant_id": "tampered"}
    elif tamper_kind == "child_effects":
        record["child_effects"] = [{"kind": "tampered"}]
    elif tamper_kind == "durable_projection_intents":
        record["durable_projection_intents"] = []
    elif tamper_kind == "canonical_aggregate_identity":
        record["canonical_aggregate_identity"]["run_id"] = "tampered-run"
    elif tamper_kind == "transition_id":
        record["transition_id"] = "ctr:v1:semantically-invalid"
    else:  # pragma: no cover - parameter list is closed.
        raise AssertionError(tamper_kind)
    await _replace_payload(
        redis_client,
        keyspace.operation_records,
        field,
        record,
        refresh_digest=True,
    )

    result = await store.commit(plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "stored_proof_canonical_validation_failed"
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1


async def _create_durable_idempotency_conflict(store, *, operation_id: str, version: int, observed_at: int):
    base = _admit_command(operation_id=operation_id, payload_version=1)
    base_plan = _accepted_plan(base, current_revision=0, current_state=None, committed_at_ms=observed_at)
    assert (await store.commit(base_plan)).committed
    conflicting = _admit_command(operation_id=operation_id, payload_version=version)
    conflicting_plan = _accepted_plan(
        conflicting,
        current_revision=0,
        current_state=None,
        committed_at_ms=observed_at + version,
    )
    result = await store.commit(conflicting_plan)
    assert result.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    return base, conflicting_plan


@pytest.mark.asyncio
async def test_prior_authority_conflict_index_without_stream_fails_closed(store, redis_client) -> None:
    base, conflicting_plan = await _create_durable_idempotency_conflict(
        store,
        operation_id="pair-stream-deleted",
        version=2,
        observed_at=12_000,
    )
    keyspace = store.keyspace(base.aggregate_identity.sha256)
    await redis_client.delete(keyspace.conflicts)

    result = await store.commit(conflicting_plan)

    assert result.status is RedisAuthorityCommitStatus.CONFLICT_EVIDENCE_STORE_UNAVAILABLE
    assert result.detail == "authority_conflict_pair_missing_member"
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
async def test_prior_authority_conflict_stream_without_index_fails_closed(store, redis_client) -> None:
    base, conflicting_plan = await _create_durable_idempotency_conflict(
        store,
        operation_id="pair-index-deleted",
        version=2,
        observed_at=12_100,
    )
    keyspace = store.keyspace(base.aggregate_identity.sha256)
    await redis_client.delete(keyspace.conflict_index)

    result = await store.commit(conflicting_plan)

    assert result.status is RedisAuthorityCommitStatus.CONFLICT_EVIDENCE_STORE_UNAVAILABLE
    assert result.detail == "authority_conflict_pair_missing_member"
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
async def test_deleted_authority_conflict_index_entry_is_detected(store, redis_client) -> None:
    base, conflict_two = await _create_durable_idempotency_conflict(
        store,
        operation_id="pair-index-entry-deleted",
        version=2,
        observed_at=12_200,
    )
    conflict_three_command = _admit_command(operation_id=base.operation_id, payload_version=3)
    conflict_three = _accepted_plan(
        conflict_three_command,
        current_revision=0,
        current_state=None,
        committed_at_ms=12_203,
    )
    assert (await store.commit(conflict_three)).status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    keyspace = store.keyspace(base.aggregate_identity.sha256)
    fields = [
        field
        for field in await redis_client.hkeys(keyspace.conflict_index)
        if field != "__authority_conflict_count"
    ]
    assert len(fields) == 2
    await redis_client.hdel(keyspace.conflict_index, fields[0])

    result = await store.commit(conflict_two)

    assert result.status is RedisAuthorityCommitStatus.CONFLICT_EVIDENCE_STORE_UNAVAILABLE
    assert result.detail == "authority_conflict_pair_cardinality_mismatch"


@pytest.mark.asyncio
async def test_deleted_authority_conflict_stream_row_is_detected(store, redis_client) -> None:
    base, conflicting_plan = await _create_durable_idempotency_conflict(
        store,
        operation_id="pair-stream-row-deleted",
        version=2,
        observed_at=12_300,
    )
    keyspace = store.keyspace(base.aggregate_identity.sha256)
    rows = await redis_client.xrange(keyspace.conflicts)
    assert len(rows) == 1
    await redis_client.xdel(keyspace.conflicts, rows[0][0])

    result = await store.commit(conflicting_plan)

    assert result.status is RedisAuthorityCommitStatus.CONFLICT_EVIDENCE_STORE_UNAVAILABLE
    assert result.detail == "authority_conflict_pair_cardinality_mismatch"
@pytest.mark.asyncio
@pytest.mark.parametrize("broken_key", ["receipts", "operation_records"])
async def test_wrong_type_operation_proof_hash_reaches_lua_fail_closed_gate(
    store,
    redis_client,
    broken_key: str,
) -> None:
    command = _admit_command(operation_id=f"wrong-type-{broken_key}")
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=30_000)
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.set(getattr(keyspace, broken_key), "wrong-type")

    result = await store.commit(plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "redis_key_type_mismatch"
    assert await redis_client.exists(keyspace.aggregate) == 0
    assert await redis_client.xlen(keyspace.conflicts) == 1
    assert await redis_client.hlen(keyspace.conflict_index) == 2


@pytest.mark.asyncio
async def test_wrong_type_transition_index_for_existing_head_fails_closed_without_python_wrongtype(
    store,
    redis_client,
) -> None:
    admit = _admit_command(operation_id="wrong-type-head-admit")
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=30_100)
    assert (await store.commit(admit_plan)).committed
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    await redis_client.delete(keyspace.transition_indexes)
    await redis_client.set(keyspace.transition_indexes, "wrong-type")
    dispatch = _dispatch_command(operation_id="wrong-type-head-dispatch")
    dispatch_plan = _accepted_plan(dispatch, current_revision=1, current_state="ready", committed_at_ms=30_101)

    result = await store.commit(dispatch_plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "redis_key_type_mismatch"
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)
    assert await redis_client.xlen(keyspace.conflicts) == 1


@pytest.mark.asyncio
async def test_wrong_type_transition_index_for_duplicate_operation_fails_closed(
    store,
    redis_client,
) -> None:
    command = _admit_command(operation_id="wrong-type-duplicate")
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=30_200)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.delete(keyspace.transition_indexes)
    await redis_client.set(keyspace.transition_indexes, "wrong-type")

    result = await store.commit(plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "redis_key_type_mismatch"
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)
    assert await redis_client.xlen(keyspace.conflicts) == 1


@pytest.mark.asyncio
async def test_unexpected_aggregate_snapshot_field_blocks_next_commit(
    store,
    redis_client,
) -> None:
    admit = _admit_command(operation_id="extra-field-admit")
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=30_300)
    assert (await store.commit(admit_plan)).committed
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    await redis_client.hset(keyspace.aggregate, "unexpected_authority_field", "forbidden")
    dispatch = _dispatch_command(operation_id="extra-field-dispatch")
    dispatch_plan = _accepted_plan(dispatch, current_revision=1, current_state="ready", committed_at_ms=30_301)

    result = await store.commit(dispatch_plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert result.detail == "aggregate_snapshot_field_count_mismatch"
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)
    assert await redis_client.hget(keyspace.aggregate, "unexpected_authority_field") == "forbidden"
    assert await redis_client.xlen(keyspace.conflicts) == 1


@pytest.mark.asyncio
async def test_repeated_logical_conflict_ignores_new_observation_timestamp(
    store,
    redis_client,
) -> None:
    original = _admit_command(operation_id="conflict-observation", payload_version=1)
    original_plan = _accepted_plan(original, current_revision=0, current_state=None, committed_at_ms=30_400)
    assert (await store.commit(original_plan)).committed
    conflicting = _admit_command(operation_id=original.operation_id, payload_version=2)
    first_conflict = _accepted_plan(conflicting, current_revision=0, current_state=None, committed_at_ms=30_401)
    later_observation = _accepted_plan(conflicting, current_revision=0, current_state=None, committed_at_ms=30_402)

    first = await store.commit(first_conflict)
    second = await store.commit(later_observation)

    assert first.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    assert second.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    keyspace = store.keyspace(original.aggregate_identity.sha256)
    assert await redis_client.hlen(keyspace.conflict_index) == 2
    assert await redis_client.xlen(keyspace.conflicts) == 1
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
async def test_different_stable_conflict_detail_code_creates_distinct_conflict(
    store,
    redis_client,
) -> None:
    admit = _admit_command(operation_id="distinct-conflict-causes")
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=30_500)
    assert (await store.commit(admit_plan)).committed
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    dispatch = _dispatch_command(operation_id="distinct-conflict-causes-dispatch")
    dispatch_plan = _accepted_plan(dispatch, current_revision=1, current_state="ready", committed_at_ms=30_501)

    await redis_client.hset(keyspace.aggregate, "unexpected_authority_field", "forbidden")
    first = await store.commit(dispatch_plan)
    assert first.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert first.detail == "aggregate_snapshot_field_count_mismatch"

    await redis_client.hdel(keyspace.aggregate, "unexpected_authority_field")
    tail = await redis_client.xrevrange(keyspace.transition_log, count=1)
    assert len(tail) == 1
    await redis_client.xdel(keyspace.transition_log, tail[0][0])
    await redis_client.xadd(
        keyspace.transition_log,
        {
            "aggregate_revision": "1",
            "transition_id": admit_plan.record.transition_id,
            "canonical_record_hash": admit_plan.record.canonical_record_hash,
            "canonical_command_hash": admit_plan.record.canonical_command_hash,
            "operation_id": admit.operation_id,
            "operation_digest": keyspace.operation_field(admit.operation_id),
            "record_json": "tampered",
            "receipt_json": "tampered",
        },
    )

    second = await store.commit(dispatch_plan)
    assert second.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert second.detail == "transition_log_tail_mismatch"

    index_rows = await redis_client.hgetall(keyspace.conflict_index)
    evidence = [json.loads(value) for key, value in index_rows.items() if key != "__authority_conflict_count"]
    assert len(evidence) == 2
    assert {row["detail_code"] for row in evidence} == {
        "aggregate_snapshot_field_count_mismatch",
        "transition_log_tail_mismatch",
    }
    assert len({row["conflict_id"] for row in evidence}) == 2
    assert await redis_client.xlen(keyspace.conflicts) == 2
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)

@pytest.mark.asyncio
async def test_policy_conflict_api_writes_authority_pair_atomically(store, redis_client) -> None:
    identity = _identity(run_id="run-policy-conflict", task_id="task-policy-conflict")
    result = await store.record_authority_conflict(
        identity,
        status=RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
        operation_id="task-admit:v1:policy-conflict",
        incoming_command_hash="1" * 64,
        stored_command_hash="2" * 64,
        observed_at_ms=1234,
        detail_code="policy_idempotency_conflict",
        detail="policy conflict without commit plan",
    )
    assert result.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    keyspace = store.keyspace(identity.sha256)
    assert int(await redis_client.hget(keyspace.conflict_index, "__authority_conflict_count")) == 1
    assert await redis_client.hlen(keyspace.conflict_index) == 2
    assert await redis_client.xlen(keyspace.conflicts) == 1
    assert not await redis_client.exists(keyspace.aggregate)
    assert await redis_client.hlen(keyspace.operation_records) == 0
    assert await redis_client.hlen(keyspace.receipts) == 0


@pytest.mark.asyncio
async def test_policy_conflict_api_deduplicates_same_identity(store, redis_client) -> None:
    identity = _identity(run_id="run-policy-dedup", task_id="task-policy-dedup")
    kwargs = dict(
        status=RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
        operation_id="task-admit:v1:policy-dedup",
        incoming_command_hash="3" * 64,
        stored_command_hash="4" * 64,
        detail_code="policy_idempotency_conflict",
        detail="first payload wins",
    )
    await store.record_authority_conflict(identity, observed_at_ms=100, **kwargs)
    await store.record_authority_conflict(identity, observed_at_ms=200, **kwargs)
    keyspace = store.keyspace(identity.sha256)
    assert int(await redis_client.hget(keyspace.conflict_index, "__authority_conflict_count")) == 1
    assert await redis_client.xlen(keyspace.conflicts) == 1

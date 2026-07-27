from __future__ import annotations

import os
from pathlib import Path

import pytest
import pytest_asyncio
import redis.asyncio as redis_async

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisAuthorityKeyspace,
    RedisAuthorityPersistenceError,
    RedisCanonicalAuthorityStore,
    evaluate_authority_commit,
)

REDIS_URL_ENV = "SPRINT81_2_REDIS_URL"


def _identity() -> CanonicalAggregateIdentity:
    return CanonicalAggregateIdentity(AggregateType.TASK, run_id="run-81-2", task_id="task-81-2")


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


def _admit_command(*, operation_id: str = "op-admit", payload_version: int = 1) -> AuthorityCommand:
    identity = _identity()
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


def _dispatch_command(*, operation_id: str) -> AuthorityCommand:
    return AuthorityCommand(
        aggregate_identity=_identity(),
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


def test_keyspace_uses_one_cluster_hash_tag() -> None:
    identity = _identity()
    keyspace = RedisAuthorityKeyspace(identity.sha256)
    keys = keyspace.commit_keys(transition_id="ctr:v1:test", operation_id="op-1")
    assert len(keys) == 6
    assert all(keyspace.hash_tag in key for key in keys)
    assert len({key.split("{")[1].split("}")[0] for key in keys}) == 1
    assert keyspace.receipt("op-1") != keyspace.receipt("op-2")
    assert keyspace.operation_record("op-1") != keyspace.operation_record("op-2")


def test_lua_script_is_packaged_next_to_core() -> None:
    script = Path(__file__).resolve().parents[2] / "src" / "hfa" / "lua" / "canonical_authority_commit.lua"
    source = script.read_text(encoding="utf-8")
    assert "Receipt-first idempotency" in source
    assert 'redis.call("SET", KEYS[2], transition_index_json)' in source
    assert 'redis.call("SET", KEYS[4], record_json)' in source
    assert 'redis.call("XADD", KEYS[5]' in source
    assert "HFA_ALLOW" not in source


@pytest.mark.asyncio
async def test_atomic_commit_persists_one_revision_record_receipt_and_intents(store, redis_client) -> None:
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
    assert snapshot.canonical_record_hash == plan.record.canonical_record_hash

    keyspace = store.keyspace(command.aggregate_identity.sha256)
    assert await redis_client.exists(keyspace.transition_index(plan.record.transition_id)) == 1
    assert await redis_client.exists(keyspace.operation_record(command.operation_id)) == 1
    assert await redis_client.exists(keyspace.receipt(command.operation_id)) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    for key in keyspace.commit_keys(
        transition_id=plan.record.transition_id,
        operation_id=command.operation_id,
    ):
        assert await redis_client.pttl(key) == -1


@pytest.mark.asyncio
async def test_persisted_receipt_probe_round_trips_into_policy_evaluator(store) -> None:
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
    assert duplicate.decision.return_existing_transition_id == plan.record.transition_id
    assert duplicate.commit_plan is None


@pytest.mark.asyncio
async def test_missing_transition_index_blocks_receipt_probe_rehydration(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=2_500)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.delete(keyspace.transition_index(plan.record.transition_id))

    with pytest.raises(RedisAuthorityPersistenceError, match="transition index is missing"):
        await store.load_receipt_probe(command.aggregate_identity, command.operation_id)


@pytest.mark.asyncio
async def test_same_plan_is_idempotent_without_extra_log_or_outbox_entries(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=3_000)
    first = await store.commit(plan)
    second = await store.commit(plan)

    assert first.status is RedisAuthorityCommitStatus.COMMITTED
    assert second.status is RedisAuthorityCommitStatus.ALREADY_APPLIED
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    assert (await store.get_aggregate_snapshot(command.aggregate_identity)).revision == 1


@pytest.mark.asyncio
async def test_same_operation_id_with_different_command_is_idempotency_conflict(store, redis_client) -> None:
    first_command = _admit_command(payload_version=1)
    first_plan = _accepted_plan(first_command, current_revision=0, current_state=None, committed_at_ms=4_000)
    assert (await store.commit(first_plan)).committed

    conflicting_command = _admit_command(payload_version=2)
    conflicting_plan = _accepted_plan(
        conflicting_command,
        current_revision=0,
        current_state=None,
        committed_at_ms=4_001,
    )
    conflict = await store.commit(conflicting_plan)

    assert conflict.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    keyspace = store.keyspace(first_command.aggregate_identity.sha256)
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    assert (await store.get_aggregate_snapshot(first_command.aggregate_identity)).revision == 1


@pytest.mark.asyncio
async def test_same_operation_id_different_revision_and_type_cannot_bypass_receipt(store, redis_client) -> None:
    admit = _admit_command(operation_id="shared-operation-id")
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=4_500)
    assert (await store.commit(admit_plan)).committed

    changed_command = _dispatch_command(operation_id="shared-operation-id")
    changed_plan = _accepted_plan(
        changed_command,
        current_revision=1,
        current_state="ready",
        committed_at_ms=4_501,
    )
    conflict = await store.commit(changed_plan)

    assert conflict.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    assert (await store.get_aggregate_snapshot(admit.aggregate_identity)).revision == 1


@pytest.mark.asyncio
async def test_concurrent_second_transition_fails_strict_revision_cas(store, redis_client) -> None:
    admit = _admit_command()
    admit_plan = _accepted_plan(admit, current_revision=0, current_state=None, committed_at_ms=5_000)
    assert (await store.commit(admit_plan)).committed

    dispatch_a = _dispatch_command(operation_id="op-dispatch-a")
    dispatch_b = _dispatch_command(operation_id="op-dispatch-b")
    plan_a = _accepted_plan(dispatch_a, current_revision=1, current_state="ready", committed_at_ms=5_001)
    plan_b = _accepted_plan(dispatch_b, current_revision=1, current_state="ready", committed_at_ms=5_002)

    assert (await store.commit(plan_a)).status is RedisAuthorityCommitStatus.COMMITTED
    stale = await store.commit(plan_b)

    assert stale.status is RedisAuthorityCommitStatus.STALE_REVISION_CONFLICT
    keyspace = store.keyspace(admit.aggregate_identity.sha256)
    assert await redis_client.xlen(keyspace.transition_log) == 2
    assert await redis_client.xlen(keyspace.outbox) == 2
    assert (await store.get_aggregate_snapshot(admit.aggregate_identity)).revision == 2


@pytest.mark.asyncio
async def test_record_without_receipt_fails_closed_without_new_mutation(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=6_000)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.delete(keyspace.receipt(command.operation_id))

    result = await store.commit(plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert await redis_client.exists(keyspace.operation_record(command.operation_id)) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    assert (await store.get_aggregate_snapshot(command.aggregate_identity)).revision == 1


@pytest.mark.asyncio
async def test_wrong_redis_key_type_fails_before_any_authority_write(store, redis_client) -> None:
    command = _admit_command()
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=7_000)
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.set(keyspace.aggregate, "wrong-type")

    result = await store.commit(plan)

    assert result.status is RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT
    assert await redis_client.exists(keyspace.transition_index(plan.record.transition_id)) == 0
    assert await redis_client.exists(keyspace.operation_record(command.operation_id)) == 0
    assert await redis_client.exists(keyspace.receipt(command.operation_id)) == 0
    assert await redis_client.exists(keyspace.transition_log) == 0
    assert await redis_client.exists(keyspace.outbox) == 0


@pytest.mark.asyncio
async def test_conflict_evidence_is_append_only_and_separate_from_lifecycle_revision(store, redis_client) -> None:
    identity = _identity()
    first_id = await store.record_conflict(
        identity,
        conflict_type="IDEMPOTENCY_CONFLICT",
        operation_id="op-conflict",
        observed_at_ms=8_000,
        detail={"reason": "different command hash"},
    )
    second_id = await store.record_conflict(
        identity,
        conflict_type="STALE_REVISION_CONFLICT",
        operation_id="op-stale",
        observed_at_ms=8_001,
        detail={"current_revision": 2, "expected_revision": 1},
    )

    assert first_id != second_id
    keyspace = store.keyspace(identity.sha256)
    assert await redis_client.xlen(keyspace.conflicts) == 2
    assert await store.get_aggregate_snapshot(identity) is None

from __future__ import annotations

import hashlib
import json

import pytest

from hfa.authority import RedisAuthorityCommitStatus, canonical_json_bytes
from hfa.dag.schema import DagRedisKey, DagTaskSeed
from hfa_control.dag_lua import DagLua
from hfa_control.task_admit_authority import (
    TaskAdmitAuthorityBinding,
    TaskAdmitAuthorityConflictError,
    TaskAdmitLegacyStateConflictError,
    TaskAdmitProjectionPendingError,
    build_task_admit_command,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def seed(**changes):
    values = dict(
        task_id="s81-3-task",
        run_id="s81-3-run",
        tenant_id="s81-3-tenant",
        agent_type="default",
        priority=5,
        admitted_at=1000.0,
        dependency_count=0,
        payload_json='{"x":1}',
    )
    values.update(changes)
    return DagTaskSeed(**values)


def _storage_envelope(payload_text: str) -> str:
    return json.dumps(
        {
            "payload": payload_text,
            "storage_sha1": hashlib.sha1(payload_text.encode("utf-8")).hexdigest(),
        },
        separators=(",", ":"),
        sort_keys=True,
    )


async def _add_unexpected_payload_field(redis_client, key: str, field: str) -> str:
    raw = await redis_client.hget(key, field)
    assert raw is not None
    envelope = json.loads(raw)
    payload = json.loads(envelope["payload"])
    payload["unexpected_v5_field"] = "tampered"
    payload_text = canonical_json_bytes(payload).decode("utf-8")
    await redis_client.hset(key, field, _storage_envelope(payload_text))
    return payload_text


async def _replace_stream_tail_field(redis_client, key: str, field: str, value: str) -> None:
    rows = await redis_client.xrevrange(key, count=1)
    assert len(rows) == 1
    row_id, payload = rows[0]
    updated = dict(payload)
    updated[field] = value
    await redis_client.xdel(key, row_id)
    await redis_client.xadd(key, updated)


async def test_flag_off_preserves_legacy_task_admit_and_writes_no_authority_keys(redis_client):
    dag = DagLua(redis_client, canonical_task_admit_binding=False)
    item = seed()
    result = await dag.task_admit(item)
    assert result.admitted is True
    command = build_task_admit_command(item)
    assert not await redis_client.exists(f"hfa:authority:v1:{{{command.aggregate_identity.sha256}}}:aggregate")


async def test_flag_on_root_admit_writes_one_canonical_revision_and_legacy_projection(redis_client):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed()
    result = await dag.task_admit(item)
    assert result.admitted and result.ready
    binding = dag._task_admit_authority_binding
    assert binding is not None
    command = build_task_admit_command(item)
    snapshot = await binding.store.get_aggregate_snapshot(command.aggregate_identity)
    assert snapshot is not None and snapshot.revision == 1 and snapshot.state == "ready"
    assert await redis_client.get(DagRedisKey.task_state(item.task_id)) == "ready"


async def test_flag_on_waiting_admit_writes_one_revision_without_ready_projection(redis_client):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id="s81-3-wait", dependency_count=2)
    result = await dag.task_admit(item)
    assert result.admitted and not result.ready
    assert await redis_client.zscore(DagRedisKey.task_ready_queue(item.tenant_id), item.task_id) is None


async def test_same_seed_retry_is_canonical_duplicate_and_legacy_idempotent(redis_client):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id="s81-3-retry")
    first = await dag.task_admit(item)
    second = await dag.task_admit(item)
    assert first.status == "seeded_root"
    assert second.status == "already_exists"
    binding = dag._task_admit_authority_binding
    command = build_task_admit_command(item)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    assert await redis_client.hlen(keyspace.operation_records) == 1
    assert await redis_client.hlen(keyspace.receipts) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1


async def test_changed_payload_same_identity_conflicts_without_legacy_mutation(redis_client):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    original = seed(task_id="s81-3-conflict")
    await dag.task_admit(original)
    before = await redis_client.hgetall(DagRedisKey.task_meta(original.task_id))
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await dag.task_admit(seed(task_id=original.task_id, payload_json='{"x":2}'))
    assert raised.value.status == "IDEMPOTENCY_CONFLICT"
    assert await redis_client.hgetall(DagRedisKey.task_meta(original.task_id)) == before


async def test_preexisting_legacy_task_without_canonical_record_is_blocked(redis_client):
    item = seed(task_id="s81-3-legacy")
    await redis_client.set(DagRedisKey.task_state(item.task_id), "ready")
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    with pytest.raises(TaskAdmitLegacyStateConflictError):
        await dag.task_admit(item)


async def test_projection_failure_leaves_commit_and_retry_repairs_projection(redis_client, monkeypatch):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id="s81-3-repair")
    binding = TaskAdmitAuthorityBinding(redis_client, dag._task_admit_canonical_projection)
    dag._task_admit_authority_binding = binding
    original = binding.legacy_admit
    calls = 0

    async def flaky(value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("projection failed")
        return await original(value)

    monkeypatch.setattr(binding, "legacy_admit", flaky)
    with pytest.raises(TaskAdmitProjectionPendingError):
        await dag.task_admit(item)
    result = await dag.task_admit(item)
    assert result.admitted is True
    binding = dag._task_admit_authority_binding
    command = build_task_admit_command(item)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    assert await redis_client.hlen(keyspace.operation_records) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1

async def test_concurrent_same_seed_creates_exactly_one_canonical_record(redis_client):
    import asyncio

    item = seed(task_id="s81-3-concurrent")
    first = DagLua(redis_client, canonical_task_admit_binding=True)
    second = DagLua(redis_client, canonical_task_admit_binding=True)
    results = await asyncio.gather(first.task_admit(item), second.task_admit(item))
    assert all(result.admitted for result in results)
    binding = first._task_admit_authority_binding
    command = build_task_admit_command(item)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    assert await redis_client.hlen(keyspace.operation_records) == 1
    assert await redis_client.hlen(keyspace.receipts) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1


async def test_canonical_conflict_never_invokes_legacy_task_admit(redis_client, monkeypatch):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id="s81-3-no-legacy-on-conflict")
    await dag.task_admit(item)
    invoked = False

    async def forbidden(_seed):
        nonlocal invoked
        invoked = True
        raise AssertionError("legacy projection must not run")

    binding = dag._task_admit_authority_binding
    assert binding is not None
    monkeypatch.setattr(binding, "legacy_admit", forbidden)
    with pytest.raises(TaskAdmitAuthorityConflictError):
        await dag.task_admit(seed(task_id=item.task_id, payload_json='{"changed":true}'))
    assert invoked is False


async def test_production_scheduler_composition_injects_flag_once(monkeypatch):
    from hfa_control.models import ControlPlaneConfig
    from hfa_control.scheduler import build_production_scheduler

    calls = 0

    def getenv(name, default=None):
        nonlocal calls
        if name == "HFA_CANONICAL_TASK_ADMIT_BINDING":
            calls += 1
            return "true"
        return default

    class RedisStub:
        pass

    class RegistryStub:
        async def list_all_workers(self, region=None):
            return []

    class ShardsStub:
        async def shard_for_group(self, worker_group, run_id):
            return 0

    monkeypatch.setattr("hfa_control.scheduler.os.getenv", getenv)
    scheduler = build_production_scheduler(
        redis=RedisStub(),
        config=ControlPlaneConfig(
            instance_id="s81-3-composition",
            scheduler_reservation_ttl_seconds=30,
        ),
        registry=RegistryStub(),
        shards=ShardsStub(),
    )
    assert scheduler.composition is not None
    dag = scheduler.composition.dag_lua
    assert dag._canonical_task_admit_binding_enabled is True
    assert calls == 1

async def test_changed_payload_conflict_writes_durable_authority_evidence(redis_client):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    original = seed(task_id="s81-3-durable-policy-conflict")
    await dag.task_admit(original)
    binding = dag._task_admit_authority_binding
    command = build_task_admit_command(original)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await dag.task_admit(seed(task_id=original.task_id, payload_json='{"changed":true}'))
    assert raised.value.status == "IDEMPOTENCY_CONFLICT"
    assert int(await redis_client.hget(keyspace.conflict_index, "__authority_conflict_count")) == 1
    assert await redis_client.xlen(keyspace.conflicts) == 1
    assert await redis_client.hlen(keyspace.operation_records) == 1
    assert await redis_client.hlen(keyspace.receipts) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1


async def test_missing_snapshot_with_valid_receipt_blocks_legacy_projection(redis_client, monkeypatch):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id="s81-3-missing-snapshot")
    await dag.task_admit(item)
    binding = dag._task_admit_authority_binding
    command = build_task_admit_command(item)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    await redis_client.delete(keyspace.aggregate)
    invoked = False

    async def forbidden(_seed):
        nonlocal invoked
        invoked = True
        raise AssertionError("legacy projection must not run")

    binding = dag._task_admit_authority_binding
    assert binding is not None
    monkeypatch.setattr(binding, "legacy_admit", forbidden)
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await dag.task_admit(item)
    assert raised.value.status == "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    assert invoked is False
    assert int(await redis_client.hget(keyspace.conflict_index, "__authority_conflict_count")) == 1


@pytest.mark.parametrize("footprint", ["meta", "remaining", "children", "ready_emitted", "run_member", "ready_queue"])
async def test_any_preexisting_task_legacy_footprint_blocks_silent_backfill(redis_client, footprint):
    item = seed(task_id=f"s81-3-footprint-{footprint}")
    if footprint == "meta":
        await redis_client.hset(DagRedisKey.task_meta(item.task_id), mapping={"run_id": item.run_id})
    elif footprint == "remaining":
        await redis_client.set(DagRedisKey.task_remaining_deps(item.task_id), 1)
    elif footprint == "children":
        await redis_client.sadd(DagRedisKey.task_children(item.task_id), "child")
    elif footprint == "ready_emitted":
        await redis_client.set(DagRedisKey.task_ready_emitted(item.task_id), 1)
    elif footprint == "run_member":
        await redis_client.sadd(DagRedisKey.run_tasks(item.run_id), item.task_id)
    else:
        await redis_client.zadd(DagRedisKey.task_ready_queue(item.tenant_id), {item.task_id: 1})
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    with pytest.raises(TaskAdmitLegacyStateConflictError):
        await dag.task_admit(item)

@pytest.mark.parametrize("stream_name", ["transition_log", "outbox"])
async def test_duplicate_retry_blocks_when_authority_tail_is_deleted(redis_client, monkeypatch, stream_name):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id=f"s81-3-corrupt-{stream_name}")
    await dag.task_admit(item)
    binding = dag._task_admit_authority_binding
    command = build_task_admit_command(item)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    stream_key = getattr(keyspace, stream_name)
    rows = await redis_client.xrange(stream_key)
    assert len(rows) == 1
    await redis_client.xdel(stream_key, rows[0][0])
    invoked = False

    async def forbidden(_seed):
        nonlocal invoked
        invoked = True
        raise AssertionError("legacy projection must not run")

    monkeypatch.setattr(binding, "legacy_admit", forbidden)
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await dag.task_admit(item)
    assert raised.value.status == "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    assert invoked is False


@pytest.mark.parametrize("key_name", ["aggregate", "receipts", "operation_records", "transition_indexes"])
async def test_wrong_type_authority_read_is_structured_and_blocks_projection(redis_client, key_name):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id=f"s81-3-wrong-type-{key_name}")
    command = build_task_admit_command(item)
    keyspace = dag._task_admit_authority_binding.store.keyspace(command.aggregate_identity.sha256) if dag._task_admit_authority_binding else None
    if keyspace is None:
        from hfa.authority import RedisCanonicalAuthorityStore
        keyspace = RedisCanonicalAuthorityStore(redis_client).keyspace(command.aggregate_identity.sha256)
    await redis_client.set(getattr(keyspace, key_name), "wrong-type")
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await dag.task_admit(item)
    assert raised.value.status == "CANONICAL_RECORD_CORRUPTION_CONFLICT"

async def test_exact_head_hash_changes_between_evaluation_and_gate_is_blocked(redis_client, monkeypatch):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id="s81-3-exact-head-race")
    await dag.task_admit(item)
    binding = dag._task_admit_authority_binding
    assert binding is not None
    command = build_task_admit_command(item)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    original_validate = binding.store.validate_authority_head
    invoked = False

    async def mutate_then_validate(identity, **expected):
        await redis_client.hset(keyspace.aggregate, "canonical_command_hash", "f" * 64)
        return await original_validate(identity, **expected)

    async def forbidden(_seed):
        nonlocal invoked
        invoked = True
        raise AssertionError("legacy projection must not run")

    monkeypatch.setattr(binding.store, "validate_authority_head", mutate_then_validate)
    monkeypatch.setattr(binding, "legacy_admit", forbidden)
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await dag.task_admit(item)
    assert raised.value.status == "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    assert invoked is False


@pytest.mark.parametrize(
    "proof_kind",
    ["operation_record", "receipt", "transition_index"],
)
async def test_exact_head_payload_tamper_between_evaluation_and_gate_is_blocked(
    redis_client, monkeypatch, proof_kind
):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id=f"s81-3-exact-payload-{proof_kind}")
    await dag.task_admit(item)
    binding = dag._task_admit_authority_binding
    assert binding is not None
    command = build_task_admit_command(item)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    operation_digest = keyspace.operation_field(command.operation_id)
    snapshot = await binding.store.get_aggregate_snapshot(command.aggregate_identity)
    assert snapshot is not None
    original_validate = binding.store.validate_authority_head
    invoked = False

    async def mutate_then_validate(identity, **expected):
        if proof_kind == "operation_record":
            tampered = await _add_unexpected_payload_field(
                redis_client, keyspace.operation_records, operation_digest
            )
            await _replace_stream_tail_field(
                redis_client, keyspace.transition_log, "record_json", tampered
            )
        elif proof_kind == "receipt":
            tampered = await _add_unexpected_payload_field(
                redis_client, keyspace.receipts, operation_digest
            )
            await _replace_stream_tail_field(
                redis_client, keyspace.transition_log, "receipt_json", tampered
            )
        else:
            await _add_unexpected_payload_field(
                redis_client, keyspace.transition_indexes, snapshot.transition_id
            )
        return await original_validate(identity, **expected)

    async def forbidden(_seed):
        nonlocal invoked
        invoked = True
        raise AssertionError("legacy projection must not run")

    monkeypatch.setattr(binding.store, "validate_authority_head", mutate_then_validate)
    monkeypatch.setattr(binding, "legacy_admit", forbidden)
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await dag.task_admit(item)
    assert raised.value.status == "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    assert "authority_head_exact_payload_mismatch" in raised.value.detail
    assert invoked is False


@pytest.mark.parametrize(
    "snapshot_field,tampered_value",
    [
        ("projection_intents_json", "[]"),
        ("updated_at_ms", "1001"),
    ],
)
async def test_duplicate_gate_derives_expected_snapshot_from_record(
    redis_client, monkeypatch, snapshot_field, tampered_value
):
    dag = DagLua(redis_client, canonical_task_admit_binding=True)
    item = seed(task_id=f"s81-3-snapshot-{snapshot_field}")
    await dag.task_admit(item)
    binding = dag._task_admit_authority_binding
    assert binding is not None
    command = build_task_admit_command(item)
    keyspace = binding.store.keyspace(command.aggregate_identity.sha256)
    await redis_client.hset(keyspace.aggregate, snapshot_field, tampered_value)
    invoked = False

    async def forbidden(_seed):
        nonlocal invoked
        invoked = True
        raise AssertionError("legacy projection must not run")

    monkeypatch.setattr(binding, "legacy_admit", forbidden)
    with pytest.raises(TaskAdmitAuthorityConflictError) as raised:
        await dag.task_admit(item)
    assert raised.value.status == "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    assert invoked is False

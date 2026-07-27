from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, observed {count}")
    return text.replace(old, new, 1)


# Python adapter hardening.
py_path = ROOT / "hfa-core/src/hfa/authority/redis_persistence.py"
py = py_path.read_text(encoding="utf-8")
py = replace_once(
    py,
    "_MAX_SAFE_INTEGER = 2**53 - 1\n",
    "_MAX_SAFE_INTEGER = 2**53 - 1\n"
    "_AGGREGATE_SNAPSHOT_FIELDS = frozenset({\n"
    "    \"canonical_aggregate_identity_sha256\",\n"
    "    \"revision\",\n"
    "    \"state\",\n"
    "    \"state_is_null\",\n"
    "    \"transition_id\",\n"
    "    \"canonical_record_hash\",\n"
    "    \"canonical_command_hash\",\n"
    "    \"operation_id\",\n"
    "    \"operation_digest\",\n"
    "    \"projection_intents_json\",\n"
    "    \"updated_at_ms\",\n"
    "})\n",
    "snapshot field constant",
)
py = replace_once(
    py,
    '''            record = _record_from_payload(record_payload)\n            receipt = _receipt_from_payload(receipt_payload)\n            raw_index = await self._redis.hget(\n                keyspace.transition_indexes,\n                keyspace.transition_field(record.transition_id),\n            )\n''',
    '''            record = _record_from_payload(record_payload)\n            receipt = _receipt_from_payload(receipt_payload)\n            index_kind = _as_text(await self._redis.type(keyspace.transition_indexes))\n            if index_kind not in {"none", "hash"}:\n                return _StoredProofPrevalidation(\n                    "INVALID",\n                    record_sha1,\n                    receipt_sha1,\n                )\n            raw_index = await self._redis.hget(\n                keyspace.transition_indexes,\n                keyspace.transition_field(record.transition_id),\n            )\n''',
    "type-aware transition index prevalidation",
)
py = replace_once(
    py,
    '''        field = keyspace.operation_field(operation_id)\n        raw_receipt = await self._redis.hget(keyspace.receipts, field)\n        raw_record = await self._redis.hget(keyspace.operation_records, field)\n        return await self._prevalidate_raw_proof(\n''',
    '''        field = keyspace.operation_field(operation_id)\n        receipt_kind = _as_text(await self._redis.type(keyspace.receipts))\n        record_kind = _as_text(await self._redis.type(keyspace.operation_records))\n        if receipt_kind not in {"none", "hash"} or record_kind not in {"none", "hash"}:\n            # Do not issue HGET against a wrong-type proof key. Lua owns the\n            # atomic key-type decision and durable corruption evidence.\n            return _StoredProofPrevalidation("ABSENT")\n        raw_receipt = await self._redis.hget(keyspace.receipts, field)\n        raw_record = await self._redis.hget(keyspace.operation_records, field)\n        return await self._prevalidate_raw_proof(\n''',
    "type-aware operation proof prevalidation",
)
py = replace_once(
    py,
    '''        data = {_as_text(key): _as_text(value) for key, value in raw_snapshot.items()}\n        operation_id = data.get("operation_id", "")\n''',
    '''        data = {_as_text(key): _as_text(value) for key, value in raw_snapshot.items()}\n        if set(data) != _AGGREGATE_SNAPSHOT_FIELDS:\n            return _StoredProofPrevalidation("INVALID")\n        operation_id = data.get("operation_id", "")\n''',
    "head exact snapshot fields",
)
py = replace_once(
    py,
    '''        raw_receipt = await self._redis.hget(keyspace.receipts, operation_digest)\n        raw_record = await self._redis.hget(keyspace.operation_records, operation_digest)\n        raw_index = await self._redis.hget(keyspace.transition_indexes, transition_id)\n''',
    '''        proof_kinds = {\n            _as_text(await self._redis.type(keyspace.receipts)),\n            _as_text(await self._redis.type(keyspace.operation_records)),\n            _as_text(await self._redis.type(keyspace.transition_indexes)),\n        }\n        if not proof_kinds.issubset({"none", "hash"}):\n            return _StoredProofPrevalidation("INVALID")\n        raw_receipt = await self._redis.hget(keyspace.receipts, operation_digest)\n        raw_record = await self._redis.hget(keyspace.operation_records, operation_digest)\n        raw_index = await self._redis.hget(keyspace.transition_indexes, transition_id)\n''',
    "head proof key type gate",
)
py = replace_once(
    py,
    '''        expected_fields = {\n            "canonical_aggregate_identity_sha256",\n            "revision",\n            "state",\n            "state_is_null",\n            "transition_id",\n            "canonical_record_hash",\n            "canonical_command_hash",\n            "operation_id",\n            "operation_digest",\n            "projection_intents_json",\n            "updated_at_ms",\n        }\n''',
    '''        expected_fields = _AGGREGATE_SNAPSHOT_FIELDS\n''',
    "shared read snapshot fields",
)
py_path.write_text(py, encoding="utf-8")


# Lua atomic writer hardening.
lua_path = ROOT / "hfa-core/src/hfa/lua/canonical_authority_commit.lua"
lua = lua_path.read_text(encoding="utf-8")
lua = replace_once(
    lua,
    '''    elseif redis.call("HGET", KEYS[7], conflict_id) ~= payload then\n        return conflict_store_unavailable("authority_conflict_index_payload_mismatch")\n    end\n''',
    '''    else\n        -- Conflict identity excludes observation metadata. Repeated observations\n        -- compare only fields bound into conflict_id; the first payload wins.\n        local stored_payload = decode_object(redis.call("HGET", KEYS[7], conflict_id))\n        local expected_stored_hash = stored_command_hash or cjson.null\n        if not stored_payload\n            or stored_payload.conflict_id ~= conflict_id\n            or stored_payload.conflict_type ~= conflict_type\n            or stored_payload.canonical_aggregate_identity_sha256 ~= identity_sha\n            or stored_payload.operation_id ~= operation_id\n            or stored_payload.operation_digest ~= operation_digest\n            or stored_payload.incoming_command_hash ~= canonical_command_hash\n            or stored_payload.stored_command_hash ~= expected_stored_hash then\n            return conflict_store_unavailable("authority_conflict_index_identity_mismatch")\n        end\n    end\n''',
    "conflict identity-bound comparison",
)
lua = replace_once(
    lua,
    '''if aggregate_exists == 1 then\n    if current_revision < 1\n''',
    '''if aggregate_exists == 1 then\n    if redis.call("HLEN", KEYS[1]) ~= 11 then\n        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "aggregate_snapshot_field_count_mismatch")\n    end\n    if current_revision < 1\n''',
    "aggregate exact field count",
)
lua_path.write_text(lua, encoding="utf-8")


# Regression coverage.
test_path = ROOT / "hfa-core/tests/authority/test_redis_canonical_authority.py"
tests = test_path.read_text(encoding="utf-8")
append = r'''

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
'''
if "test_wrong_type_operation_proof_hash_reaches_lua_fail_closed_gate" in tests:
    raise SystemExit("final regressions already present")
test_path.write_text(tests.rstrip() + "\n" + append.lstrip(), encoding="utf-8")


# Scope documentation.
doc_path = ROOT / "docs/implementation/sprint81/SPRINT81.2-canonical-authority-persistence.md"
doc = doc_path.read_text(encoding="utf-8")
doc = replace_once(
    doc,
    '''Lua compares SHA-1 digests of the exact Redis storage envelopes observed during\nthat validation. A concurrent proof change causes an internal bounded retry,\nnot an unvalidated authority decision.\n''',
    '''Python checks proof-key Redis types before issuing any `HGET`. Wrong-type\nreceipt, operation-record or transition-index keys are deliberately left for\nLua's atomic key-type gate, which returns an evidenced canonical corruption\nwithout leaking a raw Redis `WRONGTYPE` exception. Lua also compares SHA-1\ndigests of the exact Redis storage envelopes observed during canonical\nvalidation. A concurrent proof change causes an internal bounded retry, not an\nunvalidated authority decision.\n''',
    "document proof key contract",
)
doc = replace_once(
    doc,
    '''`get_aggregate_snapshot()` exact-validates:\n''',
    '''The commit path and `get_aggregate_snapshot()` share the same exact eleven-field\naggregate snapshot schema. Existing snapshots with missing or unexpected fields\nfail closed before a later lifecycle write.\n\n`get_aggregate_snapshot()` exact-validates:\n''',
    "document write/read snapshot parity",
)
doc = replace_once(
    doc,
    '''The authority-conflict index contains a reserved monotonic evidence-count field.\nBefore every decision, index cardinality and stream length must equal that count.\nIf the pair is incomplete or has an invalid Redis type, the script returns\n''',
    '''The authority-conflict index contains a reserved monotonic evidence-count field.\nConflict identity binds aggregate identity, operation ID, incoming/stored command\nhashes and conflict type. Observation time, detail text and existing-transition\nmetadata are not identity fields: the first stored payload wins and repeated\nlogical observations compare only identity-bound fields. Before every decision,\nindex cardinality and stream length must equal the evidence count. If the pair is\nincomplete or has an invalid Redis type, the script returns\n''',
    "document conflict identity contract",
)
doc_path.write_text(doc, encoding="utf-8")

print("SPRINT81_2_FINAL_PROOF_SNAPSHOT_PATCH_APPLIED")

from __future__ import annotations

from pathlib import Path


ROOT = Path(".")
REDIS = ROOT / "hfa-core/src/hfa/authority/redis_persistence.py"
LUA = ROOT / "hfa-core/src/hfa/lua/canonical_authority_commit.lua"
TESTS = ROOT / "hfa-core/tests/authority/test_redis_canonical_authority.py"
DOC = ROOT / "docs/implementation/sprint81/SPRINT81.2-canonical-authority-persistence.md"


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:120]!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    REDIS,
    '''    CANONICAL_RECORD_CORRUPTION_CONFLICT = "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    INVALID_COMMIT_PLAN = "INVALID_COMMIT_PLAN"
''',
    '''    CANONICAL_RECORD_CORRUPTION_CONFLICT = "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    CONFLICT_EVIDENCE_STORE_UNAVAILABLE = "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"
    INVALID_COMMIT_PLAN = "INVALID_COMMIT_PLAN"
''',
)

replace_once(
    LUA,
    '''-- Conflict storage must itself be healthy before any outcome that requires it.
if not key_type_ok(KEYS[7], "hash") or not key_type_ok(KEYS[8], "stream") then
    return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, "conflict_store_unavailable")
end
''',
    '''-- Conflict storage failure is not reported as an evidenced canonical conflict.
-- It is a separate fail-closed operational result because durable evidence cannot
-- be guaranteed while either conflict store has the wrong Redis type.
local conflict_index_type = redis_type(KEYS[7])
local conflict_stream_type = redis_type(KEYS[8])
if conflict_index_type ~= "none" and conflict_index_type ~= "hash" then
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "conflict_index_type_mismatch")
end
if conflict_stream_type ~= "none" and conflict_stream_type ~= "stream" then
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "conflict_stream_type_mismatch")
end
''',
)

replace_once(
    LUA,
    '''    if proof.receipt.canonical_command_hash == canonical_command_hash then
        if proof.record_payload ~= record_json
            or proof.receipt_payload ~= receipt_json
            or proof.index_payload ~= transition_index_json then
            return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", proof.receipt.canonical_command_hash, proof.receipt.transition_id, proof.receipt.aggregate_revision, "exact_duplicate_payload_mismatch")
        end
        return result("ALREADY_APPLIED", proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
    end
''',
    '''    -- Model B: the first fully validated persisted record wins. The canonical
    -- command hash binds all requested authoritative effects. Writer-generated
    -- commit metadata may differ across concurrent independent evaluations.
    if proof.receipt.canonical_command_hash == canonical_command_hash then
        return result("ALREADY_APPLIED", proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
    end
''',
)

replace_once(
    LUA,
    '''    if not outbox_tail
        or outbox_tail.aggregate_revision ~= tostring(current_revision)
        or outbox_tail.transition_id ~= stored_transition_id
        or outbox_tail.canonical_record_hash ~= stored_record_hash
        or outbox_tail.operation_id ~= stored_operation_id
        or outbox_tail.operation_digest ~= stored_operation_digest
        or outbox_tail.projection_intents_json ~= stored_projection_intents_json then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "outbox_tail_mismatch")
    end
end
''',
    '''    if not outbox_tail
        or outbox_tail.aggregate_revision ~= tostring(current_revision)
        or outbox_tail.transition_id ~= stored_transition_id
        or outbox_tail.canonical_record_hash ~= stored_record_hash
        or outbox_tail.operation_id ~= stored_operation_id
        or outbox_tail.operation_digest ~= stored_operation_digest
        or outbox_tail.projection_intents_json ~= stored_projection_intents_json then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "outbox_tail_mismatch")
    end

    -- Every accepted revision contributes exactly one index, receipt, record,
    -- transition-log entry and outbox entry. This detects deletion or insertion
    -- anywhere in the persisted history, not only corruption of the current tail.
    if redis.call("HLEN", KEYS[2]) ~= current_revision
        or redis.call("HLEN", KEYS[3]) ~= current_revision
        or redis.call("HLEN", KEYS[4]) ~= current_revision
        or redis.call("XLEN", KEYS[5]) ~= current_revision
        or redis.call("XLEN", KEYS[6]) ~= current_revision then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "historical_cardinality_mismatch")
    end
end
''',
)

tests_text = TESTS.read_text(encoding="utf-8")
marker = "\n# Sprint 81.2 final re-review regressions\n"
if marker in tests_text:
    raise SystemExit("final re-review regression block already exists")
tests_text += r'''

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
async def test_concurrent_same_command_with_different_commit_timestamps_is_already_applied(
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

    first = await store.commit(first_plan)
    second = await store.commit(second_plan)

    assert first.status is RedisAuthorityCommitStatus.COMMITTED
    assert second.status is RedisAuthorityCommitStatus.ALREADY_APPLIED
    assert second.transition_id == first_plan.record.transition_id
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    assert await redis_client.hlen(keyspace.transition_indexes) == 1
    assert await redis_client.hlen(keyspace.receipts) == 1
    assert await redis_client.hlen(keyspace.operation_records) == 1
    assert await redis_client.xlen(keyspace.transition_log) == 1
    assert await redis_client.xlen(keyspace.outbox) == 1
    assert await redis_client.xlen(keyspace.conflicts) == 0
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
async def test_conflict_index_present_and_stream_missing_can_emit_first_conflict(
    store,
    redis_client,
) -> None:
    command = _admit_command(operation_id="index-present-stream-missing", payload_version=1)
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=10_200)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.hset(keyspace.conflict_index, "preexisting-audit", "{}")
    assert await redis_client.exists(keyspace.conflicts) == 0

    conflicting = _admit_command(operation_id=command.operation_id, payload_version=2)
    conflicting_plan = _accepted_plan(
        conflicting,
        current_revision=0,
        current_state=None,
        committed_at_ms=10_201,
    )
    result = await store.commit(conflicting_plan)

    assert result.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    assert await redis_client.hlen(keyspace.conflict_index) == 2
    assert await redis_client.xlen(keyspace.conflicts) == 1
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


@pytest.mark.asyncio
async def test_conflict_stream_present_and_index_missing_can_emit_first_conflict(
    store,
    redis_client,
) -> None:
    command = _admit_command(operation_id="stream-present-index-missing", payload_version=1)
    plan = _accepted_plan(command, current_revision=0, current_state=None, committed_at_ms=10_300)
    assert (await store.commit(plan)).committed
    keyspace = store.keyspace(command.aggregate_identity.sha256)
    await redis_client.xadd(keyspace.conflicts, {"origin": "preexisting-audit"})
    assert await redis_client.exists(keyspace.conflict_index) == 0

    conflicting = _admit_command(operation_id=command.operation_id, payload_version=2)
    conflicting_plan = _accepted_plan(
        conflicting,
        current_revision=0,
        current_state=None,
        committed_at_ms=10_301,
    )
    result = await store.commit(conflicting_plan)

    assert result.status is RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT
    assert await redis_client.hlen(keyspace.conflict_index) == 1
    assert await redis_client.xlen(keyspace.conflicts) == 2
    await _assert_no_new_lifecycle(redis_client, keyspace, revision=1, log_len=1, outbox_len=1)


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
'''
TESTS.write_text(tests_text, encoding="utf-8")

replace_once(
    DOC,
    '''For an exact duplicate, the stored record, receipt and index payload bytes must
also equal the incoming payload bytes.

```yaml
same_operation_same_command_and_exact_payloads: ALREADY_APPLIED
same_operation_different_command: IDEMPOTENCY_CONFLICT
stored_proof_missing_tampered_or_inconsistent: CANONICAL_RECORD_CORRUPTION_CONFLICT
```
''',
    '''Sprint 81.2 selects **Model B — first persisted record wins**. Once the
stored receipt, record and transition index have independently passed storage
and semantic validation, canonical command hash equality is sufficient for
`ALREADY_APPLIED`. Writer-generated commit metadata such as `committed_at_ms`
may differ across concurrent independent evaluations.

```yaml
same_operation_same_canonical_command_hash: ALREADY_APPLIED
same_operation_different_command: IDEMPOTENCY_CONFLICT
stored_proof_missing_tampered_or_inconsistent: CANONICAL_RECORD_CORRUPTION_CONFLICT
```
''',
)

replace_once(
    DOC,
    '''The stream tails must match the aggregate revision, transition ID, record hash,
operation identity and stored payloads. Missing streams, missing proof objects,
or mismatched tails fail closed before any new lifecycle write.
''',
    '''The stream tails must match the aggregate revision, transition ID, record hash,
operation identity and stored payloads. The transition-index, receipt and record
hash cardinalities and both stream lengths must also equal the current revision.
Deletion or insertion anywhere in those persisted collections therefore fails
closed before any new lifecycle write.

```yaml
current_head_continuity: PROVEN
historical_cardinality_continuity: PROVEN
full_historical_content_rehash: NOT_YET_PROVEN
historical_content_tamper_detection: DEFERRED_TO_SPRINT_85
```

Sprint 81.2 does not claim that every non-head historical payload is rehashed on
every commit. Full replay reconstruction and historical content verification
remain a separately reviewed Sprint 85 responsibility.
''',
)

replace_once(
    DOC,
    '''The Python `record_conflict()` method remains only for explicit operator audit
notes. It is not used to complete a Lua conflict decision after the fact.
''',
    '''If either conflict store has the wrong Redis type, the script cannot truthfully
claim durable conflict evidence. It returns the distinct fail-closed result
`CONFLICT_EVIDENCE_STORE_UNAVAILABLE`, performs zero lifecycle mutation and
requires operator/reconciliation handling.

The Python `record_conflict()` method remains only for explicit operator audit
notes. It is not used to complete a Lua conflict decision after the fact.
''',
)

replace_once(
    DOC,
    '''  durable_conflict_atomicity_and_deduplication: PASS
  receipt_probe_lookup_binding: PASS
  snapshot_exact_validation: PASS
''',
    '''  durable_conflict_atomicity_and_deduplication: PASS
  concurrent_same_command_different_commit_metadata: PASS
  conflict_store_unavailable_result: PASS
  old_revision_proof_deletion: PASS
  old_stream_entry_deletion: PASS
  receipt_probe_lookup_binding: PASS
  snapshot_exact_validation: PASS
''',
)

replace_once(
    DOC,
    '''trusted_runtime_adapter_implemented: false
historical_revision_synthesis: false
legacy_key_migration: false
''',
    '''trusted_runtime_adapter_implemented: false
operation_digest_trust_boundary:
  issued_and_recomputed_by_Python_adapter: true
  direct_Lua_invocation: FORBIDDEN
  runtime_wiring_before_independent_binding_review: FORBIDDEN
historical_revision_synthesis: false
full_historical_content_rehash: false
legacy_key_migration: false
''',
)

print("SPRINT81_2_FINAL_REVIEW_PATCH_APPLIED")

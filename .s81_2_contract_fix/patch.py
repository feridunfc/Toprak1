from __future__ import annotations

from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, observed {count}")
    return text.replace(old, new, 1)


root = Path(__file__).resolve().parents[1]
py_path = root / "hfa-core/src/hfa/authority/redis_persistence.py"
lua_path = root / "hfa-core/src/hfa/lua/canonical_authority_commit.lua"
test_path = root / "hfa-core/tests/authority/test_redis_canonical_authority.py"
doc_path = root / "docs/implementation/sprint81/SPRINT81.2-canonical-authority-persistence.md"

# ---------------------------------------------------------------------------
# Python adapter: canonical stored-proof prevalidation, retry binding and
# operator-audit separation.
# ---------------------------------------------------------------------------
py = py_path.read_text(encoding="utf-8")
py = replace_once(
    py,
    '    CONFLICT_EVIDENCE_STORE_UNAVAILABLE = "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"\n    INVALID_COMMIT_PLAN = "INVALID_COMMIT_PLAN"',
    '    CONFLICT_EVIDENCE_STORE_UNAVAILABLE = "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"\n'
    '    PREVALIDATION_RETRY_REQUIRED = "PREVALIDATION_RETRY_REQUIRED"\n'
    '    INVALID_COMMIT_PLAN = "INVALID_COMMIT_PLAN"',
    "commit status enum",
)
py = replace_once(
    py,
    '    @property\n    def conflicts(self) -> str:\n        return f"{self.namespace}:{self.hash_tag}:conflicts"\n\n    def operation_field',
    '    @property\n    def conflicts(self) -> str:\n        return f"{self.namespace}:{self.hash_tag}:conflicts"\n\n'
    '    @property\n    def operator_audits(self) -> str:\n        return f"{self.namespace}:{self.hash_tag}:operator-audits"\n\n'
    '    def operation_field',
    "operator audit key",
)
py = replace_once(
    py,
    'def _operation_digest(operation_id: str) -> str:\n'
    '    value = _nonempty(operation_id, "operation_id")\n'
    '    encoded = value.encode("utf-8")\n'
    '    return hashlib.sha256(len(encoded).to_bytes(8, "big") + encoded).hexdigest()\n\n\n'
    'def _as_text',
    'def _operation_digest(operation_id: str) -> str:\n'
    '    value = _nonempty(operation_id, "operation_id")\n'
    '    encoded = value.encode("utf-8")\n'
    '    return hashlib.sha256(len(encoded).to_bytes(8, "big") + encoded).hexdigest()\n\n\n'
    '@dataclass(frozen=True)\n'
    'class _StoredProofPrevalidation:\n'
    '    status: str\n'
    '    record_sha1: str = ""\n'
    '    receipt_sha1: str = ""\n'
    '    index_sha1: str = ""\n\n\n'
    'def _raw_sha1(value: Any) -> str:\n'
    '    if value is None:\n'
    '        return ""\n'
    '    return hashlib.sha1(_as_text(value).encode("utf-8")).hexdigest()\n\n\n'
    'def _as_text',
    "prevalidation primitive",
)

old_commit = '''    async def commit(self, plan: _core.AuthorityCommitPlan) -> RedisAuthorityCommitResult:
        _validate_plan(plan)
        record = plan.record
        receipt = plan.receipt
        keyspace = self.keyspace(record.aggregate_identity_sha256)
        record_payload = _record_payload(record)
        receipt_payload = _receipt_payload(receipt)
        transition_index_payload = _transition_index_payload(record)
        projection_intents = record_payload["durable_projection_intents"]
        operation_digest = keyspace.operation_field(record.operation_id)
        is_create = record.operation_type in {
            _core.OperationType.TASK_ADMIT.value,
            _core.OperationType.RUN_CREATE.value,
        }
        raw = await self._commit_loader.run(
            num_keys=8,
            keys=keyspace.commit_keys(),
            args=[
                record.aggregate_identity_sha256,
                str(record.from_revision),
                str(record.to_revision),
                "1" if record.previous_state is None else "0",
                record.previous_state or "",
                "1" if record.next_state is None else "0",
                record.next_state or "",
                record.transition_id,
                record.canonical_record_hash,
                record.canonical_command_hash,
                record.operation_id,
                operation_digest,
                record.operation_type,
                str(record.committed_at_ms),
                _canonical_text(transition_index_payload),
                _canonical_text(record_payload),
                _canonical_text(receipt_payload),
                _canonical_text(projection_intents),
                "1" if is_create else "0",
            ],
        )
        return self._parse_commit_result(raw)
'''
new_commit = '''    async def _prevalidate_raw_proof(
        self,
        keyspace: RedisAuthorityKeyspace,
        *,
        operation_id: str,
        raw_record: Any,
        raw_receipt: Any,
    ) -> _StoredProofPrevalidation:
        record_sha1 = _raw_sha1(raw_record)
        receipt_sha1 = _raw_sha1(raw_receipt)
        if raw_record is None and raw_receipt is None:
            return _StoredProofPrevalidation("ABSENT")
        if raw_record is None or raw_receipt is None:
            return _StoredProofPrevalidation("INVALID", record_sha1, receipt_sha1)
        raw_index: Any = None
        try:
            _, record_payload = _decode_storage_envelope(
                raw_record,
                field_name="canonical operation record",
            )
            _, receipt_payload = _decode_storage_envelope(
                raw_receipt,
                field_name="operation receipt",
            )
            record = _record_from_payload(record_payload)
            receipt = _receipt_from_payload(receipt_payload)
            raw_index = await self._redis.hget(
                keyspace.transition_indexes,
                keyspace.transition_field(record.transition_id),
            )
            if raw_index is None:
                return _StoredProofPrevalidation(
                    "INVALID",
                    record_sha1,
                    receipt_sha1,
                )
            _, index_payload = _decode_storage_envelope(
                raw_index,
                field_name="canonical transition index",
            )
            _validate_transition_index(index_payload, record)
            _core._validate_stored_duplicate_proof(
                _core.ReceiptProbe(
                    receipt=receipt,
                    canonical_store_record=record,
                ),
                lookup_aggregate_identity_sha256=keyspace.canonical_aggregate_identity_sha256,
                lookup_operation_id=operation_id,
            )
        except (
            RedisAuthorityPersistenceError,
            _core.AuthorityContractError,
            TypeError,
            ValueError,
        ):
            return _StoredProofPrevalidation(
                "INVALID",
                record_sha1,
                receipt_sha1,
                _raw_sha1(raw_index),
            )
        return _StoredProofPrevalidation(
            "VALID",
            record_sha1,
            receipt_sha1,
            _raw_sha1(raw_index),
        )

    async def _prevalidate_operation_proof(
        self,
        keyspace: RedisAuthorityKeyspace,
        operation_id: str,
    ) -> _StoredProofPrevalidation:
        field = keyspace.operation_field(operation_id)
        raw_receipt = await self._redis.hget(keyspace.receipts, field)
        raw_record = await self._redis.hget(keyspace.operation_records, field)
        return await self._prevalidate_raw_proof(
            keyspace,
            operation_id=operation_id,
            raw_record=raw_record,
            raw_receipt=raw_receipt,
        )

    async def _prevalidate_head_proof(
        self,
        keyspace: RedisAuthorityKeyspace,
    ) -> _StoredProofPrevalidation:
        raw_snapshot = await self._redis.hgetall(keyspace.aggregate)
        if not raw_snapshot:
            return _StoredProofPrevalidation("ABSENT")
        data = {_as_text(key): _as_text(value) for key, value in raw_snapshot.items()}
        operation_id = data.get("operation_id", "")
        operation_digest = data.get("operation_digest", "")
        if not operation_id or operation_digest != keyspace.operation_field(operation_id):
            return _StoredProofPrevalidation("INVALID")
        raw_receipt = await self._redis.hget(keyspace.receipts, operation_digest)
        raw_record = await self._redis.hget(keyspace.operation_records, operation_digest)
        return await self._prevalidate_raw_proof(
            keyspace,
            operation_id=operation_id,
            raw_record=raw_record,
            raw_receipt=raw_receipt,
        )

    async def commit(self, plan: _core.AuthorityCommitPlan) -> RedisAuthorityCommitResult:
        _validate_plan(plan)
        record = plan.record
        receipt = plan.receipt
        keyspace = self.keyspace(record.aggregate_identity_sha256)
        record_payload = _record_payload(record)
        receipt_payload = _receipt_payload(receipt)
        transition_index_payload = _transition_index_payload(record)
        projection_intents = record_payload["durable_projection_intents"]
        operation_digest = keyspace.operation_field(record.operation_id)
        is_create = record.operation_type in {
            _core.OperationType.TASK_ADMIT.value,
            _core.OperationType.RUN_CREATE.value,
        }
        for _attempt in range(3):
            operation_prevalidation = await self._prevalidate_operation_proof(
                keyspace,
                record.operation_id,
            )
            head_prevalidation = await self._prevalidate_head_proof(keyspace)
            raw = await self._commit_loader.run(
                num_keys=8,
                keys=keyspace.commit_keys(),
                args=[
                    record.aggregate_identity_sha256,
                    str(record.from_revision),
                    str(record.to_revision),
                    "1" if record.previous_state is None else "0",
                    record.previous_state or "",
                    "1" if record.next_state is None else "0",
                    record.next_state or "",
                    record.transition_id,
                    record.canonical_record_hash,
                    record.canonical_command_hash,
                    record.operation_id,
                    operation_digest,
                    record.operation_type,
                    str(record.committed_at_ms),
                    _canonical_text(transition_index_payload),
                    _canonical_text(record_payload),
                    _canonical_text(receipt_payload),
                    _canonical_text(projection_intents),
                    "1" if is_create else "0",
                    operation_prevalidation.status,
                    operation_prevalidation.record_sha1,
                    operation_prevalidation.receipt_sha1,
                    operation_prevalidation.index_sha1,
                    head_prevalidation.status,
                    head_prevalidation.record_sha1,
                    head_prevalidation.receipt_sha1,
                    head_prevalidation.index_sha1,
                ],
            )
            result = self._parse_commit_result(raw)
            if result.status is not RedisAuthorityCommitStatus.PREVALIDATION_RETRY_REQUIRED:
                return result
        raise RedisAuthorityPersistenceError(
            "stored authority proof changed repeatedly during canonical commit"
        )
'''
py = replace_once(py, old_commit, new_commit, "commit method")
py = replace_once(
    py,
    '                self.keyspace(aggregate_identity.sha256).conflicts,',
    '                self.keyspace(aggregate_identity.sha256).operator_audits,',
    "operator audit stream",
)
py_path.write_text(py, encoding="utf-8")

# ---------------------------------------------------------------------------
# Lua: accepted exact-record parity, adapter-bound canonical prevalidation and
# conflict index/stream pair integrity.
# ---------------------------------------------------------------------------
lua = lua_path.read_text(encoding="utf-8")
lua = replace_once(
    lua,
    'local is_create_operation = ARGV[19]\n',
    'local is_create_operation = ARGV[19]\n'
    'local operation_prevalidation_status = ARGV[20]\n'
    'local operation_record_raw_sha1 = ARGV[21]\n'
    'local operation_receipt_raw_sha1 = ARGV[22]\n'
    'local operation_index_raw_sha1 = ARGV[23]\n'
    'local head_prevalidation_status = ARGV[24]\n'
    'local head_record_raw_sha1 = ARGV[25]\n'
    'local head_receipt_raw_sha1 = ARGV[26]\n'
    'local head_index_raw_sha1 = ARGV[27]\n',
    "prevalidation args",
)
lua = replace_once(
    lua,
    '''local function sha1_hex_ok(value)
    return type(value) == "string"
        and string.len(value) == 40
        and string.match(value, "^[0-9a-f]+$") ~= nil
end
''',
    '''local function sha1_hex_ok(value)
    return type(value) == "string"
        and string.len(value) == 40
        and string.match(value, "^[0-9a-f]+$") ~= nil
end

local function raw_sha1(value)
    if type(value) ~= "string" then
        return ""
    end
    return redis.sha1hex(value)
end

local function prevalidation_status_ok(value)
    return value == "ABSENT" or value == "VALID" or value == "INVALID"
end
''',
    "raw digest helpers",
)
old_emit = '''local function emit_conflict(conflict_type, stored_command_hash, existing_transition_id, revision, detail)
    local material = length_prefix(identity_sha)
        .. length_prefix(operation_id)
        .. length_prefix(canonical_command_hash)
        .. length_prefix(stored_command_hash or "")
        .. length_prefix(conflict_type)
    local conflict_id = redis.sha1hex(material)
    local payload = cjson.encode({
        conflict_id=conflict_id,
        conflict_type=conflict_type,
        canonical_aggregate_identity_sha256=identity_sha,
        operation_id=operation_id,
        operation_digest=operation_digest,
        incoming_command_hash=canonical_command_hash,
        stored_command_hash=stored_command_hash or cjson.null,
        existing_transition_id=existing_transition_id or cjson.null,
        observed_at_ms=committed_at_ms,
        detail=detail or ""
    })
    local inserted = redis.call("HSETNX", KEYS[7], conflict_id, payload)
    if inserted == 1 then
        redis.call("XADD", KEYS[8], "*",
            "conflict_id", conflict_id,
            "conflict_type", conflict_type,
            "operation_id", operation_id,
            "operation_digest", operation_digest,
            "incoming_command_hash", canonical_command_hash,
            "stored_command_hash", stored_command_hash or "",
            "existing_transition_id", existing_transition_id or "",
            "observed_at_ms", committed_at_ms_raw,
            "detail", detail or "",
            "conflict_json", payload
        )
    end
    return result(conflict_type, existing_transition_id, revision, detail or "")
end
'''
new_emit = '''local CONFLICT_COUNT_FIELD = "__authority_conflict_count"
local authority_conflict_count = 0

local function conflict_store_unavailable(detail)
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, detail)
end

local function conflict_pair_state()
    local index_kind = redis_type(KEYS[7])
    local stream_kind = redis_type(KEYS[8])
    if index_kind ~= "none" and index_kind ~= "hash" then
        return false, "conflict_index_type_mismatch"
    end
    if stream_kind ~= "none" and stream_kind ~= "stream" then
        return false, "conflict_stream_type_mismatch"
    end
    if (index_kind == "none") ~= (stream_kind == "none") then
        return false, "authority_conflict_pair_missing_member"
    end
    if index_kind == "none" then
        return true, 0
    end
    local count_raw = redis.call("HGET", KEYS[7], CONFLICT_COUNT_FIELD)
    local count = count_raw and tonumber(count_raw) or nil
    if not exact_nonnegative_integer(count) then
        return false, "authority_conflict_count_missing_or_invalid"
    end
    if redis.call("HLEN", KEYS[7]) ~= count + 1
        or redis.call("XLEN", KEYS[8]) ~= count then
        return false, "authority_conflict_pair_cardinality_mismatch"
    end
    return true, count
end

local function emit_conflict(conflict_type, stored_command_hash, existing_transition_id, revision, detail)
    local material = length_prefix(identity_sha)
        .. length_prefix(operation_id)
        .. length_prefix(canonical_command_hash)
        .. length_prefix(stored_command_hash or "")
        .. length_prefix(conflict_type)
    local conflict_id = redis.sha1hex(material)
    local payload = cjson.encode({
        conflict_id=conflict_id,
        conflict_type=conflict_type,
        canonical_aggregate_identity_sha256=identity_sha,
        operation_id=operation_id,
        operation_digest=operation_digest,
        incoming_command_hash=canonical_command_hash,
        stored_command_hash=stored_command_hash or cjson.null,
        existing_transition_id=existing_transition_id or cjson.null,
        observed_at_ms=committed_at_ms,
        detail=detail or ""
    })
    local inserted = redis.call("HSETNX", KEYS[7], conflict_id, payload)
    if inserted == 1 then
        authority_conflict_count = authority_conflict_count + 1
        redis.call("HSET", KEYS[7], CONFLICT_COUNT_FIELD, tostring(authority_conflict_count))
        redis.call("XADD", KEYS[8], "*",
            "conflict_id", conflict_id,
            "conflict_type", conflict_type,
            "operation_id", operation_id,
            "operation_digest", operation_digest,
            "incoming_command_hash", canonical_command_hash,
            "stored_command_hash", stored_command_hash or "",
            "existing_transition_id", existing_transition_id or "",
            "observed_at_ms", committed_at_ms_raw,
            "detail", detail or "",
            "conflict_json", payload
        )
    elseif redis.call("HGET", KEYS[7], conflict_id) ~= payload then
        return conflict_store_unavailable("authority_conflict_index_payload_mismatch")
    end
    return result(conflict_type, existing_transition_id, revision, detail or "")
end
'''
lua = replace_once(lua, old_emit, new_emit, "conflict pair implementation")
lua = replace_once(
    lua,
    '''if not sha256_hex_ok(identity_sha)
    or not sha256_hex_ok(canonical_record_hash)
    or not sha256_hex_ok(canonical_command_hash)
    or not sha256_hex_ok(operation_digest)
    or type(transition_id) ~= "string" or transition_id == ""
    or type(operation_id) ~= "string" or operation_id == ""
    or type(operation_type) ~= "string" or operation_type == "" then
    return result("INVALID_COMMIT_PLAN", nil, nil, "invalid_identity_or_hash_fields")
end

-- Conflict storage must itself be healthy before any outcome that requires it.
if not key_type_ok(KEYS[7], "hash") or not key_type_ok(KEYS[8], "stream") then
    local index_kind = redis_type(KEYS[7])
    local detail = "conflict_stream_type_mismatch"
    if index_kind ~= "none" and index_kind ~= "hash" then
        detail = "conflict_index_type_mismatch"
    end
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, detail)
end
''',
    '''if not sha256_hex_ok(identity_sha)
    or not sha256_hex_ok(canonical_record_hash)
    or not sha256_hex_ok(canonical_command_hash)
    or not sha256_hex_ok(operation_digest)
    or type(transition_id) ~= "string" or transition_id == ""
    or type(operation_id) ~= "string" or operation_id == ""
    or type(operation_type) ~= "string" or operation_type == ""
    or not prevalidation_status_ok(operation_prevalidation_status)
    or not prevalidation_status_ok(head_prevalidation_status) then
    return result("INVALID_COMMIT_PLAN", nil, nil, "invalid_identity_hash_or_prevalidation_fields")
end

local conflict_pair_ok, conflict_pair_value = conflict_pair_state()
if not conflict_pair_ok then
    return conflict_store_unavailable(conflict_pair_value)
end
authority_conflict_count = conflict_pair_value
''',
    "conflict preflight",
)
old_duplicate = '''local existing_receipt_raw = redis.call("HGET", KEYS[3], operation_digest)
local existing_record_raw = redis.call("HGET", KEYS[4], operation_digest)
if existing_receipt_raw or existing_record_raw then
    if not existing_receipt_raw or not existing_record_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_receipt_proof_incomplete")
    end
    local pre_record_payload, pre_record = decode_storage_envelope(existing_record_raw)
    if not pre_record_payload or not pre_record or type(pre_record.transition_id) ~= "string" then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_record_envelope_invalid")
    end
    local existing_index_raw = redis.call("HGET", KEYS[2], pre_record.transition_id)
    if not existing_index_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", pre_record.canonical_command_hash, pre_record.transition_id, pre_record.to_revision, "stored_transition_index_missing")
    end
    local proof, proof_error = validate_proof(
        existing_record_raw,
        existing_receipt_raw,
        existing_index_raw,
        identity_sha,
        operation_id
    )
    if not proof then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", pre_record.canonical_command_hash, pre_record.transition_id, pre_record.to_revision, proof_error)
    end
    if proof.receipt.canonical_command_hash == canonical_command_hash then
        return result("ALREADY_APPLIED", proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
    end
    return emit_conflict("IDEMPOTENCY_CONFLICT", proof.receipt.canonical_command_hash, proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
end
'''
new_duplicate = '''local existing_receipt_raw = redis.call("HGET", KEYS[3], operation_digest)
local existing_record_raw = redis.call("HGET", KEYS[4], operation_digest)
if existing_receipt_raw or existing_record_raw then
    if operation_prevalidation_status == "ABSENT" then
        return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "operation_proof_appeared")
    end
    if raw_sha1(existing_record_raw) ~= operation_record_raw_sha1
        or raw_sha1(existing_receipt_raw) ~= operation_receipt_raw_sha1 then
        return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "operation_proof_changed")
    end
    if operation_prevalidation_status == "INVALID" then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_proof_canonical_validation_failed")
    end
    if operation_prevalidation_status ~= "VALID" then
        return result("INVALID_COMMIT_PLAN", nil, nil, "operation_prevalidation_status_invalid")
    end
    if not existing_receipt_raw or not existing_record_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_receipt_proof_incomplete")
    end
    local pre_record_payload, pre_record = decode_storage_envelope(existing_record_raw)
    if not pre_record_payload or not pre_record or type(pre_record.transition_id) ~= "string" then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_record_envelope_invalid")
    end
    local existing_index_raw = redis.call("HGET", KEYS[2], pre_record.transition_id)
    if raw_sha1(existing_index_raw) ~= operation_index_raw_sha1 then
        return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "operation_index_changed")
    end
    if not existing_index_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", pre_record.canonical_command_hash, pre_record.transition_id, pre_record.to_revision, "stored_transition_index_missing")
    end
    local proof, proof_error = validate_proof(
        existing_record_raw,
        existing_receipt_raw,
        existing_index_raw,
        identity_sha,
        operation_id
    )
    if not proof then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", pre_record.canonical_command_hash, pre_record.transition_id, pre_record.to_revision, proof_error)
    end
    if proof.receipt.canonical_command_hash == canonical_command_hash then
        if proof.record_payload == record_json
            and proof.receipt_payload == receipt_json
            and proof.index_payload == transition_index_json then
            return result("ALREADY_APPLIED", proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
        end
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", proof.receipt.canonical_command_hash, proof.receipt.transition_id, proof.receipt.aggregate_revision, "exact_duplicate_payload_mismatch")
    end
    return emit_conflict("IDEMPOTENCY_CONFLICT", proof.receipt.canonical_command_hash, proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
elseif operation_prevalidation_status ~= "ABSENT" then
    return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "operation_proof_disappeared")
end
'''
lua = replace_once(lua, old_duplicate, new_duplicate, "receipt-first parity block")
lua = replace_once(
    lua,
    '''local aggregate_exists = redis.call("EXISTS", KEYS[1])
local current_revision_raw = redis.call("HGET", KEYS[1], "revision")
''',
    '''local aggregate_exists = redis.call("EXISTS", KEYS[1])
if aggregate_exists == 0 and head_prevalidation_status ~= "ABSENT" then
    return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "aggregate_head_disappeared")
end
if aggregate_exists == 1 and head_prevalidation_status == "ABSENT" then
    return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "aggregate_head_appeared")
end
local current_revision_raw = redis.call("HGET", KEYS[1], "revision")
''',
    "head prevalidation existence",
)
old_head = '''    local head_record_raw = redis.call("HGET", KEYS[4], stored_operation_digest)
    local head_receipt_raw = redis.call("HGET", KEYS[3], stored_operation_digest)
    local head_index_raw = redis.call("HGET", KEYS[2], stored_transition_id)
    if not head_record_raw or not head_receipt_raw or not head_index_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "aggregate_head_proof_missing")
    end
    local head_proof, head_error = validate_proof(
'''
new_head = '''    local head_record_raw = redis.call("HGET", KEYS[4], stored_operation_digest)
    local head_receipt_raw = redis.call("HGET", KEYS[3], stored_operation_digest)
    local head_index_raw = redis.call("HGET", KEYS[2], stored_transition_id)
    if raw_sha1(head_record_raw) ~= head_record_raw_sha1
        or raw_sha1(head_receipt_raw) ~= head_receipt_raw_sha1
        or raw_sha1(head_index_raw) ~= head_index_raw_sha1 then
        return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "aggregate_head_proof_changed")
    end
    if head_prevalidation_status == "INVALID" then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "aggregate_head_canonical_validation_failed")
    end
    if head_prevalidation_status ~= "VALID" then
        return result("INVALID_COMMIT_PLAN", nil, nil, "head_prevalidation_status_invalid")
    end
    if not head_record_raw or not head_receipt_raw or not head_index_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "aggregate_head_proof_missing")
    end
    local head_proof, head_error = validate_proof(
'''
lua = replace_once(lua, old_head, new_head, "head proof prevalidation")
lua_path.write_text(lua, encoding="utf-8")

# ---------------------------------------------------------------------------
# Tests: classifier parity, coherent-envelope tamper coverage and authority
# conflict pair-loss regressions.
# ---------------------------------------------------------------------------
tests = test_path.read_text(encoding="utf-8")
tests = replace_once(
    tests,
    '    CanonicalAggregateIdentity,\n    OperationType,',
    '    CanonicalAggregateIdentity,\n    CanonicalStoreDecision,\n    OperationType,',
    "test classifier enum import",
)
tests = replace_once(
    tests,
    '    RedisCanonicalAuthorityStore,\n    evaluate_authority_commit,',
    '    RedisCanonicalAuthorityStore,\n    classify_canonical_store_write,\n    evaluate_authority_commit,',
    "test classifier function import",
)
old_concurrent = '''@pytest.mark.asyncio
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
'''
new_concurrent = '''@pytest.mark.asyncio
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
'''
tests = replace_once(tests, old_concurrent, new_concurrent, "classifier parity test")
start = tests.index(
    '@pytest.mark.asyncio\nasync def test_conflict_index_present_and_stream_missing_can_emit_first_conflict('
)
end = tests.index('\n\nasync def _commit_two_revisions', start)
operator_test = '''@pytest.mark.asyncio
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
'''
tests = tests[:start] + operator_test + tests[end:]
# One authority conflict now stores one reserved count field plus one payload.
tests = tests.replace(
    'assert await redis_client.hlen(keyspace.conflict_index) == 1',
    'assert await redis_client.hlen(keyspace.conflict_index) == 2',
)
append_tests = r'''

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
'''
tests = tests.rstrip() + append_tests + "\n"
test_path.write_text(tests, encoding="utf-8")

# ---------------------------------------------------------------------------
# Scope document: restore accepted classifier contract and make proof/evidence
# boundaries explicit.
# ---------------------------------------------------------------------------
doc = doc_path.read_text(encoding="utf-8")
doc = replace_once(
    doc,
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
    '''Sprint 81.2 preserves the accepted Sprint 81.1 canonical-store classifier.
`ALREADY_APPLIED` requires the same deterministic transition ID, canonical
record hash and exact immutable record, receipt and transition-index payloads.
Canonical command hash equality alone is not sufficient.

```yaml
same_operation_same_exact_canonical_record: ALREADY_APPLIED
same_operation_same_command_but_different_record: CANONICAL_RECORD_CORRUPTION_CONFLICT
same_operation_different_command: IDEMPOTENCY_CONFLICT
stored_proof_missing_tampered_or_inconsistent: CANONICAL_RECORD_CORRUPTION_CONFLICT
```

Concurrent evaluators for one operation must therefore receive stable
operation-level commit metadata (`committed_at_ms`, authority writer identity
and correlation identity where present). Creating that stable metadata is a
trusted-adapter precondition and remains outside this persistence-only slice.
''',
    "document classifier contract",
)
doc = replace_once(
    doc,
    '''The Redis storage digest is an additional corruption-detection layer. It does
not replace the accepted canonical SHA-256 record hash.
''',
    '''The Redis storage digest is an additional corruption-detection layer. It does
not replace the accepted canonical SHA-256 record hash. Before each Lua commit,
the trusted Python adapter reconstructs stored records with the Sprint 81.1
canonical validator, including canonical hash recomputation, deterministic
transition identity, structured aggregate identity, operation contract and
projection-intent validation. Lua compares SHA-1 digests of the exact Redis
storage envelopes observed by that prevalidation; a race causes an internal
retry rather than an unvalidated authority decision.
''',
    "document canonical prevalidation",
)
doc = replace_once(
    doc,
    '''Conflict identity is deterministically derived from aggregate identity,
operation ID, incoming command hash, stored command hash and conflict type.
`HSETNX` deduplicates repeated identical conflict observations; `XADD` occurs
only for the first insert.

If either conflict store has the wrong Redis type, the script cannot truthfully
claim durable conflict evidence. It returns the distinct fail-closed result
`CONFLICT_EVIDENCE_STORE_UNAVAILABLE`, performs zero lifecycle mutation and
requires operator/reconciliation handling.

The Python `record_conflict()` method remains only for explicit operator audit
notes. It is not used to complete a Lua conflict decision after the fact.
''',
    '''Conflict identity is deterministically derived from aggregate identity,
operation ID, incoming command hash, stored command hash and conflict type.
The authority-conflict index contains a reserved monotonic evidence-count field.
Before every decision, index cardinality and stream length must equal that count.
`HSETNX` deduplicates repeated identical conflict observations; `XADD` occurs
only for the first insert.

```yaml
authority_conflict_pair:
  both_absent_before_first_conflict: ALLOWED
  both_present_with_matching_count: ALLOWED
  one_missing_after_prior_conflict: CONFLICT_EVIDENCE_STORE_UNAVAILABLE
  deleted_index_entry_or_stream_row: CONFLICT_EVIDENCE_STORE_UNAVAILABLE
```

If either conflict store has the wrong Redis type or the authority evidence pair
is incomplete, the script cannot truthfully claim durable conflict evidence. It
returns `CONFLICT_EVIDENCE_STORE_UNAVAILABLE`, performs zero lifecycle mutation
and requires operator/reconciliation handling.

The Python `record_conflict()` method writes to a separate operator-audit stream.
Operator notes cannot alter authority conflict pair cardinality and are never
used to complete a Lua conflict decision after the fact.
''',
    "document conflict pair",
)
doc = replace_once(
    doc,
    '''```yaml
1: VALIDATE_COMMIT_PLAN_AND_CONFLICT_STORE
2: VALIDATE_REDIS_KEY_TYPES
3: RESOLVE_OPERATION_RECEIPT_AND_RECORD_BY_STABLE_OPERATION_FIELD
4: LOAD_HISTORICAL_TRANSITION_INDEX_FROM_STORED_RECORD
5: VALIDATE_STORAGE_DIGESTS_AND_STORED_PROOF_SEMANTICS
6: COMPARE_CANONICAL_COMMAND_HASH
7: VALIDATE_EXISTING_AGGREGATE_HEAD_AND_STREAM_CONTINUITY
8: COMPARE_EXPECTED_REVISION
9: COMPARE_PREVIOUS_STATE
10: WRITE_ACCEPTED_STATE_RECORD_RECEIPT_LOG_AND_OUTBOX
```
''',
    '''```yaml
1: PYTHON_CANONICAL_PREVALIDATE_OPERATION_AND_HEAD_PROOFS
2: VALIDATE_COMMIT_PLAN_AND_AUTHORITY_CONFLICT_PAIR
3: VALIDATE_REDIS_KEY_TYPES
4: BIND_LUA_READS_TO_PREVALIDATED_RAW_ENVELOPE_DIGESTS
5: RESOLVE_OPERATION_RECEIPT_AND_RECORD_BY_STABLE_OPERATION_FIELD
6: LOAD_HISTORICAL_TRANSITION_INDEX_FROM_STORED_RECORD
7: VALIDATE_STORAGE_DIGESTS_AND_STORED_PROOF_SEMANTICS
8: APPLY_EXACT_CANONICAL_STORE_CLASSIFIER_PARITY
9: VALIDATE_EXISTING_AGGREGATE_HEAD_AND_STREAM_CONTINUITY
10: COMPARE_EXPECTED_REVISION_AND_PREVIOUS_STATE
11: WRITE_ACCEPTED_STATE_RECORD_RECEIPT_LOG_AND_OUTBOX
```
''',
    "document evaluation order",
)
doc = replace_once(
    doc,
    '  concurrent_same_command_different_commit_metadata: PASS\n  conflict_store_unavailable_result: PASS\n',
    '  Redis_and_core_classifier_same_outcome: PASS\n'
    '  refreshed_storage_digest_canonical_tamper: PASS\n'
    '  authority_conflict_pair_loss_detection: PASS\n'
    '  conflict_store_unavailable_result: PASS\n',
    "document adversarial coverage",
)
doc_path.write_text(doc, encoding="utf-8")

print("SPRINT81_2_CONTRACT_FIX_APPLIED")

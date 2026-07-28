-- Sprint 81.2 canonical authority persistence decision.
--
-- KEYS[1] aggregate head hash
-- KEYS[2] fixed transition-index hash (field = transition_id)
-- KEYS[3] fixed operation-receipt hash (field = operation_id digest)
-- KEYS[4] fixed operation-record hash (field = operation_id digest)
-- KEYS[5] append-only aggregate transition stream
-- KEYS[6] append-only aggregate outbox stream
-- KEYS[7] deterministic conflict index hash (field = conflict_id)
-- KEYS[8] append-only conflict evidence stream
--
-- Every key shares one Redis Cluster hash tag. All data-dependent validation
-- precedes an accepted lifecycle write. Conflict outcomes append their durable,
-- deduplicated evidence in this same Lua execution without consuming revision.

local identity_sha = ARGV[1]
local expected_revision = tonumber(ARGV[2])
local next_revision = tonumber(ARGV[3])
local previous_is_null = ARGV[4]
local previous_state = ARGV[5]
local next_is_null = ARGV[6]
local next_state = ARGV[7]
local transition_id = ARGV[8]
local canonical_record_hash = ARGV[9]
local canonical_command_hash = ARGV[10]
local operation_id = ARGV[11]
local operation_digest = ARGV[12]
local operation_type = ARGV[13]
local committed_at_ms = tonumber(ARGV[14])
local committed_at_ms_raw = ARGV[14]
local transition_index_json = ARGV[15]
local record_json = ARGV[16]
local receipt_json = ARGV[17]
local projection_intents_json = ARGV[18]
local is_create_operation = ARGV[19]
local operation_prevalidation_status = ARGV[20]
local operation_record_raw_sha1 = ARGV[21]
local operation_receipt_raw_sha1 = ARGV[22]
local operation_index_raw_sha1 = ARGV[23]
local head_prevalidation_status = ARGV[24]
local head_record_raw_sha1 = ARGV[25]
local head_receipt_raw_sha1 = ARGV[26]
local head_index_raw_sha1 = ARGV[27]

local function result(status, existing_transition_id, revision, detail)
    return {
        status,
        existing_transition_id or "",
        revision and tostring(revision) or "",
        detail or ""
    }
end

local function redis_type(key)
    local reply = redis.call("TYPE", key)
    if type(reply) == "table" then
        return reply["ok"]
    end
    return reply
end

local function key_type_ok(key, expected)
    local kind = redis_type(key)
    return kind == "none" or kind == expected
end

local function decode_object(raw)
    if type(raw) ~= "string" then
        return nil
    end
    local ok, value = pcall(cjson.decode, raw)
    if not ok or type(value) ~= "table" then
        return nil
    end
    return value
end

local function exact_nonnegative_integer(value)
    return value and value >= 0 and value % 1 == 0 and value <= 9007199254740991
end

local function sha256_hex_ok(value)
    return type(value) == "string"
        and string.len(value) == 64
        and string.match(value, "^[0-9a-f]+$") ~= nil
end

local function sha1_hex_ok(value)
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

local function nullable_string_ok(value)
    return value == cjson.null or type(value) == "string"
end

local function exact_keys(value, allowed, expected_count)
    if type(value) ~= "table" then
        return false
    end
    local count = 0
    for key, _ in pairs(value) do
        if not allowed[key] then
            return false
        end
        count = count + 1
    end
    return count == expected_count
end

local RECORD_KEYS = {
    schema_version=true, transition_id=true, aggregate_type=true,
    canonical_aggregate_identity=true, canonical_aggregate_identity_sha256=true,
    from_revision=true, to_revision=true, operation_type=true, operation_id=true,
    canonical_command_hash=true, previous_state=true, next_state=true,
    authoritative_metadata_changes=true, child_effects=true, causation_id=true,
    correlation_id=true, writer_id=true, committed_at_ms=true,
    durable_projection_intents=true, canonical_record_hash=true
}
local RECEIPT_KEYS = {
    operation_id=true, canonical_command_hash=true, canonical_record_hash=true,
    transition_id=true, aggregate_revision=true, operation_type=true,
    committed_at_ms=true
}
local INDEX_KEYS = {
    transition_id=true, canonical_record_hash=true, operation_id=true,
    aggregate_revision=true
}
local IDENTITY_KEYS = {aggregate_type=true, run_id=true, task_id=true}
local ENVELOPE_KEYS = {payload=true, storage_sha1=true}

local function validate_record_structure(record)
    if not exact_keys(record, RECORD_KEYS, 20) then
        return false
    end
    if record.schema_version ~= "ctr.v1"
        or type(record.transition_id) ~= "string" or record.transition_id == ""
        or (record.aggregate_type ~= "task" and record.aggregate_type ~= "run")
        or not exact_keys(record.canonical_aggregate_identity, IDENTITY_KEYS, 3)
        or record.canonical_aggregate_identity.aggregate_type ~= record.aggregate_type
        or type(record.canonical_aggregate_identity.run_id) ~= "string"
        or record.canonical_aggregate_identity.run_id == ""
        or not sha256_hex_ok(record.canonical_aggregate_identity_sha256)
        or not exact_nonnegative_integer(record.from_revision)
        or not exact_nonnegative_integer(record.to_revision)
        or record.to_revision ~= record.from_revision + 1
        or type(record.operation_type) ~= "string" or record.operation_type == ""
        or type(record.operation_id) ~= "string" or record.operation_id == ""
        or not sha256_hex_ok(record.canonical_command_hash)
        or not sha256_hex_ok(record.canonical_record_hash)
        or not nullable_string_ok(record.previous_state)
        or not nullable_string_ok(record.next_state)
        or not nullable_string_ok(record.causation_id)
        or not nullable_string_ok(record.correlation_id)
        or type(record.writer_id) ~= "string" or record.writer_id == ""
        or not exact_nonnegative_integer(record.committed_at_ms)
        or type(record.durable_projection_intents) ~= "table" then
        return false
    end
    if record.aggregate_type == "task" then
        if type(record.canonical_aggregate_identity.task_id) ~= "string"
            or record.canonical_aggregate_identity.task_id == "" then
            return false
        end
    elseif record.canonical_aggregate_identity.task_id ~= cjson.null then
        return false
    end
    return true
end

local function validate_receipt_structure(receipt)
    return exact_keys(receipt, RECEIPT_KEYS, 7)
        and type(receipt.operation_id) == "string" and receipt.operation_id ~= ""
        and sha256_hex_ok(receipt.canonical_command_hash)
        and sha256_hex_ok(receipt.canonical_record_hash)
        and type(receipt.transition_id) == "string" and receipt.transition_id ~= ""
        and exact_nonnegative_integer(receipt.aggregate_revision)
        and receipt.aggregate_revision >= 1
        and type(receipt.operation_type) == "string" and receipt.operation_type ~= ""
        and exact_nonnegative_integer(receipt.committed_at_ms)
end

local function validate_index_structure(index)
    return exact_keys(index, INDEX_KEYS, 4)
        and type(index.transition_id) == "string" and index.transition_id ~= ""
        and sha256_hex_ok(index.canonical_record_hash)
        and type(index.operation_id) == "string" and index.operation_id ~= ""
        and exact_nonnegative_integer(index.aggregate_revision)
        and index.aggregate_revision >= 1
end

local function storage_envelope(payload_json)
    return cjson.encode({
        payload = payload_json,
        storage_sha1 = redis.sha1hex(payload_json)
    })
end

local function decode_storage_envelope(raw)
    local envelope = decode_object(raw)
    if not exact_keys(envelope, ENVELOPE_KEYS, 2)
        or type(envelope.payload) ~= "string"
        or not sha1_hex_ok(envelope.storage_sha1)
        or envelope.storage_sha1 ~= redis.sha1hex(envelope.payload) then
        return nil, nil
    end
    local payload = decode_object(envelope.payload)
    if not payload then
        return nil, nil
    end
    return envelope.payload, payload
end

local function validate_proof(record_raw, receipt_raw, index_raw, expected_identity, expected_operation_id)
    local record_payload, record = decode_storage_envelope(record_raw)
    local receipt_payload, receipt = decode_storage_envelope(receipt_raw)
    local index_payload, index = decode_storage_envelope(index_raw)
    if not record or not receipt or not index then
        return nil, "stored_storage_integrity_mismatch"
    end
    if not validate_record_structure(record)
        or not validate_receipt_structure(receipt)
        or not validate_index_structure(index) then
        return nil, "stored_proof_semantic_invalid"
    end
    if record.canonical_aggregate_identity_sha256 ~= expected_identity
        or record.operation_id ~= expected_operation_id
        or receipt.operation_id ~= expected_operation_id
        or receipt.operation_id ~= record.operation_id
        or receipt.operation_type ~= record.operation_type
        or receipt.aggregate_revision ~= record.to_revision
        or receipt.transition_id ~= record.transition_id
        or receipt.canonical_command_hash ~= record.canonical_command_hash
        or receipt.canonical_record_hash ~= record.canonical_record_hash
        or receipt.committed_at_ms ~= record.committed_at_ms
        or index.transition_id ~= record.transition_id
        or index.canonical_record_hash ~= record.canonical_record_hash
        or index.operation_id ~= record.operation_id
        or index.aggregate_revision ~= record.to_revision then
        return nil, "stored_receipt_record_index_mismatch"
    end
    return {
        record_payload=record_payload,
        receipt_payload=receipt_payload,
        index_payload=index_payload,
        record=record,
        receipt=receipt,
        index=index
    }, nil
end

local function length_prefix(value)
    local text = value or ""
    return tostring(string.len(text)) .. ":" .. text
end

local CONFLICT_COUNT_FIELD = "__authority_conflict_count"
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
    -- The detail value is a stable machine-readable cause code. It is part of
    -- conflict identity so distinct corruption causes remain separately durable,
    -- while observation time and other mutable metadata remain non-identity.
    local stable_detail_code = detail or ""
    local material = length_prefix(identity_sha)
        .. length_prefix(operation_id)
        .. length_prefix(canonical_command_hash)
        .. length_prefix(stored_command_hash or "")
        .. length_prefix(conflict_type)
        .. length_prefix(stable_detail_code)
    local conflict_id = redis.sha1hex(material)
    local payload = cjson.encode({
        conflict_id=conflict_id,
        conflict_type=conflict_type,
        detail_code=stable_detail_code,
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
            "detail_code", stable_detail_code,
            "operation_id", operation_id,
            "operation_digest", operation_digest,
            "incoming_command_hash", canonical_command_hash,
            "stored_command_hash", stored_command_hash or "",
            "existing_transition_id", existing_transition_id or "",
            "observed_at_ms", committed_at_ms_raw,
            "detail", detail or "",
            "conflict_json", payload
        )
    else
        -- Conflict identity excludes observation metadata. Repeated observations
        -- compare only fields bound into conflict_id; the first payload wins.
        local stored_payload = decode_object(redis.call("HGET", KEYS[7], conflict_id))
        local expected_stored_hash = stored_command_hash or cjson.null
        if not stored_payload
            or stored_payload.conflict_id ~= conflict_id
            or stored_payload.conflict_type ~= conflict_type
            or stored_payload.detail_code ~= stable_detail_code
            or stored_payload.canonical_aggregate_identity_sha256 ~= identity_sha
            or stored_payload.operation_id ~= operation_id
            or stored_payload.operation_digest ~= operation_digest
            or stored_payload.incoming_command_hash ~= canonical_command_hash
            or stored_payload.stored_command_hash ~= expected_stored_hash then
            return conflict_store_unavailable("authority_conflict_index_identity_mismatch")
        end
    end
    return result(conflict_type, existing_transition_id, revision, detail or "")
end

local function stream_tail(key)
    local rows = redis.call("XREVRANGE", key, "+", "-", "COUNT", 1)
    if type(rows) ~= "table" or #rows ~= 1 or type(rows[1]) ~= "table" then
        return nil
    end
    local values = rows[1][2]
    if type(values) ~= "table" then
        return nil
    end
    local output = {}
    for i=1,#values,2 do
        output[values[i]] = values[i + 1]
    end
    return output
end

if not exact_nonnegative_integer(expected_revision)
    or not exact_nonnegative_integer(next_revision)
    or next_revision ~= expected_revision + 1 then
    return result("INVALID_COMMIT_PLAN", nil, nil, "non_contiguous_revision")
end
if not exact_nonnegative_integer(committed_at_ms) then
    return result("INVALID_COMMIT_PLAN", nil, nil, "invalid_committed_at_ms")
end
if previous_is_null ~= "0" and previous_is_null ~= "1" then
    return result("INVALID_COMMIT_PLAN", nil, nil, "invalid_previous_null_flag")
end
if next_is_null ~= "0" and next_is_null ~= "1" then
    return result("INVALID_COMMIT_PLAN", nil, nil, "invalid_next_null_flag")
end
if not sha256_hex_ok(identity_sha)
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
if not key_type_ok(KEYS[1], "hash")
    or not key_type_ok(KEYS[2], "hash")
    or not key_type_ok(KEYS[3], "hash")
    or not key_type_ok(KEYS[4], "hash")
    or not key_type_ok(KEYS[5], "stream")
    or not key_type_ok(KEYS[6], "stream") then
    return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "redis_key_type_mismatch")
end

local incoming_index = decode_object(transition_index_json)
local incoming_record = decode_object(record_json)
local incoming_receipt = decode_object(receipt_json)
local decoded_intents = decode_object(projection_intents_json)
if not validate_index_structure(incoming_index) then
    return result("INVALID_COMMIT_PLAN", nil, nil, "transition_index_json_invalid")
end
if not validate_record_structure(incoming_record) then
    return result("INVALID_COMMIT_PLAN", nil, nil, "record_json_invalid")
end
if not validate_receipt_structure(incoming_receipt) then
    return result("INVALID_COMMIT_PLAN", nil, nil, "receipt_json_invalid")
end
if not decoded_intents then
    return result("INVALID_COMMIT_PLAN", nil, nil, "projection_intents_json_invalid")
end
if incoming_index.transition_id ~= transition_id
    or incoming_index.canonical_record_hash ~= canonical_record_hash
    or incoming_index.operation_id ~= operation_id
    or incoming_index.aggregate_revision ~= next_revision then
    return result("INVALID_COMMIT_PLAN", nil, nil, "transition_index_argument_mismatch")
end
if incoming_record.canonical_aggregate_identity_sha256 ~= identity_sha
    or incoming_record.from_revision ~= expected_revision
    or incoming_record.to_revision ~= next_revision
    or incoming_record.transition_id ~= transition_id
    or incoming_record.canonical_record_hash ~= canonical_record_hash
    or incoming_record.canonical_command_hash ~= canonical_command_hash
    or incoming_record.operation_id ~= operation_id
    or incoming_record.operation_type ~= operation_type
    or incoming_record.committed_at_ms ~= committed_at_ms then
    return result("INVALID_COMMIT_PLAN", nil, nil, "record_argument_mismatch")
end
if incoming_receipt.operation_id ~= operation_id
    or incoming_receipt.operation_type ~= operation_type
    or incoming_receipt.aggregate_revision ~= next_revision
    or incoming_receipt.transition_id ~= transition_id
    or incoming_receipt.canonical_record_hash ~= canonical_record_hash
    or incoming_receipt.canonical_command_hash ~= canonical_command_hash
    or incoming_receipt.committed_at_ms ~= committed_at_ms then
    return result("INVALID_COMMIT_PLAN", nil, nil, "receipt_argument_mismatch")
end

-- Receipt-first idempotency: fixed hash fields are resolved before incoming
-- revision, operation type or transition ID can classify the command.
local existing_receipt_raw = redis.call("HGET", KEYS[3], operation_digest)
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

if redis.call("HEXISTS", KEYS[2], transition_id) == 1 then
    return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, transition_id, nil, "transition_index_collision")
end

local aggregate_exists = redis.call("EXISTS", KEYS[1])
if aggregate_exists == 0 and head_prevalidation_status ~= "ABSENT" then
    return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "aggregate_head_disappeared")
end
if aggregate_exists == 1 and head_prevalidation_status == "ABSENT" then
    return result("PREVALIDATION_RETRY_REQUIRED", nil, nil, "aggregate_head_appeared")
end
local current_revision_raw = redis.call("HGET", KEYS[1], "revision")
local current_revision = current_revision_raw and tonumber(current_revision_raw) or 0
if aggregate_exists == 1 and current_revision_raw == false then
    return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "aggregate_revision_missing")
end
if not exact_nonnegative_integer(current_revision) then
    return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "aggregate_revision_invalid")
end

local state_exists = redis.call("HEXISTS", KEYS[1], "state")
local state_is_null = redis.call("HGET", KEYS[1], "state_is_null")
local stored_identity = redis.call("HGET", KEYS[1], "canonical_aggregate_identity_sha256")
local stored_transition_id = redis.call("HGET", KEYS[1], "transition_id")
local stored_record_hash = redis.call("HGET", KEYS[1], "canonical_record_hash")
local stored_command_hash = redis.call("HGET", KEYS[1], "canonical_command_hash")
local stored_operation_id = redis.call("HGET", KEYS[1], "operation_id")
local stored_operation_digest = redis.call("HGET", KEYS[1], "operation_digest")
local stored_projection_intents_json = redis.call("HGET", KEYS[1], "projection_intents_json")
local stored_updated_at = tonumber(redis.call("HGET", KEYS[1], "updated_at_ms"))

if aggregate_exists == 1 then
    if redis.call("HLEN", KEYS[1]) ~= 11 then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "aggregate_snapshot_field_count_mismatch")
    end
    if current_revision < 1
        or stored_identity ~= identity_sha
        or state_exists ~= 1
        or (state_is_null ~= "0" and state_is_null ~= "1")
        or type(stored_transition_id) ~= "string" or stored_transition_id == ""
        or not sha256_hex_ok(stored_record_hash)
        or not sha256_hex_ok(stored_command_hash)
        or type(stored_operation_id) ~= "string" or stored_operation_id == ""
        or not sha256_hex_ok(stored_operation_digest)
        or type(stored_projection_intents_json) ~= "string"
        or not exact_nonnegative_integer(stored_updated_at) then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "aggregate_snapshot_incomplete")
    end

    local head_record_raw = redis.call("HGET", KEYS[4], stored_operation_digest)
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
        head_record_raw,
        head_receipt_raw,
        head_index_raw,
        identity_sha,
        stored_operation_id
    )
    if not head_proof then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, head_error)
    end
    if head_proof.record.to_revision ~= current_revision
        or head_proof.record.transition_id ~= stored_transition_id
        or head_proof.record.canonical_record_hash ~= stored_record_hash
        or head_proof.record.canonical_command_hash ~= stored_command_hash
        or head_proof.record.operation_id ~= stored_operation_id then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "aggregate_head_proof_mismatch")
    end

    local log_tail = stream_tail(KEYS[5])
    local outbox_tail = stream_tail(KEYS[6])
    if not log_tail
        or log_tail.aggregate_revision ~= tostring(current_revision)
        or log_tail.transition_id ~= stored_transition_id
        or log_tail.canonical_record_hash ~= stored_record_hash
        or log_tail.canonical_command_hash ~= stored_command_hash
        or log_tail.operation_id ~= stored_operation_id
        or log_tail.operation_digest ~= stored_operation_digest
        or log_tail.record_json ~= head_proof.record_payload
        or log_tail.receipt_json ~= head_proof.receipt_payload then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "transition_log_tail_mismatch")
    end
    if not outbox_tail
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

if current_revision < expected_revision then
    return result("FUTURE_REVISION_CONFLICT", nil, current_revision, "")
end
if current_revision > expected_revision then
    if is_create_operation == "1" and expected_revision == 0 then
        return emit_conflict("AGGREGATE_ALREADY_EXISTS_CONFLICT", stored_command_hash, stored_transition_id, current_revision, "")
    end
    return result("STALE_REVISION_CONFLICT", nil, current_revision, "")
end

if previous_is_null == "1" then
    if current_revision ~= 0 then
        if state_exists == 0 or state_is_null ~= "1" then
            return result("ILLEGAL_STATE_TRANSITION", nil, current_revision, "previous_state_mismatch")
        end
    elseif state_exists == 1 and state_is_null ~= "1" then
        return result("ILLEGAL_STATE_TRANSITION", nil, current_revision, "previous_state_mismatch")
    end
else
    if state_exists == 0 or state_is_null == "1" or redis.call("HGET", KEYS[1], "state") ~= previous_state then
        return result("ILLEGAL_STATE_TRANSITION", nil, current_revision, "previous_state_mismatch")
    end
end

local record_envelope = storage_envelope(record_json)
local receipt_envelope = storage_envelope(receipt_json)
local index_envelope = storage_envelope(transition_index_json)
redis.call("HSET", KEYS[2], transition_id, index_envelope)
redis.call("HSET", KEYS[3], operation_digest, receipt_envelope)
redis.call("HSET", KEYS[4], operation_digest, record_envelope)
redis.call(
    "HSET", KEYS[1],
    "canonical_aggregate_identity_sha256", identity_sha,
    "revision", tostring(next_revision),
    "state", next_state,
    "state_is_null", next_is_null,
    "transition_id", transition_id,
    "canonical_record_hash", canonical_record_hash,
    "canonical_command_hash", canonical_command_hash,
    "operation_id", operation_id,
    "operation_digest", operation_digest,
    "projection_intents_json", projection_intents_json,
    "updated_at_ms", committed_at_ms_raw
)
redis.call("XADD", KEYS[5], "*",
    "aggregate_revision", tostring(next_revision),
    "transition_id", transition_id,
    "canonical_record_hash", canonical_record_hash,
    "canonical_command_hash", canonical_command_hash,
    "operation_id", operation_id,
    "operation_digest", operation_digest,
    "operation_type", operation_type,
    "committed_at_ms", committed_at_ms_raw,
    "record_json", record_json,
    "receipt_json", receipt_json
)
redis.call("XADD", KEYS[6], "*",
    "aggregate_revision", tostring(next_revision),
    "transition_id", transition_id,
    "canonical_record_hash", canonical_record_hash,
    "operation_id", operation_id,
    "operation_digest", operation_digest,
    "projection_intents_json", projection_intents_json,
    "committed_at_ms", committed_at_ms_raw
)

return result("COMMITTED", transition_id, next_revision, "")

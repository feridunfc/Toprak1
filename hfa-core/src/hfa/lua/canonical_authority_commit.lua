-- Sprint 81.2 canonical authority persistence commit.
--
-- KEYS[1] aggregate hash
-- KEYS[2] immutable transition-ID uniqueness index
-- KEYS[3] immutable operation receipt
-- KEYS[4] immutable canonical record indexed by operation ID
-- KEYS[5] append-only aggregate transition stream
-- KEYS[6] append-only aggregate outbox stream
--
-- Every key shares one Redis Cluster hash tag. All validation precedes the
-- first write so a data-dependent rejection cannot leave a partial commit.

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
local operation_type = ARGV[12]
local committed_at_ms = ARGV[13]
local transition_index_json = ARGV[14]
local record_json = ARGV[15]
local receipt_json = ARGV[16]
local projection_intents_json = ARGV[17]
local is_create_operation = ARGV[18]

local function result(status, existing_transition_id, revision, detail)
    return {
        status,
        existing_transition_id or "",
        revision and tostring(revision) or "",
        detail or ""
    }
end

local function key_type_ok(key, expected)
    local reply = redis.call("TYPE", key)
    local kind = reply
    if type(reply) == "table" then
        kind = reply["ok"]
    end
    return kind == "none" or kind == expected
end

local function decode_object(raw)
    local ok, value = pcall(cjson.decode, raw)
    if not ok or type(value) ~= "table" then
        return nil
    end
    return value
end

if not expected_revision or not next_revision or next_revision ~= expected_revision + 1 then
    return result("INVALID_COMMIT_PLAN", nil, nil, "non_contiguous_revision")
end
if previous_is_null ~= "0" and previous_is_null ~= "1" then
    return result("INVALID_COMMIT_PLAN", nil, nil, "invalid_previous_null_flag")
end
if next_is_null ~= "0" and next_is_null ~= "1" then
    return result("INVALID_COMMIT_PLAN", nil, nil, "invalid_next_null_flag")
end

if not key_type_ok(KEYS[1], "hash")
    or not key_type_ok(KEYS[2], "string")
    or not key_type_ok(KEYS[3], "string")
    or not key_type_ok(KEYS[4], "string")
    or not key_type_ok(KEYS[5], "stream")
    or not key_type_ok(KEYS[6], "stream") then
    return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, "redis_key_type_mismatch")
end

local incoming_index = decode_object(transition_index_json)
local incoming_record = decode_object(record_json)
local incoming_receipt = decode_object(receipt_json)
local decoded_intents = decode_object(projection_intents_json)
if not incoming_index then
    return result("INVALID_COMMIT_PLAN", nil, nil, "transition_index_json_invalid")
end
if not incoming_record then
    return result("INVALID_COMMIT_PLAN", nil, nil, "record_json_invalid")
end
if not incoming_receipt then
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
    or incoming_record.operation_type ~= operation_type then
    return result("INVALID_COMMIT_PLAN", nil, nil, "record_argument_mismatch")
end

if incoming_receipt.operation_id ~= operation_id
    or incoming_receipt.operation_type ~= operation_type
    or incoming_receipt.aggregate_revision ~= next_revision
    or incoming_receipt.transition_id ~= transition_id
    or incoming_receipt.canonical_record_hash ~= canonical_record_hash
    or incoming_receipt.canonical_command_hash ~= canonical_command_hash then
    return result("INVALID_COMMIT_PLAN", nil, nil, "receipt_argument_mismatch")
end

-- Receipt-first idempotency. KEYS[3] and KEYS[4] are stable for an operation ID,
-- even when the incoming command changes operation type, revision or state.
local existing_receipt_json = redis.call("GET", KEYS[3])
local existing_record_json = redis.call("GET", KEYS[4])
if existing_receipt_json or existing_record_json then
    if not existing_receipt_json or not existing_record_json then
        return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, "stored_receipt_proof_incomplete")
    end
    local existing_receipt = decode_object(existing_receipt_json)
    local existing_record = decode_object(existing_record_json)
    if not existing_receipt or not existing_record then
        return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, "stored_proof_json_invalid")
    end
    if existing_record.canonical_aggregate_identity_sha256 ~= identity_sha
        or existing_record.operation_id ~= operation_id
        or existing_receipt.operation_id ~= operation_id
        or existing_receipt.operation_id ~= existing_record.operation_id
        or existing_receipt.operation_type ~= existing_record.operation_type
        or existing_receipt.aggregate_revision ~= existing_record.to_revision
        or existing_receipt.transition_id ~= existing_record.transition_id
        or existing_receipt.canonical_command_hash ~= existing_record.canonical_command_hash
        or existing_receipt.canonical_record_hash ~= existing_record.canonical_record_hash
        or existing_receipt.committed_at_ms ~= existing_record.committed_at_ms then
        return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, "stored_receipt_record_mismatch")
    end
    if existing_record.transition_id == transition_id then
        local existing_index_json = redis.call("GET", KEYS[2])
        local existing_index = existing_index_json and decode_object(existing_index_json) or nil
        if not existing_index
            or existing_index.transition_id ~= existing_record.transition_id
            or existing_index.canonical_record_hash ~= existing_record.canonical_record_hash
            or existing_index.operation_id ~= existing_record.operation_id
            or existing_index.aggregate_revision ~= existing_record.to_revision then
            return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, "stored_transition_index_mismatch")
        end
    end
    if existing_receipt.canonical_command_hash == canonical_command_hash
        and existing_receipt.canonical_record_hash == canonical_record_hash
        and existing_receipt.transition_id == transition_id then
        return result("ALREADY_APPLIED", existing_receipt.transition_id, existing_receipt.aggregate_revision, "")
    end
    return result("IDEMPOTENCY_CONFLICT", existing_receipt.transition_id, existing_receipt.aggregate_revision, "")
end

if redis.call("EXISTS", KEYS[2]) == 1 then
    return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, "transition_index_collision")
end

local current_revision_raw = redis.call("HGET", KEYS[1], "revision")
local current_revision = current_revision_raw and tonumber(current_revision_raw) or 0
if not current_revision then
    return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, "aggregate_revision_invalid")
end

if current_revision < expected_revision then
    return result("FUTURE_REVISION_CONFLICT", nil, current_revision, "")
end
if current_revision > expected_revision then
    if is_create_operation == "1" and expected_revision == 0 then
        return result("AGGREGATE_ALREADY_EXISTS_CONFLICT", nil, current_revision, "")
    end
    return result("STALE_REVISION_CONFLICT", nil, current_revision, "")
end

local state_exists = redis.call("HEXISTS", KEYS[1], "state")
local state_is_null = redis.call("HGET", KEYS[1], "state_is_null")
local stored_identity = redis.call("HGET", KEYS[1], "canonical_aggregate_identity_sha256")
if stored_identity and stored_identity ~= identity_sha then
    return result("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, current_revision, "aggregate_identity_mismatch")
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

redis.call("SET", KEYS[2], transition_index_json)
redis.call("SET", KEYS[3], receipt_json)
redis.call("SET", KEYS[4], record_json)
redis.call(
    "HSET", KEYS[1],
    "canonical_aggregate_identity_sha256", identity_sha,
    "revision", tostring(next_revision),
    "state", next_state,
    "state_is_null", next_is_null,
    "transition_id", transition_id,
    "canonical_record_hash", canonical_record_hash,
    "updated_at_ms", committed_at_ms
)
redis.call("XADD", KEYS[5], "*",
    "aggregate_revision", tostring(next_revision),
    "transition_id", transition_id,
    "canonical_record_hash", canonical_record_hash,
    "operation_id", operation_id,
    "operation_type", operation_type,
    "committed_at_ms", committed_at_ms,
    "record_json", record_json
)
redis.call("XADD", KEYS[6], "*",
    "aggregate_revision", tostring(next_revision),
    "transition_id", transition_id,
    "canonical_record_hash", canonical_record_hash,
    "projection_intents_json", projection_intents_json,
    "committed_at_ms", committed_at_ms
)

return result("COMMITTED", transition_id, next_revision, "")

-- Read-only, atomic validation of the complete current canonical authority head.
-- KEYS: aggregate, transition-indexes, receipts, operation-records, log, outbox
local identity_sha = ARGV[1]
local expected_operation_id = ARGV[2]
local expected_operation_digest = ARGV[3]
local expected_transition_id = ARGV[4]
local expected_revision = tonumber(ARGV[5])
local expected_command_hash = ARGV[6]
local expected_record_hash = ARGV[7]
local expected_record_payload = ARGV[8]
local expected_receipt_payload = ARGV[9]
local expected_index_payload = ARGV[10]
local expected_state = ARGV[11]
local expected_state_is_null = ARGV[12]
local expected_projection_intents = ARGV[13]
local expected_updated_at_ms = ARGV[14]

local function redis_type(key)
    local reply = redis.call("TYPE", key)
    if type(reply) == "table" then return reply["ok"] end
    return reply
end
local function invalid(detail) return {"INVALID", detail} end
local function decode_object(raw)
    if type(raw) ~= "string" then return nil end
    local ok, value = pcall(cjson.decode, raw)
    if not ok or type(value) ~= "table" then return nil end
    return value
end
local function decode_envelope(raw)
    local env = decode_object(raw)
    if not env or type(env.payload) ~= "string" or type(env.storage_sha1) ~= "string" then return nil end
    if redis.sha1hex(env.payload) ~= env.storage_sha1 then return nil end
    return decode_object(env.payload), env.payload
end
local function stream_tail(key)
    local rows = redis.call("XREVRANGE", key, "+", "-", "COUNT", 1)
    if type(rows) ~= "table" or #rows ~= 1 then return nil end
    local output = {}
    for i=1,#rows[1][2],2 do output[rows[1][2][i]] = rows[1][2][i+1] end
    return output
end

if redis_type(KEYS[1]) ~= "hash" or redis_type(KEYS[2]) ~= "hash"
    or redis_type(KEYS[3]) ~= "hash" or redis_type(KEYS[4]) ~= "hash"
    or redis_type(KEYS[5]) ~= "stream" or redis_type(KEYS[6]) ~= "stream" then
    return invalid("authority_head_key_type_mismatch")
end
local expected_snapshot_fields = {
    canonical_aggregate_identity_sha256=true, revision=true, state=true, state_is_null=true,
    transition_id=true, canonical_record_hash=true, canonical_command_hash=true,
    operation_id=true, operation_digest=true, projection_intents_json=true, updated_at_ms=true
}
if redis.call("HLEN", KEYS[1]) ~= 11 then return invalid("aggregate_snapshot_field_count_mismatch") end
local snapshot_keys = redis.call("HKEYS", KEYS[1])
for _, key in ipairs(snapshot_keys) do
    if not expected_snapshot_fields[key] then return invalid("aggregate_snapshot_field_set_mismatch") end
end
local revision = tonumber(redis.call("HGET", KEYS[1], "revision"))
local stored_identity = redis.call("HGET", KEYS[1], "canonical_aggregate_identity_sha256")
local operation_id = redis.call("HGET", KEYS[1], "operation_id")
local operation_digest = redis.call("HGET", KEYS[1], "operation_digest")
local transition_id = redis.call("HGET", KEYS[1], "transition_id")
local record_hash = redis.call("HGET", KEYS[1], "canonical_record_hash")
local command_hash = redis.call("HGET", KEYS[1], "canonical_command_hash")
local state = redis.call("HGET", KEYS[1], "state")
local state_is_null = redis.call("HGET", KEYS[1], "state_is_null")
local intents = redis.call("HGET", KEYS[1], "projection_intents_json")
local updated_at_ms = redis.call("HGET", KEYS[1], "updated_at_ms")
if not revision or revision < 1 or revision % 1 ~= 0 or stored_identity ~= identity_sha
    or operation_id ~= expected_operation_id
    or operation_digest ~= expected_operation_digest
    or transition_id ~= expected_transition_id
    or revision ~= expected_revision
    or command_hash ~= expected_command_hash
    or record_hash ~= expected_record_hash
    or state ~= expected_state
    or state_is_null ~= expected_state_is_null
    or intents ~= expected_projection_intents
    or updated_at_ms ~= expected_updated_at_ms then
    return invalid("aggregate_snapshot_incomplete_or_mismatch")
end
local record, record_payload = decode_envelope(redis.call("HGET", KEYS[4], operation_digest))
local receipt, receipt_payload = decode_envelope(redis.call("HGET", KEYS[3], operation_digest))
local index, index_payload = decode_envelope(redis.call("HGET", KEYS[2], transition_id))
if not record or not receipt or not index then return invalid("authority_head_proof_missing_or_invalid") end
if record_payload ~= expected_record_payload
    or receipt_payload ~= expected_receipt_payload
    or index_payload ~= expected_index_payload then
    return invalid("authority_head_exact_payload_mismatch")
end
if record.operation_id ~= operation_id or receipt.operation_id ~= operation_id
    or record.transition_id ~= transition_id or receipt.transition_id ~= transition_id
    or index.transition_id ~= transition_id or record.to_revision ~= revision
    or receipt.aggregate_revision ~= revision or index.aggregate_revision ~= revision
    or record.canonical_record_hash ~= record_hash or receipt.canonical_record_hash ~= record_hash
    or index.canonical_record_hash ~= record_hash or record.canonical_command_hash ~= command_hash
    or receipt.canonical_command_hash ~= command_hash or record.next_state ~= (state_is_null == "1" and cjson.null or state) then
    return invalid("authority_head_proof_mismatch")
end
local log_tail = stream_tail(KEYS[5])
local outbox_tail = stream_tail(KEYS[6])
if not log_tail or log_tail.aggregate_revision ~= tostring(revision)
    or log_tail.transition_id ~= transition_id or log_tail.canonical_record_hash ~= record_hash
    or log_tail.canonical_command_hash ~= command_hash or log_tail.operation_id ~= operation_id
    or log_tail.operation_digest ~= operation_digest or log_tail.record_json ~= record_payload
    or log_tail.receipt_json ~= receipt_payload then
    return invalid("transition_log_tail_mismatch")
end
if not outbox_tail or outbox_tail.aggregate_revision ~= tostring(revision)
    or outbox_tail.transition_id ~= transition_id or outbox_tail.canonical_record_hash ~= record_hash
    or outbox_tail.operation_id ~= operation_id or outbox_tail.operation_digest ~= operation_digest
    or outbox_tail.projection_intents_json ~= intents then
    return invalid("outbox_tail_mismatch")
end
if redis.call("HLEN", KEYS[2]) ~= revision or redis.call("HLEN", KEYS[3]) ~= revision
    or redis.call("HLEN", KEYS[4]) ~= revision or redis.call("XLEN", KEYS[5]) ~= revision
    or redis.call("XLEN", KEYS[6]) ~= revision then
    return invalid("historical_cardinality_mismatch")
end
return {"VALID", ""}

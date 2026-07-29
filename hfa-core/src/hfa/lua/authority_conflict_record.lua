-- Atomically record policy-level authority conflict evidence without lifecycle mutation.
local conflict_index = KEYS[1]
local conflict_stream = KEYS[2]
local identity_sha = ARGV[1]
local operation_id = ARGV[2]
local operation_digest = ARGV[3]
local incoming_command_hash = ARGV[4]
local stored_command_hash = ARGV[5]
local conflict_type = ARGV[6]
local detail_code = ARGV[7]
local observed_at_ms = ARGV[8]
local existing_transition_id = ARGV[9]
local revision = ARGV[10]
local detail = ARGV[11]
local COUNT_FIELD = "__authority_conflict_count"

local function redis_type(key)
    local reply = redis.call("TYPE", key)
    if type(reply) == "table" then return reply["ok"] end
    return reply
end
local function result(status, transition_id, rev, info)
    return {status, transition_id or "", rev or "", info or ""}
end
local function length_prefix(value)
    return tostring(string.len(value or "")) .. ":" .. (value or "")
end
local index_kind = redis_type(conflict_index)
local stream_kind = redis_type(conflict_stream)
if index_kind ~= "none" and index_kind ~= "hash" then
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "conflict_index_type_mismatch")
end
if stream_kind ~= "none" and stream_kind ~= "stream" then
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "conflict_stream_type_mismatch")
end
if (index_kind == "none") ~= (stream_kind == "none") then
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "authority_conflict_pair_missing_member")
end
local count = 0
if index_kind ~= "none" then
    count = tonumber(redis.call("HGET", conflict_index, COUNT_FIELD))
    if not count or count < 0 or count % 1 ~= 0 then
        return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "authority_conflict_count_missing_or_invalid")
    end
    if redis.call("HLEN", conflict_index) ~= count + 1 or redis.call("XLEN", conflict_stream) ~= count then
        return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "authority_conflict_pair_cardinality_mismatch")
    end
end
local material = length_prefix(identity_sha)
    .. length_prefix(operation_id)
    .. length_prefix(incoming_command_hash)
    .. length_prefix(stored_command_hash)
    .. length_prefix(conflict_type)
    .. length_prefix(detail_code)
local conflict_id = redis.sha1hex(material)
local stored_hash_json = stored_command_hash == "" and cjson.null or stored_command_hash
local transition_json = existing_transition_id == "" and cjson.null or existing_transition_id
local revision_json = revision == "" and cjson.null or tonumber(revision)
local payload = cjson.encode({
    conflict_id=conflict_id,
    conflict_type=conflict_type,
    detail_code=detail_code,
    canonical_aggregate_identity_sha256=identity_sha,
    operation_id=operation_id,
    operation_digest=operation_digest,
    incoming_command_hash=incoming_command_hash,
    stored_command_hash=stored_hash_json,
    existing_transition_id=transition_json,
    aggregate_revision=revision_json,
    observed_at_ms=tonumber(observed_at_ms),
    detail=detail
})
local inserted = redis.call("HSETNX", conflict_index, conflict_id, payload)
if inserted == 1 then
    count = count + 1
    redis.call("HSET", conflict_index, COUNT_FIELD, tostring(count))
    redis.call("XADD", conflict_stream, "*",
        "conflict_id", conflict_id,
        "conflict_type", conflict_type,
        "detail_code", detail_code,
        "operation_id", operation_id,
        "operation_digest", operation_digest,
        "incoming_command_hash", incoming_command_hash,
        "stored_command_hash", stored_command_hash,
        "existing_transition_id", existing_transition_id,
        "aggregate_revision", revision,
        "observed_at_ms", observed_at_ms,
        "detail", detail,
        "conflict_json", payload)
else
    local existing = cjson.decode(redis.call("HGET", conflict_index, conflict_id))
    if existing.conflict_id ~= conflict_id or existing.conflict_type ~= conflict_type
        or existing.detail_code ~= detail_code
        or existing.canonical_aggregate_identity_sha256 ~= identity_sha
        or existing.operation_id ~= operation_id
        or existing.operation_digest ~= operation_digest
        or existing.incoming_command_hash ~= incoming_command_hash
        or existing.stored_command_hash ~= stored_hash_json then
        return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "authority_conflict_index_identity_mismatch")
    end
end
return result(conflict_type, existing_transition_id, revision, detail)

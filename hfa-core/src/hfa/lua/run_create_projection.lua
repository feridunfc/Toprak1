-- Sprint 84.4 replay-safe legacy RUN admitted projection.
--
-- KEYS[1] projection receipt HASH
-- KEYS[2] legacy RUN state STRING
-- KEYS[3] control STREAM
--
-- ARGV[1..13] immutable proof and projection parameters; remaining values are
-- event field-name/value pairs.

local receipt_key = KEYS[1]
local state_key = KEYS[2]
local stream_key = KEYS[3]

local operation_id = ARGV[1]
local reservation_proof = ARGV[2]
local transition_id = ARGV[3]
local record_hash = ARGV[4]
local command_hash = ARGV[5]
local revision = ARGV[6]
local run_id = ARGV[7]
local tenant_id = ARGV[8]
local legacy_state = ARGV[9]
local state_ttl = tonumber(ARGV[10])
local event_hash = ARGV[11]
local stream_maxlen = tonumber(ARGV[12])
local field_count = tonumber(ARGV[13])

local function conflict(detail)
    return {"canonical_run_create_evidence_conflict", "", detail}
end

if not state_ttl or state_ttl <= 0 then
    return conflict("invalid_state_ttl")
end
if not stream_maxlen or stream_maxlen <= 0 then
    return conflict("invalid_stream_maxlen")
end
if not field_count or field_count <= 0 then
    return conflict("invalid_event_field_count")
end
if #ARGV ~= 13 + (field_count * 2) then
    return conflict("event_field_arity_mismatch")
end

local receipt_type = redis.call("TYPE", receipt_key)["ok"]
if receipt_type ~= "none" and receipt_type ~= "hash" then
    return conflict("projection_receipt_wrong_type")
end

if receipt_type == "hash" then
    local expected = {
        "operation_id", operation_id,
        "reservation_proof_sha256", reservation_proof,
        "canonical_transition_id", transition_id,
        "canonical_record_hash", record_hash,
        "canonical_command_hash", command_hash,
        "canonical_revision", revision,
        "run_id", run_id,
        "tenant_id", tenant_id,
        "legacy_state", legacy_state,
        "event_payload_hash", event_hash
    }
    for index = 1, #expected, 2 do
        local stored = redis.call("HGET", receipt_key, expected[index])
        if not stored or stored ~= expected[index + 1] then
            return conflict("projection_receipt_proof_mismatch")
        end
    end
    local stream_entry_id = redis.call("HGET", receipt_key, "stream_entry_id")
    if not stream_entry_id or stream_entry_id == "" then
        return conflict("projection_receipt_missing_stream_entry_id")
    end
    return {"canonical_run_create_already_projected", stream_entry_id, ""}
end

local state_type = redis.call("TYPE", state_key)["ok"]
if state_type ~= "none" then
    return conflict("legacy_run_state_exists_without_projection_receipt")
end

local stream_type = redis.call("TYPE", stream_key)["ok"]
if stream_type ~= "none" and stream_type ~= "stream" then
    return conflict("control_stream_wrong_type")
end

if stream_type == "stream" then
    local entries = redis.call("XRANGE", stream_key, "-", "+")
    for _, entry in ipairs(entries) do
        local fields = entry[2]
        local observed_event_type = ""
        local observed_run_id = ""
        for index = 1, #fields, 2 do
            if fields[index] == "event_type" then
                observed_event_type = fields[index + 1]
            elseif fields[index] == "run_id" then
                observed_run_id = fields[index + 1]
            end
        end
        if observed_event_type == "RunAdmitted" and observed_run_id == run_id then
            return conflict("legacy_run_event_exists_without_projection_receipt")
        end
    end
end

local event_args = {}
for index = 1, field_count * 2 do
    event_args[index] = ARGV[13 + index]
end

-- All Redis key types and argument shapes were validated before the first write.
local stream_entry_id = redis.call(
    "XADD", stream_key, "MAXLEN", "~", stream_maxlen, "*", unpack(event_args)
)
redis.call("SET", state_key, legacy_state, "EX", state_ttl)
redis.call(
    "HSET", receipt_key,
    "operation_id", operation_id,
    "reservation_proof_sha256", reservation_proof,
    "canonical_transition_id", transition_id,
    "canonical_record_hash", record_hash,
    "canonical_command_hash", command_hash,
    "canonical_revision", revision,
    "run_id", run_id,
    "tenant_id", tenant_id,
    "legacy_state", legacy_state,
    "event_payload_hash", event_hash,
    "stream_entry_id", stream_entry_id
)
redis.call("PERSIST", receipt_key)
return {"run_admitted", stream_entry_id, ""}

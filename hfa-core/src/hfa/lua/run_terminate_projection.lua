-- Sprint 84.6: bounded, replay-safe canonical RUN_TERMINATE projection.
--
-- This script never reads TASK state. Canonical authority plus the immutable
-- terminal aggregate proof are the sole inputs to this projection. Historical
-- terminal-event discovery is performed off-path by bounded migration; online
-- projection consults only an O(1) durable evidence HASH.
--
-- KEYS
--  1 projection receipt HASH
--  2 terminal-event evidence/readiness index HASH
--  3 RUN state STRING
--  4 RUN metadata HASH
--  5 RUN result HASH
--  6 cp_running ZSET
--  7 results STREAM
--
-- ARGV
--  1 operation_id
--  2 terminal_proof_sha256
--  3 canonical_transition_id
--  4 canonical_record_hash
--  5 canonical_command_hash
--  6 canonical_revision
--  7 run_id
--  8 tenant_id
--  9 final_state
-- 10 finalized_at_ms
-- 11 worker_instance_id
-- 12 trigger_task_id
-- 13 trigger_terminal_state
-- 14 task_count
-- 15 done_count
-- 16 failed_count
-- 17 skipped_count
-- 18 run_state_ttl_seconds
-- 19 run_meta_ttl_seconds
-- 20 run_result_ttl_seconds
-- 21 results_stream_maxlen
-- 22 result_payload_json
-- 23 result_payload_sha256
--
-- Return: [status, stream_entry_id, detail]

local receipt_key = KEYS[1]
local terminal_event_index_key = KEYS[2]
local run_state_key = KEYS[3]
local run_meta_key = KEYS[4]
local run_result_key = KEYS[5]
local cp_running_key = KEYS[6]
local results_stream = KEYS[7]

local operation_id = ARGV[1] or ""
local proof_sha256 = ARGV[2] or ""
local transition_id = ARGV[3] or ""
local record_hash = ARGV[4] or ""
local command_hash = ARGV[5] or ""
local revision = ARGV[6] or ""
local run_id = ARGV[7] or ""
local tenant_id = ARGV[8] or ""
local final_state = ARGV[9] or ""
local finalized_at_ms = ARGV[10] or ""
local worker_instance_id = ARGV[11] or ""
local trigger_task_id = ARGV[12] or ""
local trigger_terminal_state = ARGV[13] or ""
local task_count = ARGV[14] or ""
local done_count = ARGV[15] or ""
local failed_count = ARGV[16] or ""
local skipped_count = ARGV[17] or ""
local run_state_ttl = tonumber(ARGV[18])
local run_meta_ttl = tonumber(ARGV[19])
local run_result_ttl = tonumber(ARGV[20])
local results_stream_maxlen = tonumber(ARGV[21])
local payload_json = ARGV[22] or ""
local payload_sha256 = ARGV[23] or ""

local function type_of(key)
    return redis.call("TYPE", key)["ok"]
end

local function result(status, stream_entry_id, detail)
    return {status or "", stream_entry_id or "", detail or ""}
end

local function conflict(detail)
    return result("canonical_run_terminate_projection_conflict", "", detail)
end

local function exact_integer(raw, minimum)
    if raw == "" or string.match(raw, "^%d+$") == nil then
        return nil
    end
    local value = tonumber(raw)
    if not value or value < minimum or value > 9007199254740991 or value ~= math.floor(value) then
        return nil
    end
    return value
end

if operation_id == "" or proof_sha256 == "" or transition_id == "" or
   record_hash == "" or command_hash == "" or run_id == "" or tenant_id == "" then
    return conflict("projection_identity_missing")
end
if string.match(proof_sha256, "^[0-9a-f]+$") == nil or #proof_sha256 ~= 64 or
   string.match(record_hash, "^[0-9a-f]+$") == nil or #record_hash ~= 64 or
   string.match(command_hash, "^[0-9a-f]+$") == nil or #command_hash ~= 64 or
   string.match(payload_sha256, "^[0-9a-f]+$") == nil or #payload_sha256 ~= 64 then
    return conflict("projection_hash_invalid")
end
if final_state ~= "done" and final_state ~= "failed" then
    return conflict("projection_terminal_state_invalid")
end
if not exact_integer(revision, 1) or not exact_integer(finalized_at_ms, 0) or
   not exact_integer(task_count, 1) or not exact_integer(done_count, 0) or
   not exact_integer(failed_count, 0) or not exact_integer(skipped_count, 0) then
    return conflict("projection_numeric_contract_invalid")
end
if tonumber(done_count) + tonumber(failed_count) + tonumber(skipped_count) ~= tonumber(task_count) then
    return conflict("projection_count_mismatch")
end
if final_state == "done" and tonumber(failed_count) ~= 0 then
    return conflict("projection_done_with_failures")
end
if final_state == "failed" and tonumber(failed_count) < 1 then
    return conflict("projection_failed_without_failure")
end
if not run_state_ttl or run_state_ttl <= 0 or not run_meta_ttl or run_meta_ttl <= 0 or
   not run_result_ttl or run_result_ttl <= 0 or not results_stream_maxlen or results_stream_maxlen <= 0 then
    return conflict("projection_ttl_or_stream_limit_invalid")
end
if payload_json == "" then
    return conflict("projection_payload_missing")
end

local receipt_type = type_of(receipt_key)
if receipt_type ~= "none" and receipt_type ~= "hash" then
    return conflict("projection_receipt_wrong_type")
end

local event_id = redis.sha1hex(
    "RUN_TERMINATE\31" .. run_id .. "\31" .. final_state .. "\31" .. task_count
)
local event_type = "RunCompleted"
if final_state == "failed" then
    event_type = "RunFailed"
end
local evidence_field = "run:" .. run_id

local function validate_index_contract()
    if type_of(terminal_event_index_key) ~= "hash" then
        return false, "terminal_event_index_missing_or_wrong_type"
    end
    if redis.call("TTL", terminal_event_index_key) ~= -1 then
        return false, "terminal_event_index_not_persistent"
    end
    if redis.call("HGET", terminal_event_index_key, "__contract__:schema_version") ~= "1" or
       redis.call("HGET", terminal_event_index_key, "__contract__:producer_contract_version") ~= "1" then
        return false, "terminal_event_index_contract_invalid"
    end
    return true, ""
end

local function decode_evidence(raw)
    if not raw or raw == "" then
        return nil, "terminal_event_evidence_missing"
    end
    local ok, decoded = pcall(cjson.decode, raw)
    if not ok or type(decoded) ~= "table" then
        return nil, "terminal_event_evidence_malformed"
    end
    return decoded, ""
end

local function validate_canonical_evidence(stream_entry_id)
    local contract_ok, contract_detail = validate_index_contract()
    if not contract_ok then
        return false, contract_detail
    end
    local evidence, evidence_detail = decode_evidence(
        redis.call("HGET", terminal_event_index_key, evidence_field)
    )
    if not evidence then
        return false, evidence_detail
    end
    local expected = {
        "schema_version", "1",
        "run_id", run_id,
        "tenant_id", tenant_id,
        "event_type", event_type,
        "final_state", final_state,
        "event_id", event_id,
        "source", "canonical_run_terminate",
        "operation_id", operation_id,
        "terminal_proof_sha256", proof_sha256,
        "canonical_transition_id", transition_id,
        "canonical_record_hash", record_hash,
        "canonical_command_hash", command_hash,
        "canonical_revision", revision,
        "stream_entry_id", stream_entry_id,
    }
    for index = 1, #expected, 2 do
        if evidence[expected[index]] ~= expected[index + 1] then
            return false, "terminal_event_evidence_mismatch"
        end
    end
    return true, ""
end

if receipt_type == "hash" then
    local expected = {
        "operation_id", operation_id,
        "terminal_proof_sha256", proof_sha256,
        "canonical_transition_id", transition_id,
        "canonical_record_hash", record_hash,
        "canonical_command_hash", command_hash,
        "canonical_revision", revision,
        "run_id", run_id,
        "tenant_id", tenant_id,
        "final_state", final_state,
        "finalized_at_ms", finalized_at_ms,
        "result_payload_sha256", payload_sha256,
        "event_id", event_id,
    }
    for index = 1, #expected, 2 do
        local stored = redis.call("HGET", receipt_key, expected[index])
        if not stored or stored ~= expected[index + 1] then
            return conflict("projection_receipt_proof_mismatch")
        end
    end
    local stream_entry_id = redis.call("HGET", receipt_key, "stream_entry_id") or ""
    if stream_entry_id == "" then
        return conflict("projection_receipt_missing_stream_entry_id")
    end
    local evidence_ok, evidence_detail = validate_canonical_evidence(stream_entry_id)
    if not evidence_ok then
        return conflict(evidence_detail)
    end

    if type_of(run_state_key) ~= "string" or redis.call("GET", run_state_key) ~= final_state then
        return conflict("projected_run_state_mismatch")
    end
    if type_of(run_meta_key) ~= "hash" or type_of(run_result_key) ~= "hash" then
        return conflict("projected_run_hash_missing_or_wrong_type")
    end
    local meta_expected = {
        "run_id", run_id,
        "tenant_id", tenant_id,
        "state", final_state,
        "finalized_at_ms", finalized_at_ms,
        "finalization_operation", "RUN_TERMINATE",
        "finalization_source", "terminal_task_aggregate",
        "terminal_proof_sha256", proof_sha256,
        "canonical_transition_id", transition_id,
        "canonical_record_hash", record_hash,
        "canonical_revision", revision,
        "result_event_id", event_id,
    }
    for index = 1, #meta_expected, 2 do
        local stored = redis.call("HGET", run_meta_key, meta_expected[index])
        if not stored or stored ~= meta_expected[index + 1] then
            return conflict("projected_run_meta_mismatch")
        end
    end
    local result_expected = {
        "run_id", run_id,
        "tenant_id", tenant_id,
        "status", final_state,
        "payload", payload_json,
        "finalized_at_ms", finalized_at_ms,
        "finalization_operation", "RUN_TERMINATE",
        "finalization_source", "terminal_task_aggregate",
        "terminal_proof_sha256", proof_sha256,
        "canonical_transition_id", transition_id,
        "canonical_record_hash", record_hash,
        "canonical_revision", revision,
        "result_event_id", event_id,
    }
    for index = 1, #result_expected, 2 do
        local stored = redis.call("HGET", run_result_key, result_expected[index])
        if not stored or stored ~= result_expected[index + 1] then
            return conflict("projected_run_result_mismatch")
        end
    end
    local duplicate_running_type = type_of(cp_running_key)
    if duplicate_running_type ~= "none" and duplicate_running_type ~= "zset" then
        return conflict("projected_running_index_wrong_type")
    end
    if duplicate_running_type == "zset" and redis.call("ZSCORE", cp_running_key, run_id) then
        return conflict("projected_running_index_not_cleared")
    end
    local replay_stream_type = type_of(results_stream)
    if replay_stream_type ~= "none" and replay_stream_type ~= "stream" then
        return conflict("projected_results_stream_wrong_type")
    end
    if replay_stream_type == "stream" then
        local rows = redis.call("XRANGE", results_stream, stream_entry_id, stream_entry_id)
        if #rows == 1 then
            local fields = rows[1][2]
            local observed = {}
            for index = 1, #fields, 2 do
                observed[fields[index]] = fields[index + 1]
            end
            if observed["event_id"] ~= event_id or observed["run_id"] ~= run_id or
               observed["terminal_proof_sha256"] ~= proof_sha256 or
               observed["canonical_transition_id"] ~= transition_id or
               observed["canonical_record_hash"] ~= record_hash or
               observed["canonical_revision"] ~= revision then
                return conflict("projected_terminal_event_mismatch")
            end
        elseif #rows > 1 then
            return conflict("projected_terminal_event_duplicate_stream_id")
        end
        -- Zero rows is allowed after stream trimming. The exact receipt and
        -- co-evicted durable evidence index remain the replay authority proof.
    end
    return result("canonical_run_terminate_already_projected", stream_entry_id, "")
end

local contract_ok, contract_detail = validate_index_contract()
if not contract_ok then
    return conflict(contract_detail)
end
local existing_evidence = redis.call("HGET", terminal_event_index_key, evidence_field)
if existing_evidence then
    local decoded, decode_detail = decode_evidence(existing_evidence)
    if not decoded then
        return conflict(decode_detail)
    end
    return conflict("preexisting_terminal_event_evidence")
end

local run_state_type = type_of(run_state_key)
if run_state_type ~= "string" then
    return conflict("legacy_run_state_missing_or_wrong_type")
end
local observed_run_state = redis.call("GET", run_state_key) or ""
local nonterminal_run_states = {
    admitted = true,
    queued = true,
    scheduled = true,
    running = true,
    rescheduled = true,
    pending = true,
}
if not nonterminal_run_states[observed_run_state] then
    return conflict("legacy_terminal_footprint_without_projection_receipt")
end

local meta_type = type_of(run_meta_key)
if meta_type ~= "none" and meta_type ~= "hash" then
    return conflict("run_meta_wrong_type")
end
local result_type = type_of(run_result_key)
if result_type ~= "none" and result_type ~= "hash" then
    return conflict("run_result_wrong_type")
end
if result_type == "hash" and redis.call("HLEN", run_result_key) > 0 then
    return conflict("run_result_preexists_without_projection_receipt")
end
local running_type = type_of(cp_running_key)
if running_type ~= "none" and running_type ~= "zset" then
    return conflict("running_projection_wrong_type")
end
local stream_type = type_of(results_stream)
if stream_type ~= "none" and stream_type ~= "stream" then
    return conflict("results_stream_wrong_type")
end

if meta_type == "hash" then
    local meta_run_id = redis.call("HGET", run_meta_key, "run_id") or ""
    local meta_tenant_id = redis.call("HGET", run_meta_key, "tenant_id") or ""
    if meta_run_id ~= "" and meta_run_id ~= run_id then
        return conflict("run_meta_run_id_mismatch")
    end
    if meta_tenant_id ~= "" and meta_tenant_id ~= tenant_id then
        return conflict("run_meta_tenant_id_mismatch")
    end
end

if redis.call("HGET", terminal_event_index_key, "__migration__:status") ~= "ready" or
   redis.call("HGET", terminal_event_index_key, "__migration__:results_stream_key") ~= results_stream or
   redis.call("HGET", terminal_event_index_key, "__migration__:source_history_complete") ~= "1" then
    return conflict("terminal_event_migration_not_ready")
end

local error_text = ""
if final_state == "failed" then
    error_text = "aggregate_task_failure"
end
local completed_at = tostring(tonumber(finalized_at_ms) / 1000.0)

-- All key types and contradictions are validated before the first write. Redis
-- executes the remaining projection transaction without yielding.
redis.call("SET", run_state_key, final_state, "EX", run_state_ttl)
redis.call(
    "HSET", run_meta_key,
    "run_id", run_id,
    "tenant_id", tenant_id,
    "state", final_state,
    "finalized_at_ms", finalized_at_ms,
    "finalization_operation", "RUN_TERMINATE",
    "finalization_source", "terminal_task_aggregate",
    "finalization_trigger_task_id", trigger_task_id,
    "task_count", task_count,
    "done_count", done_count,
    "failed_count", failed_count,
    "skipped_count", skipped_count,
    "result_event_id", event_id,
    "terminal_proof_sha256", proof_sha256,
    "canonical_transition_id", transition_id,
    "canonical_record_hash", record_hash,
    "canonical_revision", revision
)
redis.call("EXPIRE", run_meta_key, run_meta_ttl)
redis.call(
    "HSET", run_result_key,
    "run_id", run_id,
    "tenant_id", tenant_id,
    "status", final_state,
    "payload", payload_json,
    "cost_cents", "0",
    "tokens_used", "0",
    "error", error_text,
    "completed_at", completed_at,
    "finalized_at_ms", finalized_at_ms,
    "finalization_operation", "RUN_TERMINATE",
    "finalization_source", "terminal_task_aggregate",
    "trigger_task_id", trigger_task_id,
    "task_count", task_count,
    "done_count", done_count,
    "failed_count", failed_count,
    "skipped_count", skipped_count,
    "result_event_id", event_id,
    "terminal_proof_sha256", proof_sha256,
    "canonical_transition_id", transition_id,
    "canonical_record_hash", record_hash,
    "canonical_revision", revision
)
redis.call("EXPIRE", run_result_key, run_result_ttl)
redis.call("ZREM", cp_running_key, run_id)
local stream_entry_id = redis.call(
    "XADD", results_stream, "MAXLEN", "~", results_stream_maxlen, "*",
    "event_id", event_id,
    "event_type", event_type,
    "run_id", run_id,
    "tenant_id", tenant_id,
    "worker_id", worker_instance_id,
    "payload", payload_json,
    "error", error_text,
    "cost_cents", "0",
    "tokens_used", "0",
    "completed_at", completed_at,
    "finalized_at_ms", finalized_at_ms,
    "finalization_operation", "RUN_TERMINATE",
    "finalization_source", "terminal_task_aggregate",
    "trigger_task_id", trigger_task_id,
    "task_count", task_count,
    "done_count", done_count,
    "failed_count", failed_count,
    "skipped_count", skipped_count,
    "terminal_proof_sha256", proof_sha256,
    "canonical_transition_id", transition_id,
    "canonical_record_hash", record_hash,
    "canonical_revision", revision
)
local evidence_json = cjson.encode({
    schema_version = "1",
    run_id = run_id,
    tenant_id = tenant_id,
    event_type = event_type,
    final_state = final_state,
    event_id = event_id,
    source = "canonical_run_terminate",
    operation_id = operation_id,
    terminal_proof_sha256 = proof_sha256,
    canonical_transition_id = transition_id,
    canonical_record_hash = record_hash,
    canonical_command_hash = command_hash,
    canonical_revision = revision,
    stream_entry_id = stream_entry_id
})
redis.call("HSET", terminal_event_index_key, evidence_field, evidence_json)
redis.call("PERSIST", terminal_event_index_key)
redis.call(
    "HSET", receipt_key,
    "operation_id", operation_id,
    "terminal_proof_sha256", proof_sha256,
    "canonical_transition_id", transition_id,
    "canonical_record_hash", record_hash,
    "canonical_command_hash", command_hash,
    "canonical_revision", revision,
    "run_id", run_id,
    "tenant_id", tenant_id,
    "final_state", final_state,
    "finalized_at_ms", finalized_at_ms,
    "result_payload_sha256", payload_sha256,
    "event_id", event_id,
    "stream_entry_id", stream_entry_id
)
redis.call("PERSIST", receipt_key)
return result("canonical_run_terminate_projected", stream_entry_id, "")

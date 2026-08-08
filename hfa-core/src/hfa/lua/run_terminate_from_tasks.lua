-- run_terminate_from_tasks.lua
-- Sprint 83.2: operation-scoped RUN_TERMINATE from aggregate TASK truth.
--
-- This script owns RUN-only terminal projection mutation. It never changes a
-- TASK state, dependency, queue, claim, or output. The caller must invoke it
-- only after a TASK terminal commit or during terminal-delivery recovery.
--
-- KEYS
--  1 run_state_key
--  2 run_meta_key
--  3 run_result_key
--  4 cp_running_zset
--  5 run_tasks_set
--  6 results_stream
--  7 runtime_truth_conflict_index
--  8 runtime_truth_conflict_stream
--  9 terminal-event evidence/readiness index HASH
--
-- ARGV
--  1 run_id
--  2 tenant_id
--  3 trigger_task_id
--  4 finalized_at_ms
--  5 run_state_ttl_seconds
--  6 run_meta_ttl_seconds
--  7 run_result_ttl_seconds
--  8 results_stream_maxlen
--  9 task_state_prefix
-- 10 task_state_suffix
-- 11 task_meta_prefix
-- 12 task_meta_suffix
-- 13 worker_instance_id
-- 14 trigger_terminal_state
--
-- Return
-- [finalized, status, final_state, task_count, done_count, failed_count,
--  skipped_count, already_finalized, ack_allowed]

local run_state_key = KEYS[1]
local run_meta_key = KEYS[2]
local run_result_key = KEYS[3]
local cp_running_zset = KEYS[4]
local run_tasks_set = KEYS[5]
local results_stream = KEYS[6]
local conflict_index = KEYS[7]
local conflict_stream = KEYS[8]
local terminal_event_index_key = KEYS[9]

local run_id = ARGV[1] or ""
local tenant_id = ARGV[2] or ""
local trigger_task_id = ARGV[3] or ""
local finalized_at_ms_raw = ARGV[4] or ""
local run_state_ttl_raw = ARGV[5] or ""
local run_meta_ttl_raw = ARGV[6] or ""
local run_result_ttl_raw = ARGV[7] or ""
local results_stream_maxlen_raw = ARGV[8] or ""
local task_state_prefix = ARGV[9] or ""
local task_state_suffix = ARGV[10] or ""
local task_meta_prefix = ARGV[11] or ""
local task_meta_suffix = ARGV[12] or ""
local worker_instance_id = ARGV[13] or ""
local trigger_terminal_state = ARGV[14] or ""

local finalized_at_ms = tonumber(finalized_at_ms_raw)
local run_state_ttl = tonumber(run_state_ttl_raw)
local run_meta_ttl = tonumber(run_meta_ttl_raw)
local run_result_ttl = tonumber(run_result_ttl_raw)
local results_stream_maxlen = tonumber(results_stream_maxlen_raw)

local function type_of(key)
    return redis.call("TYPE", key)["ok"]
end

local function result(finalized, status, final_state, task_count, done_count,
                      failed_count, skipped_count, already_finalized, ack_allowed)
    return {
        finalized,
        status,
        final_state or "",
        task_count or 0,
        done_count or 0,
        failed_count or 0,
        skipped_count or 0,
        already_finalized or 0,
        ack_allowed or 0,
    }
end

local function conflict_pair_available()
    local index_type = type_of(conflict_index)
    local stream_type = type_of(conflict_stream)
    if index_type ~= "none" and index_type ~= "hash" then
        return false
    end
    if stream_type ~= "none" and stream_type ~= "stream" then
        return false
    end

    local index_exists = redis.call("EXISTS", conflict_index)
    local stream_exists = redis.call("EXISTS", conflict_stream)
    if index_exists ~= stream_exists then
        return false
    end

    if index_exists == 1 then
        local index_count = redis.call("HLEN", conflict_index)
        if redis.call("HEXISTS", conflict_index, "__runtime_truth_conflict_count") == 1 then
            index_count = index_count - 1
        end
        local stream_count = redis.call("XLEN", conflict_stream)
        if index_count ~= stream_count then
            return false
        end
    end
    return true
end

local function persist_conflict(status, detail_code, observed_run_state)
    if not conflict_pair_available() then
        return result(
            0,
            "truth_conflict_evidence_store_unavailable",
            "",
            0,
            0,
            0,
            0,
            0,
            0
        )
    end

    local operation = "RUN_TERMINATE"
    local conflict_id = redis.sha1hex(
        operation .. "\31" .. run_id .. "\31" .. trigger_task_id .. "\31" ..
        status .. "\31" .. detail_code .. "\31" .. (observed_run_state or "")
    )
    local payload = cjson.encode({
        schema_version = 1,
        conflict_id = conflict_id,
        operation = operation,
        run_id = run_id,
        task_id = trigger_task_id,
        status = status,
        conflict_type = status,
        detail_code = detail_code,
        observed_run_state = observed_run_state or "",
        source = "run_terminate_from_tasks.lua",
    })

    local inserted = redis.call("HSETNX", conflict_index, conflict_id, payload)
    if inserted == 1 then
        redis.call("HINCRBY", conflict_index, "__runtime_truth_conflict_count", 1)
        redis.call(
            "XADD",
            conflict_stream,
            "*",
            "conflict_id", conflict_id,
            "operation", operation,
            "run_id", run_id,
            "task_id", trigger_task_id,
            "status", status,
            "conflict_type", status,
            "detail_code", detail_code,
            "observed_run_state", observed_run_state or "",
            "payload", payload
        )
    end

    return result(0, status, "", 0, 0, 0, 0, 0, 0)
end

if run_id == "" then
    return persist_conflict("run_truth_corruption_conflict", "identity_run_id_missing", "")
end
if tenant_id == "" then
    return persist_conflict("run_truth_corruption_conflict", "identity_tenant_id_missing", "")
end
if trigger_task_id == "" then
    return persist_conflict("run_truth_corruption_conflict", "identity_trigger_task_id_missing", "")
end
if not finalized_at_ms or finalized_at_ms < 0 then
    return persist_conflict("run_truth_corruption_conflict", "finalized_at_ms_invalid", "")
end
if not run_state_ttl or run_state_ttl <= 0 or
   not run_meta_ttl or run_meta_ttl <= 0 or
   not run_result_ttl or run_result_ttl <= 0 or
   not results_stream_maxlen or results_stream_maxlen <= 0 then
    return persist_conflict("run_truth_corruption_conflict", "ttl_or_stream_limit_invalid", "")
end
if task_state_prefix == "" or task_meta_prefix == "" then
    return persist_conflict("run_truth_corruption_conflict", "task_key_prefix_missing", "")
end

local run_state_type = type_of(run_state_key)
if run_state_type == "none" then
    return persist_conflict("run_truth_missing", "run_state_missing", "")
end
if run_state_type ~= "string" then
    return persist_conflict("run_truth_corruption_conflict", "run_state_wrong_type", run_state_type)
end

local observed_run_state = redis.call("GET", run_state_key) or ""
if observed_run_state == "" then
    return persist_conflict("run_truth_corruption_conflict", "run_state_empty", observed_run_state)
end

local run_tasks_type = type_of(run_tasks_set)
if run_tasks_type == "none" then
    return persist_conflict("run_truth_corruption_conflict", "run_tasks_missing", observed_run_state)
end
if run_tasks_type ~= "set" then
    return persist_conflict("run_truth_corruption_conflict", "run_tasks_wrong_type", observed_run_state)
end
if redis.call("SCARD", run_tasks_set) == 0 then
    return persist_conflict("run_truth_corruption_conflict", "run_tasks_empty", observed_run_state)
end
if redis.call("SISMEMBER", run_tasks_set, trigger_task_id) ~= 1 then
    return persist_conflict("run_truth_corruption_conflict", "trigger_task_not_in_run", observed_run_state)
end

local run_meta_type = type_of(run_meta_key)
if run_meta_type ~= "none" and run_meta_type ~= "hash" then
    return persist_conflict("run_truth_corruption_conflict", "run_meta_wrong_type", observed_run_state)
end
local run_result_type = type_of(run_result_key)
if run_result_type ~= "none" and run_result_type ~= "hash" then
    return persist_conflict("run_truth_corruption_conflict", "run_result_wrong_type", observed_run_state)
end
local running_type = type_of(cp_running_zset)
if running_type ~= "none" and running_type ~= "zset" then
    return persist_conflict("run_truth_corruption_conflict", "running_projection_wrong_type", observed_run_state)
end
local result_stream_type = type_of(results_stream)
if result_stream_type ~= "none" and result_stream_type ~= "stream" then
    return persist_conflict("run_truth_corruption_conflict", "result_stream_wrong_type", observed_run_state)
end

if run_meta_type == "hash" then
    local meta_run_id = redis.call("HGET", run_meta_key, "run_id") or ""
    local meta_tenant_id = redis.call("HGET", run_meta_key, "tenant_id") or ""
    if meta_run_id ~= "" and meta_run_id ~= run_id then
        return persist_conflict("run_truth_corruption_conflict", "run_meta_run_id_mismatch", observed_run_state)
    end
    if meta_tenant_id ~= "" and meta_tenant_id ~= tenant_id then
        return persist_conflict("run_truth_corruption_conflict", "run_meta_tenant_id_mismatch", observed_run_state)
    end
end

local tasks = redis.call("SMEMBERS", run_tasks_set)
table.sort(tasks)
local task_count = #tasks
local done_count = 0
local failed_count = 0
local skipped_count = 0
local nonterminal_count = 0

local failure_terminal = {
    failed = true,
    blocked_by_failure = true,
    dead_lettered = true,
    rejected = true,
    cancelled = true,
}
local nonterminal = {
    pending = true,
    ready = true,
    scheduled = true,
    running = true,
    admitted = true,
    queued = true,
    rescheduled = true,
}

for _, task_id in ipairs(tasks) do
    local task_state_key = task_state_prefix .. task_id .. task_state_suffix
    local task_meta_key = task_meta_prefix .. task_id .. task_meta_suffix
    local task_state_type = type_of(task_state_key)
    local task_meta_type = type_of(task_meta_key)

    if task_state_type == "none" then
        return persist_conflict("task_truth_corruption_conflict", "task_state_missing:" .. task_id, observed_run_state)
    end
    if task_state_type ~= "string" then
        return persist_conflict("task_truth_corruption_conflict", "task_state_wrong_type:" .. task_id, observed_run_state)
    end
    if task_meta_type == "none" then
        return persist_conflict("task_truth_corruption_conflict", "task_meta_missing:" .. task_id, observed_run_state)
    end
    if task_meta_type ~= "hash" then
        return persist_conflict("task_truth_corruption_conflict", "task_meta_wrong_type:" .. task_id, observed_run_state)
    end

    local meta_task_id = redis.call("HGET", task_meta_key, "task_id") or ""
    local meta_run_id = redis.call("HGET", task_meta_key, "run_id") or ""
    local meta_tenant_id = redis.call("HGET", task_meta_key, "tenant_id") or ""
    if meta_task_id ~= task_id then
        return persist_conflict("task_truth_corruption_conflict", "task_meta_task_id_mismatch:" .. task_id, observed_run_state)
    end
    if meta_run_id ~= run_id then
        return persist_conflict("task_truth_corruption_conflict", "task_meta_run_id_mismatch:" .. task_id, observed_run_state)
    end
    if meta_tenant_id ~= "" and meta_tenant_id ~= tenant_id then
        return persist_conflict("task_truth_corruption_conflict", "task_meta_tenant_id_mismatch:" .. task_id, observed_run_state)
    end

    local task_state = redis.call("GET", task_state_key) or ""
    if task_state == "done" then
        done_count = done_count + 1
    elseif task_state == "skipped" then
        skipped_count = skipped_count + 1
    elseif failure_terminal[task_state] then
        failed_count = failed_count + 1
    elseif nonterminal[task_state] then
        nonterminal_count = nonterminal_count + 1
    else
        return persist_conflict("task_truth_corruption_conflict", "task_state_unknown:" .. task_id .. ":" .. task_state, observed_run_state)
    end
end

if nonterminal_count > 0 then
    return result(0, "not_ready", "", task_count, done_count, failed_count, skipped_count, 0, 1)
end

local final_state = "done"
if failed_count > 0 then
    final_state = "failed"
end

local event_type = "RunCompleted"
if final_state == "failed" then
    event_type = "RunFailed"
end
local event_id = redis.sha1hex(
    "RUN_TERMINATE\31" .. run_id .. "\31" .. final_state .. "\31" .. tostring(task_count)
)
local evidence_field = "run:" .. run_id

local function validate_terminal_event_index()
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

local function decode_terminal_evidence(raw)
    if not raw or raw == "" then
        return nil, "terminal_event_evidence_missing"
    end
    local ok, decoded = pcall(cjson.decode, raw)
    if not ok or type(decoded) ~= "table" then
        return nil, "terminal_event_evidence_malformed"
    end
    return decoded, ""
end

local terminal_run_states = {
    done = true,
    failed = true,
    rejected = true,
    dead_lettered = true,
}
local nonterminal_run_states = {
    admitted = true,
    queued = true,
    scheduled = true,
    running = true,
    rescheduled = true,
    pending = true,
}

if terminal_run_states[observed_run_state] then
    if observed_run_state ~= final_state then
        return persist_conflict("run_truth_terminal_conflict", "run_terminal_outcome_mismatch", observed_run_state)
    end
    if run_result_type ~= "hash" then
        return persist_conflict("run_truth_corruption_conflict", "terminal_run_result_missing", observed_run_state)
    end
    local result_run_id = redis.call("HGET", run_result_key, "run_id") or ""
    local result_tenant_id = redis.call("HGET", run_result_key, "tenant_id") or ""
    local result_status = redis.call("HGET", run_result_key, "status") or ""
    local finalization_source = redis.call("HGET", run_result_key, "finalization_source") or ""
    if result_run_id ~= run_id or result_tenant_id ~= tenant_id or
       result_status ~= final_state or finalization_source ~= "terminal_task_aggregate" then
        return persist_conflict("run_truth_corruption_conflict", "terminal_run_result_mismatch", observed_run_state)
    end
    local index_ok, index_detail = validate_terminal_event_index()
    if not index_ok then
        return persist_conflict("run_truth_corruption_conflict", index_detail, observed_run_state)
    end
    local evidence, evidence_detail = decode_terminal_evidence(
        redis.call("HGET", terminal_event_index_key, evidence_field)
    )
    if not evidence then
        return persist_conflict("run_truth_corruption_conflict", evidence_detail, observed_run_state)
    end
    local evidence_expected = {
        "schema_version", "1",
        "run_id", run_id,
        "tenant_id", tenant_id,
        "event_type", event_type,
        "final_state", final_state,
        "event_id", event_id,
    }
    for index = 1, #evidence_expected, 2 do
        if evidence[evidence_expected[index]] ~= evidence_expected[index + 1] then
            return persist_conflict("run_truth_corruption_conflict", "terminal_event_evidence_mismatch", observed_run_state)
        end
    end
    local evidence_source = evidence["source"] or ""
    if evidence_source ~= "legacy_run_terminate" and evidence_source ~= "historical_backfill" then
        return persist_conflict("run_truth_corruption_conflict", "terminal_event_evidence_source_mismatch", observed_run_state)
    end
    return result(1, "already_finalized", final_state, task_count, done_count,
                  failed_count, skipped_count, 1, 1)
end

if not nonterminal_run_states[observed_run_state] then
    return persist_conflict("run_truth_corruption_conflict", "run_state_unknown", observed_run_state)
end

if run_result_type == "hash" and redis.call("HLEN", run_result_key) > 0 then
    return persist_conflict("run_truth_corruption_conflict", "run_result_preexists_before_terminal", observed_run_state)
end

local index_ok, index_detail = validate_terminal_event_index()
if not index_ok then
    return persist_conflict("run_truth_corruption_conflict", index_detail, observed_run_state)
end
local preexisting_evidence = redis.call("HGET", terminal_event_index_key, evidence_field)
if preexisting_evidence then
    local decoded, decode_detail = decode_terminal_evidence(preexisting_evidence)
    if not decoded then
        return persist_conflict("run_truth_corruption_conflict", decode_detail, observed_run_state)
    end
    return persist_conflict("run_truth_terminal_conflict", "terminal_event_evidence_preexists", observed_run_state)
end

local summary = {
    task_count = task_count,
    done_count = done_count,
    failed_count = failed_count,
    skipped_count = skipped_count,
    trigger_task_id = trigger_task_id,
    trigger_terminal_state = trigger_terminal_state,
}
local payload = cjson.encode(summary)
local error_text = ""
if final_state == "failed" then
    error_text = "aggregate_task_failure"
end
local completed_at = tostring(finalized_at_ms / 1000.0)

-- Every operation below is type-validated before the first write. Redis Lua
-- executes this commit without yielding, so RUN state/result/event/projection
-- converge as one runtime transaction.
redis.call("SET", run_state_key, final_state, "EX", run_state_ttl)
redis.call(
    "HSET",
    run_meta_key,
    "run_id", run_id,
    "tenant_id", tenant_id,
    "state", final_state,
    "finalized_at_ms", finalized_at_ms_raw,
    "finalization_operation", "RUN_TERMINATE",
    "finalization_source", "terminal_task_aggregate",
    "finalization_trigger_task_id", trigger_task_id,
    "task_count", tostring(task_count),
    "done_count", tostring(done_count),
    "failed_count", tostring(failed_count),
    "skipped_count", tostring(skipped_count),
    "result_event_id", event_id
)
redis.call("EXPIRE", run_meta_key, run_meta_ttl)
redis.call(
    "HSET",
    run_result_key,
    "run_id", run_id,
    "tenant_id", tenant_id,
    "status", final_state,
    "payload", payload,
    "cost_cents", "0",
    "tokens_used", "0",
    "error", error_text,
    "completed_at", completed_at,
    "finalized_at_ms", finalized_at_ms_raw,
    "finalization_operation", "RUN_TERMINATE",
    "finalization_source", "terminal_task_aggregate",
    "trigger_task_id", trigger_task_id,
    "task_count", tostring(task_count),
    "done_count", tostring(done_count),
    "failed_count", tostring(failed_count),
    "skipped_count", tostring(skipped_count),
    "result_event_id", event_id
)
redis.call("EXPIRE", run_result_key, run_result_ttl)
redis.call("ZREM", cp_running_zset, run_id)
local stream_entry_id = redis.call(
    "XADD",
    results_stream,
    "MAXLEN", "~", results_stream_maxlen,
    "*",
    "event_id", event_id,
    "event_type", event_type,
    "run_id", run_id,
    "tenant_id", tenant_id,
    "worker_id", worker_instance_id,
    "payload", payload,
    "error", error_text,
    "cost_cents", "0",
    "tokens_used", "0",
    "completed_at", completed_at,
    "finalized_at_ms", finalized_at_ms_raw,
    "finalization_operation", "RUN_TERMINATE",
    "finalization_source", "terminal_task_aggregate",
    "trigger_task_id", trigger_task_id,
    "task_count", tostring(task_count),
    "done_count", tostring(done_count),
    "failed_count", tostring(failed_count),
    "skipped_count", tostring(skipped_count)
)
local evidence_json = cjson.encode({
    schema_version = "1",
    run_id = run_id,
    tenant_id = tenant_id,
    event_type = event_type,
    final_state = final_state,
    event_id = event_id,
    source = "legacy_run_terminate",
    stream_entry_id = stream_entry_id
})
redis.call("HSET", terminal_event_index_key, evidence_field, evidence_json)
redis.call("PERSIST", terminal_event_index_key)

return result(1, "finalized", final_state, task_count, done_count,
              failed_count, skipped_count, 0, 1)

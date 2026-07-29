-- hfa-core/src/hfa/lua/task_heartbeat.lua
-- IRONCLAD Sprint 82.2 — Atomic fenced heartbeat with RUN truth guard.
--
-- KEYS
-- 1  task_state_key
-- 2  task_meta_key
-- 3  task_running_zset
-- 4  run_state_key
-- 5  global_runtime_truth_conflict_index
-- 6  global_runtime_truth_conflict_stream
--
-- ARGV
-- 1  task_id
-- 2  run_id
-- 3  tenant_id
-- 4  worker_instance_id
-- 5  claim_epoch
-- 6  now_ms
--
-- RETURN
-- { status }

local task_state_key        = KEYS[1]
local task_meta_key         = KEYS[2]
local task_running_zset     = KEYS[3]
local run_state_key         = KEYS[4]
local truth_conflict_index  = KEYS[5]
local truth_conflict_stream = KEYS[6]

local task_id                = ARGV[1]
local run_id                 = ARGV[2] or ''
local tenant_id              = ARGV[3]
local worker_instance_id     = ARGV[4]
local expected_claim_epoch   = ARGV[5]
local now_ms                 = ARGV[6]

local OPERATION = 'TASK_HEARTBEAT'
local TRUTH_COUNT_FIELD = '__runtime_truth_conflict_count'

local function redis_type(key)
    local reply = redis.call('TYPE', key)
    if type(reply) == 'table' then return reply['ok'] end
    return reply
end

local function length_prefix(value)
    local text = value or ''
    return tostring(string.len(text)) .. ':' .. text
end

local function truth_conflict_pair_state()
    local index_kind = redis_type(truth_conflict_index)
    local stream_kind = redis_type(truth_conflict_stream)
    if index_kind ~= 'none' and index_kind ~= 'hash' then
        return false, 'truth_conflict_index_type_mismatch'
    end
    if stream_kind ~= 'none' and stream_kind ~= 'stream' then
        return false, 'truth_conflict_stream_type_mismatch'
    end
    if (index_kind == 'none') ~= (stream_kind == 'none') then
        return false, 'truth_conflict_pair_missing_member'
    end
    if index_kind == 'none' then return true, 0 end
    local count_raw = redis.call('HGET', truth_conflict_index, TRUTH_COUNT_FIELD)
    local count = count_raw and tonumber(count_raw) or nil
    if not count or count < 0 or count % 1 ~= 0 then
        return false, 'truth_conflict_count_missing_or_invalid'
    end
    if redis.call('HLEN', truth_conflict_index) ~= count + 1
        or redis.call('XLEN', truth_conflict_stream) ~= count then
        return false, 'truth_conflict_pair_cardinality_mismatch'
    end
    return true, count
end

local function emit_truth_conflict(status, detail_code, observed_run_state)
    local pair_ok, pair_value = truth_conflict_pair_state()
    if not pair_ok then
        return {'truth_conflict_evidence_store_unavailable'}
    end
    local count = pair_value
    local material = length_prefix(OPERATION)
        .. length_prefix(run_id)
        .. length_prefix(task_id)
        .. length_prefix(status)
        .. length_prefix(detail_code)
        .. length_prefix(observed_run_state or '')
    local conflict_id = redis.sha1hex(material)
    local state_json = observed_run_state == nil and cjson.null or observed_run_state
    local payload = cjson.encode({
        conflict_id=conflict_id,
        operation=OPERATION,
        conflict_type=status,
        detail_code=detail_code,
        run_id=run_id,
        task_id=task_id,
        observed_run_state=state_json,
        observed_at_ms=tonumber(now_ms),
        claim_epoch=expected_claim_epoch,
        worker_instance_id=worker_instance_id
    })
    local inserted = redis.call('HSETNX', truth_conflict_index, conflict_id, payload)
    if inserted == 1 then
        count = count + 1
        redis.call('HSET', truth_conflict_index, TRUTH_COUNT_FIELD, tostring(count))
        redis.call('XADD', truth_conflict_stream, '*',
            'conflict_id', conflict_id,
            'operation', OPERATION,
            'conflict_type', status,
            'detail_code', detail_code,
            'run_id', run_id,
            'task_id', task_id,
            'observed_run_state', observed_run_state or '',
            'observed_at_ms', now_ms,
            'claim_epoch', expected_claim_epoch,
            'worker_instance_id', worker_instance_id,
            'conflict_json', payload)
    else
        local raw = redis.call('HGET', truth_conflict_index, conflict_id)
        local ok, existing = pcall(cjson.decode, raw)
        local expected_state = observed_run_state == nil and cjson.null or observed_run_state
        if not ok or type(existing) ~= 'table'
            or existing.conflict_id ~= conflict_id
            or existing.operation ~= OPERATION
            or existing.conflict_type ~= status
            or existing.detail_code ~= detail_code
            or existing.run_id ~= run_id
            or existing.task_id ~= task_id
            or existing.observed_run_state ~= expected_state then
            return {'truth_conflict_evidence_store_unavailable'}
        end
    end
    return {status}
end

local current_state = redis.call('GET', task_state_key)
if current_state ~= 'running' then
    return {'illegal_transition'}
end

if redis.call('EXISTS', task_meta_key) == 0 then
    return {'missing_task_meta'}
end
local authoritative_identity = redis.call('HMGET', task_meta_key, 'task_id', 'run_id')
local authoritative_task_id = authoritative_identity[1]
local authoritative_run_id = authoritative_identity[2]
if not authoritative_task_id or authoritative_task_id == '' then
    return {'identity_task_id_missing'}
end
if authoritative_task_id ~= task_id then
    return {'identity_task_id_mismatch'}
end
if not authoritative_run_id or authoritative_run_id == '' then
    return {'identity_run_id_missing'}
end
if authoritative_run_id ~= run_id then
    return {'identity_run_id_mismatch'}
end

local run_kind = redis_type(run_state_key)
if run_kind == 'none' then
    return emit_truth_conflict('run_truth_missing', 'run_state_missing', nil)
end
if run_kind ~= 'string' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_key_type_mismatch', run_kind)
end
local run_state = redis.call('GET', run_state_key)
if not run_state or run_state == '' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_empty_or_unreadable', run_state)
end
local nonterminal = {
    admitted=true, queued=true, scheduled=true, running=true, rescheduled=true
}
local terminal = {
    done=true, failed=true, rejected=true, dead_lettered=true
}
if terminal[run_state] then
    return emit_truth_conflict('run_truth_terminal_conflict', 'run_state_terminal', run_state)
end
if not nonterminal[run_state] then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_unknown', run_state)
end

local fence = redis.call('HMGET', task_meta_key,
    'worker_instance_id',
    'claim_epoch',
    'heartbeat_owner'
)
local stored_worker = fence[1] or ''
local stored_claim_epoch = fence[2] or ''
local stored_hb_owner = fence[3] or ''
local effective_owner = stored_worker
if effective_owner == '' then
    effective_owner = stored_hb_owner
end

if effective_owner ~= '' and effective_owner ~= worker_instance_id then
    return {'owner_mismatch'}
end
if expected_claim_epoch ~= '' and stored_claim_epoch ~= expected_claim_epoch then
    return {'claim_epoch_mismatch'}
end

redis.call('HSET', task_meta_key,
    'last_heartbeat_at_ms', now_ms,
    'heartbeat_at_ms', now_ms,
    'heartbeat_owner', worker_instance_id
)
redis.call('ZADD', task_running_zset, tonumber(now_ms), task_id)

return {'heartbeat_accepted'}

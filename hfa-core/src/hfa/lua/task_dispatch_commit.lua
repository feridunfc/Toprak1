-- hfa-core/src/hfa/lua/task_dispatch_commit.lua
-- IRONCLAD Sprint 82.1 — Atomic DAG dispatch commit with RUN runtime-truth guard.
--
-- KEYS
-- 1  task_state_key
-- 2  task_meta_key
-- 3  task_scheduled_zset
-- 4  control_stream_key
-- 5  shard_stream_key
-- 6  tenant_ready_queue
-- 7  task_running_zset
-- 8  run_state_key
-- 9  global_runtime_truth_conflict_index
-- 10 global_runtime_truth_conflict_stream
--
-- ARGV remains backward compatible with the pre-Sprint-82.1 contract.

local task_state_key        = KEYS[1]
local task_meta_key         = KEYS[2]
local task_scheduled_zset   = KEYS[3]
local control_stream        = KEYS[4]
local shard_stream          = KEYS[5]
local tenant_ready_queue    = KEYS[6]
local task_running_zset     = KEYS[7]
local run_state_key         = KEYS[8]
local truth_conflict_index  = KEYS[9]
local truth_conflict_stream = KEYS[10]

local task_id               = ARGV[1]
local run_id                = ARGV[2]
local tenant_id             = ARGV[3]
local agent_type            = ARGV[4]
local worker_group          = ARGV[5]
local shard                 = tostring(ARGV[6])
local priority              = tostring(ARGV[7])
local admitted_at           = tostring(ARGV[8])
local scheduled_at          = tostring(ARGV[9])
local task_state_ttl        = tonumber(ARGV[10]) or 86400
local task_meta_ttl         = tonumber(ARGV[11]) or 86400
local control_maxlen        = tonumber(ARGV[12]) or 10000
local shard_maxlen          = tonumber(ARGV[13]) or 10000
local trace_parent          = ARGV[14] or ''
local trace_state           = ARGV[15] or ''
local policy                = ARGV[16] or 'LEAST_LOADED'
local region                = ARGV[17] or ''
local payload_json          = ARGV[18] or '{}'
local scheduler_epoch       = ARGV[19] or ''

local OPERATION = 'TASK_DISPATCH'
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
        return {'truth_conflict_evidence_store_unavailable', pair_value}
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
        observed_at_ms=tonumber(scheduled_at),
        scheduler_epoch=scheduler_epoch
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
            'observed_at_ms', scheduled_at,
            'scheduler_epoch', scheduler_epoch,
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
            return {'truth_conflict_evidence_store_unavailable', 'truth_conflict_index_identity_mismatch'}
        end
    end
    return {status, detail_code}
end

-- Establish exact task/run identity before the supplied RUN key is read.
-- These reads do not mutate lifecycle state.
local current = redis.call('GET', task_state_key)
if not current then return {'missing_state', ''} end

if redis.call('EXISTS', task_meta_key) == 0 then return {'missing_task_meta', ''} end
local authoritative_identity = redis.call('HMGET', task_meta_key, 'task_id', 'run_id')
local authoritative_task_id = authoritative_identity[1]
local authoritative_run_id  = authoritative_identity[2]
if not authoritative_task_id or authoritative_task_id == '' then return {'identity_task_id_missing', ''} end
if authoritative_task_id ~= task_id then return {'identity_task_id_mismatch', authoritative_task_id} end
if not authoritative_run_id or authoritative_run_id == '' then return {'identity_run_id_missing', ''} end
if authoritative_run_id ~= run_id then return {'identity_run_id_mismatch', authoritative_run_id} end

-- Existing task guard statuses remain unchanged once exact identity is known.
if current == 'running' then return {'already_running', current} end
if current == 'scheduled' then return {'already_scheduled', current} end
if current == 'done' or current == 'failed' or current == 'blocked_by_failure'
        or current == 'dead_lettered' or current == 'skipped' then
    return {'illegal_transition', current}
end
if current ~= 'ready' then return {'state_conflict', current} end

-- RUN truth is authoritative after exact task/run identity validation. This
-- guard and its durable observation happen before every lifecycle mutation.
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

redis.call('ZREM', tenant_ready_queue, task_id)
redis.call('SET', task_state_key, 'scheduled', 'EX', task_state_ttl)
redis.call('HSET', task_meta_key,
    'task_id', task_id, 'run_id', run_id, 'tenant_id', tenant_id,
    'agent_type', agent_type, 'worker_group', worker_group, 'shard', shard,
    'priority', priority, 'admitted_at', admitted_at, 'scheduled_at', scheduled_at,
    'dispatch_policy', policy, 'dispatch_region', region, 'payload_json', payload_json,
    'trace_parent', trace_parent, 'trace_state', trace_state,
    'scheduler_epoch', scheduler_epoch)
redis.call('EXPIRE', task_meta_key, task_meta_ttl)
redis.call('ZADD', task_scheduled_zset, scheduled_at, task_id)
redis.call('EXPIRE', task_scheduled_zset, task_meta_ttl)
redis.call('XADD', control_stream, 'MAXLEN', '~', control_maxlen, '*',
    'event_type', 'TaskScheduled', 'task_id', task_id, 'run_id', run_id,
    'tenant_id', tenant_id, 'agent_type', agent_type, 'worker_group', worker_group,
    'shard', shard, 'region', region, 'policy', policy, 'scheduled_at', scheduled_at,
    'trace_parent', trace_parent, 'trace_state', trace_state,
    'scheduler_epoch', scheduler_epoch)
redis.call('XADD', shard_stream, 'MAXLEN', '~', shard_maxlen, '*',
    'event_type', 'TaskRequested', 'task_id', task_id, 'run_id', run_id,
    'tenant_id', tenant_id, 'agent_type', agent_type, 'worker_group', worker_group,
    'shard', shard, 'priority', priority, 'payload_json', payload_json,
    'requested_at', scheduled_at, 'trace_parent', trace_parent,
    'trace_state', trace_state, 'scheduler_epoch', scheduler_epoch)
return {'committed', current}

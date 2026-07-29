-- hfa-core/src/hfa/lua/task_requeue.lua
-- Sprint 82.3 — Monotonic requeue with atomic TASK/RUN truth convergence.
--
-- claim_epoch is a lifetime generation counter and is never reset.  Requeue
-- clears worker/scheduler ownership only; the next claim increments the stored
-- epoch so stale worker heartbeats and completions remain fenced.
--
-- KEYS
-- 1  task_state_key
-- 2  task_meta_key
-- 3  tenant_ready_queue
-- 4  task_running_zset
-- 5  completion_stream
-- 6  run_state_key
-- 7  global_runtime_truth_conflict_index
-- 8  global_runtime_truth_conflict_stream
--
-- ARGV
-- 1  task_id
-- 2  run_id
-- 3  tenant_id
-- 4  expected_state       must be "running"
-- 5  now_ms
-- 6  ready_score
-- 7  max_requeue_count
-- 8  reason_code
-- 9  stream_maxlen
--
-- RETURN
-- { status, value }
--   TASK_REQUEUED        value = requeue_count
--   TASK_RETRY_EXHAUSTED value = requeue_count
--   TASK_ALREADY_REQUEUED value = requeue_count
--   TASK_TERMINAL        value = current state
--   TASK_STATE_CONFLICT  value = current state/detail
--   run_truth_*          value = 0
--   task_truth_*         value = 0

local task_state_key       = KEYS[1]
local task_meta_key        = KEYS[2]
local tenant_ready_queue   = KEYS[3]
local task_running_zset    = KEYS[4]
local completion_stream    = KEYS[5]
local run_state_key        = KEYS[6]
local truth_conflict_index = KEYS[7]
local truth_conflict_stream = KEYS[8]

local task_id           = ARGV[1]
local run_id            = ARGV[2] or ''
local tenant_id         = ARGV[3]
local expected_state    = ARGV[4]
local now_ms            = ARGV[5]
local ready_score       = ARGV[6]
local max_requeue_count = tonumber(ARGV[7])
local reason_code       = ARGV[8]
local stream_maxlen     = tonumber(ARGV[9])

local OPERATION = 'TASK_REQUEUE'
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

local function task_terminal(s)
    return s == 'done'
        or s == 'failed'
        or s == 'blocked_by_failure'
        or s == 'dead_lettered'
        or s == 'skipped'
end

local function failure(status, value)
    return {status, value or '0'}
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

local function emit_truth_conflict(status, detail_code, observed_run_state, observed_task_state)
    local pair_ok, pair_value = truth_conflict_pair_state()
    if not pair_ok then
        return failure('truth_conflict_evidence_store_unavailable')
    end

    local count = pair_value
    local material = length_prefix(OPERATION)
        .. length_prefix(run_id)
        .. length_prefix(task_id)
        .. length_prefix(status)
        .. length_prefix(detail_code)
        .. length_prefix(observed_run_state or '')
    local conflict_id = redis.sha1hex(material)
    local run_state_json = observed_run_state == nil and cjson.null or observed_run_state
    local task_state_json = observed_task_state == nil and cjson.null or observed_task_state
    local payload = cjson.encode({
        conflict_id=conflict_id,
        operation=OPERATION,
        conflict_type=status,
        detail_code=detail_code,
        run_id=run_id,
        task_id=task_id,
        observed_run_state=run_state_json,
        observed_task_state=task_state_json,
        observed_at_ms=tonumber(now_ms),
        tenant_id=tenant_id,
        reason_code=reason_code
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
            'observed_task_state', observed_task_state or '',
            'observed_at_ms', now_ms,
            'tenant_id', tenant_id,
            'reason_code', reason_code,
            'conflict_json', payload)
    else
        local raw = redis.call('HGET', truth_conflict_index, conflict_id)
        local ok, existing = pcall(cjson.decode, raw)
        local expected_run_state = observed_run_state == nil and cjson.null or observed_run_state
        local expected_task_state = observed_task_state == nil and cjson.null or observed_task_state
        if not ok or type(existing) ~= 'table'
            or existing.conflict_id ~= conflict_id
            or existing.operation ~= OPERATION
            or existing.conflict_type ~= status
            or existing.detail_code ~= detail_code
            or existing.run_id ~= run_id
            or existing.task_id ~= task_id
            or existing.observed_run_state ~= expected_run_state
            or existing.observed_task_state ~= expected_task_state then
            return failure('truth_conflict_evidence_store_unavailable')
        end
    end
    return failure(status)
end

-- Identity is authoritative before any RUN key is read or conflict is recorded.
local meta_kind = redis_type(task_meta_key)
if meta_kind == 'none' then
    return failure('TASK_STATE_CONFLICT', 'missing_task_meta')
end
if meta_kind ~= 'hash' then
    return failure('TASK_STATE_CONFLICT', 'task_meta_type_mismatch')
end

local authoritative_identity = redis.call('HMGET', task_meta_key, 'task_id', 'run_id', 'tenant_id')
local authoritative_task_id = authoritative_identity[1]
local authoritative_run_id = authoritative_identity[2]
local authoritative_tenant_id = authoritative_identity[3]
if not authoritative_task_id or authoritative_task_id == '' then
    return failure('identity_task_id_missing')
end
if authoritative_task_id ~= task_id then
    return failure('identity_task_id_mismatch')
end
if not authoritative_run_id or authoritative_run_id == '' then
    return failure('identity_run_id_missing')
end
if authoritative_run_id ~= run_id then
    return failure('identity_run_id_mismatch')
end
if authoritative_tenant_id and authoritative_tenant_id ~= '' and authoritative_tenant_id ~= tenant_id then
    return failure('identity_tenant_id_mismatch')
end

local run_kind = redis_type(run_state_key)
local observed_run_state = nil
if run_kind == 'string' then
    observed_run_state = redis.call('GET', run_state_key)
elseif run_kind ~= 'none' then
    observed_run_state = run_kind
end

local task_kind = redis_type(task_state_key)
if task_kind == 'none' then
    return emit_truth_conflict('task_truth_missing', 'task_state_missing', observed_run_state, nil)
end
if task_kind ~= 'string' then
    return emit_truth_conflict('task_truth_corruption_conflict', 'task_state_key_type_mismatch', observed_run_state, task_kind)
end
local current_state = redis.call('GET', task_state_key)
if not current_state or current_state == '' then
    return emit_truth_conflict('task_truth_corruption_conflict', 'task_state_empty_or_unreadable', observed_run_state, current_state)
end

if task_terminal(current_state) then
    return {'TASK_TERMINAL', current_state}
end
if current_state ~= expected_state then
    if current_state == 'ready' then
        local existing_count = tonumber(redis.call('HGET', task_meta_key, 'requeue_count') or '0') or 0
        if existing_count > 0 then
            return {'TASK_ALREADY_REQUEUED', tostring(existing_count)}
        end
    end
    return {'TASK_STATE_CONFLICT', current_state}
end

-- A new requeue is authorized only by a known nonterminal RUN truth.
if run_kind == 'none' then
    return emit_truth_conflict('run_truth_missing', 'run_state_missing', nil, current_state)
end
if run_kind ~= 'string' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_key_type_mismatch', run_kind, current_state)
end
local run_state = observed_run_state
if not run_state or run_state == '' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_empty_or_unreadable', run_state, current_state)
end
local nonterminal = {
    admitted=true, queued=true, scheduled=true, running=true, rescheduled=true
}
local terminal = {
    done=true, failed=true, rejected=true, dead_lettered=true
}
if terminal[run_state] then
    return emit_truth_conflict('run_truth_terminal_conflict', 'run_state_terminal', run_state, current_state)
end
if not nonterminal[run_state] then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_unknown', run_state, current_state)
end

local raw_count = redis.call('HGET', task_meta_key, 'requeue_count')
local retries = tonumber(raw_count or '0') or 0
retries = retries + 1

redis.call('ZREM', task_running_zset, task_id)

if retries > max_requeue_count then
    redis.call('SET', task_state_key, 'failed')
    redis.call('HSET', task_meta_key,
        'requeue_count',        tostring(retries),
        'last_requeue_reason',  reason_code,
        'failed_at_ms',         now_ms,
        'worker_instance_id',   '',
        'scheduler_epoch',      '',
        'last_heartbeat_at_ms', '0'
    )
    redis.call('XADD', completion_stream, 'MAXLEN', '~', stream_maxlen, '*',
        'event_type',    'TaskFailed',
        'task_id',       task_id,
        'run_id',        run_id,
        'tenant_id',     tenant_id,
        'reason_code',   'STALE_RETRY_EXHAUSTED',
        'requeue_count', tostring(retries),
        'at_ms',         now_ms
    )
    return {'TASK_RETRY_EXHAUSTED', tostring(retries)}
end

redis.call('SET', task_state_key, 'ready')
redis.call('HSET', task_meta_key,
    'requeue_count',        tostring(retries),
    'last_requeue_reason',  reason_code,
    'last_requeue_at_ms',   now_ms,
    'worker_instance_id',   '',
    'scheduler_epoch',      '',
    'last_heartbeat_at_ms', '0'
)
redis.call('ZADD', tenant_ready_queue, 'NX', ready_score, task_id)
redis.call('XADD', completion_stream, 'MAXLEN', '~', stream_maxlen, '*',
    'event_type',    'TaskRequeued',
    'task_id',       task_id,
    'run_id',        run_id,
    'tenant_id',     tenant_id,
    'reason_code',   reason_code,
    'requeue_count', tostring(retries),
    'at_ms',         now_ms
)

return {'TASK_REQUEUED', tostring(retries)}

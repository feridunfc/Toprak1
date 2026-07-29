-- hfa-core/src/hfa/lua/run_recovery_commit.lua
-- Sprint 82.3 — Atomic run recovery mutation with TASK/RUN truth convergence.
--
-- The Python caller pre-reads the run task SET only to construct dynamic KEYS.
-- This script revalidates the SET type/cardinality/membership and every task
-- identity before it mutates RUN state, metadata, the running projection or
-- appends scheduler-facing recovery events.
--
-- KEYS
-- 1  run_state_key
-- 2  run_meta_key
-- 3  running_zset
-- 4  run_tasks_set
-- 5  global_runtime_truth_conflict_index
-- 6  global_runtime_truth_conflict_stream
-- 7  control_stream
-- 8+ task_state_key, task_meta_key pairs in task_id order
--
-- ARGV
-- 1  run_id
-- 2  now_ms
-- 3  running_score
-- 4  max_reschedule_attempts
-- 5  expected_reschedule_count
-- 6  requested_action        RESCHEDULE | DEAD_LETTER
-- 7  run_state_ttl_seconds
-- 8  run_meta_ttl_seconds
-- 9  task_count
-- 10 reason_code
-- 11 stream_maxlen
-- 12.. task_ids in the same order as KEYS pairs
--
-- RETURN
-- { status, reschedule_count, observed_run_state, conflict_task_id }

local run_state_key         = KEYS[1]
local run_meta_key          = KEYS[2]
local running_zset          = KEYS[3]
local run_tasks_set         = KEYS[4]
local truth_conflict_index  = KEYS[5]
local truth_conflict_stream = KEYS[6]
local control_stream        = KEYS[7]

local run_id                    = ARGV[1] or ''
local now_ms                    = ARGV[2] or ''
local running_score             = ARGV[3] or ''
local max_reschedule_attempts   = tonumber(ARGV[4])
local expected_reschedule_count = tonumber(ARGV[5])
local requested_action          = ARGV[6] or ''
local run_state_ttl             = tonumber(ARGV[7])
local run_meta_ttl              = tonumber(ARGV[8])
local task_count                = tonumber(ARGV[9])
local reason_code               = ARGV[10] or ''
local stream_maxlen             = tonumber(ARGV[11])
local now_ms_number             = tonumber(now_ms)
local running_score_number      = tonumber(running_score)

local OPERATION = 'RUN_RECOVERY'
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

local function failure(status, observed_run_state, task_id)
    return {
        status,
        tostring(expected_reschedule_count or 0),
        observed_run_state or '',
        task_id or ''
    }
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

local function emit_truth_conflict(status, detail_code, observed_run_state, task_id, observed_task_state)
    local pair_ok, pair_value = truth_conflict_pair_state()
    if not pair_ok then
        return failure('truth_conflict_evidence_store_unavailable', observed_run_state, task_id)
    end

    local count = pair_value
    local material = length_prefix(OPERATION)
        .. length_prefix(run_id)
        .. length_prefix(task_id or '')
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
        task_id=task_id or '',
        observed_run_state=run_state_json,
        observed_task_state=task_state_json,
        observed_at_ms=now_ms_number,
        requested_action=requested_action,
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
            'task_id', task_id or '',
            'observed_run_state', observed_run_state or '',
            'observed_task_state', observed_task_state or '',
            'observed_at_ms', now_ms,
            'requested_action', requested_action,
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
            or existing.task_id ~= (task_id or '')
            or existing.observed_run_state ~= expected_run_state
            or existing.observed_task_state ~= expected_task_state then
            return failure('truth_conflict_evidence_store_unavailable', observed_run_state, task_id)
        end
    end
    return failure(status, observed_run_state, task_id)
end

if run_id == '' then
    return failure('identity_run_id_missing')
end
if requested_action ~= 'RESCHEDULE' and requested_action ~= 'DEAD_LETTER' then
    return failure('invalid_recovery_action')
end
if not now_ms_number or now_ms_number < 0
    or not running_score_number
    or not max_reschedule_attempts or max_reschedule_attempts < 0
        or max_reschedule_attempts % 1 ~= 0
    or not expected_reschedule_count or expected_reschedule_count < 0
        or expected_reschedule_count % 1 ~= 0
    or not run_state_ttl or run_state_ttl <= 0 or run_state_ttl % 1 ~= 0
    or not run_meta_ttl or run_meta_ttl <= 0 or run_meta_ttl % 1 ~= 0
    or not task_count or task_count < 0 or task_count % 1 ~= 0
    or not stream_maxlen or stream_maxlen <= 0 or stream_maxlen % 1 ~= 0 then
    return failure('invalid_recovery_contract')
end
if #KEYS ~= 7 + (task_count * 2) or #ARGV ~= 11 + task_count then
    return failure('recovery_contract_cardinality_mismatch')
end

local run_kind = redis_type(run_state_key)
if run_kind == 'none' then
    return emit_truth_conflict('run_truth_missing', 'run_state_missing', nil, '', nil)
end
if run_kind ~= 'string' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_key_type_mismatch', run_kind, '', nil)
end
local run_state = redis.call('GET', run_state_key)
if not run_state or run_state == '' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_empty_or_unreadable', run_state, '', nil)
end

local meta_kind = redis_type(run_meta_key)
if meta_kind == 'none' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_meta_missing', run_state, '', nil)
end
if meta_kind ~= 'hash' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_meta_type_mismatch', run_state, '', nil)
end
local run_identity = redis.call(
    'HMGET',
    run_meta_key,
    'run_id',
    'tenant_id',
    'agent_type',
    'worker_group'
)
local stored_run_id = run_identity[1]
local tenant_id = run_identity[2]
local agent_type = run_identity[3] or ''
local previous_worker = run_identity[4] or ''
if stored_run_id and stored_run_id ~= '' and stored_run_id ~= run_id then
    return failure('identity_run_id_mismatch', run_state, '')
end
if not tenant_id or tenant_id == '' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_tenant_id_missing', run_state, '', nil)
end

local task_set_kind = redis_type(run_tasks_set)
if task_set_kind == 'none' then
    return emit_truth_conflict('task_truth_missing', 'run_task_index_missing', run_state, '', nil)
end
if task_set_kind ~= 'set' then
    return emit_truth_conflict('task_truth_corruption_conflict', 'run_task_index_type_mismatch', run_state, '', task_set_kind)
end
if redis.call('SCARD', run_tasks_set) ~= task_count then
    return emit_truth_conflict('task_truth_corruption_conflict', 'run_task_index_cardinality_mismatch', run_state, '', tostring(task_count))
end
if task_count == 0 then
    return emit_truth_conflict('task_truth_missing', 'run_has_no_task_authority', run_state, '', nil)
end

local terminal_run_states = {
    done=true, failed=true, rejected=true, dead_lettered=true
}
local actionable_run_states = {
    running=true, scheduled=true, rescheduled=true
}
local run_is_terminal = terminal_run_states[run_state] == true
local first_task_id = ''
local first_task_state = ''

for index = 1, task_count do
    local task_id = ARGV[11 + index]
    local task_state_key = KEYS[6 + (index * 2)]
    local task_meta_key = KEYS[7 + (index * 2)]

    if not task_id or task_id == '' or redis.call('SISMEMBER', run_tasks_set, task_id) ~= 1 then
        return emit_truth_conflict('task_truth_corruption_conflict', 'run_task_membership_mismatch', run_state, task_id or '', nil)
    end

    local task_meta_kind = redis_type(task_meta_key)
    if task_meta_kind == 'none' then
        return emit_truth_conflict('task_truth_missing', 'task_meta_missing', run_state, task_id, nil)
    end
    if task_meta_kind ~= 'hash' then
        return emit_truth_conflict('task_truth_corruption_conflict', 'task_meta_type_mismatch', run_state, task_id, task_meta_kind)
    end
    local task_identity = redis.call('HMGET', task_meta_key, 'task_id', 'run_id')
    if not task_identity[1] or task_identity[1] == '' then
        return emit_truth_conflict('task_truth_corruption_conflict', 'task_id_missing', run_state, task_id, nil)
    end
    if task_identity[1] ~= task_id then
        return emit_truth_conflict('task_truth_corruption_conflict', 'task_id_mismatch', run_state, task_id, nil)
    end
    if not task_identity[2] or task_identity[2] == '' then
        return emit_truth_conflict('task_truth_corruption_conflict', 'task_run_id_missing', run_state, task_id, nil)
    end
    if task_identity[2] ~= run_id then
        return emit_truth_conflict('task_truth_corruption_conflict', 'task_run_id_mismatch', run_state, task_id, nil)
    end

    local task_kind = redis_type(task_state_key)
    if task_kind == 'none' then
        return emit_truth_conflict('task_truth_missing', 'task_state_missing', run_state, task_id, nil)
    end
    if task_kind ~= 'string' then
        return emit_truth_conflict('task_truth_corruption_conflict', 'task_state_key_type_mismatch', run_state, task_id, task_kind)
    end
    local task_state = redis.call('GET', task_state_key)
    if not task_state or task_state == '' then
        return emit_truth_conflict('task_truth_corruption_conflict', 'task_state_empty_or_unreadable', run_state, task_id, task_state)
    end
    if index == 1 then
        first_task_id = task_id
        first_task_state = task_state
    end

    local task_is_terminal = task_state == 'done'
        or task_state == 'failed'
        or task_state == 'blocked_by_failure'
        or task_state == 'dead_lettered'
        or task_state == 'skipped'
    if run_is_terminal ~= task_is_terminal then
        local detail = task_is_terminal and 'task_terminal_run_nonterminal' or 'run_terminal_task_nonterminal'
        local status = task_is_terminal and 'task_truth_terminal_conflict' or 'run_truth_terminal_conflict'
        return emit_truth_conflict(status, detail, run_state, task_id, task_state)
    end
end

-- A terminal RUN in the active projection is never silently cleaned up. Even
-- when every task is terminal, the stale projection is an explicit candidate.
if run_is_terminal then
    return emit_truth_conflict('run_truth_terminal_conflict', 'terminal_run_in_running_projection', run_state, '', nil)
end
if actionable_run_states[run_state] ~= true then
    return emit_truth_conflict('run_truth_corruption_conflict', 'run_state_not_recovery_actionable', run_state, '', nil)
end

-- Terminalizing RUN while TASK remains nonterminal would create a new
-- contradiction. Coordinated TASK terminalization requires an explicit
-- reconciliation command and is deliberately outside this automatic sweep.
if requested_action == 'DEAD_LETTER' then
    return emit_truth_conflict(
        'task_truth_terminal_conflict',
        'dead_letter_requires_explicit_task_terminalization',
        run_state,
        first_task_id,
        first_task_state
    )
end

-- Redis scripts do not roll back earlier writes after a runtime command error.
-- Validate every mutation target before the first lifecycle mutation.
local running_kind = redis_type(running_zset)
if running_kind ~= 'none' and running_kind ~= 'zset' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'running_projection_type_mismatch', run_state, '', running_kind)
end
local control_kind = redis_type(control_stream)
if control_kind ~= 'none' and control_kind ~= 'stream' then
    return emit_truth_conflict('run_truth_corruption_conflict', 'control_stream_type_mismatch', run_state, '', control_kind)
end

local raw_count = redis.call('HGET', run_meta_key, 'reschedule_count')
local stored_count = tonumber(raw_count or '0')
if not stored_count or stored_count < 0 or stored_count % 1 ~= 0 then
    return emit_truth_conflict('run_truth_corruption_conflict', 'reschedule_count_invalid', run_state, '', raw_count)
end
if stored_count ~= expected_reschedule_count then
    return failure('RUN_RECOVERY_COUNT_CONFLICT', run_state, '')
end
if stored_count >= max_reschedule_attempts then
    return failure('RUN_RECOVERY_DECISION_CONFLICT', run_state, '')
end

local new_count = stored_count + 1
redis.call('SET', run_state_key, 'rescheduled', 'EX', run_state_ttl)
redis.call('HSET', run_meta_key,
    'state', 'rescheduled',
    'reschedule_count', tostring(new_count),
    'rescheduled_at_ms', now_ms,
    'last_recovery_reason', reason_code
)
redis.call('EXPIRE', run_meta_key, run_meta_ttl)
redis.call('ZADD', running_zset, running_score, run_id)
redis.call('XADD', control_stream, 'MAXLEN', '~', stream_maxlen, '*',
    'event_type', 'RunRescheduled',
    'run_id', run_id,
    'tenant_id', tenant_id,
    'previous_worker', previous_worker,
    'reschedule_count', tostring(new_count),
    'reason', reason_code,
    'rescheduled_at', tostring(now_ms_number / 1000)
)
redis.call('XADD', control_stream, 'MAXLEN', '~', stream_maxlen, '*',
    'event_type', 'RunAdmitted',
    'run_id', run_id,
    'tenant_id', tenant_id,
    'agent_type', agent_type,
    'priority', '5',
    'admitted_at', tostring(now_ms_number / 1000)
)
return {'RUN_RESCHEDULED', tostring(new_count), 'rescheduled', ''}

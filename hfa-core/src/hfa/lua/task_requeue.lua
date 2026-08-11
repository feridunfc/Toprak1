-- hfa-core/src/hfa/lua/task_requeue.lua
-- Sprint 82.3 — Monotonic requeue with atomic TASK/RUN truth convergence.
--
-- claim_epoch is a lifetime generation counter and is never reset. Requeue
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
-- 7  run_tasks_set
-- 8  global_runtime_truth_conflict_index
-- 9  global_runtime_truth_conflict_stream
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
-- 10 projection_mode      empty/legacy, "canonical_project", or "canonical_deliver"
-- 11 canonical_transition_id
-- 12 canonical_record_hash
-- 13 canonical_command_hash
-- 14 canonical_revision
-- 15 canonical_operation_id
-- 16 canonical_operation_type
-- 17 claim_transition_id
-- 18 claim_record_hash
-- 19 claim_command_hash
-- 20 claim_revision
-- 21 claim_operation_id
-- 22 claim_epoch
-- 23 retry_attempt
--
-- RETURN
-- { status, value }

local task_state_key        = KEYS[1]
local task_meta_key         = KEYS[2]
local tenant_ready_queue    = KEYS[3]
local task_running_zset     = KEYS[4]
local completion_stream     = KEYS[5]
local run_state_key         = KEYS[6]
local run_tasks_set         = KEYS[7]
local truth_conflict_index  = KEYS[8]
local truth_conflict_stream = KEYS[9]

local task_id           = ARGV[1] or ''
local run_id            = ARGV[2] or ''
local tenant_id         = ARGV[3] or ''
local expected_state    = ARGV[4] or ''
local now_ms            = ARGV[5] or ''
local ready_score       = ARGV[6] or ''
local max_requeue_count = tonumber(ARGV[7])
local reason_code       = ARGV[8] or ''
local stream_maxlen     = tonumber(ARGV[9])
local now_ms_number     = tonumber(now_ms)
local ready_score_number = tonumber(ready_score)
local projection_mode = ARGV[10] or ''
local canonical_transition_id = ARGV[11] or ''
local canonical_record_hash = ARGV[12] or ''
local canonical_command_hash = ARGV[13] or ''
local canonical_revision = tonumber(ARGV[14])
local canonical_operation_id = ARGV[15] or ''
local canonical_operation_type = ARGV[16] or ''
local claim_transition_id = ARGV[17] or ''
local claim_record_hash = ARGV[18] or ''
local claim_command_hash = ARGV[19] or ''
local claim_revision = tonumber(ARGV[20])
local claim_operation_id = ARGV[21] or ''
local claim_epoch = tonumber(ARGV[22])
local retry_attempt = tonumber(ARGV[23])

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
        observed_at_ms=now_ms_number,
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

if task_id == '' or run_id == '' or tenant_id == '' then
    return failure('identity_required_field_missing')
end
if expected_state ~= 'running' then
    return failure('invalid_requeue_expected_state')
end
if not now_ms_number or now_ms_number < 0
    or not ready_score_number
    or not max_requeue_count or max_requeue_count < 0 or max_requeue_count % 1 ~= 0
    or not stream_maxlen or stream_maxlen <= 0 or stream_maxlen % 1 ~= 0 then
    return failure('invalid_requeue_contract')
end
if projection_mode ~= ''
    and projection_mode ~= 'canonical_project'
    and projection_mode ~= 'canonical_deliver' then
    return failure('invalid_requeue_projection_mode')
end
if projection_mode == 'canonical_project' or projection_mode == 'canonical_deliver' then
    if canonical_transition_id == ''
        or canonical_record_hash == ''
        or canonical_command_hash == ''
        or not canonical_revision or canonical_revision < 1 or canonical_revision % 1 ~= 0
        or canonical_operation_id == ''
        or canonical_operation_type ~= 'TASK_REQUEUE'
        or claim_transition_id == ''
        or claim_record_hash == ''
        or claim_command_hash == ''
        or not claim_revision or claim_revision < 1 or claim_revision % 1 ~= 0
        or claim_operation_id == ''
        or not claim_epoch or claim_epoch < 1 or claim_epoch % 1 ~= 0
        or not retry_attempt or retry_attempt < 1 or retry_attempt % 1 ~= 0
        or canonical_revision ~= claim_revision + 1 then
        return failure('canonical_requeue_proof_invalid')
    end
end

-- Resolve a trustworthy task/run binding before RUN truth or conflict evidence.
local meta_kind = redis_type(task_meta_key)
local binding_from_membership = false
if meta_kind == 'none' then
    local membership_kind = redis_type(run_tasks_set)
    if membership_kind ~= 'set' or redis.call('SISMEMBER', run_tasks_set, task_id) ~= 1 then
        return failure('identity_run_membership_missing')
    end
    binding_from_membership = true
elseif meta_kind ~= 'hash' then
    return failure('TASK_STATE_CONFLICT', 'task_meta_type_mismatch')
else
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
if binding_from_membership then
    return emit_truth_conflict('task_truth_corruption_conflict', 'task_meta_missing', observed_run_state, current_state)
end

if projection_mode == 'canonical_deliver' then
    local delivery_proof = redis.call('HMGET', task_meta_key,
        'requeue_canonical_transition_id',
        'requeue_canonical_record_hash',
        'requeue_canonical_command_hash',
        'requeue_canonical_revision',
        'requeue_canonical_operation_id',
        'requeue_count',
        'requeue_notification_operation_id')
    if delivery_proof[1] ~= canonical_transition_id
        or delivery_proof[2] ~= canonical_record_hash
        or delivery_proof[3] ~= canonical_command_hash
        or tonumber(delivery_proof[4] or '') ~= canonical_revision
        or delivery_proof[5] ~= canonical_operation_id
        or tonumber(delivery_proof[6] or '') ~= retry_attempt then
        return failure('canonical_requeue_delivery_proof_mismatch')
    end
    if delivery_proof[7] == canonical_operation_id then
        return {'TASK_REQUEUE_DELIVERY_ALREADY_APPLIED', tostring(retry_attempt)}
    end
    local completion_kind = redis_type(completion_stream)
    if completion_kind ~= 'none' and completion_kind ~= 'stream' then
        return failure('canonical_requeue_delivery_stream_type_mismatch')
    end
    redis.call('XADD', completion_stream, 'MAXLEN', '~', stream_maxlen, '*',
        'event_type', 'TaskRequeued',
        'task_id', task_id,
        'run_id', run_id,
        'tenant_id', tenant_id,
        'reason_code', reason_code,
        'requeue_count', tostring(retry_attempt),
        'canonical_transition_id', canonical_transition_id,
        'canonical_revision', tostring(canonical_revision),
        'canonical_operation_id', canonical_operation_id,
        'at_ms', now_ms)
    redis.call('HSET', task_meta_key,
        'requeue_notification_operation_id', canonical_operation_id)
    return {'TASK_REQUEUE_DELIVERED', tostring(retry_attempt)}
end

if task_terminal(current_state) then
    return {'TASK_TERMINAL', current_state}
end
if projection_mode == 'canonical_project' and current_state == 'ready' then
    local projected = redis.call('HMGET', task_meta_key,
        'canonical_transition_id',
        'canonical_record_hash',
        'canonical_command_hash',
        'canonical_revision',
        'canonical_operation_id',
        'requeue_count')
    if projected[1] == canonical_transition_id
        and projected[2] == canonical_record_hash
        and projected[3] == canonical_command_hash
        and tonumber(projected[4] or '') == canonical_revision
        and projected[5] == canonical_operation_id
        and tonumber(projected[6] or '') == retry_attempt then
        return {'TASK_ALREADY_REQUEUED', tostring(retry_attempt)}
    end
    return emit_truth_conflict(
        'task_truth_corruption_conflict',
        'canonical_requeue_projection_mismatch',
        observed_run_state,
        current_state)
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

-- Redis scripts do not roll back earlier writes after a runtime command error.
-- Validate every mutation target before the first lifecycle mutation.
local running_kind = redis_type(task_running_zset)
if running_kind ~= 'none' and running_kind ~= 'zset' then
    return emit_truth_conflict('task_truth_corruption_conflict', 'task_running_projection_type_mismatch', run_state, current_state)
end
if projection_mode ~= 'canonical_project' then
    local completion_kind = redis_type(completion_stream)
    if completion_kind ~= 'none' and completion_kind ~= 'stream' then
        return emit_truth_conflict('task_truth_corruption_conflict', 'completion_stream_type_mismatch', run_state, current_state)
    end
end

if projection_mode == 'canonical_project' then
    if retry_attempt > max_requeue_count then
        return failure('canonical_requeue_retry_policy_conflict', tostring(retry_attempt))
    end
    local ready_kind = redis_type(tenant_ready_queue)
    if ready_kind ~= 'none' and ready_kind ~= 'zset' then
        return emit_truth_conflict('task_truth_corruption_conflict', 'ready_queue_type_mismatch', run_state, current_state)
    end

    local proof = redis.call('HMGET', task_meta_key,
        'worker_instance_id',
        'scheduler_epoch',
        'claim_epoch',
        'canonical_transition_id',
        'canonical_record_hash',
        'canonical_command_hash',
        'canonical_revision',
        'canonical_operation_id',
        'claim_canonical_transition_id',
        'claim_canonical_record_hash',
        'claim_canonical_command_hash',
        'claim_canonical_revision',
        'claim_canonical_operation_id',
        'requeue_count')
    if not proof[1] or proof[1] == ''
        or not proof[2] or proof[2] == ''
        or tonumber(proof[3] or '') ~= claim_epoch
        or proof[4] ~= claim_transition_id
        or proof[5] ~= claim_record_hash
        or proof[6] ~= claim_command_hash
        or tonumber(proof[7] or '') ~= claim_revision
        or proof[8] ~= claim_operation_id
        or proof[9] ~= claim_transition_id
        or proof[10] ~= claim_record_hash
        or proof[11] ~= claim_command_hash
        or tonumber(proof[12] or '') ~= claim_revision
        or proof[13] ~= claim_operation_id
        or tonumber(proof[14] or '0') ~= retry_attempt - 1 then
        return emit_truth_conflict(
            'task_truth_corruption_conflict',
            'canonical_requeue_claim_projection_mismatch',
            run_state,
            current_state)
    end

    redis.call('ZREM', task_running_zset, task_id)
    redis.call('SET', task_state_key, 'ready')
    redis.call('HSET', task_meta_key,
        'requeue_count', tostring(retry_attempt),
        'last_requeue_reason', reason_code,
        'last_requeue_at_ms', now_ms,
        'worker_instance_id', '',
        'scheduler_epoch', '',
        'last_heartbeat_at_ms', '0',
        'canonical_transition_id', canonical_transition_id,
        'canonical_record_hash', canonical_record_hash,
        'canonical_command_hash', canonical_command_hash,
        'canonical_revision', tostring(canonical_revision),
        'canonical_operation_id', canonical_operation_id,
        'requeue_canonical_transition_id', canonical_transition_id,
        'requeue_canonical_record_hash', canonical_record_hash,
        'requeue_canonical_command_hash', canonical_command_hash,
        'requeue_canonical_revision', tostring(canonical_revision),
        'requeue_canonical_operation_id', canonical_operation_id)
    redis.call('ZADD', tenant_ready_queue, 'NX', ready_score, task_id)
    return {'TASK_REQUEUED', tostring(retry_attempt)}
end

local raw_count = redis.call('HGET', task_meta_key, 'requeue_count')
local retries = tonumber(raw_count or '0')
if not retries or retries < 0 or retries % 1 ~= 0 then
    return emit_truth_conflict('task_truth_corruption_conflict', 'requeue_count_invalid', run_state, current_state)
end
retries = retries + 1

if retries <= max_requeue_count then
    local ready_kind = redis_type(tenant_ready_queue)
    if ready_kind ~= 'none' and ready_kind ~= 'zset' then
        return emit_truth_conflict('task_truth_corruption_conflict', 'ready_queue_type_mismatch', run_state, current_state)
    end
end

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

-- hfa-core/src/hfa/lua/task_claim_start.lua
-- IRONCLAD Sprint 82.2 — Atomic task claim with epoch fencing and RUN truth guard.
--
-- KEYS
-- 1  task_state_key
-- 2  task_meta_key
-- 3  task_scheduled_zset
-- 4  task_running_zset
-- 5  reservation_key
-- 6  task_owner_key
-- 7  run_state_key
-- 8  global_runtime_truth_conflict_index
-- 9  global_runtime_truth_conflict_stream
--
-- ARGV
-- 1  task_id
-- 2  worker_instance_id
-- 3  claimed_at_ms
-- 4  state_ttl
-- 5  meta_ttl
-- 6  heartbeat_score
-- 7  expected_scheduler_epoch
-- 8  allow_legacy_direct_claim
-- 9  explicit_run_id
-- 10 canonical_projection_enabled
-- 11 canonical_tenant_id
-- 12 canonical_transition_id
-- 13 canonical_record_hash
-- 14 canonical_command_hash
-- 15 canonical_revision
-- 16 canonical_operation_id
-- 17 canonical_previous_claim_epoch
-- 18 canonical_claim_epoch
-- 19 dispatch_transition_id
-- 20 dispatch_record_hash
-- 21 dispatch_command_hash
-- 22 dispatch_revision
-- 23 dispatch_operation_id
-- 24 dispatch_attempt
--
-- RETURN
-- { status, claim_epoch, scheduler_epoch, worker_instance_id, task_id }

local task_state_key        = KEYS[1]
local task_meta_key         = KEYS[2]
local task_scheduled_zset   = KEYS[3]
local task_running_zset     = KEYS[4]
local reservation_key       = KEYS[5]
local task_owner_key        = KEYS[6] or ''
local run_state_key         = KEYS[7]
local truth_conflict_index  = KEYS[8]
local truth_conflict_stream = KEYS[9]

local task_id                   = ARGV[1]
local worker_instance_id        = ARGV[2]
local claimed_at_ms             = ARGV[3]
local state_ttl                 = tonumber(ARGV[4])
local meta_ttl                  = tonumber(ARGV[5])
local heartbeat_score           = tonumber(ARGV[6])
local expected_scheduler_epoch  = ARGV[7]
local allow_legacy_direct_claim = ARGV[8] or '0'
local run_id                    = ARGV[9] or ''
local canonical_projection      = ARGV[10] == '1'
local canonical_tenant_id       = ARGV[11] or ''
local canonical_transition_id   = ARGV[12] or ''
local canonical_record_hash     = ARGV[13] or ''
local canonical_command_hash    = ARGV[14] or ''
local canonical_revision        = ARGV[15] or ''
local canonical_operation_id    = ARGV[16] or ''
local canonical_previous_epoch  = ARGV[17] or ''
local canonical_claim_epoch     = ARGV[18] or ''
local dispatch_transition_id    = ARGV[19] or ''
local dispatch_record_hash      = ARGV[20] or ''
local dispatch_command_hash     = ARGV[21] or ''
local dispatch_revision         = ARGV[22] or ''
local dispatch_operation_id     = ARGV[23] or ''
local dispatch_attempt          = ARGV[24] or ''

local OPERATION = 'TASK_CLAIM'
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

local function claim_failure(status)
    return {status, '', '', '', task_id}
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

local MAX_SAFE_INTEGER = 9007199254740991

local function exact_safe_integer(value)
    local number = tonumber(value)
    return value ~= ''
        and number ~= nil
        and number >= 0
        and number <= MAX_SAFE_INTEGER
        and number % 1 == 0
end

local function valid_sha256(value)
    return type(value) == 'string'
        and string.len(value) == 64
        and string.match(value, '^[0-9a-f]+$') ~= nil
end

local function canonical_projection_input_valid()
    if not canonical_projection then return true end
    if task_id == ''
        or run_id == ''
        or worker_instance_id == ''
        or expected_scheduler_epoch == ''
        or expected_scheduler_epoch == '0'
        or canonical_tenant_id == ''
        or canonical_transition_id == ''
        or canonical_operation_id == ''
        or dispatch_transition_id == ''
        or dispatch_operation_id == ''
        or not valid_sha256(canonical_record_hash)
        or not valid_sha256(canonical_command_hash)
        or not valid_sha256(dispatch_record_hash)
        or not valid_sha256(dispatch_command_hash) then
        return false
    end
    if not exact_safe_integer(canonical_revision)
        or not exact_safe_integer(canonical_previous_epoch)
        or not exact_safe_integer(canonical_claim_epoch)
        or not exact_safe_integer(dispatch_revision)
        or not exact_safe_integer(dispatch_attempt) then
        return false
    end
    if tonumber(dispatch_revision) < 1
        or tonumber(dispatch_attempt) < 1 then
        return false
    end
    if tonumber(canonical_revision) ~= tonumber(dispatch_revision) + 1 then
        return false
    end
    return tonumber(canonical_claim_epoch)
        == tonumber(canonical_previous_epoch) + 1
end

local function canonical_dispatch_proof_matches()
    local values = redis.call(
        'HMGET',
        task_meta_key,
        'tenant_id',
        'scheduler_epoch',
        'dispatch_attempt',
        'dispatch_worker_id',
        'canonical_transition_id',
        'canonical_record_hash',
        'canonical_command_hash',
        'canonical_revision',
        'canonical_operation_id',
        'claim_epoch'
    )
    local current_claim_epoch = values[10] or '0'
    return values[1] == canonical_tenant_id
        and values[2] == expected_scheduler_epoch
        and values[3] == dispatch_attempt
        and values[4] == worker_instance_id
        and values[5] == dispatch_transition_id
        and values[6] == dispatch_record_hash
        and values[7] == dispatch_command_hash
        and values[8] == dispatch_revision
        and values[9] == dispatch_operation_id
        and current_claim_epoch == canonical_previous_epoch
end

local function canonical_claim_exact_duplicate()
    local values = redis.call(
        'HMGET',
        task_meta_key,
        'tenant_id',
        'worker_instance_id',
        'scheduler_epoch',
        'claimed_at_ms',
        'claim_epoch',
        'claim_canonical_transition_id',
        'claim_canonical_record_hash',
        'claim_canonical_command_hash',
        'claim_canonical_revision',
        'claim_canonical_operation_id',
        'dispatch_canonical_transition_id',
        'dispatch_canonical_record_hash',
        'dispatch_canonical_command_hash',
        'dispatch_canonical_revision',
        'dispatch_canonical_operation_id',
        'dispatch_attempt',
        'dispatch_worker_id'
    )
    return values[1] == canonical_tenant_id
        and values[2] == worker_instance_id
        and values[3] == expected_scheduler_epoch
        and values[4] == claimed_at_ms
        and values[5] == canonical_claim_epoch
        and values[6] == canonical_transition_id
        and values[7] == canonical_record_hash
        and values[8] == canonical_command_hash
        and values[9] == canonical_revision
        and values[10] == canonical_operation_id
        and values[11] == dispatch_transition_id
        and values[12] == dispatch_record_hash
        and values[13] == dispatch_command_hash
        and values[14] == dispatch_revision
        and values[15] == dispatch_operation_id
        and values[16] == dispatch_attempt
        and values[17] == worker_instance_id
end

local function emit_truth_conflict(status, detail_code, observed_run_state)
    local pair_ok, pair_value = truth_conflict_pair_state()
    if not pair_ok then
        return claim_failure('truth_conflict_evidence_store_unavailable')
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
        observed_at_ms=tonumber(claimed_at_ms),
        scheduler_epoch=expected_scheduler_epoch,
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
            'observed_at_ms', claimed_at_ms,
            'scheduler_epoch', expected_scheduler_epoch,
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
            return claim_failure('truth_conflict_evidence_store_unavailable')
        end
    end
    return claim_failure(status)
end

local current_state = redis.call('GET', task_state_key)
if not current_state then
    return claim_failure('task_missing')
end

if redis.call('EXISTS', task_meta_key) == 0 then
    return claim_failure('missing_task_meta')
end
local authoritative_identity = redis.call('HMGET', task_meta_key, 'task_id', 'run_id')
local authoritative_task_id = authoritative_identity[1]
local authoritative_run_id = authoritative_identity[2]
if not authoritative_task_id or authoritative_task_id == '' then
    return claim_failure('identity_task_id_missing')
end
if authoritative_task_id ~= task_id then
    return claim_failure('identity_task_id_mismatch')
end
if not authoritative_run_id or authoritative_run_id == '' then
    return claim_failure('identity_run_id_missing')
end
if authoritative_run_id ~= run_id then
    return claim_failure('identity_run_id_mismatch')
end

if canonical_projection and not canonical_projection_input_valid() then
    return claim_failure('canonical_projection_conflict')
end

local task_terminal = {
    done=true,
    failed=true,
    rejected=true,
    blocked_by_failure=true,
    dead_lettered=true,
    skipped=true
}

if canonical_projection and task_terminal[current_state] then
    if canonical_claim_exact_duplicate() then
        return {
            'canonical_claim_already_projected',
            canonical_claim_epoch,
            expected_scheduler_epoch,
            worker_instance_id,
            task_id
        }
    end
    return claim_failure('canonical_projection_conflict')
end

if current_state ~= 'scheduled' and current_state ~= 'running' then
    return claim_failure('task_state_conflict')
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

if canonical_projection then
    if current_state == 'running' then
        if canonical_claim_exact_duplicate() then
            return {
                'canonical_claim_already_projected',
                canonical_claim_epoch,
                expected_scheduler_epoch,
                worker_instance_id,
                task_id
            }
        end
        return claim_failure('canonical_projection_conflict')
    end
    if not canonical_dispatch_proof_matches() then
        return claim_failure('canonical_projection_conflict')
    end
elseif current_state == 'running' then
    return claim_failure('task_already_owned')
end

local legacy_direct_claim = false
local has_reservation = redis.call('EXISTS', reservation_key)
local owner_index_worker = ''
local owner_index_task = ''
local owner_index_epoch = ''
local has_owner_index = 0

if task_owner_key ~= '' then
    has_owner_index = redis.call('EXISTS', task_owner_key)
    if has_owner_index == 1 then
        local owner_index = redis.call('HMGET', task_owner_key, 'worker_id', 'task_id', 'scheduler_epoch')
        owner_index_worker = owner_index[1] or ''
        owner_index_task = owner_index[2] or ''
        owner_index_epoch = owner_index[3] or ''
    end
end

if has_reservation == 0 then
    if expected_scheduler_epoch ~= '' and has_owner_index == 1 and owner_index_worker ~= worker_instance_id then
        return {'reservation_worker_mismatch', '', '', owner_index_worker or '', task_id}
    end
    if expected_scheduler_epoch == '' and allow_legacy_direct_claim == '1' then
        legacy_direct_claim = true
    else
        return claim_failure('reservation_missing')
    end
end

local reserved_worker = worker_instance_id
local reserved_task = task_id
local reserved_epoch = ''

if not legacy_direct_claim then
    reserved_worker = redis.call('HGET', reservation_key, 'worker_id')
    reserved_task = redis.call('HGET', reservation_key, 'task_id')
    reserved_epoch = redis.call('HGET', reservation_key, 'scheduler_epoch')

    if reserved_worker ~= worker_instance_id then
        return {'reservation_worker_mismatch', '', '', reserved_worker or '', task_id}
    end
    if reserved_task ~= task_id then
        return claim_failure('reservation_task_mismatch')
    end
    if has_owner_index == 1 then
        if owner_index_worker ~= reserved_worker then
            return {'reservation_worker_mismatch', '', '', owner_index_worker or '', task_id}
        end
        if owner_index_task ~= reserved_task then
            return claim_failure('reservation_task_mismatch')
        end
        if owner_index_epoch ~= reserved_epoch then
            return {'reservation_epoch_mismatch', '', owner_index_epoch or '', '', task_id}
        end
    end
    if expected_scheduler_epoch ~= '' then
        if (not reserved_epoch) or reserved_epoch ~= expected_scheduler_epoch then
            return {'reservation_epoch_mismatch', '', reserved_epoch or '', '', task_id}
        end
    end
end

local new_claim_epoch
if canonical_projection then
    new_claim_epoch = tonumber(canonical_claim_epoch)
    redis.call('HSET', task_meta_key,
        'claim_epoch', canonical_claim_epoch,
        'dispatch_canonical_transition_id', dispatch_transition_id,
        'dispatch_canonical_record_hash', dispatch_record_hash,
        'dispatch_canonical_command_hash', dispatch_command_hash,
        'dispatch_canonical_revision', dispatch_revision,
        'dispatch_canonical_operation_id', dispatch_operation_id,
        'claim_canonical_transition_id', canonical_transition_id,
        'claim_canonical_record_hash', canonical_record_hash,
        'claim_canonical_command_hash', canonical_command_hash,
        'claim_canonical_revision', canonical_revision,
        'claim_canonical_operation_id', canonical_operation_id,
        'canonical_transition_id', canonical_transition_id,
        'canonical_record_hash', canonical_record_hash,
        'canonical_command_hash', canonical_command_hash,
        'canonical_revision', canonical_revision,
        'canonical_operation_id', canonical_operation_id
    )
else
    new_claim_epoch = redis.call(
        'HINCRBY',
        task_meta_key,
        'claim_epoch',
        1
    )
end
redis.call('SET', task_state_key, 'running', 'EX', state_ttl)
redis.call('HSET', task_meta_key,
    'worker_instance_id', worker_instance_id,
    'scheduler_epoch', reserved_epoch or '',
    'claimed_at_ms', claimed_at_ms,
    'last_heartbeat_at_ms', claimed_at_ms
)
redis.call('EXPIRE', task_meta_key, meta_ttl)
redis.call('ZREM', task_scheduled_zset, task_id)
redis.call('ZADD', task_running_zset, heartbeat_score, task_id)

if not legacy_direct_claim then
    redis.call('DEL', reservation_key)
    if task_owner_key ~= '' then
        redis.call('DEL', task_owner_key)
    end
end

return {
    'task_claimed',
    tostring(new_claim_epoch),
    reserved_epoch or '',
    worker_instance_id,
    task_id,
}

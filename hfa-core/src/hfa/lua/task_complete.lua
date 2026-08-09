-- hfa-core/src/hfa/lua/task_complete.lua
-- IRONCLAD Sprint 82.2 — Owner-fenced task completion with RUN truth guard.
-- Sprint 84.7C1 — optional proof-bound canonical TASK terminal projection.
--
-- KEYS
-- 1  task_state_key
-- 2  task_meta_key
-- 3  task_children_key
-- 4  task_output_key
-- 5  tenant_ready_queue
-- 6  task_running_zset
-- 7  run_state_key
-- 8  global_runtime_truth_conflict_index
-- 9  global_runtime_truth_conflict_stream
--
-- ARGV
-- 1   task_id
-- 2   run_id
-- 3   tenant_id
-- 4   terminal_state
-- 5   finished_at_ms
-- 6   state_ttl_seconds
-- 7   meta_ttl_seconds
-- 8   output_ttl_seconds
-- 9   ready_score
-- 10  reason_code
-- 11  expected_worker_instance_id
-- 12  output_json
-- 13  child_state_prefix
-- 14  child_state_suffix
-- 15  child_remaining_prefix
-- 16  child_remaining_suffix
-- 17  child_ready_emitted_prefix
-- 18  child_ready_emitted_suffix
-- 19  expected_scheduler_epoch
-- 20  expected_claim_epoch
-- 21  canonical_projection_enabled
-- 22  canonical_transition_id
-- 23  canonical_record_hash
-- 24  canonical_command_hash
-- 25  canonical_revision
-- 26  canonical_operation_id
-- 27  canonical_operation_type
-- 28  claim_transition_id
-- 29  claim_record_hash
-- 30  claim_command_hash
-- 31  claim_revision
-- 32  claim_operation_id
-- 33  canonical_claim_epoch
-- 34  output_sha256
--
-- RETURN
-- { committed_flag, status_string, unlocked_count, already_terminal_flag, blocked_count }

local task_state_key            = KEYS[1]
local task_meta_key             = KEYS[2]
local task_children_key         = KEYS[3]
local task_output_key           = KEYS[4]
local tenant_ready_queue        = KEYS[5]
local task_running_zset         = KEYS[6]
local run_state_key             = KEYS[7]
local truth_conflict_index      = KEYS[8]
local truth_conflict_stream     = KEYS[9]

local task_id                   = ARGV[1]
local run_id                    = ARGV[2] or ''
local tenant_id                 = ARGV[3]
local terminal_state            = ARGV[4]
local finished_at_ms            = ARGV[5]
local state_ttl                 = tonumber(ARGV[6])
local meta_ttl                  = tonumber(ARGV[7])
local output_ttl                = tonumber(ARGV[8])
local ready_score               = tonumber(ARGV[9])
local reason_code               = ARGV[10]
local expected_worker_id        = ARGV[11]
local output_json               = ARGV[12]
local child_state_pfx           = ARGV[13]
local child_state_sfx           = ARGV[14]
local child_remaining_pfx       = ARGV[15]
local child_remaining_sfx       = ARGV[16]
local child_ready_emitted_pfx   = ARGV[17]
local child_ready_emitted_sfx   = ARGV[18]
local expected_scheduler_epoch  = ARGV[19]
local expected_claim_epoch      = ARGV[20]
local canonical_projection      = ARGV[21] == '1'
local canonical_transition_id   = ARGV[22] or ''
local canonical_record_hash     = ARGV[23] or ''
local canonical_command_hash    = ARGV[24] or ''
local canonical_revision        = ARGV[25] or ''
local canonical_operation_id    = ARGV[26] or ''
local canonical_operation_type  = ARGV[27] or ''
local claim_transition_id       = ARGV[28] or ''
local claim_record_hash         = ARGV[29] or ''
local claim_command_hash        = ARGV[30] or ''
local claim_revision            = ARGV[31] or ''
local claim_operation_id        = ARGV[32] or ''
local canonical_claim_epoch     = ARGV[33] or ''
local output_sha256             = ARGV[34] or ''

if terminal_state ~= 'done' and terminal_state ~= 'failed' then
    return {0, 'invalid_terminal_state', 0, 0, 0}
end

local OPERATION = terminal_state == 'done' and 'TASK_COMPLETE' or 'TASK_FAIL'
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

local function is_terminal(s)
    return s == 'done'
        or s == 'failed'
        or s == 'blocked_by_failure'
        or s == 'dead_lettered'
        or s == 'skipped'
end

local function completion_failure(status)
    return {0, status, 0, 0, 0}
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
        or tenant_id == ''
        or expected_worker_id == ''
        or expected_scheduler_epoch == ''
        or expected_scheduler_epoch == '0'
        or canonical_transition_id == ''
        or canonical_operation_id == ''
        or claim_transition_id == ''
        or claim_operation_id == ''
        or not valid_sha256(canonical_record_hash)
        or not valid_sha256(canonical_command_hash)
        or not valid_sha256(claim_record_hash)
        or not valid_sha256(claim_command_hash) then
        return false
    end
    if not exact_safe_integer(canonical_revision)
        or not exact_safe_integer(claim_revision)
        or not exact_safe_integer(canonical_claim_epoch) then
        return false
    end
    if tonumber(claim_revision) < 1
        or tonumber(canonical_revision) ~= tonumber(claim_revision) + 1
        or tonumber(canonical_claim_epoch) < 1 then
        return false
    end
    if canonical_claim_epoch ~= expected_claim_epoch then
        return false
    end
    if terminal_state == 'done' then
        return canonical_operation_type == 'TASK_COMPLETE'
            and valid_sha256(output_sha256)
    end
    return canonical_operation_type == 'TASK_FAIL' and output_sha256 == ''
end

local function canonical_claim_proof_matches()
    local values = redis.call('HMGET', task_meta_key,
        'tenant_id',
        'worker_instance_id',
        'scheduler_epoch',
        'claim_epoch',
        'claim_canonical_transition_id',
        'claim_canonical_record_hash',
        'claim_canonical_command_hash',
        'claim_canonical_revision',
        'claim_canonical_operation_id',
        'canonical_transition_id',
        'canonical_record_hash',
        'canonical_command_hash',
        'canonical_revision',
        'canonical_operation_id'
    )
    return values[1] == tenant_id
        and values[2] == expected_worker_id
        and values[3] == expected_scheduler_epoch
        and values[4] == canonical_claim_epoch
        and values[5] == claim_transition_id
        and values[6] == claim_record_hash
        and values[7] == claim_command_hash
        and values[8] == claim_revision
        and values[9] == claim_operation_id
        and values[10] == claim_transition_id
        and values[11] == claim_record_hash
        and values[12] == claim_command_hash
        and values[13] == claim_revision
        and values[14] == claim_operation_id
end

local function canonical_terminal_exact_duplicate()
    local values = redis.call('HMGET', task_meta_key,
        'completed_at_ms',
        'terminal_state',
        'completion_reason',
        'worker_instance_id',
        'scheduler_epoch',
        'claim_epoch',
        'terminal_canonical_transition_id',
        'terminal_canonical_record_hash',
        'terminal_canonical_command_hash',
        'terminal_canonical_revision',
        'terminal_canonical_operation_id',
        'terminal_canonical_operation_type',
        'terminal_output_sha256',
        'claim_canonical_transition_id',
        'claim_canonical_record_hash',
        'claim_canonical_command_hash',
        'claim_canonical_revision',
        'claim_canonical_operation_id',
        'canonical_transition_id',
        'canonical_record_hash',
        'canonical_command_hash',
        'canonical_revision',
        'canonical_operation_id'
    )
    if values[1] ~= finished_at_ms
        or values[2] ~= terminal_state
        or values[3] ~= reason_code
        or values[4] ~= expected_worker_id
        or values[5] ~= expected_scheduler_epoch
        or values[6] ~= canonical_claim_epoch
        or values[7] ~= canonical_transition_id
        or values[8] ~= canonical_record_hash
        or values[9] ~= canonical_command_hash
        or values[10] ~= canonical_revision
        or values[11] ~= canonical_operation_id
        or values[12] ~= canonical_operation_type
        or values[13] ~= output_sha256
        or values[14] ~= claim_transition_id
        or values[15] ~= claim_record_hash
        or values[16] ~= claim_command_hash
        or values[17] ~= claim_revision
        or values[18] ~= claim_operation_id
        or values[19] ~= canonical_transition_id
        or values[20] ~= canonical_record_hash
        or values[21] ~= canonical_command_hash
        or values[22] ~= canonical_revision
        or values[23] ~= canonical_operation_id then
        return false
    end
    if terminal_state == 'done' then
        if redis_type(task_output_key) ~= 'string' then
            return false
        end
        return redis.call('GET', task_output_key) == output_json
    end
    return true
end

local function canonical_projection_preflight()
    if not canonical_projection then return true, {} end

    local running_kind = redis_type(task_running_zset)
    if running_kind ~= 'none' and running_kind ~= 'zset' then
        return false, {}
    end
    local ready_kind = redis_type(tenant_ready_queue)
    if ready_kind ~= 'none' and ready_kind ~= 'zset' then
        return false, {}
    end
    local children_kind = redis_type(task_children_key)
    if children_kind ~= 'none' and children_kind ~= 'set' then
        return false, {}
    end
    if terminal_state == 'done' then
        local output_kind = redis_type(task_output_key)
        if output_kind ~= 'none' and output_kind ~= 'string' then
            return false, {}
        end
    end

    local children = redis.call('SMEMBERS', task_children_key)
    for _, child_id in ipairs(children) do
        local child_state_key = child_state_pfx .. child_id .. child_state_sfx
        local child_state_kind = redis_type(child_state_key)
        if child_state_kind ~= 'none' and child_state_kind ~= 'string' then
            return false, {}
        end
        local child_state = redis.call('GET', child_state_key)
        if terminal_state == 'done' and child_state == 'pending' then
            local child_remaining_key = child_remaining_pfx .. child_id .. child_remaining_sfx
            local child_emitted_key = child_ready_emitted_pfx .. child_id .. child_ready_emitted_sfx
            if redis_type(child_remaining_key) ~= 'string' then
                return false, {}
            end
            local remaining_raw = redis.call('GET', child_remaining_key)
            if type(remaining_raw) ~= 'string'
                or string.match(remaining_raw, '^%d+$') == nil
                or not exact_safe_integer(remaining_raw)
                or tonumber(remaining_raw) < 1 then
                return false, {}
            end
            local emitted_kind = redis_type(child_emitted_key)
            if emitted_kind ~= 'none' and emitted_kind ~= 'string' then
                return false, {}
            end
        end
    end
    return true, children
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
        return completion_failure('truth_conflict_evidence_store_unavailable')
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
        observed_at_ms=tonumber(finished_at_ms),
        scheduler_epoch=expected_scheduler_epoch,
        claim_epoch=expected_claim_epoch,
        worker_instance_id=expected_worker_id
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
            'observed_at_ms', finished_at_ms,
            'scheduler_epoch', expected_scheduler_epoch,
            'claim_epoch', expected_claim_epoch,
            'worker_instance_id', expected_worker_id,
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
            return completion_failure('truth_conflict_evidence_store_unavailable')
        end
    end
    return completion_failure(status)
end

local current_state = redis.call('GET', task_state_key)
if not current_state then
    return {0, 'missing_task', 0, 0, 0}
end
if canonical_projection and not canonical_projection_input_valid() then
    return {0, 'canonical_projection_conflict', 0, 0, 0}
end
if is_terminal(current_state) then
    if canonical_projection then
        if canonical_terminal_exact_duplicate() then
            return {1, 'canonical_terminal_already_projected', 0, 1, 0}
        end
        return {0, 'canonical_projection_conflict', 0, 1, 0}
    end
    return {0, 'already_terminal', 0, 1, 0}
end
if current_state ~= 'running' then
    return {0, 'illegal_transition', 0, 0, 0}
end
if canonical_projection and not canonical_claim_proof_matches() then
    return {0, 'canonical_projection_conflict', 0, 0, 0}
end

if redis.call('EXISTS', task_meta_key) == 0 then
    return completion_failure('missing_task_meta')
end
local authoritative_identity = redis.call('HMGET', task_meta_key, 'task_id', 'run_id')
local authoritative_task_id = authoritative_identity[1]
local authoritative_run_id = authoritative_identity[2]
if not authoritative_task_id or authoritative_task_id == '' then
    return completion_failure('identity_task_id_missing')
end
if authoritative_task_id ~= task_id then
    return completion_failure('identity_task_id_mismatch')
end
if not authoritative_run_id or authoritative_run_id == '' then
    return completion_failure('identity_run_id_missing')
end
if authoritative_run_id ~= run_id then
    return completion_failure('identity_run_id_mismatch')
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
    'scheduler_epoch',
    'claim_epoch'
)
local stored_worker = fence[1] or ''
local stored_sched_ep = fence[2] or ''
local stored_claim_ep = fence[3] or ''

if expected_claim_epoch ~= '' and stored_claim_ep ~= expected_claim_epoch then
    return {0, 'claim_epoch_mismatch', 0, 0, 0}
end
if expected_scheduler_epoch ~= '' and stored_sched_ep ~= expected_scheduler_epoch then
    return {0, 'scheduler_epoch_mismatch', 0, 0, 0}
end
if expected_worker_id ~= '' and stored_worker ~= expected_worker_id then
    return {0, 'task_owner_mismatch', 0, 0, 0}
end

local canonical_children = {}
if canonical_projection then
    local preflight_ok, preflight_children = canonical_projection_preflight()
    if not preflight_ok then
        return {0, 'canonical_projection_conflict', 0, 0, 0}
    end
    canonical_children = preflight_children
end

redis.call('SET', task_state_key, terminal_state, 'EX', state_ttl)
redis.call('HSET', task_meta_key,
    'completed_at_ms', finished_at_ms,
    'terminal_state', terminal_state,
    'completion_reason', reason_code,
    'worker_instance_id', expected_worker_id
)
if canonical_projection then
    redis.call('HSET', task_meta_key,
        'terminal_canonical_transition_id', canonical_transition_id,
        'terminal_canonical_record_hash', canonical_record_hash,
        'terminal_canonical_command_hash', canonical_command_hash,
        'terminal_canonical_revision', canonical_revision,
        'terminal_canonical_operation_id', canonical_operation_id,
        'terminal_canonical_operation_type', canonical_operation_type,
        'terminal_output_sha256', output_sha256,
        'canonical_transition_id', canonical_transition_id,
        'canonical_record_hash', canonical_record_hash,
        'canonical_command_hash', canonical_command_hash,
        'canonical_revision', canonical_revision,
        'canonical_operation_id', canonical_operation_id
    )
end
redis.call('EXPIRE', task_meta_key, meta_ttl)
redis.call('ZREM', task_running_zset, task_id)

if terminal_state == 'done' and output_json ~= '' then
    redis.call('SET', task_output_key, output_json, 'EX', output_ttl)
end

if terminal_state ~= 'done' then
    if not canonical_projection then
        -- Preserve the locked legacy behavior. Historical failure propagation
        -- remains outside this script when canonical TASK terminal authority
        -- is disabled.
        return {1, 'committed', 0, 0, 0}
    end

    local children = canonical_children
    -- Canonical TASK_FAIL carries DEPENDENCY_FAILURE_FANOUT_INTENT. Apply the
    -- direct-child projection in the same Redis/Lua transaction as the parent
    -- terminal state so the durable intent is never acknowledged without its
    -- corresponding projection. This is subordinate to canonical TASK_FAIL
    -- authority in C1 foundation mode; FailureSweeper remains legacy/default.
    local blocked = 0
    for _, child_id in ipairs(children) do
        local child_state_key = child_state_pfx .. child_id .. child_state_sfx
        local child_state = redis.call('GET', child_state_key)
        if child_state == 'pending' or child_state == 'ready' then
            redis.call('SET', child_state_key, 'blocked_by_failure', 'EX', state_ttl)
            if child_state == 'ready' then
                redis.call('ZREM', tenant_ready_queue, child_id)
            end
            blocked = blocked + 1
        end
    end
    return {1, 'task_terminal_projected', 0, 0, blocked}
end

local unlocked = 0
local children = canonical_projection
    and canonical_children
    or redis.call('SMEMBERS', task_children_key)
for _, child_id in ipairs(children) do
    local child_state_key = child_state_pfx .. child_id .. child_state_sfx
    local child_remaining_key = child_remaining_pfx .. child_id .. child_remaining_sfx
    local child_emitted_key = child_ready_emitted_pfx .. child_id .. child_ready_emitted_sfx
    local child_state = redis.call('GET', child_state_key)
    if child_state == 'pending' then
        local remaining = tonumber(redis.call('DECR', child_remaining_key))
        if remaining == nil then remaining = 0 end
        if remaining < 0 then
            redis.call('SET', child_remaining_key, 0)
            remaining = 0
        end
        if remaining == 0 and redis.call('EXISTS', child_emitted_key) == 0 then
            redis.call('SET', child_state_key, 'ready', 'EX', state_ttl)
            redis.call('SET', child_emitted_key, '1', 'EX', state_ttl)
            redis.call('ZADD', tenant_ready_queue, 'NX', ready_score, child_id)
            unlocked = unlocked + 1
        end
    end
end

return {1, canonical_projection and 'task_terminal_projected' or 'committed', unlocked, 0, 0}

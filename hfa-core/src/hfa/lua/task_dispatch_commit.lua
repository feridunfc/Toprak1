-- hfa-core/src/hfa/lua/task_dispatch_commit.lua
-- Sprint 84.1 — TASK_DISPATCH legacy projection behind canonical authority.
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
-- 11..16 own TASK canonical head/index/receipt/record/log/outbox
--
-- ARGV 1..19 preserve the pre-Sprint-84.1 contract.
-- 20 dispatch_attempt
-- 21 worker_id
-- 22 canonical_transition_id
-- 23 canonical_record_hash
-- 24 canonical_command_hash
-- 25 canonical_revision
-- 26 canonical_operation_id
-- 27 canonical_projection: 1 or 0
-- 28 exact canonical own-head witness JSON

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
local dispatch_attempt      = tostring(ARGV[20] or '1')
local worker_id             = ARGV[21] or ''
local canonical_transition_id = ARGV[22] or ''
local canonical_record_hash = ARGV[23] or ''
local canonical_command_hash = ARGV[24] or ''
local canonical_revision    = tostring(ARGV[25] or '0')
local canonical_operation_id = ARGV[26] or ''

local OPERATION = 'TASK_DISPATCH'
local TRUTH_COUNT_FIELD = '__runtime_truth_conflict_count'
local canonical_projection = ARGV[27] == '1'

-- F04.4B: proof and effects share this Lua invocation. Run this before
-- runtime duplicate checks AND truth-conflict events (which also mutate).
-- Projection metadata is bound to the accepted record, never runtime authority.
if canonical_projection then
    local function conflict() return {'canonical_task_dispatch_proof_conflict'} end
    local ok, own = pcall(cjson.decode, ARGV[28] or '')
    if #KEYS ~= 16 or #ARGV ~= 28 or not ok or type(own) ~= 'table' or #own ~= 14 then
        return conflict()
    end
    for i=1,14 do
        if type(own[i]) ~= 'string' then return conflict() end
    end
    local decoded, record = pcall(cjson.decode, own[8])
    if not decoded or type(record) ~= 'table' then return conflict() end
    local identity = record.canonical_aggregate_identity
    local meta = record.authoritative_metadata_changes
    local effects = record.child_effects
    if type(identity) ~= 'table' or identity.aggregate_type ~= 'task'
        or identity.run_id ~= run_id or identity.task_id ~= task_id
        or record.operation_type ~= 'TASK_DISPATCH'
        or own[2] ~= 'task-dispatch:v1:' .. own[1] .. ':attempt:' .. dispatch_attempt
        or own[2] ~= canonical_operation_id or own[4] ~= canonical_transition_id
        or own[5] ~= canonical_revision or own[6] ~= canonical_command_hash
        or own[7] ~= canonical_record_hash or own[11] ~= 'scheduled' or own[12] ~= '0'
        or type(meta) ~= 'table' or type(effects) ~= 'table' then
        return conflict()
    end
    if meta.task_id ~= task_id or meta.run_id ~= run_id or meta.tenant_id ~= tenant_id
        or meta.worker_id ~= worker_id or meta.worker_group ~= worker_group
        or meta.scheduler_epoch ~= scheduler_epoch or meta.dispatch_attempt ~= tonumber(dispatch_attempt)
        or meta.agent_type ~= agent_type or meta.shard ~= tonumber(shard)
        or meta.priority ~= tonumber(priority) or meta.admitted_at_ms ~= tonumber(admitted_at)
        or meta.scheduled_at_ms ~= tonumber(scheduled_at) or meta.policy ~= policy
        or meta.region ~= region or meta.payload_json ~= payload_json
        or meta.trace_parent ~= trace_parent or meta.trace_state ~= trace_state
        or meta.scheduled_zset ~= task_scheduled_zset or meta.running_zset ~= task_running_zset
        or meta.ready_queue ~= tenant_ready_queue or meta.control_stream ~= control_stream
        or meta.shard_stream ~= shard_stream then
        return conflict()
    end
    local reservation = effects.worker_reservation
    local intents = record.durable_projection_intents
    if type(reservation) ~= 'table' or reservation.worker_id ~= worker_id
        or reservation.task_id ~= task_id or reservation.scheduler_epoch ~= scheduler_epoch
        or type(intents) ~= 'table' or #intents ~= 2
        or type(intents[1]) ~= 'table' or intents[1].kind ~= 'CONTROL_NOTIFICATION'
        or intents[1].stream ~= control_stream
        or type(intents[2]) ~= 'table' or intents[2].kind ~= 'TASK_REQUEST_MESSAGE'
        or intents[2].stream ~= shard_stream then
        return conflict()
    end
    local checked = validate_authority_head(
        {KEYS[11], KEYS[12], KEYS[13], KEYS[14], KEYS[15], KEYS[16]}, own
    )
    if checked[1] ~= 'VALID' then return conflict() end
elseif (ARGV[27] ~= nil and ARGV[27] ~= '0') or #KEYS ~= 10
    or canonical_transition_id ~= '' or canonical_operation_id ~= ''
    or canonical_command_hash ~= '' or canonical_record_hash ~= '' then
    return {'canonical_task_dispatch_proof_conflict'}
end

local function redis_type(key)
    local reply = redis.call('TYPE', key)
    if type(reply) == 'table' then return reply['ok'] end
    return reply
end

local function length_prefix(value)
    local text = value or ''
    return tostring(string.len(text)) .. ':' .. text
end

local function valid_sha256(value)
    return type(value) == 'string'
        and string.len(value) == 64
        and string.match(value, '^[0-9a-f]+$') ~= nil
end

local function canonical_input_valid()
    if not canonical_projection then return true end
    local attempt_number = tonumber(dispatch_attempt)
    local revision_number = tonumber(canonical_revision)
    return worker_id ~= ''
        and scheduler_epoch ~= ''
        and scheduler_epoch ~= '0'
        and canonical_operation_id ~= ''
        and attempt_number ~= nil
        and attempt_number >= 1
        and attempt_number % 1 == 0
        and revision_number ~= nil
        and revision_number >= 1
        and revision_number % 1 == 0
        and valid_sha256(canonical_record_hash)
        and valid_sha256(canonical_command_hash)
end

local function exact_canonical_projection()
    if not canonical_projection then return false end
    local proof = redis.call(
        'HMGET',
        task_meta_key,
        'canonical_transition_id',
        'canonical_record_hash',
        'canonical_command_hash',
        'canonical_revision',
        'canonical_operation_id',
        'dispatch_attempt',
        'dispatch_worker_id'
    )
    return proof[1] == canonical_transition_id
        and proof[2] == canonical_record_hash
        and proof[3] == canonical_command_hash
        and tostring(proof[4] or '') == canonical_revision
        and proof[5] == canonical_operation_id
        and tostring(proof[6] or '') == dispatch_attempt
        and proof[7] == worker_id
end

local function has_canonical_projection_proof()
    local value = redis.call(
        'HGET',
        task_meta_key,
        'canonical_transition_id'
    )
    return value ~= false and value ~= nil and value ~= ''
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
    local count_raw = redis.call(
        'HGET',
        truth_conflict_index,
        TRUTH_COUNT_FIELD
    )
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

local function emit_truth_conflict(
    status,
    detail_code,
    observed_run_state
)
    local pair_ok, pair_value = truth_conflict_pair_state()
    if not pair_ok then
        return {
            'truth_conflict_evidence_store_unavailable',
            pair_value
        }
    end
    local count = pair_value
    local material = length_prefix(OPERATION)
        .. length_prefix(run_id)
        .. length_prefix(task_id)
        .. length_prefix(status)
        .. length_prefix(detail_code)
        .. length_prefix(observed_run_state or '')
    local conflict_id = redis.sha1hex(material)
    local state_json = (
        observed_run_state == nil
        and cjson.null
        or observed_run_state
    )
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
    local inserted = redis.call(
        'HSETNX',
        truth_conflict_index,
        conflict_id,
        payload
    )
    if inserted == 1 then
        count = count + 1
        redis.call(
            'HSET',
            truth_conflict_index,
            TRUTH_COUNT_FIELD,
            tostring(count)
        )
        redis.call(
            'XADD',
            truth_conflict_stream,
            '*',
            'conflict_id', conflict_id,
            'operation', OPERATION,
            'conflict_type', status,
            'detail_code', detail_code,
            'run_id', run_id,
            'task_id', task_id,
            'observed_run_state', observed_run_state or '',
            'observed_at_ms', scheduled_at,
            'scheduler_epoch', scheduler_epoch,
            'conflict_json', payload
        )
    else
        local raw = redis.call(
            'HGET',
            truth_conflict_index,
            conflict_id
        )
        local ok, existing = pcall(cjson.decode, raw)
        local expected_state = (
            observed_run_state == nil
            and cjson.null
            or observed_run_state
        )
        if not ok
            or type(existing) ~= 'table'
            or existing.conflict_id ~= conflict_id
            or existing.operation ~= OPERATION
            or existing.conflict_type ~= status
            or existing.detail_code ~= detail_code
            or existing.run_id ~= run_id
            or existing.task_id ~= task_id
            or existing.observed_run_state ~= expected_state then
            return {
                'truth_conflict_evidence_store_unavailable',
                'truth_conflict_index_identity_mismatch'
            }
        end
    end
    return {status, detail_code}
end

if not canonical_input_valid() then
    return {
        'canonical_projection_input_invalid',
        'canonical_projection_proof_invalid'
    }
end

local state_kind = redis_type(task_state_key)
if state_kind == 'none' then return {'missing_state', ''} end
if state_kind ~= 'string' then
    return {'state_key_type_mismatch', state_kind}
end

local meta_kind = redis_type(task_meta_key)
if meta_kind == 'none' then return {'missing_task_meta', ''} end
if meta_kind ~= 'hash' then
    return {'task_meta_key_type_mismatch', meta_kind}
end

local current = redis.call('GET', task_state_key)
if not current then return {'missing_state', ''} end

local authoritative_identity = redis.call(
    'HMGET',
    task_meta_key,
    'task_id',
    'run_id'
)
local authoritative_task_id = authoritative_identity[1]
local authoritative_run_id  = authoritative_identity[2]
if not authoritative_task_id or authoritative_task_id == '' then
    return {'identity_task_id_missing', ''}
end
if authoritative_task_id ~= task_id then
    return {
        'identity_task_id_mismatch',
        authoritative_task_id
    }
end
if not authoritative_run_id or authoritative_run_id == '' then
    return {'identity_run_id_missing', ''}
end
if authoritative_run_id ~= run_id then
    return {
        'identity_run_id_mismatch',
        authoritative_run_id
    }
end

local terminal = {
    done=true,
    failed=true,
    blocked_by_failure=true,
    dead_lettered=true,
    skipped=true
}

if canonical_projection then
    if current == 'scheduled'
        or current == 'running'
        or terminal[current] then
        if exact_canonical_projection() then
            return {'already_projected', current}
        end
        return {
            'canonical_projection_conflict',
            'canonical_projection_proof_mismatch'
        }
    end
    if current == 'ready' and has_canonical_projection_proof() then
        if exact_canonical_projection() then
            return {
                'canonical_projection_regressed',
                'legacy_state_returned_to_ready'
            }
        end
        return {
            'canonical_projection_conflict',
            'canonical_projection_proof_mismatch'
        }
    end
else
    if current == 'running' then
        return {'already_running', current}
    end
    if current == 'scheduled' then
        return {'already_scheduled', current}
    end
    if terminal[current] then
        return {'illegal_transition', current}
    end
end

if current ~= 'ready' then
    return {'state_conflict', current}
end

local run_kind = redis_type(run_state_key)
if run_kind == 'none' then
    return emit_truth_conflict(
        'run_truth_missing',
        'run_state_missing',
        nil
    )
end
if run_kind ~= 'string' then
    return emit_truth_conflict(
        'run_truth_corruption_conflict',
        'run_state_key_type_mismatch',
        run_kind
    )
end
local run_state = redis.call('GET', run_state_key)
if not run_state or run_state == '' then
    return emit_truth_conflict(
        'run_truth_corruption_conflict',
        'run_state_empty_or_unreadable',
        run_state
    )
end
local nonterminal = {
    admitted=true,
    queued=true,
    scheduled=true,
    running=true,
    rescheduled=true
}
local run_terminal = {
    done=true,
    failed=true,
    rejected=true,
    dead_lettered=true
}
if run_terminal[run_state] then
    return emit_truth_conflict(
        'run_truth_terminal_conflict',
        'run_state_terminal',
        run_state
    )
end
if not nonterminal[run_state] then
    return emit_truth_conflict(
        'run_truth_corruption_conflict',
        'run_state_unknown',
        run_state
    )
end

redis.call('ZREM', tenant_ready_queue, task_id)
redis.call(
    'SET',
    task_state_key,
    'scheduled',
    'EX',
    task_state_ttl
)
redis.call(
    'HSET',
    task_meta_key,
    'task_id', task_id,
    'run_id', run_id,
    'tenant_id', tenant_id,
    'agent_type', agent_type,
    'worker_group', worker_group,
    'shard', shard,
    'priority', priority,
    'admitted_at', admitted_at,
    'scheduled_at', scheduled_at,
    'dispatch_policy', policy,
    'dispatch_region', region,
    'payload_json', payload_json,
    'trace_parent', trace_parent,
    'trace_state', trace_state,
    'scheduler_epoch', scheduler_epoch,
    'dispatch_attempt', dispatch_attempt,
    'dispatch_worker_id', worker_id
)
if canonical_projection then
    redis.call(
        'HSET',
        task_meta_key,
        'canonical_transition_id', canonical_transition_id,
        'canonical_record_hash', canonical_record_hash,
        'canonical_command_hash', canonical_command_hash,
        'canonical_revision', canonical_revision,
        'canonical_operation_id', canonical_operation_id
    )
end
redis.call('EXPIRE', task_meta_key, task_meta_ttl)
redis.call(
    'ZADD',
    task_scheduled_zset,
    scheduled_at,
    task_id
)
redis.call('EXPIRE', task_scheduled_zset, task_meta_ttl)
redis.call(
    'XADD',
    control_stream,
    'MAXLEN',
    '~',
    control_maxlen,
    '*',
    'event_type', 'TaskScheduled',
    'task_id', task_id,
    'run_id', run_id,
    'tenant_id', tenant_id,
    'agent_type', agent_type,
    'worker_id', worker_id,
    'worker_group', worker_group,
    'shard', shard,
    'region', region,
    'policy', policy,
    'scheduled_at', scheduled_at,
    'trace_parent', trace_parent,
    'trace_state', trace_state,
    'scheduler_epoch', scheduler_epoch,
    'dispatch_attempt', dispatch_attempt,
    'canonical_transition_id', canonical_transition_id
)
redis.call(
    'XADD',
    shard_stream,
    'MAXLEN',
    '~',
    shard_maxlen,
    '*',
    'event_type', 'TaskRequested',
    'task_id', task_id,
    'run_id', run_id,
    'tenant_id', tenant_id,
    'agent_type', agent_type,
    'worker_id', worker_id,
    'worker_group', worker_group,
    'shard', shard,
    'priority', priority,
    'payload_json', payload_json,
    'requested_at', scheduled_at,
    'trace_parent', trace_parent,
    'trace_state', trace_state,
    'scheduler_epoch', scheduler_epoch,
    'dispatch_attempt', dispatch_attempt,
    'canonical_transition_id', canonical_transition_id
)
return {'committed', current}

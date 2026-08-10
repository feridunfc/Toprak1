-- Operation-scoped admission resource reservation.
--
-- KEYS[1] reservation receipt HASH
-- KEYS[2] concurrent-run counter STRING
-- KEYS[3] budget-reserved-cents counter STRING
-- KEYS[4] tenant inflight counter STRING
--
-- ARGV[1]  action: reserve | finalize | release | settle
-- ARGV[2]  operation_id
-- ARGV[3]  run_id
-- ARGV[4]  tenant_id
-- ARGV[5]  estimated_cost_cents
-- ARGV[6]  reservation_version
-- ARGV[7]  proof_sha256
-- ARGV[8]  now_ms
-- ARGV[9]  concurrent_run_limit (-1 = unbounded)
-- ARGV[10] budget_limit_cents (-1 = unbounded)
-- ARGV[11] tenant_inflight_limit (-1 = unbounded)
-- ARGV[12] released_receipt_ttl_seconds
-- ARGV[13] run_terminate_operation_id (settle only)
-- ARGV[14] terminal_proof_sha256 (settle only)
-- ARGV[15] canonical_transition_id (settle only)
-- ARGV[16] canonical_record_hash (settle only)
-- ARGV[17] canonical_command_hash (settle only)
-- ARGV[18] canonical_revision (settle only)
-- ARGV[19] final_state (settle only)
-- ARGV[20] settlement_proof_sha256 (settle only)

local receipt_key = KEYS[1]
local concurrent_key = KEYS[2]
local budget_key = KEYS[3]
local inflight_key = KEYS[4]

local action = ARGV[1]
local operation_id = ARGV[2]
local run_id = ARGV[3]
local tenant_id = ARGV[4]
local cost_raw = ARGV[5]
local version_raw = ARGV[6]
local proof_sha256 = ARGV[7]
local now_raw = ARGV[8]
local concurrent_limit_raw = ARGV[9]
local budget_limit_raw = ARGV[10]
local inflight_limit_raw = ARGV[11]
local released_receipt_ttl_raw = ARGV[12]
local run_terminate_operation_id = ARGV[13]
local terminal_proof_sha256 = ARGV[14]
local canonical_transition_id = ARGV[15]
local canonical_record_hash = ARGV[16]
local canonical_command_hash = ARGV[17]
local canonical_revision_raw = ARGV[18]
local final_state = ARGV[19]
local settlement_proof_sha256 = ARGV[20]

local MAX_SAFE_INTEGER = 9007199254740991
local OPERATION_PREFIX = 'run-create:v1:'
local TERMINATE_OPERATION_PREFIX = 'run-terminate:v1:'

local function redis_type_name(key)
    local reply = redis.call('TYPE', key)
    if type(reply) == 'table' then
        return reply['ok']
    end
    return reply
end

local function decimal_integer(raw, minimum)
    if type(raw) ~= 'string' then
        return nil
    end
    if raw ~= '0' and string.match(raw, '^[1-9]%d*$') == nil then
        return nil
    end
    local value = tonumber(raw)
    if value == nil or value ~= math.floor(value) then
        return nil
    end
    if value < minimum or value > MAX_SAFE_INTEGER then
        return nil
    end
    return value
end

local function optional_limit(raw)
    if raw == '-1' then
        return -1
    end
    return decimal_integer(raw, 0)
end

local function valid_operation_id(value)
    if type(value) ~= 'string' or string.len(value) ~= 78 then
        return false
    end
    if string.sub(value, 1, 14) ~= OPERATION_PREFIX then
        return false
    end
    local suffix = string.sub(value, 15)
    return string.len(suffix) == 64
        and string.match(suffix, '^[0-9a-f]+$') ~= nil
end

local function valid_run_terminate_operation_id(value)
    if type(value) ~= 'string' or string.len(value) ~= 81 then
        return false
    end
    if string.sub(value, 1, 17) ~= TERMINATE_OPERATION_PREFIX then
        return false
    end
    local suffix = string.sub(value, 18)
    return string.len(suffix) == 64
        and string.match(suffix, '^[0-9a-f]+$') ~= nil
end

local function valid_sha256(value)
    return type(value) == 'string'
        and string.len(value) == 64
        and string.match(value, '^[0-9a-f]+$') ~= nil
end

-- Missing counters are a valid zero baseline. Existing counters must already be
-- persistent STRING integers. This primitive never converts an expiring legacy
-- counter into persistent canonical accounting and never repairs a wrong type.
local function read_initial_counter(key)
    local key_type = redis_type_name(key)
    if key_type == 'none' then
        return 0
    end
    if key_type ~= 'string' or redis.call('TTL', key) ~= -1 then
        return nil
    end
    return decimal_integer(redis.call('GET', key), 0)
end

local function read_active_counter(key)
    if redis_type_name(key) ~= 'string' or redis.call('TTL', key) ~= -1 then
        return nil
    end
    return decimal_integer(redis.call('GET', key), 0)
end

local function active_resource_ownership(cost)
    local concurrent = read_active_counter(concurrent_key)
    local budget = read_active_counter(budget_key)
    local inflight = read_active_counter(inflight_key)
    if concurrent == nil or budget == nil or inflight == nil then
        return nil
    end
    if concurrent < 1 or budget < cost or inflight < 1 then
        return nil
    end
    return {concurrent, budget, inflight}
end

local cost = decimal_integer(cost_raw, 0)
local version = decimal_integer(version_raw, 1)
local now_ms = decimal_integer(now_raw, 0)
local concurrent_limit = optional_limit(concurrent_limit_raw)
local budget_limit = optional_limit(budget_limit_raw)
local inflight_limit = optional_limit(inflight_limit_raw)
local released_receipt_ttl = decimal_integer(released_receipt_ttl_raw, 1)

if action ~= 'reserve'
    and action ~= 'finalize'
    and action ~= 'release'
    and action ~= 'settle' then
    return {'invalid_input', '', '0'}
end
if not valid_operation_id(operation_id) or run_id == '' or tenant_id == '' then
    return {'invalid_input', '', '0'}
end
if cost == nil or version ~= 1 or now_ms == nil then
    return {'invalid_input', '', '0'}
end
if concurrent_limit == nil or budget_limit == nil or inflight_limit == nil then
    return {'invalid_input', '', '0'}
end
if released_receipt_ttl == nil or not valid_sha256(proof_sha256) then
    return {'invalid_input', '', '0'}
end

local canonical_revision = nil
if action == 'settle' then
    canonical_revision = decimal_integer(canonical_revision_raw, 1)
    if not valid_run_terminate_operation_id(run_terminate_operation_id)
        or not valid_sha256(terminal_proof_sha256)
        or type(canonical_transition_id) ~= 'string'
        or canonical_transition_id == ''
        or not valid_sha256(canonical_record_hash)
        or not valid_sha256(canonical_command_hash)
        or canonical_revision == nil
        or (final_state ~= 'done' and final_state ~= 'failed')
        or not valid_sha256(settlement_proof_sha256)
    then
        return {'invalid_input', '', '0'}
    end
end

local receipt_type = redis_type_name(receipt_key)
if receipt_type ~= 'none' and receipt_type ~= 'hash' then
    return {'reservation_conflict', '', '0'}
end

local receipt_exists = receipt_type == 'hash'
if receipt_exists then
    local stored = redis.call(
        'HMGET',
        receipt_key,
        'operation_id',
        'run_id',
        'tenant_id',
        'estimated_cost_cents',
        'reservation_version',
        'proof_sha256',
        'state'
    )

    -- Immutable proof is checked before resource inspection or mutation.
    if stored[1] ~= operation_id
        or stored[2] ~= run_id
        or stored[3] ~= tenant_id
        or stored[4] ~= cost_raw
        or stored[5] ~= version_raw
        or stored[6] ~= proof_sha256
    then
        return {'reservation_conflict', stored[7] or '', '0'}
    end

    local state = stored[7]
    if state ~= 'RESERVED'
        and state ~= 'FINALIZED'
        and state ~= 'RELEASED'
        and state ~= 'SETTLED' then
        return {'reservation_conflict', state or '', '0'}
    end

    if action == 'settle' then
        if state == 'SETTLED' then
            local terminal = redis.call(
                'HMGET', receipt_key,
                'run_terminate_operation_id', 'terminal_proof_sha256',
                'canonical_transition_id', 'canonical_record_hash',
                'canonical_command_hash', 'canonical_revision',
                'final_state', 'settlement_proof_sha256'
            )
            if terminal[1] ~= run_terminate_operation_id
                or terminal[2] ~= terminal_proof_sha256
                or terminal[3] ~= canonical_transition_id
                or terminal[4] ~= canonical_record_hash
                or terminal[5] ~= canonical_command_hash
                or terminal[6] ~= canonical_revision_raw
                or terminal[7] ~= final_state
                or terminal[8] ~= settlement_proof_sha256
            then
                return {'reservation_conflict', state, '0'}
            end
            return {'already_settled', state, '0'}
        end
        if state ~= 'FINALIZED' then
            return {'reservation_state_conflict', state, '0'}
        end
        if redis.call('TTL', receipt_key) ~= -1 then
            return {'resource_state_conflict', state, '0'}
        end
        local owned = active_resource_ownership(cost)
        if owned == nil then
            return {'resource_state_conflict', state, '0'}
        end
        redis.call('DECRBY', concurrent_key, '1')
        redis.call('DECRBY', budget_key, cost_raw)
        redis.call('DECRBY', inflight_key, '1')
        redis.call(
            'HSET', receipt_key,
            'state', 'SETTLED',
            'settled_at_ms', now_raw,
            'run_terminate_operation_id', run_terminate_operation_id,
            'terminal_proof_sha256', terminal_proof_sha256,
            'canonical_transition_id', canonical_transition_id,
            'canonical_record_hash', canonical_record_hash,
            'canonical_command_hash', canonical_command_hash,
            'canonical_revision', canonical_revision_raw,
            'final_state', final_state,
            'settlement_proof_sha256', settlement_proof_sha256
        )
        redis.call('PERSIST', receipt_key)
        return {'settled', 'SETTLED', '1'}
    end

    if action == 'reserve' then
        if state == 'RELEASED' then
            local released_ttl = redis.call('TTL', receipt_key)
            if released_ttl <= 0 then
                return {'reservation_conflict', state, '0'}
            end
            return {'already_released', state, '0'}
        end
        if state == 'SETTLED' then
            return {'reservation_state_conflict', state, '0'}
        end
        if redis.call('TTL', receipt_key) ~= -1 then
            return {'resource_state_conflict', state, '0'}
        end
        if active_resource_ownership(cost) == nil then
            return {'resource_state_conflict', state, '0'}
        end
        if state == 'FINALIZED' then
            return {'already_finalized', state, '0'}
        end
        return {'already_reserved', state, '0'}
    end

    if action == 'finalize' then
        if state == 'RELEASED' then
            return {'reservation_state_conflict', state, '0'}
        end
        if state == 'SETTLED' then
            return {'reservation_state_conflict', state, '0'}
        end
        if redis.call('TTL', receipt_key) ~= -1 then
            return {'resource_state_conflict', state, '0'}
        end
        if active_resource_ownership(cost) == nil then
            return {'resource_state_conflict', state, '0'}
        end
        if state == 'FINALIZED' then
            return {'already_finalized', state, '0'}
        end
        redis.call(
            'HSET', receipt_key,
            'state', 'FINALIZED',
            'finalized_at_ms', now_raw
        )
        redis.call('PERSIST', receipt_key)
        return {'finalized', 'FINALIZED', '0'}
    end

    if state == 'RELEASED' then
        local released_ttl = redis.call('TTL', receipt_key)
        if released_ttl <= 0 then
            return {'reservation_conflict', state, '0'}
        end
        return {'already_released', state, '0'}
    end
    if state == 'SETTLED' then
        return {'reservation_state_conflict', state, '0'}
    end
    if state == 'FINALIZED' then
        return {'reservation_state_conflict', state, '0'}
    end
    if redis.call('TTL', receipt_key) ~= -1 then
        return {'resource_state_conflict', state, '0'}
    end

    local owned = active_resource_ownership(cost)
    if owned == nil then
        return {'resource_state_conflict', state, '0'}
    end

    -- Every release invariant is validated before the first mutation.
    redis.call('DECRBY', concurrent_key, '1')
    redis.call('DECRBY', budget_key, cost_raw)
    redis.call('DECRBY', inflight_key, '1')
    redis.call(
        'HSET', receipt_key,
        'state', 'RELEASED',
        'released_at_ms', now_raw
    )
    redis.call('EXPIRE', receipt_key, released_receipt_ttl)
    return {'released', 'RELEASED', '1'}
end

if action ~= 'reserve' then
    return {'reservation_missing', '', '0'}
end

-- Validate current type, TTL, integer domain, arithmetic and every limit before
-- the first mutation. Existing expiring counters fail closed and retain their
-- original value and TTL.
local concurrent = read_initial_counter(concurrent_key)
local budget = read_initial_counter(budget_key)
local inflight = read_initial_counter(inflight_key)
if concurrent == nil or budget == nil or inflight == nil then
    return {'resource_state_conflict', '', '0'}
end
if concurrent > MAX_SAFE_INTEGER - 1
    or inflight > MAX_SAFE_INTEGER - 1
    or cost > MAX_SAFE_INTEGER - budget
then
    return {'resource_state_conflict', '', '0'}
end

local next_concurrent = concurrent + 1
local next_budget = budget + cost
local next_inflight = inflight + 1

if concurrent_limit >= 0 and next_concurrent > concurrent_limit then
    return {'concurrent_run_quota_exceeded', '', '0'}
end
if budget_limit >= 0 and next_budget > budget_limit then
    return {'budget_exceeded', '', '0'}
end
if inflight_limit >= 0 and next_inflight > inflight_limit then
    return {'tenant_inflight_exceeded', '', '0'}
end

redis.call('INCRBY', concurrent_key, '1')
redis.call('INCRBY', budget_key, cost_raw)
redis.call('INCRBY', inflight_key, '1')
redis.call(
    'HSET', receipt_key,
    'operation_id', operation_id,
    'run_id', run_id,
    'tenant_id', tenant_id,
    'estimated_cost_cents', cost_raw,
    'reservation_version', version_raw,
    'proof_sha256', proof_sha256,
    'state', 'RESERVED',
    'created_at_ms', now_raw,
    'finalized_at_ms', '',
    'released_at_ms', ''
)
redis.call('PERSIST', receipt_key)
return {'reserved', 'RESERVED', '1'}

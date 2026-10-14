-- hfa-core/src/hfa/lua/task_admit.lua
-- IRONCLAD Sprint 0/1 — Atomic DAG task admission.
--
-- Seeds a task into Redis.  If dependency_count == 0 the task is immediately
-- placed on the tenant ready queue.  Idempotent: re-admission of an existing
-- exact compatible postimage returns 'already_exists' without mutation.
-- Knowable command failures are prevalidated; Redis Lua rollback is not assumed.
--
-- ─── KEYS ──────────────────────────────────────────────────────────────────
-- 1  task_state_key           hfa:dag:task:<id>:state
-- 2  task_meta_key            hfa:dag:task:<id>:meta
-- 3  task_remaining_deps_key  hfa:dag:task:<id>:remaining_deps
-- 4  task_children_key        hfa:dag:task:<id>:children
-- 5  task_ready_emitted_key   hfa:dag:task:<id>:ready_emitted
-- 6  tenant_ready_queue_key   hfa:dag:tenant:<tid>:ready
-- 7  run_tasks_key            hfa:dag:run:<rid>:tasks
-- 8  tenant_active_set_key    hfa:dag:tenants:active
-- 9..14 canonical own TASK head/index/receipt/record/log/outbox (canonical mode)
-- 15  global task-owner HASH, 16 owner integrity header (only when ARGV[19] == '1')
--
-- ─── ARGV ──────────────────────────────────────────────────────────────────
-- 1   task_id
-- 2   run_id
-- 3   tenant_id
-- 4   agent_type
-- 5   priority
-- 6   admitted_at              epoch ms float (used as ready queue score)
-- 7   payload_json
-- 8   trace_parent
-- 9   trace_state
-- 10  dependency_count
-- 11  task_state_ttl
-- 12  task_meta_ttl
-- 13  ready_ttl
-- 14  region
-- 15  policy
-- 16  canonical_projection   1 or 0
-- 17  canonical_head_witness exact accepted TASK_ADMIT witness JSON
-- 18  current_head_witness    optional exact later TASK head (read-only retry)
-- 19  owner_required         '1' = validate the global owner inventory before the first write
-- 20  owner_expected         exact owner value derived from the stored canonical record
-- 21.. child_task_ids         variadic tail
--
-- ─── RETURN ────────────────────────────────────────────────────────────────
-- { status }   where status ∈ { already_exists, seeded_root, seeded_waiting }

local task_state_key        = KEYS[1]
local task_meta_key         = KEYS[2]
local task_remaining_key    = KEYS[3]
local task_children_key     = KEYS[4]
local task_ready_emitted    = KEYS[5]
local tenant_ready_queue    = KEYS[6]
local run_tasks_key         = KEYS[7]
local tenant_active_set     = KEYS[8]

local task_id               = ARGV[1]
local run_id                = ARGV[2]
local tenant_id             = ARGV[3]
local agent_type            = ARGV[4]
local priority              = ARGV[5]
local admitted_at           = ARGV[6]
local payload_json          = ARGV[7]  or '{}'
local trace_parent          = ARGV[8]  or ''
local trace_state           = ARGV[9]  or ''
local dependency_count      = tonumber(ARGV[10]) or 0
local task_state_ttl        = tonumber(ARGV[11])
local task_meta_ttl         = tonumber(ARGV[12])
local ready_ttl             = tonumber(ARGV[13])
local region                = ARGV[14] or ''
local policy                = ARGV[15] or 'LEAST_LOADED'

local progressed_head = nil

-- F04.4A: existing runtime state cannot replace canonical evidence, even on
-- duplicates. Revalidate the accepted own proof in this invocation before
-- EXISTS or the first projection write. Legacy mode retains its semantics.
if ARGV[16] == '1' then
    local function conflict() return {'canonical_task_admit_proof_conflict'} end
    local ok, own = pcall(cjson.decode, ARGV[17] or '')
    if (#KEYS ~= 14 and #KEYS ~= 16) or not ok or type(own) ~= 'table' or #own ~= 14 then
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
    local expected_state = dependency_count <= 0 and 'ready' or 'pending'
    if type(identity) ~= 'table' or identity.aggregate_type ~= 'task'
        or identity.run_id ~= run_id or identity.task_id ~= task_id
        or record.operation_type ~= 'TASK_ADMIT'
        or own[2] ~= 'task-admit:v1:' .. own[1]
        or own[5] ~= '1' or own[11] ~= expected_state or own[12] ~= '0'
        or type(meta) ~= 'table' or type(effects) ~= 'table' then
        return conflict()
    end
    -- Bind the contested runtime arguments to this exact accepted record.
    if meta.task_id ~= task_id or meta.run_id ~= run_id or meta.tenant_id ~= tenant_id
        or meta.agent_type ~= agent_type or meta.priority ~= tonumber(priority)
        or meta.admitted_at_ms ~= tonumber(admitted_at) or meta.payload_json ~= payload_json
        or meta.trace_parent ~= trace_parent or meta.trace_state ~= trace_state
        or meta.dependency_count ~= dependency_count or meta.region ~= region
        or meta.policy ~= policy then
        return conflict()
    end
    local children = effects.child_task_ids
    local membership = effects.run_tasks_membership
    if type(children) ~= 'table' or #children ~= #ARGV - 20
        or type(membership) ~= 'table' or membership.run_id ~= run_id
        or membership.task_id ~= task_id then return conflict() end
    for i=1,#children do
        if children[i] ~= ARGV[20+i] then return conflict() end
    end
    local head_ok, later = pcall(cjson.decode, ARGV[18] or '[]')
    if not head_ok or type(later) ~= 'table' then return conflict() end
    local proof_keys = {KEYS[9], KEYS[10], KEYS[11], KEYS[12], KEYS[13], KEYS[14]}
    if #later == 0 then
        local checked = validate_authority_head(proof_keys, own)
        if checked[1] ~= 'VALID' then return conflict() end
    else
        -- Historical admission can only classify a read-only progressed retry.
        -- Validate exact current head and fixed historical envelopes in this
        -- same invocation; never materialize the old postimage after progress.
        if #later ~= 14 then return conflict() end
        for i=1,14 do if type(later[i]) ~= 'string' then return conflict() end end
        if later[1] ~= own[1] or not tonumber(later[5]) or tonumber(later[5]) <= 1 then return conflict() end
        local decoded_head, head_record = pcall(cjson.decode, later[8])
        if not decoded_head or type(head_record) ~= 'table' then return conflict() end
        local hi = head_record.canonical_aggregate_identity
        if type(hi) ~= 'table' or hi.aggregate_type ~= 'task'
            or hi.run_id ~= run_id or hi.task_id ~= task_id then return conflict() end
        local checked = validate_authority_head(proof_keys, later)
        if checked[1] ~= 'VALID' then return conflict() end
        local function exact_envelope(key, field, payload)
            local raw = redis.call('HGET', key, field)
            if not raw then return false end
            local good, env = pcall(cjson.decode, raw)
            return good and type(env) == 'table' and env.payload == payload
                and redis.sha1hex(payload) == env.storage_sha1
        end
        if not exact_envelope(KEYS[12], own[3], own[8])
            or not exact_envelope(KEYS[11], own[3], own[9])
            or not exact_envelope(KEYS[10], own[4], own[10]) then return conflict() end
        progressed_head = head_record
    end
    -- F07 v2: the owner inventory is validated in THIS invocation, before EXISTS or any write,
    -- for fresh materialization, exact duplicates and recover-only replays alike.
    if ARGV[19] == '1' then
        local function owner_conflict() return {'canonical_task_admit_owner_conflict'} end
        local function rtype(key)
            local reply = redis.call('TYPE', key)
            if type(reply) == 'table' then return reply['ok'] end
            return reply
        end
        if #KEYS ~= 16 or type(ARGV[20]) ~= 'string' or ARGV[20] == '' then return owner_conflict() end
        if rtype(KEYS[15]) ~= 'hash' or rtype(KEYS[16]) ~= 'hash'
            or redis.call('HLEN', KEYS[16]) ~= 7
            or redis.call('PTTL', KEYS[15]) ~= -1 or redis.call('PTTL', KEYS[16]) ~= -1 then
            return owner_conflict()
        end
        local flat = redis.call('HGETALL', KEYS[16])
        local header = {}
        for i=1,#flat,2 do header[flat[i]] = flat[i+1] end
        local count = tonumber(header.owner_count)
        if header.schema ~= 'task-owner-header:v1' or header.state ~= 'ACTIVE'
            or not count or count < 1 or count ~= math.floor(count)
            or redis.call('HLEN', KEYS[15]) ~= count
            or redis.call('HGET', KEYS[15], task_id) ~= ARGV[20] then
            return owner_conflict()
        end
    elseif ARGV[19] ~= '0' or #KEYS ~= 14 then
        return {'canonical_task_admit_proof_conflict'}
    end
elseif ARGV[16] ~= '0' or #KEYS ~= 8 or ARGV[19] ~= '0' then
    return {'canonical_task_admit_proof_conflict'}
end

-- F06: every knowable target/argument error is rejected before the first write.
local function reject(status) return {status or 'task_admit_effect_conflict'} end
local function kind(key)
    local value = redis.call('TYPE', key)
    return type(value) == 'table' and value['ok'] or value
end
local function ttl_valid(value)
    return value and value > 0 and value <= 2147483647 and value == math.floor(value)
end
local score = tonumber(admitted_at)
if not task_id or task_id == '' or not run_id or run_id == '' or not tenant_id or tenant_id == ''
    or not score or score ~= score or math.abs(score) > 9007199254740991
    or not tonumber(ARGV[10]) or dependency_count < 0
    or dependency_count > 9007199254740991 or dependency_count ~= math.floor(dependency_count)
    or not ttl_valid(task_state_ttl) or not ttl_valid(task_meta_ttl) or not ttl_valid(ready_ttl) then
    return reject('task_admit_input_conflict')
end
local destinations = {
    'hfa:dag:task:' .. task_id .. ':state', 'hfa:dag:task:' .. task_id .. ':meta',
    'hfa:dag:task:' .. task_id .. ':remaining_deps', 'hfa:dag:task:' .. task_id .. ':children',
    'hfa:dag:task:' .. task_id .. ':ready_emitted', 'hfa:dag:tenant:' .. tenant_id .. ':ready',
    'hfa:dag:run:' .. run_id .. ':tasks', 'hfa:dag:tenants:active',
}
local types = {'string', 'hash', 'string', 'set', 'string', 'zset', 'set', 'set'}
local observed = {}
for i=1,8 do
    if KEYS[i] ~= destinations[i] then return reject() end
    observed[i] = kind(KEYS[i])
    if observed[i] ~= 'none' and observed[i] ~= types[i] then
        return reject('task_admit_target_type_conflict')
    end
end
local children, child_count = {}, 0
for i=21,#ARGV do
    if ARGV[i] == '' then return reject('task_admit_input_conflict') end
    if not children[ARGV[i]] then children[ARGV[i]] = true; child_count = child_count + 1 end
end
local expected_meta = {
    task_id=task_id, run_id=run_id, tenant_id=tenant_id, agent_type=agent_type,
    priority=priority, admitted_at=admitted_at, payload_json=payload_json,
    trace_parent=trace_parent, trace_state=trace_state, region=region, policy=policy,
}
local function meta_matches()
    if observed[2] ~= 'hash' then return false end
    for field,value in pairs(expected_meta) do
        if redis.call('HGET', task_meta_key, field) ~= value then return false end
    end
    return true
end
local member = redis.call('SISMEMBER', run_tasks_key, task_id)
local queued_score = redis.call('ZSCORE', tenant_ready_queue, task_id)
local state = observed[1] == 'string' and redis.call('GET', task_state_key) or false
if progressed_head then
    -- Later durable lifecycle evidence proves admission was consumable. Only
    -- compatible identity/state may be acknowledged; no queue/TTL reconstruction.
    local states = {ready=true, pending=true, scheduled=true, running=true, done=true, failed=true, skipped=true}
    if not states[state] or not meta_matches()
        or (state ~= progressed_head.next_state and state ~= progressed_head.previous_state
            and state ~= (dependency_count == 0 and 'ready' or 'pending')) then return reject() end
    return {'already_exists'}
end
local footprint = member == 1 or queued_score ~= false
for i=1,5 do if observed[i] ~= 'none' then footprint = true end end
if footprint then
    -- A partial postimage is an explicit operations-owned conflict, never an
    -- automatic repair or admitted success. Complete original postimage is a
    -- read-only duplicate; absolute expiries are preserved.
    if state ~= (dependency_count == 0 and 'ready' or 'pending')
        or not meta_matches() or redis.call('HLEN', task_meta_key) ~= 11
        or member ~= 1 or observed[3] ~= 'string'
        or redis.call('GET', task_remaining_key) ~= tostring(dependency_count)
        or redis.call('SCARD', task_children_key) ~= child_count then return reject() end
    for child,_ in pairs(children) do
        if redis.call('SISMEMBER', task_children_key, child) ~= 1 then return reject() end
    end
    for _,i in ipairs({1,2,3,7}) do
        if redis.call('PTTL', KEYS[i]) <= 0 then return reject() end
    end
    if child_count > 0 and redis.call('PTTL', task_children_key) <= 0 then return reject() end
    if dependency_count == 0 then
        if not queued_score or tonumber(queued_score) ~= score
            or redis.call('GET', task_ready_emitted) ~= '1'
            or redis.call('SISMEMBER', tenant_active_set, tenant_id) ~= 1
            or redis.call('PTTL', task_ready_emitted) <= 0
            or redis.call('PTTL', tenant_ready_queue) <= 0 then return reject() end
    elseif observed[5] ~= 'none' or queued_score ~= false then return reject() end
    return {'already_exists'}
end

-- Register this task in its run.
redis.call('SADD', run_tasks_key, task_id)
redis.call('EXPIRE', run_tasks_key, task_meta_ttl)

-- Write task metadata.
redis.call('HSET', task_meta_key,
    'task_id',       task_id,
    'run_id',        run_id,
    'tenant_id',     tenant_id,
    'agent_type',    agent_type,
    'priority',      priority,
    'admitted_at',   admitted_at,
    'payload_json',  payload_json,
    'trace_parent',  trace_parent,
    'trace_state',   trace_state,
    'region',        region,
    'policy',        policy
)
redis.call('EXPIRE', task_meta_key, task_meta_ttl)

-- Write remaining deps counter (always, even when 0 — tests assert its presence).
redis.call('SET', task_remaining_key, tostring(dependency_count), 'EX', task_state_ttl)

-- Register child task ids.
for i = 21, #ARGV do
    redis.call('SADD', task_children_key, ARGV[i])
end
if #ARGV >= 21 then
    redis.call('EXPIRE', task_children_key, task_meta_ttl)
end

if dependency_count <= 0 then
    redis.call('SET', task_state_key, 'ready', 'EX', task_state_ttl)
    redis.call('ZADD', tenant_ready_queue, 'NX', admitted_at, task_id)
    redis.call('EXPIRE', tenant_ready_queue, ready_ttl)
    redis.call('SET', task_ready_emitted, '1', 'EX', ready_ttl)
    redis.call('SADD', tenant_active_set, tenant_id)
    return {'seeded_root'}
else
    redis.call('SET', task_state_key, 'pending', 'EX', task_state_ttl)
    return {'seeded_waiting'}
end

-- hfa-core/src/hfa/lua/task_admit.lua
-- IRONCLAD Sprint 0/1 — Atomic DAG task admission.
--
-- Seeds a task into Redis.  If dependency_count == 0 the task is immediately
-- placed on the tenant ready queue.  Idempotent: re-admission of an existing
-- task_id returns 'already_exists' without mutation.
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
-- 16.. child_task_ids          variadic tail
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
local task_state_ttl        = tonumber(ARGV[11]) or 86400
local task_meta_ttl         = tonumber(ARGV[12]) or 86400
local ready_ttl             = tonumber(ARGV[13]) or 86400
local region                = ARGV[14] or ''
local policy                = ARGV[15] or 'LEAST_LOADED'

-- Idempotency guard.
if redis.call('EXISTS', task_state_key) == 1 then
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
for i = 16, #ARGV do
    redis.call('SADD', task_children_key, ARGV[i])
end
if #ARGV >= 16 then
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

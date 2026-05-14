-- hfa-core/src/hfa/lua/task_dispatch_commit.lua
-- IRONCLAD Sprint 0/1 — Atomic DAG dispatch commit.
--
-- Transitions a task: ready → scheduled.
-- Writes to the SCHEDULED zset (NOT the running zset — running is set at claim).
-- Emits TaskScheduled to control stream and TaskRequested to shard stream.
--
-- ─── KEYS ──────────────────────────────────────────────────────────────────
-- 1  task_state_key         hfa:dag:task:<id>:state
-- 2  task_meta_key          hfa:dag:task:<id>:meta
-- 3  task_scheduled_zset    hfa:dag:tenant:<tid>:scheduled
-- 4  control_stream_key
-- 5  shard_stream_key
-- 6  tenant_ready_queue     hfa:dag:tenant:<tid>:ready  (ZREM on success)
--
-- ─── ARGV ──────────────────────────────────────────────────────────────────
-- 1   task_id
-- 2   run_id
-- 3   tenant_id
-- 4   agent_type
-- 5   worker_group
-- 6   shard
-- 7   priority
-- 8   admitted_at
-- 9   scheduled_at           epoch ms
-- 10  task_state_ttl
-- 11  task_meta_ttl
-- 12  control_stream_maxlen
-- 13  shard_stream_maxlen
-- 14  trace_parent
-- 15  trace_state
-- 16  policy
-- 17  region
-- 18  payload_json
--
-- ─── RETURN ────────────────────────────────────────────────────────────────
-- { status, current_or_prev_state }
--
-- Status values:
--   committed
--   missing_state
--   already_running
--   already_scheduled
--   illegal_transition

local task_state_key        = KEYS[1]
local task_meta_key         = KEYS[2]
local task_scheduled_zset   = KEYS[3]
local control_stream        = KEYS[4]
local shard_stream          = KEYS[5]
local tenant_ready_queue    = KEYS[6]

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

-- ── Guard ─────────────────────────────────────────────────────────────────
local current = redis.call('GET', task_state_key)
if not current then
    return {'missing_state', ''}
end

if current == 'running' then
    return {'already_running', current}
end

if current == 'scheduled' then
    return {'already_scheduled', current}
end

if current == 'done' or current == 'failed' or current == 'blocked_by_failure'
        or current == 'dead_lettered' or current == 'skipped' then
    return {'illegal_transition', current}
end

if current ~= 'ready' then
    return {'state_conflict', current}
end

-- ── Commit ────────────────────────────────────────────────────────────────
-- Remove from ready queue atomically here — prevents lost-work window.
-- Python side must ONLY peek, never ZREM before this script runs.
redis.call('ZREM', tenant_ready_queue, task_id)
redis.call('SET', task_state_key, 'scheduled', 'EX', task_state_ttl)
redis.call('HSET', task_meta_key,
    'task_id',        task_id,
    'run_id',         run_id,
    'tenant_id',      tenant_id,
    'agent_type',     agent_type,
    'worker_group',   worker_group,
    'shard',          shard,
    'priority',       priority,
    'admitted_at',    admitted_at,
    'scheduled_at',   scheduled_at,
    'dispatch_policy', policy,
    'dispatch_region', region,
    'payload_json',   payload_json,
    'trace_parent',   trace_parent,
    'trace_state',    trace_state
)
redis.call('EXPIRE', task_meta_key, task_meta_ttl)
redis.call('ZADD', task_scheduled_zset, scheduled_at, task_id)
redis.call('EXPIRE', task_scheduled_zset, task_meta_ttl)

-- ── Emit events ───────────────────────────────────────────────────────────
redis.call('XADD', control_stream, 'MAXLEN', '~', control_maxlen, '*',
    'event_type',   'TaskScheduled',
    'task_id',      task_id,
    'run_id',       run_id,
    'tenant_id',    tenant_id,
    'agent_type',   agent_type,
    'worker_group', worker_group,
    'shard',        shard,
    'region',       region,
    'policy',       policy,
    'scheduled_at', scheduled_at,
    'trace_parent', trace_parent,
    'trace_state',  trace_state
)

redis.call('XADD', shard_stream, 'MAXLEN', '~', shard_maxlen, '*',
    'event_type',   'TaskRequested',
    'task_id',      task_id,
    'run_id',       run_id,
    'tenant_id',    tenant_id,
    'agent_type',   agent_type,
    'worker_group', worker_group,
    'shard',        shard,
    'priority',     priority,
    'payload_json', payload_json,
    'requested_at', scheduled_at,
    'trace_parent', trace_parent,
    'trace_state',  trace_state
)

return {'committed', current}

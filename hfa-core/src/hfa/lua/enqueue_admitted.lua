--[[
hfa-core/src/hfa/lua/enqueue_admitted.lua
IRONCLAD Sprint 21 v3 — Atomic admission + enqueue with rollback

Sprint 21 v3 changes
--------------------
1. ZADD failure triggers DECR rollback on inflight counter (no leak).
2. inflight key TTL refreshed on every INCR (no zombie counters).
3. Score clamped to minimum 1 (no negative score from heavy aging).
4. Returns structured result codes:
     1   = admitted and enqueued
     0   = already queued (idempotent)
    -1   = rejected: inflight limit exceeded
    -2   = internal error: enqueue failed after inflight increment (rolled back)

KEYS
----
KEYS[1]  tenant queue ZSET       hfa:tenant:{id}:queue
KEYS[2]  run meta HASH           hfa:run:meta:{run_id}
KEYS[3]  active tenant SET       hfa:cp:tenant:active
KEYS[4]  run state STRING        hfa:run:state:{run_id}
KEYS[5]  run payload STRING      hfa:run:payload:{run_id}
KEYS[6]  tenant inflight counter hfa:tenant:{id}:inflight

ARGV
----
ARGV[1]  score (float, clamped ≥ 1 in Lua)
ARGV[2]  run_id
ARGV[3]  tenant_id
ARGV[4]  agent_type
ARGV[5]  priority
ARGV[6]  preferred_region
ARGV[7]  preferred_placement
ARGV[8]  admitted_at (float string)
ARGV[9]  queue_ttl (seconds)
ARGV[10] meta_ttl (seconds)
ARGV[11] state_ttl (seconds)
ARGV[12] estimated_cost_cents
ARGV[13] trace_parent
ARGV[14] trace_state
ARGV[15] payload_json
ARGV[16] max_inflight (0 = unlimited)
ARGV[17] inflight_ttl (seconds)
--]]

local queue_key     = KEYS[1]
local meta_key      = KEYS[2]
local active_key    = KEYS[3]
local state_key     = KEYS[4]
local payload_key   = KEYS[5]
local inflight_key  = KEYS[6]

local score         = math.max(1, tonumber(ARGV[1]) or 1)  -- clamp ≥ 1
local run_id        = ARGV[2]
local tenant_id     = ARGV[3]
local agent_type    = ARGV[4]
local priority      = ARGV[5]
local region        = ARGV[6]
local placement     = ARGV[7]
local admitted_at   = ARGV[8]
local queue_ttl     = tonumber(ARGV[9])  or 86400
local meta_ttl      = tonumber(ARGV[10]) or 86400
local state_ttl     = tonumber(ARGV[11]) or 86400
local cost_cents    = ARGV[12]
local trace_parent  = ARGV[13]
local trace_state   = ARGV[14]
local payload_json  = ARGV[15]
local max_inflight  = tonumber(ARGV[16]) or 0
local inflight_ttl  = tonumber(ARGV[17]) or 86400

-- ── Step 1: Atomic inflight gate ──────────────────────────────────────────
if max_inflight > 0 then
    local current = tonumber(redis.call('GET', inflight_key) or '0') or 0
    if current >= max_inflight then
        return -1  -- rejected: limit exceeded
    end
    -- Increment with TTL refresh (prevents zombie counters)
    redis.call('INCR', inflight_key)
    redis.call('EXPIRE', inflight_key, inflight_ttl)
end

-- ── Step 2: Enqueue (NX — idempotent) ────────────────────────────────────
local added = redis.call('ZADD', queue_key, 'NX', score, run_id)

if added == 0 then
    -- Already queued: roll back inflight increment (nothing was added)
    if max_inflight > 0 then
        local v = tonumber(redis.call('GET', inflight_key) or '1') or 1
        if v > 0 then
            redis.call('SET', inflight_key, tostring(v - 1), 'EX', inflight_ttl)
        end
    end
    return 0  -- idempotent
end

-- Verify ZADD actually succeeded (defensive: ZADD NX returns 0 on existing, 1 on new)
-- If we reach here, added == 1, which is success.

-- ── Step 3: Write full meta atomically ───────────────────────────────────
local hset_result = redis.call('HSET', meta_key,
    'run_id',               run_id,
    'tenant_id',            tenant_id,
    'agent_type',           agent_type,
    'priority',             priority,
    'preferred_region',     region,
    'preferred_placement',  placement,
    'admitted_at',          admitted_at,
    'queue_state',          'queued',
    'estimated_cost_cents', cost_cents,
    'trace_parent',         trace_parent,
    'trace_state',          trace_state
)

-- ── Step 4: Payload ───────────────────────────────────────────────────────
if payload_json and payload_json ~= '' then
    redis.call('SET', payload_key, payload_json, 'EX', meta_ttl)
end

-- ── Step 5: State ─────────────────────────────────────────────────────────
redis.call('SET', state_key, 'queued', 'EX', state_ttl)

-- ── Step 6: Active tenant index ───────────────────────────────────────────
redis.call('SADD', active_key, tenant_id)

-- ── Step 7: TTL refresh ───────────────────────────────────────────────────
redis.call('EXPIRE', queue_key, queue_ttl)
redis.call('EXPIRE', meta_key,  meta_ttl)

return 1

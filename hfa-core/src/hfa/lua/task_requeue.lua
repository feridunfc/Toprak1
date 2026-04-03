-- hfa-core/src/hfa/lua/task_requeue.lua
-- IRONCLAD Sprint 2 patch — Monotonic claim_epoch on requeue.
--
-- CRITICAL FIX: claim_epoch must NEVER be reset to 0.
-- It is a monotonically increasing generation counter for the lifetime of the task.
-- On requeue, only worker_instance_id and scheduler_epoch are cleared.
-- The next successful claim will HINCRBY claim_epoch by 1, producing a strictly
-- higher generation (e.g. 1 → 2, 2 → 3) that makes all old fence tokens invalid.
--
-- Why this matters:
--   Resetting claim_epoch to 0 means the next claim produces epoch=1 again,
--   which is identical to the old value.  A zombie completion from an old worker
--   would then pass the claim_epoch check if it also sent epoch=1.
--   By keeping claim_epoch monotonic, old fence tokens can never be valid again.
--
-- ─── KEYS ──────────────────────────────────────────────────────────────────
-- 1  task_state_key
-- 2  task_meta_key
-- 3  tenant_ready_queue
-- 4  task_running_zset
-- 5  completion_stream
--
-- ─── ARGV ──────────────────────────────────────────────────────────────────
-- 1  task_id
-- 2  tenant_id
-- 3  expected_state       must be "running"
-- 4  now_ms
-- 5  ready_score
-- 6  max_requeue_count
-- 7  reason_code
-- 8  stream_maxlen
--
-- ─── RETURN ────────────────────────────────────────────────────────────────
-- { status, value }
--   TASK_REQUEUED        value = requeue_count
--   TASK_RETRY_EXHAUSTED value = requeue_count
--   TASK_TERMINAL        value = current state
--   TASK_STATE_CONFLICT  value = current state

local task_state_key     = KEYS[1]
local task_meta_key      = KEYS[2]
local tenant_ready_queue = KEYS[3]
local task_running_zset  = KEYS[4]
local completion_stream  = KEYS[5]

local task_id           = ARGV[1]
local tenant_id         = ARGV[2]
local expected_state    = ARGV[3]
local now_ms            = ARGV[4]
local ready_score       = ARGV[5]
local max_requeue_count = tonumber(ARGV[6])
local reason_code       = ARGV[7]
local stream_maxlen     = tonumber(ARGV[8])

local function is_terminal(s)
    return s == 'done'
        or s == 'failed'
        or s == 'blocked_by_failure'
        or s == 'dead_lettered'
        or s == 'skipped'
end

-- ── State guard ───────────────────────────────────────────────────────────
local current_state = redis.call('GET', task_state_key)
if not current_state then
    return {'TASK_STATE_CONFLICT', 'missing_state'}
end

if is_terminal(current_state) then
    return {'TASK_TERMINAL', current_state}
end

if current_state ~= expected_state then
    return {'TASK_STATE_CONFLICT', current_state}
end

-- ── Retry counter ─────────────────────────────────────────────────────────
local raw_count = redis.call('HGET', task_meta_key, 'requeue_count')
local retries = tonumber(raw_count or '0') or 0
retries = retries + 1

-- ── Remove from running zset (always) ────────────────────────────────────
redis.call('ZREM', task_running_zset, task_id)

-- ── Retry exhausted → dead_lettered ──────────────────────────────────────
if retries > max_requeue_count then
    redis.call('SET', task_state_key, 'dead_lettered')
    redis.call('HSET', task_meta_key,
        'requeue_count',        tostring(retries),
        'last_requeue_reason',  reason_code,
        'dead_lettered_at_ms',  now_ms,
        -- Clear identity fields so no write can succeed with old values.
        -- claim_epoch is NOT reset — it stays at its current value.
        -- The next claim will INCR it to a strictly higher generation.
        'worker_instance_id',   '',
        'scheduler_epoch',      '',
        'last_heartbeat_at_ms', '0'
    )
    redis.call('XADD', completion_stream, 'MAXLEN', '~', stream_maxlen, '*',
        'event_type',    'TaskDeadLettered',
        'task_id',       task_id,
        'tenant_id',     tenant_id,
        'reason_code',   'STALE_RETRY_EXHAUSTED',
        'requeue_count', tostring(retries),
        'at_ms',         now_ms
    )
    return {'TASK_RETRY_EXHAUSTED', tostring(retries)}
end

-- ── Requeue → ready ───────────────────────────────────────────────────────
redis.call('SET', task_state_key, 'ready')
redis.call('HSET', task_meta_key,
    'requeue_count',        tostring(retries),
    'last_requeue_reason',  reason_code,
    'last_requeue_at_ms',   now_ms,
    -- Clear identity so old completion/heartbeat can't pass owner_mismatch check.
    -- CRITICAL: claim_epoch is intentionally NOT touched.
    -- It stays at current value (e.g. 1).
    -- Next claim: HINCRBY claim_epoch 1 → produces 2.
    -- Old fence token with epoch=1 is then rejected by claim_epoch_mismatch.
    'worker_instance_id',   '',
    'scheduler_epoch',      '',
    'last_heartbeat_at_ms', '0'
)
redis.call('ZADD', tenant_ready_queue, 'NX', ready_score, task_id)
redis.call('XADD', completion_stream, 'MAXLEN', '~', stream_maxlen, '*',
    'event_type',    'TaskRequeued',
    'task_id',       task_id,
    'tenant_id',     tenant_id,
    'reason_code',   reason_code,
    'requeue_count', tostring(retries),
    'at_ms',         now_ms
)

return {'TASK_REQUEUED', tostring(retries)}

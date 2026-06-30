-- hfa-core/src/hfa/lua/task_complete.lua
-- IRONCLAD Sprint 2 — Owner-fenced task completion.
--
-- Sprint 2 change: completion now requires the full fencing tuple:
--   worker_instance_id + scheduler_epoch + claim_epoch
-- A stale worker with an older claim_epoch is rejected deterministically.
--
-- Sprint 1 invariants preserved:
--   - only current_state == "running" may complete
--   - child unlock only for child_state == "pending"
--
-- ─── KEYS ──────────────────────────────────────────────────────────────────
-- 1  task_state_key
-- 2  task_meta_key
-- 3  task_children_key
-- 4  task_output_key
-- 5  tenant_ready_queue
-- 6  task_running_zset
--
-- ─── ARGV ──────────────────────────────────────────────────────────────────
-- 1   task_id
-- 2   run_id
-- 3   tenant_id
-- 4   terminal_state              "done" | "failed"
-- 5   finished_at_ms
-- 6   state_ttl_seconds
-- 7   meta_ttl_seconds
-- 8   output_ttl_seconds
-- 9   ready_score
-- 10  reason_code
-- 11  expected_worker_instance_id  "" = skip fence
-- 12  output_json                  "" = no output
-- 13  child_state_prefix
-- 14  child_state_suffix
-- 15  child_remaining_prefix
-- 16  child_remaining_suffix
-- 17  child_ready_emitted_prefix
-- 18  child_ready_emitted_suffix
-- 19  expected_scheduler_epoch     "" = skip fence
-- 20  expected_claim_epoch         "" = skip fence
--
-- ─── RETURN ────────────────────────────────────────────────────────────────
-- { committed_flag, status_string, unlocked_count, already_terminal_flag }
--
-- Status values:
--   committed
--   already_terminal
--   illegal_transition
--   missing_task
--   invalid_terminal_state
--   owner_mismatch
--   scheduler_epoch_mismatch
--   claim_epoch_mismatch

local task_state_key            = KEYS[1]
local task_meta_key             = KEYS[2]
local task_children_key         = KEYS[3]
local task_output_key           = KEYS[4]
local tenant_ready_queue        = KEYS[5]
local task_running_zset         = KEYS[6]

local task_id                   = ARGV[1]
local run_id                    = ARGV[2]
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

-- Validate requested terminal state before any Redis reads.
if terminal_state ~= 'done' and terminal_state ~= 'failed' then
    return {0, 'invalid_terminal_state', 0, 0}
end

local function is_terminal(s)
    return s == 'done'
        or s == 'failed'
        or s == 'blocked_by_failure'
        or s == 'dead_lettered'
        or s == 'skipped'
end

-- ── Read current state ────────────────────────────────────────────────────
local current_state = redis.call('GET', task_state_key)
if not current_state then
    return {0, 'missing_task', 0, 0}
end

if is_terminal(current_state) then
    return {0, 'already_terminal', 0, 1}
end

-- CRITICAL (Sprint 1): only "running" may complete.
if current_state ~= 'running' then
    return {0, 'illegal_transition', 0, 0}
end

-- ── Full fence check (Sprint 2) ───────────────────────────────────────────
-- Read all three fence fields in one HMGET to minimize round-trips.
local fence = redis.call('HMGET', task_meta_key,
    'worker_instance_id',
    'scheduler_epoch',
    'claim_epoch'
)
local stored_worker   = fence[1] or ''
local stored_sched_ep = fence[2] or ''
local stored_claim_ep = fence[3] or ''

-- Fence priority:
--   1) claim_epoch catches zombie/stale completions first
--   2) scheduler_epoch catches stale scheduler ownership
--   3) worker ownership catches wrong live owner
if expected_claim_epoch ~= '' then
    if stored_claim_ep ~= expected_claim_epoch then
        return {0, 'claim_epoch_mismatch', 0, 0}
    end
end

if expected_scheduler_epoch ~= '' then
    if stored_sched_ep ~= expected_scheduler_epoch then
        return {0, 'scheduler_epoch_mismatch', 0, 0}
    end
end

if expected_worker_id ~= '' then
    if stored_worker ~= expected_worker_id then
        return {0, 'task_owner_mismatch', 0, 0}
    end
end

-- ── Commit parent state ───────────────────────────────────────────────────
redis.call('SET', task_state_key, terminal_state, 'EX', state_ttl)
redis.call('HSET', task_meta_key,
    'completed_at_ms',    finished_at_ms,
    'terminal_state',     terminal_state,
    'completion_reason',  reason_code,
    'worker_instance_id', expected_worker_id
)
redis.call('EXPIRE', task_meta_key, meta_ttl)
redis.call('ZREM', task_running_zset, task_id)

-- ── Persist output ────────────────────────────────────────────────────────
if terminal_state == 'done' and output_json ~= '' then
    redis.call('SET', task_output_key, output_json, 'EX', output_ttl)
end

if terminal_state ~= 'done' then
    return {1, 'committed', 0, 0}
end

-- ── Child unlock (CRITICAL Sprint 1: only pending children) ──────────────
local unlocked = 0
local children = redis.call('SMEMBERS', task_children_key)

for _, child_id in ipairs(children) do
    local child_state_key     = child_state_pfx     .. child_id .. child_state_sfx
    local child_remaining_key = child_remaining_pfx .. child_id .. child_remaining_sfx
    local child_emitted_key   = child_ready_emitted_pfx .. child_id .. child_ready_emitted_sfx

    local child_state = redis.call('GET', child_state_key)

    -- Only pending children are eligible for unlock.
    if child_state == 'pending' then
        local remaining = tonumber(redis.call('DECR', child_remaining_key))
        if remaining == nil then remaining = 0 end
        if remaining < 0 then
            redis.call('SET', child_remaining_key, 0)
            remaining = 0
        end
        if remaining == 0 then
            if redis.call('EXISTS', child_emitted_key) == 0 then
                redis.call('SET', child_state_key, 'ready', 'EX', state_ttl)
                redis.call('SET', child_emitted_key, '1', 'EX', state_ttl)
                redis.call('ZADD', tenant_ready_queue, 'NX', ready_score, child_id)
                unlocked = unlocked + 1
            end
        end
    end
end

return {1, 'committed', unlocked, 0}

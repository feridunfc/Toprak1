-- hfa-core/src/hfa/lua/task_claim_start.lua
-- IRONCLAD Sprint 2 — Atomic task claim with epoch fencing.
--
-- Sprint 2 change: successful claim now atomically increments claim_epoch
-- (INCR on meta hash field).  The new claim_epoch is returned to the caller
-- so it can be carried end-to-end for completion/heartbeat fencing.
--
-- Ownership tuple written on successful claim:
--   worker_instance_id   = ARGV[2]
--   scheduler_epoch      = reservation.scheduler_epoch
--   claim_epoch          = previous_value + 1   (atomic HINCRBY)
--   claimed_at_ms        = ARGV[3]
--   last_heartbeat_at_ms = ARGV[3]
--
-- Field names match TaskMetaField constants in hfa/dag/schema.py.
--
-- ─── KEYS ──────────────────────────────────────────────────────────────────
-- 1  task_state_key         hfa:dag:task:<id>:state
-- 2  task_meta_key          hfa:dag:task:<id>:meta
-- 3  task_scheduled_zset    hfa:dag:tenant:<tid>:scheduled
-- 4  task_running_zset      hfa:dag:tenant:<tid>:running
-- 5  reservation_key        hfa:dag:worker:<wid>:reservation
--
-- ─── ARGV ──────────────────────────────────────────────────────────────────
-- 1  task_id
-- 2  worker_instance_id
-- 3  claimed_at_ms
-- 4  state_ttl
-- 5  meta_ttl
-- 6  heartbeat_score
-- 7  expected_scheduler_epoch   "" to skip epoch check
--
-- ─── RETURN ────────────────────────────────────────────────────────────────
-- { status, claim_epoch, scheduler_epoch, worker_instance_id, task_id }
--
-- On success:  status = "task_claimed"
-- On failure:  status = error code, remaining fields = ""
--
-- Error codes:
--   task_missing
--   task_already_owned
--   task_state_conflict
--   reservation_missing
--   reservation_worker_mismatch
--   reservation_task_mismatch
--   reservation_epoch_mismatch

local task_state_key        = KEYS[1]
local task_meta_key         = KEYS[2]
local task_scheduled_zset   = KEYS[3]
local task_running_zset     = KEYS[4]
local reservation_key       = KEYS[5]

local task_id                   = ARGV[1]
local worker_instance_id        = ARGV[2]
local claimed_at_ms             = ARGV[3]
local state_ttl                 = tonumber(ARGV[4])
local meta_ttl                  = tonumber(ARGV[5])
local heartbeat_score           = tonumber(ARGV[6])
local expected_scheduler_epoch  = ARGV[7]

-- ── State guard ───────────────────────────────────────────────────────────
local current_state = redis.call('GET', task_state_key)
if not current_state then
    return {'task_missing', '', '', '', task_id}
end

if current_state == 'running' then
    return {'task_already_owned', '', '', '', task_id}
end

-- Reservation guard
-- COMPATIBILITY ONLY:
--   * scheduler_epoch supplied  => reservation is mandatory
--   * scheduler_epoch empty     => legacy/direct claim is allowed, but only
--                                  for tasks currently in scheduled state.
local legacy_direct_claim = false
local has_reservation = redis.call('EXISTS', reservation_key)

if has_reservation == 0 then
    if current_state == 'scheduled' and expected_scheduler_epoch == '' then
        legacy_direct_claim = true
    else
        return {'reservation_missing', '', '', '', task_id}
    end
end

if current_state ~= 'scheduled' then
    return {'task_state_conflict', '', '', '', task_id}
end

local reserved_worker = worker_instance_id
local reserved_task   = task_id
local reserved_epoch  = ''

if not legacy_direct_claim then
    reserved_worker = redis.call('HGET', reservation_key, 'worker_id')
    reserved_task   = redis.call('HGET', reservation_key, 'task_id')
    reserved_epoch  = redis.call('HGET', reservation_key, 'scheduler_epoch')

    if reserved_worker ~= worker_instance_id then
        return {'reservation_worker_mismatch', '', '', reserved_worker or '', task_id}
    end

    if reserved_task ~= task_id then
        return {'reservation_task_mismatch', '', '', '', task_id}
    end

    if expected_scheduler_epoch ~= '' then
        if (not reserved_epoch) or reserved_epoch ~= expected_scheduler_epoch then
            return {'reservation_epoch_mismatch', '', reserved_epoch or '', '', task_id}
        end
    end
end

-- Commit
-- Atomically increment claim_epoch.  HINCRBY initializes to 0 if missing,
-- then adds 1, so the first-ever claim produces claim_epoch = 1.
local new_claim_epoch = redis.call('HINCRBY', task_meta_key, 'claim_epoch', 1)

redis.call('SET', task_state_key, 'running', 'EX', state_ttl)
redis.call('HSET', task_meta_key,
    'worker_instance_id',   worker_instance_id,
    'scheduler_epoch',      reserved_epoch or '',
    'claimed_at_ms',        claimed_at_ms,
    'last_heartbeat_at_ms', claimed_at_ms
)
redis.call('EXPIRE', task_meta_key, meta_ttl)

-- Move from scheduled → running zset.
redis.call('ZREM', task_scheduled_zset, task_id)
redis.call('ZADD', task_running_zset, heartbeat_score, task_id)

-- Consume the reservation.
if not legacy_direct_claim then
    redis.call('DEL', reservation_key)
end

return {
    'task_claimed',
    tostring(new_claim_epoch),
    reserved_epoch or '',
    worker_instance_id,
    task_id,
}

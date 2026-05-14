-- hfa-core/src/hfa/lua/task_heartbeat.lua
-- IRONCLAD Sprint 2 patch — Atomic fenced heartbeat.
--
-- Replaces the Python read-compare-write heartbeat path with a single atomic
-- Lua CAS.  Prevents stale heartbeats from refreshing liveness after the task
-- has been requeued and re-claimed by a different worker.
--
-- Fence checks (in order):
--   1. state == "running"
--   2. stored worker_instance_id matches expected
--   3. stored claim_epoch matches expected (if expected != "")
--
-- On success: updates last_heartbeat_at_ms + refreshes running zset score.
--
-- ─── KEYS ──────────────────────────────────────────────────────────────────
-- 1  task_state_key         hfa:dag:task:<id>:state
-- 2  task_meta_key          hfa:dag:task:<id>:meta
-- 3  task_running_zset      hfa:dag:tenant:<tid>:running
--
-- ─── ARGV ──────────────────────────────────────────────────────────────────
-- 1  task_id
-- 2  tenant_id
-- 3  worker_instance_id
-- 4  claim_epoch            "" to skip epoch check
-- 5  now_ms                 new heartbeat timestamp + running zset score
--
-- ─── RETURN ────────────────────────────────────────────────────────────────
-- { status }
--   heartbeat_accepted
--   illegal_transition     state is not "running"
--   owner_mismatch         worker_instance_id does not match
--   claim_epoch_mismatch   claim_epoch does not match

local task_state_key    = KEYS[1]
local task_meta_key     = KEYS[2]
local task_running_zset = KEYS[3]

local task_id              = ARGV[1]
local tenant_id            = ARGV[2]
local worker_instance_id   = ARGV[3]
local expected_claim_epoch = ARGV[4]
local now_ms               = ARGV[5]

-- ── State guard ───────────────────────────────────────────────────────────
local current_state = redis.call('GET', task_state_key)
if current_state ~= 'running' then
    return {'illegal_transition'}
end

-- ── Fence check ───────────────────────────────────────────────────────────
local fence = redis.call('HMGET', task_meta_key, 'worker_instance_id', 'claim_epoch')
local stored_worker      = fence[1] or ''
local stored_claim_epoch = fence[2] or ''

if stored_worker ~= worker_instance_id then
    return {'owner_mismatch'}
end

if expected_claim_epoch ~= '' and stored_claim_epoch ~= expected_claim_epoch then
    return {'claim_epoch_mismatch'}
end

-- ── Update heartbeat ──────────────────────────────────────────────────────
redis.call('HSET', task_meta_key, 'last_heartbeat_at_ms', now_ms)
redis.call('ZADD', task_running_zset, tonumber(now_ms), task_id)

return {'heartbeat_accepted'}

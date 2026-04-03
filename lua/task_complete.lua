-- task_complete.lua
--
-- Atomic task completion + output persistence + owner fence + child unlock
--
-- Return:
--   { committed_flag, status, unlocked_count, already_terminal_flag }
--
-- Status:
--   committed
--   already_terminal
--   illegal_transition
--   missing_task
--   invalid_terminal_state
--   run_terminal_block
--   owner_mismatch
--   child_missing
--
-- KEYS
--   1  task_state_key
--   2  task_meta_key
--   3  run_state_key
--   4  child_set_key
--   5  ready_zset_key
--   6  output_key
--
-- ARGV
--   1  task_id
--   2  run_id
--   3  tenant_id
--   4  terminal_state                  done | failed
--   5  completed_at_ms
--   6  state_ttl_seconds
--   7  meta_ttl_seconds
--   8  ready_zset_member_prefix
--   9  child_meta_prefix
--  10  child_meta_suffix
--  11  child_state_prefix
--  12  child_state_suffix
--  13  child_remaining_prefix
--  14  child_remaining_suffix
--  15  child_ready_key_prefix
--  16  child_ready_key_suffix
--  17  ready_score_ms
--  18  expected_worker_id
--  19  output_json                      "" if no output should be written
--  20  output_ttl_seconds
--  21  require_running_state            "1" | "0"

local task_state_key = KEYS[1]
local task_meta_key = KEYS[2]
local run_state_key = KEYS[3]
local child_set_key = KEYS[4]
local ready_zset_key = KEYS[5]
local output_key = KEYS[6]

local task_id = ARGV[1]
local run_id = ARGV[2]
local tenant_id = ARGV[3]
local terminal_state = ARGV[4]
local completed_at_ms = tonumber(ARGV[5])
local state_ttl = tonumber(ARGV[6])
local meta_ttl = tonumber(ARGV[7])
local ready_member_prefix = ARGV[8]
local child_meta_prefix = ARGV[9]
local child_meta_suffix = ARGV[10]
local child_state_prefix = ARGV[11]
local child_state_suffix = ARGV[12]
local child_remaining_prefix = ARGV[13]
local child_remaining_suffix = ARGV[14]
local child_ready_prefix = ARGV[15]
local child_ready_suffix = ARGV[16]
local ready_score_ms = tonumber(ARGV[17])
local expected_worker_id = ARGV[18]
local output_json = ARGV[19]
local output_ttl = tonumber(ARGV[20])
local require_running_state = ARGV[21] == "1"

local function is_terminal(s)
  return s == 'done'
      or s == 'failed'
      or s == 'skipped'
      or s == 'blocked_by_failure'
      or s == 'dead_lettered'
end

if terminal_state ~= 'done' and terminal_state ~= 'failed' then
  return {0, 'invalid_terminal_state', 0, 0}
end

local current_run_state = redis.call('GET', run_state_key)
if current_run_state
   and current_run_state ~= 'running'
   and current_run_state ~= 'scheduled'
   and current_run_state ~= 'admitted' then
  return {0, 'run_terminal_block', 0, 0}
end

local current_state = redis.call('GET', task_state_key)
if not current_state then
  return {0, 'missing_task', 0, 0}
end

if is_terminal(current_state) then
  return {0, 'already_terminal', 0, 1}
end

if require_running_state then
  if current_state ~= 'running' then
    return {0, 'illegal_transition', 0, 0}
  end
else
  if current_state ~= 'running' and current_state ~= 'scheduled' then
    return {0, 'illegal_transition', 0, 0}
  end
end

-- owner fence
local current_owner = redis.call('HGET', task_meta_key, 'owner_worker_id')
if expected_worker_id ~= '' then
  if (not current_owner) or current_owner ~= expected_worker_id then
    return {0, 'owner_mismatch', 0, 0}
  end
end

-- prevalidate children BEFORE mutating parent state
local children = {}
if terminal_state == 'done' then
  children = redis.call('SMEMBERS', child_set_key)
  for _, child_id in ipairs(children) do
    local cmeta = child_meta_prefix .. child_id .. child_meta_suffix
    if redis.call('EXISTS', cmeta) == 0 then
      return {0, 'child_missing', 0, 0}
    end
  end
end

-- commit parent terminal state
redis.call('SET', task_state_key, terminal_state, 'EX', state_ttl)

redis.call('HSET', task_meta_key,
  'task_id', task_id,
  'run_id', run_id,
  'tenant_id', tenant_id,
  'completed_at_ms', tostring(completed_at_ms),
  'terminal_state', terminal_state,
  'owner_worker_id', expected_worker_id
)
redis.call('EXPIRE', task_meta_key, meta_ttl)

-- persist output only on DONE and only if provided
if terminal_state == 'done' and output_json ~= '' then
  redis.call('SET', output_key, output_json, 'EX', output_ttl)
end

if terminal_state == 'failed' then
  return {1, 'committed', 0, 0}
end

local unlocked = 0

for _, child_id in ipairs(children) do
  local child_state_key = child_state_prefix .. child_id .. child_state_suffix
  local child_remaining_key = child_remaining_prefix .. child_id .. child_remaining_suffix
  local child_ready_key = child_ready_prefix .. child_id .. child_ready_suffix

  local child_state = redis.call('GET', child_state_key)
  if not child_state then
    redis.call('SET', child_state_key, 'pending', 'EX', state_ttl)
    child_state = 'pending'
  end

  if (not is_terminal(child_state)) and child_state ~= 'ready' then
    local remaining = redis.call('DECR', child_remaining_key)
    if tonumber(remaining) <= 0 then
      redis.call('SET', child_remaining_key, 0)
      local already_ready = redis.call('GET', child_ready_key)
      if not already_ready then
        redis.call('SET', child_state_key, 'ready', 'EX', state_ttl)
        redis.call('SET', child_ready_key, '1', 'EX', state_ttl)
        redis.call('ZADD', ready_zset_key, ready_score_ms, ready_member_prefix .. child_id)
        unlocked = unlocked + 1
      end
    end
  end
end

return {1, 'committed', unlocked, 0}
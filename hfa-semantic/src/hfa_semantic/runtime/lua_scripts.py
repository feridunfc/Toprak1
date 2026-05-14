"""
hfa-semantic/src/hfa_semantic/runtime/lua_scripts.py

Production Lua Scripts for Atomic State Management

CRITICAL: All state mutations go through Lua for atomicity.
"""

# Full state merge script (READ + MERGE + WRITE + EVICTION) atomically
# CRITICAL: This performs true merge (count increment) not last-write-wins
LUA_STATE_MERGE = """
local key = KEYS[1]
local zset_key = KEYS[2]
local ttl_sec = tonumber(ARGV[1])
local partition = ARGV[2]
local increment_value = tonumber(ARGV[3])  -- e.g., +1 for counter
local timestamp = tonumber(ARGV[4])

-- Step 1: Read current state
local current_data = redis.call('GET', key)
local current_count = 0

if current_data then
  local status, parsed = pcall(cjson.decode, current_data)
  if status and parsed.count then
    current_count = tonumber(parsed.count)
  end
end

-- Step 2: MERGE: increment count (NOT overwrite)
local new_count = current_count + increment_value

-- Step 3: Atomic write (merged state + eviction tracking + TTL)
local final_obj = cjson.encode({
  count = new_count,
  last_increment_ms = timestamp,
  version = (current_count > 0 and 1 or 0)
})

redis.call('SET', key, final_obj, 'EX', ttl_sec)
redis.call('ZADD', zset_key, timestamp, partition)
redis.call('EXPIRE', zset_key, ttl_sec)

-- Return merged count (for client verification)
return new_count
"""

# Dedup check + mark (SETNX pattern, no race condition)
LUA_DEDUP_TRY_ACCEPT = """
local key = KEYS[1]
local ttl_sec = tonumber(ARGV[1])
local data = ARGV[2]

-- Atomic: SET if not exists + TTL
local result = redis.call('SET', key, data, 'EX', ttl_sec, 'NX')

-- Return: 1 if we set it (new), 0 if already existed
if result then
  return 1
else
  return 0
end
"""

# Watermark progression (monotonic, atomic)
LUA_WATERMARK_OBSERVE = """
local key = KEYS[1]
local ttl_sec = tonumber(ARGV[1])
local event_time_ms = tonumber(ARGV[2])
local allowed_lateness_ms = tonumber(ARGV[3])

-- Read current watermark
local current_data = redis.call('GET', key)
local watermark_ms = 0
local on_time_count = 0
local late_count = 0

if current_data then
  local status, parsed = pcall(cjson.decode, current_data)
  if status then
    watermark_ms = tonumber(parsed.watermark_ms or 0)
    on_time_count = tonumber(parsed.on_time_count or 0)
    late_count = tonumber(parsed.late_count or 0)
  end
end

-- Check lateness
local is_late = 0
if event_time_ms < watermark_ms - allowed_lateness_ms then
  is_late = 1
  late_count = late_count + 1
else
  on_time_count = on_time_count + 1
  -- Update watermark only for on-time events
  if event_time_ms > watermark_ms then
    watermark_ms = event_time_ms
  end
end

-- Persist updated watermark
local updated = cjson.encode({
  watermark_ms = watermark_ms,
  on_time_count = on_time_count,
  late_count = late_count,
  last_update_ms = tonumber(ARGV[4])
})

redis.call('SET', key, updated, 'EX', ttl_sec)

-- Return: is_late (0 or 1)
return is_late
"""

# Bulk eviction (batch delete for performance)
LUA_EVICT_BULK = """
local zset_key = KEYS[1]
local state_prefix = ARGV[1]  -- e.g., "semantic:state:rule1:"
local batch_size = tonumber(ARGV[2])

-- Get oldest partitions
local oldest = redis.call('ZRANGE', zset_key, 0, batch_size - 1)

-- Delete each partition's state key
local count = 0
for _, partition in ipairs(oldest) do
  local state_key = state_prefix .. tostring(partition)
  redis.call('DEL', state_key)
  count = count + 1
end

-- Remove from zset
redis.call('ZREM', zset_key, unpack(oldest))

return count
"""

class LuaScripts:
    """Container for production Lua scripts."""
    
    STATE_MERGE = LUA_STATE_MERGE
    DEDUP_TRY_ACCEPT = LUA_DEDUP_TRY_ACCEPT
    WATERMARK_OBSERVE = LUA_WATERMARK_OBSERVE
    EVICT_BULK = LUA_EVICT_BULK


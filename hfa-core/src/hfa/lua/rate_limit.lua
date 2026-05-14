--[[
hfa-core/src/hfa/lua/rate_limit.lua
IRONCLAD Sprint 15 — Atomic sliding-window tenant rate limiter

Algorithm
---------
Sliding 1-second window using a Redis ZSET.
All three operations (trim → count → insert) run atomically in one Lua
transaction — there is no TOCTOU window between check and consume.

Arguments
---------
KEYS[1]  tenant rate ZSET key         (e.g. "hfa:tenant:acme:rate")
ARGV[1]  max_runs_per_second (int)    limit threshold
ARGV[2]  now (float)                  current unix timestamp (microsecond precision)
ARGV[3]  member (string)              unique entry key (prevents ZADD collision)
ARGV[4]  ttl (int seconds)            EXPIRE window for auto-cleanup

Returns
-------
1   admitted — slot consumed, request may proceed
0   rejected — limit reached, caller should return HTTP 429

Guarantees
----------
* Atomicity: Redis executes Lua scripts as a single transaction.
  No other command can interleave between ZREMRANGEBYSCORE, ZCARD, and ZADD.
* Idempotency: unique member per call prevents double-counting retries
  that arrive within the same microsecond.
* Self-cleaning: EXPIRE ensures the ZSET is removed after ttl seconds
  of inactivity — no unbounded growth.
* Precision: window is exactly 1.0 second (now - 1.0 to now).
  Entries older than window_start are pruned before the count check.

Load via SCRIPT LOAD and call with EVALSHA for production efficiency.
See hfa-core/src/hfa/lua/loader.py.
--]]

local key          = KEYS[1]
local limit        = tonumber(ARGV[1])
local now          = tonumber(ARGV[2])
local member       = ARGV[3]
local ttl          = tonumber(ARGV[4])
local window_start = now - 1.0

-- 1. Trim entries outside the 1-second window
redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- 2. Count remaining entries in window
local current = redis.call('ZCARD', key)

-- 3. Reject if at or above limit
if current >= limit then
    return 0
end

-- 4. Admit: record this request and refresh TTL
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, ttl)
return 1


--[[
Atomic Compare-and-Set state transition.

Return codes:
  1   = success
  0   = CAS miss
 -1   = initial-write blocked
 -2   = illegal transition
 -3   = terminal blocked
 -4   = unknown prior state (fail closed)
--]]

local state_key = KEYS[1]
local run_id    = ARGV[1]
local to_state  = ARGV[2]
local expected  = ARGV[3]
local ttl       = tonumber(ARGV[4]) or 86400
local mode      = ARGV[5] or "cas"

local terminal = {done=true, failed=true, rejected=true, dead_lettered=true}
local valid = {
    admitted     = {queued=true, scheduled=true, rejected=true, failed=true},
    queued       = {scheduled=true, failed=true, rejected=true},
    scheduled    = {running=true, failed=true},
    running      = {done=true, failed=true, dead_lettered=true, rescheduled=true},
    rescheduled  = {admitted=true, failed=true},
}

local current = redis.call('GET', state_key)
if current == false then current = nil end
local prior = current or ""

if mode == "initial" then
    if current ~= nil then
        return {-1, prior}
    end
    redis.call('SET', state_key, to_state, 'EX', ttl)
    return {1, prior}
end

if expected ~= nil and expected ~= "" then
    if current ~= expected then
        return {0, prior}
    end
end

if current ~= nil and terminal[current] then
    return {-3, prior}
end

if current ~= nil then
    local allowed = valid[current]
    if allowed == nil then
        return {-4, prior}
    end
    if not allowed[to_state] then
        return {-2, prior}
    end
end

redis.call('SET', state_key, to_state, 'EX', ttl)
return {1, prior}

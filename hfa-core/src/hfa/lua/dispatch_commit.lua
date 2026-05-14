-- KEYS:
-- 1 = state_key
-- 2 = meta_key
-- 3 = running_zset

-- ARGV:
-- 1  = run_id
-- 2  = tenant_id
-- 3  = agent_type
-- 4  = worker_group
-- 5  = shard
-- 6  = reschedule_count
-- 7  = admitted_at
-- 8  = scheduled_at
-- 9  = state_ttl
-- 10 = meta_ttl

-- 🔥 EXTENDED
-- 11 = priority
-- 12 = payload_json
-- 13 = trace_parent
-- 14 = trace_state
-- 15 = policy
-- 16 = region
-- 17 = control_stream
-- 18 = shard_stream

local state = redis.call("GET", KEYS[1])

if not state then
    return {"state_conflict", "missing"}
end

if state ~= "admitted" and state ~= "queued" then
    if state == "scheduled" or state == "running" then
        return {"already_running", state}
    else
        return {"illegal_transition", state}
    end
end

-- 🔒 CAS state transition
redis.call("SET", KEYS[1], "scheduled", "EX", ARGV[9])

-- 🔥 META WRITE (LOSSLESS)
redis.call("HSET", KEYS[2],
    "run_id", ARGV[1],
    "tenant_id", ARGV[2],
    "agent_type", ARGV[3],
    "worker_group", ARGV[4],
    "shard", ARGV[5],
    "reschedule_count", ARGV[6],
    "admitted_at", ARGV[7],
    "scheduled_at", ARGV[8],
    "queue_state", "scheduled",
    "priority", ARGV[11],
    "payload_json", ARGV[12],
    "trace_parent", ARGV[13],
    "trace_state", ARGV[14],
    "policy", ARGV[15],
    "region", ARGV[16]
)

redis.call("EXPIRE", KEYS[2], ARGV[10])

-- 🔥 RUNNING ZSET
redis.call("ZADD", KEYS[3], ARGV[8], ARGV[1])

-- 🔥 CONTROL STREAM
if ARGV[17] ~= "" then
    redis.call("XADD", ARGV[17], "*",
        "event_type", "RunScheduled",
        "run_id", ARGV[1],
        "tenant_id", ARGV[2],
        "agent_type", ARGV[3],
        "worker_group", ARGV[4],
        "shard", ARGV[5],
        "priority", ARGV[11],
        "trace_parent", ARGV[13],
        "trace_state", ARGV[14]
    )
end

-- 🔥 SHARD STREAM
if ARGV[18] ~= "" then
    redis.call("XADD", ARGV[18], "*",
        "event_type", "RunRequested",
        "run_id", ARGV[1],
        "tenant_id", ARGV[2],
        "agent_type", ARGV[3],
        "worker_group", ARGV[4],
        "shard", ARGV[5],
        "priority", ARGV[11],
        "trace_parent", ARGV[13],
        "trace_state", ARGV[14]
    )
end

return {"committed", ""}
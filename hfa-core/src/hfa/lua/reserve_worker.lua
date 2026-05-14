
-- reserve_worker.lua
-- KEYS: 1=reservation_key
-- ARGV: 1=worker_id 2=task_id 3=scheduler_epoch 4=reserved_at_ms 5=ttl_seconds 6=scheduler_id

local reservation_key = KEYS[1]
local worker_id = ARGV[1]
local task_id = ARGV[2]
local scheduler_epoch = ARGV[3]
local reserved_at_ms = ARGV[4]
local ttl_seconds = tonumber(ARGV[5])
local scheduler_id = ARGV[6]

if redis.call('EXISTS', reservation_key) == 1 then
    return {"reservation_conflict", ""}
end

redis.call('HSET', reservation_key,
    'worker_id', worker_id,
    'task_id', task_id,
    'scheduler_epoch', scheduler_epoch,
    'reserved_at_ms', reserved_at_ms,
    'scheduler_id', scheduler_id
)
redis.call('EXPIRE', reservation_key, ttl_seconds)

return {"reservation_created", scheduler_epoch}

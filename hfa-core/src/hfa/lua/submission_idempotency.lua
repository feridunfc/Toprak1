-- Tenant-scoped single-task submission idempotency gate.
--
-- KEYS[1] idempotency HASH
-- ARGV:
--   1 action: reserve | finalize | release
--   2 tenant_id
--   3 request_fingerprint
--   4 run_id
--   5 task_id
--   6 owner_token
--   7 timestamp_ms
--   8 ttl_seconds
--   9 result_json

local key = KEYS[1]
local action = ARGV[1]
local tenant_id = ARGV[2]
local fingerprint = ARGV[3]
local run_id = ARGV[4]
local task_id = ARGV[5]
local owner_token = ARGV[6]
local timestamp_ms = ARGV[7]
local ttl_seconds = tonumber(ARGV[8])
local result_json = ARGV[9]

if not ttl_seconds or ttl_seconds <= 0 then
  return {"INVALID_EVIDENCE", "", "", ""}
end

local function key_type()
  local result = redis.call("TYPE", key)
  if type(result) == "table" then
    return result["ok"]
  end
  return result
end

local function current()
  return redis.call(
    "HMGET",
    key,
    "schema_version",
    "tenant_id",
    "request_fingerprint",
    "run_id",
    "task_id",
    "state",
    "owner_token",
    "result_json",
    "created_at_ms",
    "updated_at_ms"
  )
end

local function valid_common(values)
  return (
    values[1] == "1"
    and values[2] == tenant_id
    and values[3] ~= false
    and values[3] ~= ""
    and values[4] ~= false
    and values[4] ~= ""
    and values[5] ~= false
    and values[5] ~= ""
    and values[6] ~= false
    and values[6] ~= ""
  )
end

if action == "reserve" then
  local kind = key_type()
  if kind == "none" then
    redis.call(
      "HSET",
      key,
      "schema_version", "1",
      "tenant_id", tenant_id,
      "request_fingerprint", fingerprint,
      "run_id", run_id,
      "task_id", task_id,
      "state", "IN_PROGRESS",
      "owner_token", owner_token,
      "result_json", "",
      "created_at_ms", timestamp_ms,
      "updated_at_ms", timestamp_ms
    )
    redis.call("EXPIRE", key, ttl_seconds)
    return {"RESERVED", run_id, task_id, ""}
  end

  if kind ~= "hash" then
    return {"INVALID_EVIDENCE", "", "", ""}
  end

  local values = current()
  if not valid_common(values) then
    return {"INVALID_EVIDENCE", "", "", ""}
  end

  if values[3] ~= fingerprint then
    return {
      "DIFFERENT_REQUEST",
      values[4],
      values[5],
      ""
    }
  end

  if values[6] == "FINAL" then
    if values[8] == false or values[8] == "" then
      return {"INVALID_EVIDENCE", values[4], values[5], ""}
    end
    return {
      "FINAL_SAME_REQUEST",
      values[4],
      values[5],
      values[8]
    }
  end

  if values[6] == "IN_PROGRESS" then
    local created_at_ms = tonumber(values[9])
    local updated_at_ms = tonumber(values[10])
    local ttl_remaining = redis.call("TTL", key)
    if (
      values[7] == false
      or values[7] == ""
      or not created_at_ms
      or created_at_ms < 0
      or not updated_at_ms
      or updated_at_ms < created_at_ms
      or ttl_remaining <= 0
    ) then
      return {"INVALID_EVIDENCE", values[4], values[5], ""}
    end
    return {
      "IN_PROGRESS_SAME_REQUEST",
      values[4],
      values[5],
      "",
      tostring(created_at_ms),
      tostring(updated_at_ms),
      tostring(ttl_remaining)
    }
  end

  return {"INVALID_EVIDENCE", values[4], values[5], ""}
end

if action == "finalize" then
  if key_type() ~= "hash" then
    return {"INVALID_EVIDENCE", "", "", ""}
  end
  local values = current()
  if not valid_common(values) then
    return {"INVALID_EVIDENCE", "", "", ""}
  end
  if (
    values[3] ~= fingerprint
    or values[4] ~= run_id
    or values[5] ~= task_id
    or values[6] ~= "IN_PROGRESS"
    or values[7] ~= owner_token
    or result_json == ""
  ) then
    return {"INVALID_EVIDENCE", values[4], values[5], ""}
  end
  redis.call(
    "HSET",
    key,
    "state", "FINAL",
    "result_json", result_json,
    "updated_at_ms", timestamp_ms
  )
  redis.call("EXPIRE", key, ttl_seconds)
  return {"FINALIZED", run_id, task_id, result_json}
end

if action == "release" then
  if key_type() ~= "hash" then
    return {"INVALID_EVIDENCE", "", "", ""}
  end
  local values = current()
  if not valid_common(values) then
    return {"INVALID_EVIDENCE", "", "", ""}
  end
  if (
    values[3] ~= fingerprint
    or values[4] ~= run_id
    or values[5] ~= task_id
    or values[6] ~= "IN_PROGRESS"
    or values[7] ~= owner_token
  ) then
    return {"INVALID_EVIDENCE", values[4], values[5], ""}
  end
  redis.call("DEL", key)
  return {"RELEASED", run_id, task_id, ""}
end

return {"INVALID_EVIDENCE", "", "", ""}

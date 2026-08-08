-- Sprint 84.5: atomically capture one immutable RUN terminal aggregate proof.
--
-- This script is evidence-only. It never mutates RUN lifecycle projection state,
-- TASK lifecycle state, canonical authority state, queues, claims, or resources.
--
-- KEYS
--  1 terminal proof HASH
--  2 RUN->TASK set
--  3 legacy RUN state STRING
--  4 RUN metadata HASH
--
-- ARGV
--  1 run_id
--  2 tenant_id
--  3 task_state_prefix
--  4 task_state_suffix
--  5 task_meta_prefix
--  6 task_meta_suffix
--  7 finalized_at_ms
--  8 worker_instance_id
--  9 trigger_task_id
-- 10 trigger_terminal_state
-- 11 canonical_expected_revision
-- 12 canonical_previous_state
--
-- Return
-- [status, proof_sha256, proof_payload_json, task_count, done_count,
--  failed_count, skipped_count, final_state, finalized_at_ms,
--  worker_instance_id, trigger_task_id, trigger_terminal_state,
--  canonical_expected_revision, canonical_previous_state]

local proof_key = KEYS[1]
local run_tasks_key = KEYS[2]
local run_state_key = KEYS[3]
local run_meta_key = KEYS[4]

local run_id = ARGV[1] or ""
local tenant_id = ARGV[2] or ""
local task_state_prefix = ARGV[3] or ""
local task_state_suffix = ARGV[4] or ""
local task_meta_prefix = ARGV[5] or ""
local task_meta_suffix = ARGV[6] or ""
local finalized_at_ms = ARGV[7] or ""
local worker_instance_id = ARGV[8] or ""
local trigger_task_id = ARGV[9] or ""
local trigger_terminal_state = ARGV[10] or ""
local expected_revision = ARGV[11] or ""
local previous_state = ARGV[12] or ""

local function type_of(key)
    return redis.call("TYPE", key)["ok"]
end

local function response(status, proof_sha256, proof_payload_json, task_count,
                        done_count, failed_count, skipped_count, final_state,
                        stored_finalized_at_ms, stored_worker_instance_id,
                        stored_trigger_task_id, stored_trigger_terminal_state,
                        stored_expected_revision, stored_previous_state)
    return {
        status or "",
        proof_sha256 or "",
        proof_payload_json or "",
        task_count or 0,
        done_count or 0,
        failed_count or 0,
        skipped_count or 0,
        final_state or "",
        stored_finalized_at_ms or "",
        stored_worker_instance_id or "",
        stored_trigger_task_id or "",
        stored_trigger_terminal_state or "",
        stored_expected_revision or "",
        stored_previous_state or "",
    }
end

local function conflict(status)
    return response(status, "", "", 0, 0, 0, 0, "", "", "", "", "", "", "")
end

-- Pure Lua/Redis-bit SHA-256. The digest is computed inside the same Redis Lua
-- execution that captures TASK aggregate truth, so mutable TASK reads are never
-- round-tripped to Python for hashing.
local K = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
    0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
    0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
    0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
    0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
}

local function u32(value)
    return value % 4294967296
end

local function sha256(message)
    local bytes = {string.byte(message, 1, #message)}
    local bit_length = #bytes * 8
    bytes[#bytes + 1] = 0x80
    while (#bytes % 64) ~= 56 do
        bytes[#bytes + 1] = 0
    end

    local high = math.floor(bit_length / 4294967296)
    local low = bit_length % 4294967296
    local length_words = {high, low}
    for _, word in ipairs(length_words) do
        bytes[#bytes + 1] = math.floor(word / 16777216) % 256
        bytes[#bytes + 1] = math.floor(word / 65536) % 256
        bytes[#bytes + 1] = math.floor(word / 256) % 256
        bytes[#bytes + 1] = word % 256
    end

    local H = {
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    }

    for chunk = 1, #bytes, 64 do
        local w = {}
        for i = 0, 15 do
            local j = chunk + (i * 4)
            w[i] = u32(
                bytes[j] * 16777216 +
                bytes[j + 1] * 65536 +
                bytes[j + 2] * 256 +
                bytes[j + 3]
            )
        end
        for i = 16, 63 do
            local x = w[i - 15]
            local y = w[i - 2]
            local s0 = bit.bxor(bit.ror(x, 7), bit.ror(x, 18), bit.rshift(x, 3))
            local s1 = bit.bxor(bit.ror(y, 17), bit.ror(y, 19), bit.rshift(y, 10))
            w[i] = u32(w[i - 16] + s0 + w[i - 7] + s1)
        end

        local a, b, c, d = H[1], H[2], H[3], H[4]
        local e, f, g, h = H[5], H[6], H[7], H[8]
        for i = 0, 63 do
            local S1 = bit.bxor(bit.ror(e, 6), bit.ror(e, 11), bit.ror(e, 25))
            local ch = bit.bxor(bit.band(e, f), bit.band(bit.bnot(e), g))
            local temp1 = u32(h + S1 + ch + K[i + 1] + w[i])
            local S0 = bit.bxor(bit.ror(a, 2), bit.ror(a, 13), bit.ror(a, 22))
            local maj = bit.bxor(bit.band(a, b), bit.band(a, c), bit.band(b, c))
            local temp2 = u32(S0 + maj)
            h = g
            g = f
            f = e
            e = u32(d + temp1)
            d = c
            c = b
            b = a
            a = u32(temp1 + temp2)
        end

        H[1] = u32(H[1] + a)
        H[2] = u32(H[2] + b)
        H[3] = u32(H[3] + c)
        H[4] = u32(H[4] + d)
        H[5] = u32(H[5] + e)
        H[6] = u32(H[6] + f)
        H[7] = u32(H[7] + g)
        H[8] = u32(H[8] + h)
    end

    local parts = {}
    for i = 1, 8 do
        parts[i] = string.format("%08x", H[i])
    end
    return table.concat(parts)
end

local function exact_nonnegative_integer(raw)
    if raw == "" or string.match(raw, "^%d+$") == nil then
        return nil
    end
    local value = tonumber(raw)
    if not value or value < 0 or value > 9007199254740991 or value ~= math.floor(value) then
        return nil
    end
    return value
end

if run_id == "" or tenant_id == "" then
    return conflict("terminal_proof_identity_invalid")
end
if task_state_prefix == "" or task_meta_prefix == "" then
    return conflict("terminal_proof_key_contract_invalid")
end
if not exact_nonnegative_integer(finalized_at_ms) then
    return conflict("terminal_proof_timestamp_invalid")
end
local revision_number = exact_nonnegative_integer(expected_revision)
if not revision_number or revision_number < 1 then
    return conflict("terminal_proof_revision_invalid")
end
if previous_state ~= "pending" and previous_state ~= "running" then
    return conflict("terminal_proof_previous_state_invalid")
end

local proof_type = type_of(proof_key)
if proof_type ~= "none" and proof_type ~= "hash" then
    return conflict("terminal_proof_receipt_wrong_type")
end
local run_tasks_type = type_of(run_tasks_key)
if run_tasks_type ~= "set" then
    return conflict("terminal_proof_run_tasks_missing_or_wrong_type")
end
if redis.call("SCARD", run_tasks_key) == 0 then
    return conflict("terminal_proof_run_tasks_empty")
end
local run_state_type = type_of(run_state_key)
if run_state_type ~= "string" then
    return conflict("terminal_proof_run_state_missing_or_wrong_type")
end
local run_meta_type = type_of(run_meta_key)
if run_meta_type ~= "none" and run_meta_type ~= "hash" then
    return conflict("terminal_proof_run_meta_wrong_type")
end

local observed_run_state = redis.call("GET", run_state_key) or ""
local legacy_terminal = {
    done = true,
    failed = true,
    rejected = true,
    dead_lettered = true,
}
if legacy_terminal[observed_run_state] then
    return conflict("legacy_terminal_footprint_without_canonical_authority")
end

if run_meta_type == "hash" then
    local meta_run_id = redis.call("HGET", run_meta_key, "run_id") or ""
    local meta_tenant_id = redis.call("HGET", run_meta_key, "tenant_id") or ""
    if meta_run_id ~= "" and meta_run_id ~= run_id then
        return conflict("terminal_proof_run_meta_identity_mismatch")
    end
    if meta_tenant_id ~= "" and meta_tenant_id ~= tenant_id then
        return conflict("terminal_proof_run_meta_tenant_mismatch")
    end
end

local tasks = redis.call("SMEMBERS", run_tasks_key)
table.sort(tasks)
local evidence = {}
local done_count = 0
local failed_count = 0
local skipped_count = 0
local nonterminal_count = 0

local failure_terminal = {
    failed = true,
    blocked_by_failure = true,
    dead_lettered = true,
    rejected = true,
    cancelled = true,
}
local nonterminal = {
    pending = true,
    ready = true,
    scheduled = true,
    running = true,
    admitted = true,
    queued = true,
    rescheduled = true,
}

for _, task_id in ipairs(tasks) do
    local task_state_key = task_state_prefix .. task_id .. task_state_suffix
    local task_meta_key = task_meta_prefix .. task_id .. task_meta_suffix
    if type_of(task_state_key) ~= "string" then
        return conflict("terminal_proof_task_state_missing_or_wrong_type")
    end
    if type_of(task_meta_key) ~= "hash" then
        return conflict("terminal_proof_task_meta_missing_or_wrong_type")
    end
    local task_meta_task_id = redis.call("HGET", task_meta_key, "task_id") or ""
    local task_meta_run_id = redis.call("HGET", task_meta_key, "run_id") or ""
    local task_meta_tenant_id = redis.call("HGET", task_meta_key, "tenant_id") or ""
    if task_meta_task_id ~= task_id or task_meta_run_id ~= run_id then
        return conflict("terminal_proof_task_identity_mismatch")
    end
    if task_meta_tenant_id ~= "" and task_meta_tenant_id ~= tenant_id then
        return conflict("terminal_proof_task_tenant_mismatch")
    end

    local task_state = redis.call("GET", task_state_key) or ""
    if task_state == "done" then
        done_count = done_count + 1
    elseif task_state == "skipped" then
        skipped_count = skipped_count + 1
    elseif failure_terminal[task_state] then
        failed_count = failed_count + 1
    elseif nonterminal[task_state] then
        nonterminal_count = nonterminal_count + 1
    else
        return conflict("terminal_proof_task_state_unknown")
    end
    evidence[#evidence + 1] = {task_id = task_id, state = task_state}
end

local task_count = #tasks
if nonterminal_count > 0 then
    if proof_type == "hash" then
        return conflict("terminal_proof_changed_after_capture")
    end
    return response(
        "not_ready", "", "", task_count, done_count, failed_count,
        skipped_count, "", "", "", "", "", "", ""
    )
end

local final_state = "done"
if failed_count > 0 then
    final_state = "failed"
end

local task_json = {}
for index, row in ipairs(evidence) do
    task_json[index] = "{\"state\":" .. cjson.encode(row.state) ..
        ",\"task_id\":" .. cjson.encode(row.task_id) .. "}"
end
local proof_payload_json =
    "{\"done_count\":" .. tostring(done_count) ..
    ",\"failed_count\":" .. tostring(failed_count) ..
    ",\"final_state\":" .. cjson.encode(final_state) ..
    ",\"run_id\":" .. cjson.encode(run_id) ..
    ",\"schema_version\":1" ..
    ",\"skipped_count\":" .. tostring(skipped_count) ..
    ",\"task_count\":" .. tostring(task_count) ..
    ",\"tasks\":[" .. table.concat(task_json, ",") .. "]" ..
    ",\"tenant_id\":" .. cjson.encode(tenant_id) .. "}"
local proof_sha256 = sha256(proof_payload_json)

if proof_type == "hash" then
    local expected_fields = {
        "schema_version", "1",
        "run_id", run_id,
        "tenant_id", tenant_id,
        "proof_sha256", proof_sha256,
        "proof_payload_json", proof_payload_json,
        "task_count", tostring(task_count),
        "done_count", tostring(done_count),
        "failed_count", tostring(failed_count),
        "skipped_count", tostring(skipped_count),
        "final_state", final_state,
    }
    for index = 1, #expected_fields, 2 do
        local stored = redis.call("HGET", proof_key, expected_fields[index])
        if not stored or stored ~= expected_fields[index + 1] then
            return conflict("terminal_proof_changed_after_capture")
        end
    end
    local stored_finalized_at_ms = redis.call("HGET", proof_key, "finalized_at_ms") or ""
    local stored_worker_instance_id = redis.call("HGET", proof_key, "worker_instance_id") or ""
    local stored_trigger_task_id = redis.call("HGET", proof_key, "trigger_task_id") or ""
    local stored_trigger_terminal_state = redis.call("HGET", proof_key, "trigger_terminal_state") or ""
    local stored_expected_revision = redis.call("HGET", proof_key, "canonical_expected_revision") or ""
    local stored_previous_state = redis.call("HGET", proof_key, "canonical_previous_state") or ""
    if stored_finalized_at_ms == "" or stored_expected_revision == "" or stored_previous_state == "" then
        return conflict("terminal_proof_receipt_corrupt")
    end
    return response(
        "already_captured", proof_sha256, proof_payload_json, task_count,
        done_count, failed_count, skipped_count, final_state,
        stored_finalized_at_ms, stored_worker_instance_id,
        stored_trigger_task_id, stored_trigger_terminal_state,
        stored_expected_revision, stored_previous_state
    )
end

redis.call(
    "HSET", proof_key,
    "schema_version", "1",
    "run_id", run_id,
    "tenant_id", tenant_id,
    "proof_sha256", proof_sha256,
    "proof_payload_json", proof_payload_json,
    "task_count", tostring(task_count),
    "done_count", tostring(done_count),
    "failed_count", tostring(failed_count),
    "skipped_count", tostring(skipped_count),
    "final_state", final_state,
    "finalized_at_ms", finalized_at_ms,
    "worker_instance_id", worker_instance_id,
    "trigger_task_id", trigger_task_id,
    "trigger_terminal_state", trigger_terminal_state,
    "canonical_expected_revision", expected_revision,
    "canonical_previous_state", previous_state
)
redis.call("PERSIST", proof_key)

return response(
    "captured", proof_sha256, proof_payload_json, task_count, done_count,
    failed_count, skipped_count, final_state, finalized_at_ms,
    worker_instance_id, trigger_task_id, trigger_terminal_state,
    expected_revision, previous_state
)

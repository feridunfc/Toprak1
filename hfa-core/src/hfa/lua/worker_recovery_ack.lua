-- F04.4H: own RUN proof and recovery XACK share this atomic boundary.
-- KEYS: delivery stream, RUN head/index/receipt/record/log/outbox.
-- No TASK authority, lifecycle decision, settlement or projection writes here.
local function reject() return {'recovery_ack_proof_conflict', '0'} end
local function decode(raw)
    local ok, value = pcall(cjson.decode, raw or '')
    if ok and type(value) == 'table' then return value end
end
local own = decode(ARGV[1])
if #KEYS ~= 7 or #ARGV ~= 12 or not own or #own ~= 14 then return reject() end
for i=1,14 do if type(own[i]) ~= 'string' then return reject() end end
local run_id, tenant_id, task_id = ARGV[2], ARGV[3], ARGV[4]
local group, message_id, final_state = ARGV[5], ARGV[6], ARGV[7]
if run_id == '' or tenant_id == '' or task_id == '' or group == ''
    or not string.match(message_id, '^%d+%-%d+$')
    or (final_state ~= 'done' and final_state ~= 'failed') then return reject() end
local record, receipt = decode(own[8]), decode(own[9])
if not record or not receipt then return reject() end
local identity, meta = record.canonical_aggregate_identity, record.authoritative_metadata_changes
if type(identity) ~= 'table' or identity.aggregate_type ~= 'run'
    or identity.run_id ~= run_id or identity.task_id ~= cjson.null
    or record.operation_type ~= 'RUN_TERMINATE' or receipt.operation_type ~= 'RUN_TERMINATE'
    or type(meta) ~= 'table' or meta.run_id ~= run_id or meta.tenant_id ~= tenant_id
    or type(meta.terminal_evidence) ~= 'table' then return reject() end
local evidence = meta.terminal_evidence
if evidence.terminal_proof_sha256 ~= ARGV[12] or evidence.final_state ~= final_state
    or type(evidence.tasks) ~= 'table' or #evidence.tasks ~= 1
    or type(evidence.tasks[1]) ~= 'table' or evidence.tasks[1].task_id ~= task_id
    or record.operation_id ~= own[2] or receipt.operation_id ~= own[2]
    or own[4] ~= ARGV[8] or own[5] ~= ARGV[11]
    or own[6] ~= ARGV[10] or own[7] ~= ARGV[9]
    or own[11] ~= final_state or own[12] ~= '0' then return reject() end
local prefix = 'hfa:authority:v1:{' .. own[1] .. '}:'
local suffixes = {'aggregate', 'transition-indexes', 'receipts', 'operation-records', 'log', 'outbox'}
for i=1,6 do if KEYS[1+i] ~= prefix .. suffixes[i] then return reject() end end
local checked = validate_authority_head({KEYS[2],KEYS[3],KEYS[4],KEYS[5],KEYS[6],KEYS[7]}, own)
if checked[1] ~= 'VALID' then return reject() end

-- Bind the exact transport entry to the accepted RUN decision, not runtime
-- task_state/task_meta/run_tasks. Missing/trimmed/foreign entries stay pending.
if redis.call('TYPE', KEYS[1])['ok'] ~= 'stream' then return reject() end
local rows = redis.call('XRANGE', KEYS[1], message_id, message_id, 'COUNT', 1)
if #rows ~= 1 then return reject() end
local fields = {}
for i=1,#rows[1][2],2 do
    local key = rows[1][2][i]
    if fields[key] ~= nil then return reject() end
    fields[key] = rows[1][2][i+1]
end
if fields.run_id ~= run_id or fields.task_id ~= task_id or fields.tenant_id ~= tenant_id then return reject() end
local pending = redis.pcall('XPENDING', KEYS[1], group, message_id, message_id, 1)
if pending.err then return reject() end
if #pending == 0 then return {'already_acked', '0'} end
-- First and only mutating command. No new receipt, TTL, event or queue effect.
return {'acked', tostring(redis.call('XACK', KEYS[1], group, message_id))}

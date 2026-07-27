from __future__ import annotations

import runpy
from pathlib import Path

root = Path(__file__).resolve().parents[1]
lua_path = root / "hfa-core/src/hfa/lua/canonical_authority_commit.lua"
text = lua_path.read_text(encoding="utf-8")

actual_conflict = '''-- Conflict storage failure is not reported as an evidenced canonical conflict.
-- It is a separate fail-closed operational result because durable evidence cannot
-- be guaranteed while either conflict store has the wrong Redis type.
local conflict_index_type = redis_type(KEYS[7])
local conflict_stream_type = redis_type(KEYS[8])
if conflict_index_type ~= "none" and conflict_index_type ~= "hash" then
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "conflict_index_type_mismatch")
end
if conflict_stream_type ~= "none" and conflict_stream_type ~= "stream" then
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, "conflict_stream_type_mismatch")
end
'''
normalized_conflict = '''-- Conflict storage must itself be healthy before any outcome that requires it.
if not key_type_ok(KEYS[7], "hash") or not key_type_ok(KEYS[8], "stream") then
    local index_kind = redis_type(KEYS[7])
    local detail = "conflict_stream_type_mismatch"
    if index_kind ~= "none" and index_kind ~= "hash" then
        detail = "conflict_index_type_mismatch"
    end
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, detail)
end
'''
if text.count(actual_conflict) != 1:
    raise SystemExit(
        f"conflict preflight normalization expected one match, observed {text.count(actual_conflict)}"
    )
text = text.replace(actual_conflict, normalized_conflict, 1)

actual_duplicate = '''local existing_receipt_raw = redis.call("HGET", KEYS[3], operation_digest)
local existing_record_raw = redis.call("HGET", KEYS[4], operation_digest)
if existing_receipt_raw or existing_record_raw then
    if not existing_receipt_raw or not existing_record_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_receipt_proof_incomplete")
    end
    local pre_record_payload, pre_record = decode_storage_envelope(existing_record_raw)
    if not pre_record_payload or not pre_record or type(pre_record.transition_id) ~= "string" then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_record_envelope_invalid")
    end
    local existing_index_raw = redis.call("HGET", KEYS[2], pre_record.transition_id)
    if not existing_index_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", pre_record.canonical_command_hash, pre_record.transition_id, pre_record.to_revision, "stored_transition_index_missing")
    end
    local proof, proof_error = validate_proof(
        existing_record_raw,
        existing_receipt_raw,
        existing_index_raw,
        identity_sha,
        operation_id
    )
    if not proof then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", pre_record.canonical_command_hash, pre_record.transition_id, pre_record.to_revision, proof_error)
    end
    -- Model B: the first fully validated persisted record wins. The canonical
    -- command hash binds all requested authoritative effects. Writer-generated
    -- commit metadata may differ across concurrent independent evaluations.
    if proof.receipt.canonical_command_hash == canonical_command_hash then
        return result("ALREADY_APPLIED", proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
    end
    return emit_conflict("IDEMPOTENCY_CONFLICT", proof.receipt.canonical_command_hash, proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
end
'''
normalized_duplicate = '''local existing_receipt_raw = redis.call("HGET", KEYS[3], operation_digest)
local existing_record_raw = redis.call("HGET", KEYS[4], operation_digest)
if existing_receipt_raw or existing_record_raw then
    if not existing_receipt_raw or not existing_record_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_receipt_proof_incomplete")
    end
    local pre_record_payload, pre_record = decode_storage_envelope(existing_record_raw)
    if not pre_record_payload or not pre_record or type(pre_record.transition_id) ~= "string" then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", nil, nil, nil, "stored_record_envelope_invalid")
    end
    local existing_index_raw = redis.call("HGET", KEYS[2], pre_record.transition_id)
    if not existing_index_raw then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", pre_record.canonical_command_hash, pre_record.transition_id, pre_record.to_revision, "stored_transition_index_missing")
    end
    local proof, proof_error = validate_proof(
        existing_record_raw,
        existing_receipt_raw,
        existing_index_raw,
        identity_sha,
        operation_id
    )
    if not proof then
        return emit_conflict("CANONICAL_RECORD_CORRUPTION_CONFLICT", pre_record.canonical_command_hash, pre_record.transition_id, pre_record.to_revision, proof_error)
    end
    if proof.receipt.canonical_command_hash == canonical_command_hash then
        return result("ALREADY_APPLIED", proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
    end
    return emit_conflict("IDEMPOTENCY_CONFLICT", proof.receipt.canonical_command_hash, proof.receipt.transition_id, proof.receipt.aggregate_revision, "")
end
'''
if text.count(actual_duplicate) != 1:
    raise SystemExit(
        f"duplicate normalization expected one match, observed {text.count(actual_duplicate)}"
    )
text = text.replace(actual_duplicate, normalized_duplicate, 1)

lua_path.write_text(text, encoding="utf-8")
runpy.run_path(str(Path(__file__).with_name("patch.py")), run_name="__main__")

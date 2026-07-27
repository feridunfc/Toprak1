from __future__ import annotations

import runpy
from pathlib import Path

root = Path(__file__).resolve().parents[1]
lua_path = root / "hfa-core/src/hfa/lua/canonical_authority_commit.lua"
text = lua_path.read_text(encoding="utf-8")
actual = '''-- Conflict storage failure is not reported as an evidenced canonical conflict.
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
normalized = '''-- Conflict storage must itself be healthy before any outcome that requires it.
if not key_type_ok(KEYS[7], "hash") or not key_type_ok(KEYS[8], "stream") then
    local index_kind = redis_type(KEYS[7])
    local detail = "conflict_stream_type_mismatch"
    if index_kind ~= "none" and index_kind ~= "hash" then
        detail = "conflict_index_type_mismatch"
    end
    return result("CONFLICT_EVIDENCE_STORE_UNAVAILABLE", nil, nil, detail)
end
'''
if text.count(actual) != 1:
    raise SystemExit(f"conflict preflight normalization expected one match, observed {text.count(actual)}")
lua_path.write_text(text.replace(actual, normalized, 1), encoding="utf-8")
runpy.run_path(str(Path(__file__).with_name("patch.py")), run_name="__main__")

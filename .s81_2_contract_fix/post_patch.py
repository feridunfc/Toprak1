from __future__ import annotations

from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, observed {count}")
    return text.replace(old, new, 1)


root = Path(__file__).resolve().parents[1]
py_path = root / "hfa-core/src/hfa/authority/redis_persistence.py"
test_path = root / "hfa-core/tests/authority/test_redis_canonical_authority.py"

py = py_path.read_text(encoding="utf-8")
old = '''    async def _prevalidate_head_proof(
        self,
        keyspace: RedisAuthorityKeyspace,
    ) -> _StoredProofPrevalidation:
        raw_snapshot = await self._redis.hgetall(keyspace.aggregate)
        if not raw_snapshot:
            return _StoredProofPrevalidation("ABSENT")
        data = {_as_text(key): _as_text(value) for key, value in raw_snapshot.items()}
        operation_id = data.get("operation_id", "")
        operation_digest = data.get("operation_digest", "")
        if not operation_id or operation_digest != keyspace.operation_field(operation_id):
            return _StoredProofPrevalidation("INVALID")
        raw_receipt = await self._redis.hget(keyspace.receipts, operation_digest)
        raw_record = await self._redis.hget(keyspace.operation_records, operation_digest)
        return await self._prevalidate_raw_proof(
            keyspace,
            operation_id=operation_id,
            raw_record=raw_record,
            raw_receipt=raw_receipt,
        )
'''
new = '''    async def _prevalidate_head_proof(
        self,
        keyspace: RedisAuthorityKeyspace,
    ) -> _StoredProofPrevalidation:
        aggregate_kind = _as_text(await self._redis.type(keyspace.aggregate))
        if aggregate_kind == "none":
            return _StoredProofPrevalidation("ABSENT")
        if aggregate_kind != "hash":
            return _StoredProofPrevalidation("INVALID")
        raw_snapshot = await self._redis.hgetall(keyspace.aggregate)
        if not raw_snapshot:
            return _StoredProofPrevalidation("INVALID")
        data = {_as_text(key): _as_text(value) for key, value in raw_snapshot.items()}
        operation_id = data.get("operation_id", "")
        operation_digest = data.get("operation_digest", "")
        transition_id = data.get("transition_id", "")
        if (
            not operation_id
            or operation_digest != keyspace.operation_field(operation_id)
            or not transition_id
        ):
            return _StoredProofPrevalidation("INVALID")
        raw_receipt = await self._redis.hget(keyspace.receipts, operation_digest)
        raw_record = await self._redis.hget(keyspace.operation_records, operation_digest)
        raw_index = await self._redis.hget(keyspace.transition_indexes, transition_id)
        if raw_record is None or raw_receipt is None:
            return _StoredProofPrevalidation(
                "INVALID",
                _raw_sha1(raw_record),
                _raw_sha1(raw_receipt),
                _raw_sha1(raw_index),
            )
        return await self._prevalidate_raw_proof(
            keyspace,
            operation_id=operation_id,
            raw_record=raw_record,
            raw_receipt=raw_receipt,
        )
'''
py = replace_once(py, old, new, "head prevalidation edge cases")
py_path.write_text(py, encoding="utf-8")

tests = test_path.read_text(encoding="utf-8")
tests = replace_once(
    tests,
    '    assert result.detail == "stored_transition_index_missing"\n',
    '    assert result.detail == "stored_proof_canonical_validation_failed"\n',
    "missing index canonical detail",
)
test_path.write_text(tests, encoding="utf-8")

print("SPRINT81_2_POST_PATCH_APPLIED")

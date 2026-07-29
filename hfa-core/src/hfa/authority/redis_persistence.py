"""Redis/Lua persistence adapter for canonical authority commit plans.

Sprint 81.2 adds a persistence boundary only. It does not wire existing
scheduler/worker writers to this adapter, migrate historical keys, or authorize
runtime cutover.

All keys participating in one authority decision share the same Redis Cluster
hash tag. The Lua script performs receipt-first proof validation, strict
revision/state CAS, immutable record/receipt insertion, aggregate-head
continuity checks, durable conflict recording, and append-only log/outbox writes
in one Redis script execution.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from hfa.lua.loader import LuaScriptLoader

from . import canonical_transition as _core

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_SAFE_INTEGER = 2**53 - 1
_AGGREGATE_SNAPSHOT_FIELDS = frozenset({
    "canonical_aggregate_identity_sha256",
    "revision",
    "state",
    "state_is_null",
    "transition_id",
    "canonical_record_hash",
    "canonical_command_hash",
    "operation_id",
    "operation_digest",
    "projection_intents_json",
    "updated_at_ms",
})


class RedisAuthorityPersistenceError(RuntimeError):
    """Raised when Redis/Lua authority persistence is unavailable or unsafe."""


class RedisAuthorityCorruptionError(RedisAuthorityPersistenceError):
    """Raised when persisted canonical evidence is present but invalid."""


class RedisAuthorityCommitStatus(str, Enum):
    COMMITTED = "COMMITTED"
    ALREADY_APPLIED = "ALREADY_APPLIED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    AGGREGATE_ALREADY_EXISTS_CONFLICT = "AGGREGATE_ALREADY_EXISTS_CONFLICT"
    STALE_REVISION_CONFLICT = "STALE_REVISION_CONFLICT"
    FUTURE_REVISION_CONFLICT = "FUTURE_REVISION_CONFLICT"
    ILLEGAL_STATE_TRANSITION = "ILLEGAL_STATE_TRANSITION"
    CANONICAL_RECORD_CORRUPTION_CONFLICT = "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    CONFLICT_EVIDENCE_STORE_UNAVAILABLE = "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"
    PREVALIDATION_RETRY_REQUIRED = "PREVALIDATION_RETRY_REQUIRED"
    INVALID_COMMIT_PLAN = "INVALID_COMMIT_PLAN"


@dataclass(frozen=True)
class RedisAuthorityCommitResult:
    status: RedisAuthorityCommitStatus
    transition_id: str | None
    aggregate_revision: int | None
    detail: str = ""

    @property
    def committed(self) -> bool:
        return self.status is RedisAuthorityCommitStatus.COMMITTED


@dataclass(frozen=True)
class PersistedAggregateSnapshot:
    canonical_aggregate_identity_sha256: str
    revision: int
    state: str | None
    transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    operation_id: str
    operation_digest: str
    projection_intents_json: str
    updated_at_ms: int


@dataclass(frozen=True)
class RedisAuthorityKeyspace:
    """Cluster-slot-safe fixed Redis keys for one aggregate authority."""

    canonical_aggregate_identity_sha256: str
    namespace: str = "hfa:authority:v1"

    def __post_init__(self) -> None:
        _sha256(self.canonical_aggregate_identity_sha256, "canonical_aggregate_identity_sha256")
        if type(self.namespace) is not str or not self.namespace or "{" in self.namespace or "}" in self.namespace:
            raise ValueError("namespace must be a non-empty Redis key prefix without hash-tag braces")

    @property
    def hash_tag(self) -> str:
        return "{" + self.canonical_aggregate_identity_sha256 + "}"

    @property
    def aggregate(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:aggregate"

    @property
    def transition_indexes(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:transition-indexes"

    @property
    def receipts(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:receipts"

    @property
    def operation_records(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:operation-records"

    @property
    def transition_log(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:log"

    @property
    def outbox(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:outbox"

    @property
    def conflict_index(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:conflict-index"

    @property
    def conflicts(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:conflicts"

    @property
    def operator_audits(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:operator-audits"

    def operation_field(self, operation_id: str) -> str:
        return _operation_digest(operation_id)

    def transition_field(self, transition_id: str) -> str:
        return _nonempty(transition_id, "transition_id")

    def commit_keys(self) -> list[str]:
        return [
            self.aggregate,
            self.transition_indexes,
            self.receipts,
            self.operation_records,
            self.transition_log,
            self.outbox,
            self.conflict_index,
            self.conflicts,
        ]


def _nonempty(value: Any, field_name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _sha256(value: Any, field_name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be lowercase SHA-256 hex")
    return value


def _safe_int(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum or value > _MAX_SAFE_INTEGER:
        raise ValueError(f"{field_name} must be an exact safe integer >= {minimum}")
    return value


def _operation_digest(operation_id: str) -> str:
    value = _nonempty(operation_id, "operation_id")
    encoded = value.encode("utf-8")
    return hashlib.sha256(len(encoded).to_bytes(8, "big") + encoded).hexdigest()


@dataclass(frozen=True)
class _StoredProofPrevalidation:
    status: str
    record_sha1: str = ""
    receipt_sha1: str = ""
    index_sha1: str = ""


def _raw_sha1(value: Any) -> str:
    if value is None:
        return ""
    return hashlib.sha1(_as_text(value).encode("utf-8")).hexdigest()


def _as_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _canonical_text(value: Any) -> str:
    return _core.canonical_json_bytes(value).decode("utf-8")


def _record_payload(record: _core.CanonicalTransitionRecord) -> dict[str, Any]:
    _core.validate_canonical_transition_record(record)
    payload = record.immutable_payload()
    payload["canonical_record_hash"] = record.canonical_record_hash
    return payload


def _receipt_payload(receipt: _core.OperationReceipt) -> dict[str, Any]:
    return {
        "operation_id": receipt.operation_id,
        "canonical_command_hash": receipt.canonical_command_hash,
        "canonical_record_hash": receipt.canonical_record_hash,
        "transition_id": receipt.transition_id,
        "aggregate_revision": receipt.aggregate_revision,
        "operation_type": receipt.operation_type,
        "committed_at_ms": receipt.committed_at_ms,
    }


def _transition_index_payload(record: _core.CanonicalTransitionRecord) -> dict[str, Any]:
    return {
        "transition_id": record.transition_id,
        "canonical_record_hash": record.canonical_record_hash,
        "operation_id": record.operation_id,
        "aggregate_revision": record.to_revision,
    }


def _record_from_payload(payload: Mapping[str, Any]) -> _core.CanonicalTransitionRecord:
    try:
        structured = payload["canonical_aggregate_identity"]
        if not isinstance(structured, Mapping):
            raise TypeError("canonical_aggregate_identity must be an object")
        identity = _core.CanonicalAggregateIdentity(
            aggregate_type=_core.AggregateType(payload["aggregate_type"]),
            run_id=structured["run_id"],
            task_id=structured.get("task_id"),
        )
        values = {
            "schema_version": payload["schema_version"],
            "transition_id": payload["transition_id"],
            "aggregate_identity": identity,
            "aggregate_identity_sha256": payload["canonical_aggregate_identity_sha256"],
            "from_revision": payload["from_revision"],
            "to_revision": payload["to_revision"],
            "operation_type": payload["operation_type"],
            "operation_id": payload["operation_id"],
            "canonical_command_hash": payload["canonical_command_hash"],
            "canonical_record_hash": payload["canonical_record_hash"],
            "previous_state": payload.get("previous_state"),
            "next_state": payload.get("next_state"),
            "authoritative_metadata_changes": _core._freeze(payload.get("authoritative_metadata_changes")),
            "child_effects": _core._freeze(payload.get("child_effects")),
            "causation_id": payload.get("causation_id"),
            "correlation_id": payload.get("correlation_id"),
            "writer_id": payload["writer_id"],
            "committed_at_ms": payload["committed_at_ms"],
            "durable_projection_intents": tuple(
                _core._freeze(item) for item in payload.get("durable_projection_intents", [])
            ),
        }
        record = _core.CanonicalTransitionRecord(_token=_core._INTERNAL_TOKEN, **values)
        _core.validate_canonical_transition_record(record)
        return record
    except (KeyError, TypeError, ValueError, _core.AuthorityContractError) as exc:
        raise RedisAuthorityPersistenceError(f"invalid persisted canonical record: {exc}") from exc


def _receipt_from_payload(payload: Mapping[str, Any]) -> _core.OperationReceipt:
    try:
        receipt = _core.OperationReceipt(
            _token=_core._INTERNAL_TOKEN,
            operation_id=payload["operation_id"],
            canonical_command_hash=payload["canonical_command_hash"],
            canonical_record_hash=payload["canonical_record_hash"],
            transition_id=payload["transition_id"],
            aggregate_revision=payload["aggregate_revision"],
            operation_type=payload["operation_type"],
            committed_at_ms=payload["committed_at_ms"],
        )
        _core._validate_receipt_internal(receipt)
        return receipt
    except (KeyError, TypeError, ValueError, _core.AuthorityContractError) as exc:
        raise RedisAuthorityPersistenceError(f"invalid persisted operation receipt: {exc}") from exc


def _decode_object(raw: str | bytes, *, field_name: str) -> Mapping[str, Any]:
    try:
        value = json.loads(_as_text(raw))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RedisAuthorityPersistenceError(f"{field_name} is not valid UTF-8 JSON") from exc
    if not isinstance(value, Mapping):
        raise RedisAuthorityPersistenceError(f"{field_name} must decode to an object")
    return value


def _decode_storage_envelope(raw: str | bytes, *, field_name: str) -> tuple[str, Mapping[str, Any]]:
    envelope = _decode_object(raw, field_name=f"{field_name} envelope")
    if set(envelope) != {"payload", "storage_sha1"}:
        raise RedisAuthorityPersistenceError(f"{field_name} envelope fields are invalid")
    payload = envelope.get("payload")
    digest = envelope.get("storage_sha1")
    if type(payload) is not str or type(digest) is not str or _SHA1_RE.fullmatch(digest) is None:
        raise RedisAuthorityPersistenceError(f"{field_name} envelope values are invalid")
    actual = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    if actual != digest:
        raise RedisAuthorityPersistenceError(f"{field_name} storage integrity digest mismatch")
    return payload, _decode_object(payload, field_name=field_name)


def _validate_transition_index(payload: Mapping[str, Any], record: _core.CanonicalTransitionRecord) -> None:
    if dict(payload) != _transition_index_payload(record):
        raise RedisAuthorityPersistenceError("transition index does not match canonical record")


def _validate_plan(plan: _core.AuthorityCommitPlan) -> None:
    if not isinstance(plan, _core.AuthorityCommitPlan):
        raise TypeError("plan must be AuthorityCommitPlan")
    record = plan.record
    receipt = plan.receipt
    _core.validate_canonical_transition_record(record)
    _core._validate_receipt_internal(receipt)
    if plan.aggregate_identity != record.aggregate_identity:
        raise RedisAuthorityPersistenceError("commit plan aggregate identity mismatch")
    if plan.aggregate_revision != record.to_revision:
        raise RedisAuthorityPersistenceError("commit plan revision mismatch")
    if (
        receipt.operation_id != record.operation_id
        or receipt.canonical_command_hash != record.canonical_command_hash
        or receipt.canonical_record_hash != record.canonical_record_hash
        or receipt.transition_id != record.transition_id
        or receipt.aggregate_revision != record.to_revision
        or receipt.operation_type != record.operation_type
        or receipt.committed_at_ms != record.committed_at_ms
    ):
        raise RedisAuthorityPersistenceError("commit plan receipt/record mismatch")


class RedisCanonicalAuthorityStore:
    """Persist accepted authority commit plans using one Redis Lua execution."""

    def __init__(self, redis: Any, *, namespace: str = "hfa:authority:v1") -> None:
        self._redis = redis
        self._namespace = namespace
        script_path = Path(__file__).resolve().parent.parent / "lua" / "canonical_authority_commit.lua"
        self._commit_loader = LuaScriptLoader(redis, script_path)
        conflict_script_path = Path(__file__).resolve().parent.parent / "lua" / "authority_conflict_record.lua"
        self._conflict_loader = LuaScriptLoader(redis, conflict_script_path)
        head_validate_path = Path(__file__).resolve().parent.parent / "lua" / "authority_head_validate.lua"
        self._head_validate_loader = LuaScriptLoader(redis, head_validate_path)

    async def initialise(self) -> None:
        try:
            await self._commit_loader.load()
            await self._conflict_loader.load()
            await self._head_validate_loader.load()
        except Exception as exc:
            raise RedisAuthorityPersistenceError(f"authority Lua initialisation failed: {exc}") from exc

    def keyspace(self, canonical_aggregate_identity_sha256: str) -> RedisAuthorityKeyspace:
        return RedisAuthorityKeyspace(canonical_aggregate_identity_sha256, self._namespace)

    async def _prevalidate_raw_proof(
        self,
        keyspace: RedisAuthorityKeyspace,
        *,
        operation_id: str,
        raw_record: Any,
        raw_receipt: Any,
    ) -> _StoredProofPrevalidation:
        record_sha1 = _raw_sha1(raw_record)
        receipt_sha1 = _raw_sha1(raw_receipt)
        if raw_record is None and raw_receipt is None:
            return _StoredProofPrevalidation("ABSENT")
        if raw_record is None or raw_receipt is None:
            return _StoredProofPrevalidation("INVALID", record_sha1, receipt_sha1)
        raw_index: Any = None
        try:
            _, record_payload = _decode_storage_envelope(
                raw_record,
                field_name="canonical operation record",
            )
            _, receipt_payload = _decode_storage_envelope(
                raw_receipt,
                field_name="operation receipt",
            )
            record = _record_from_payload(record_payload)
            receipt = _receipt_from_payload(receipt_payload)
            index_kind = _as_text(await self._redis.type(keyspace.transition_indexes))
            if index_kind not in {"none", "hash"}:
                return _StoredProofPrevalidation(
                    "INVALID",
                    record_sha1,
                    receipt_sha1,
                )
            raw_index = await self._redis.hget(
                keyspace.transition_indexes,
                keyspace.transition_field(record.transition_id),
            )
            if raw_index is None:
                return _StoredProofPrevalidation(
                    "INVALID",
                    record_sha1,
                    receipt_sha1,
                )
            _, index_payload = _decode_storage_envelope(
                raw_index,
                field_name="canonical transition index",
            )
            _validate_transition_index(index_payload, record)
            _core._validate_stored_duplicate_proof(
                _core.ReceiptProbe(
                    receipt=receipt,
                    canonical_store_record=record,
                ),
                lookup_aggregate_identity_sha256=keyspace.canonical_aggregate_identity_sha256,
                lookup_operation_id=operation_id,
            )
        except (
            RedisAuthorityPersistenceError,
            _core.AuthorityContractError,
            TypeError,
            ValueError,
        ):
            return _StoredProofPrevalidation(
                "INVALID",
                record_sha1,
                receipt_sha1,
                _raw_sha1(raw_index),
            )
        return _StoredProofPrevalidation(
            "VALID",
            record_sha1,
            receipt_sha1,
            _raw_sha1(raw_index),
        )

    async def _prevalidate_operation_proof(
        self,
        keyspace: RedisAuthorityKeyspace,
        operation_id: str,
    ) -> _StoredProofPrevalidation:
        field = keyspace.operation_field(operation_id)
        receipt_kind = _as_text(await self._redis.type(keyspace.receipts))
        record_kind = _as_text(await self._redis.type(keyspace.operation_records))
        if receipt_kind not in {"none", "hash"} or record_kind not in {"none", "hash"}:
            # Do not issue HGET against a wrong-type proof key. Lua owns the
            # atomic key-type decision and durable corruption evidence.
            return _StoredProofPrevalidation("ABSENT")
        try:
            receipt_type = _as_text(await self._redis.type(keyspace.receipts))
            record_type = _as_text(await self._redis.type(keyspace.operation_records))
            if receipt_type not in {"none", "hash"} or record_type not in {"none", "hash"}:
                raise RedisAuthorityCorruptionError("stored receipt proof key type mismatch")
            raw_receipt = await self._redis.hget(keyspace.receipts, field)
            raw_record = await self._redis.hget(keyspace.operation_records, field)
        except RedisAuthorityCorruptionError:
            raise
        except Exception as exc:
            raise RedisAuthorityPersistenceError(f"receipt proof Redis read failed: {exc}") from exc
        return await self._prevalidate_raw_proof(
            keyspace,
            operation_id=operation_id,
            raw_record=raw_record,
            raw_receipt=raw_receipt,
        )

    async def _prevalidate_head_proof(
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
        if set(data) != _AGGREGATE_SNAPSHOT_FIELDS:
            return _StoredProofPrevalidation("INVALID")
        operation_id = data.get("operation_id", "")
        operation_digest = data.get("operation_digest", "")
        transition_id = data.get("transition_id", "")
        if (
            not operation_id
            or operation_digest != keyspace.operation_field(operation_id)
            or not transition_id
        ):
            return _StoredProofPrevalidation("INVALID")
        proof_kinds = {
            _as_text(await self._redis.type(keyspace.receipts)),
            _as_text(await self._redis.type(keyspace.operation_records)),
            _as_text(await self._redis.type(keyspace.transition_indexes)),
        }
        if not proof_kinds.issubset({"none", "hash"}):
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

    async def commit(self, plan: _core.AuthorityCommitPlan) -> RedisAuthorityCommitResult:
        try:
            return await self._commit_impl(plan)
        except RedisAuthorityPersistenceError:
            raise
        except Exception as exc:
            raise RedisAuthorityPersistenceError(f"canonical authority commit failed: {exc}") from exc

    async def _commit_impl(self, plan: _core.AuthorityCommitPlan) -> RedisAuthorityCommitResult:
        _validate_plan(plan)
        record = plan.record
        receipt = plan.receipt
        keyspace = self.keyspace(record.aggregate_identity_sha256)
        record_payload = _record_payload(record)
        receipt_payload = _receipt_payload(receipt)
        transition_index_payload = _transition_index_payload(record)
        projection_intents = record_payload["durable_projection_intents"]
        operation_digest = keyspace.operation_field(record.operation_id)
        is_create = record.operation_type in {
            _core.OperationType.TASK_ADMIT.value,
            _core.OperationType.RUN_CREATE.value,
        }
        for _attempt in range(3):
            operation_prevalidation = await self._prevalidate_operation_proof(
                keyspace,
                record.operation_id,
            )
            head_prevalidation = await self._prevalidate_head_proof(keyspace)
            raw = await self._commit_loader.run(
                num_keys=8,
                keys=keyspace.commit_keys(),
                args=[
                    record.aggregate_identity_sha256,
                    str(record.from_revision),
                    str(record.to_revision),
                    "1" if record.previous_state is None else "0",
                    record.previous_state or "",
                    "1" if record.next_state is None else "0",
                    record.next_state or "",
                    record.transition_id,
                    record.canonical_record_hash,
                    record.canonical_command_hash,
                    record.operation_id,
                    operation_digest,
                    record.operation_type,
                    str(record.committed_at_ms),
                    _canonical_text(transition_index_payload),
                    _canonical_text(record_payload),
                    _canonical_text(receipt_payload),
                    _canonical_text(projection_intents),
                    "1" if is_create else "0",
                    operation_prevalidation.status,
                    operation_prevalidation.record_sha1,
                    operation_prevalidation.receipt_sha1,
                    operation_prevalidation.index_sha1,
                    head_prevalidation.status,
                    head_prevalidation.record_sha1,
                    head_prevalidation.receipt_sha1,
                    head_prevalidation.index_sha1,
                ],
            )
            result = self._parse_commit_result(raw)
            if result.status is not RedisAuthorityCommitStatus.PREVALIDATION_RETRY_REQUIRED:
                return result
        raise RedisAuthorityPersistenceError(
            "stored authority proof changed repeatedly during canonical commit"
        )

    async def load_receipt_probe(
        self,
        aggregate_identity: _core.CanonicalAggregateIdentity,
        operation_id: str,
    ) -> _core.ReceiptProbe | None:
        if not isinstance(aggregate_identity, _core.CanonicalAggregateIdentity):
            raise TypeError("aggregate_identity must be CanonicalAggregateIdentity")
        operation_id = _nonempty(operation_id, "operation_id")
        keyspace = self.keyspace(aggregate_identity.sha256)
        field = keyspace.operation_field(operation_id)
        try:
            receipt_type = _as_text(await self._redis.type(keyspace.receipts))
            record_type = _as_text(await self._redis.type(keyspace.operation_records))
            index_type = _as_text(await self._redis.type(keyspace.transition_indexes))
            if receipt_type not in {"none", "hash"}:
                raise RedisAuthorityCorruptionError("operation receipt key type mismatch")
            if record_type not in {"none", "hash"}:
                raise RedisAuthorityCorruptionError("canonical operation record key type mismatch")
            if index_type not in {"none", "hash"}:
                raise RedisAuthorityCorruptionError("canonical transition index key type mismatch")
            raw_receipt = await self._redis.hget(keyspace.receipts, field)
            raw_record = await self._redis.hget(keyspace.operation_records, field)
        except RedisAuthorityCorruptionError:
            raise
        except Exception as exc:
            raise RedisAuthorityPersistenceError(f"receipt proof Redis read failed: {exc}") from exc
        if raw_receipt is None and raw_record is None:
            return None
        if raw_receipt is None or raw_record is None:
            raise RedisAuthorityCorruptionError("stored receipt proof is incomplete")
        try:
            _, receipt_payload = _decode_storage_envelope(
                raw_receipt, field_name="operation receipt"
            )
            _, record_payload = _decode_storage_envelope(
                raw_record, field_name="canonical operation record"
            )
            receipt = _receipt_from_payload(receipt_payload)
            record = _record_from_payload(record_payload)
        except (RedisAuthorityPersistenceError, _core.AuthorityContractError, TypeError, ValueError) as exc:
            raise RedisAuthorityCorruptionError(f"stored receipt proof is invalid: {exc}") from exc
        try:
            raw_index = await self._redis.hget(
                keyspace.transition_indexes, keyspace.transition_field(record.transition_id)
            )
        except Exception as exc:
            raise RedisAuthorityPersistenceError(f"transition index Redis read failed: {exc}") from exc
        if raw_index is None:
            raise RedisAuthorityCorruptionError("canonical transition index is missing")
        try:
            _, index_payload = _decode_storage_envelope(
                raw_index, field_name="canonical transition index"
            )
            _validate_transition_index(index_payload, record)
            probe = _core.ReceiptProbe(receipt=receipt, canonical_store_record=record)
        except (RedisAuthorityPersistenceError, _core.AuthorityContractError, TypeError, ValueError) as exc:
            raise RedisAuthorityCorruptionError(f"stored transition proof is invalid: {exc}") from exc
        try:
            _core._validate_stored_duplicate_proof(
                probe,
                lookup_aggregate_identity_sha256=aggregate_identity.sha256,
                lookup_operation_id=operation_id,
            )
        except _core.AuthorityContractError as exc:
            raise RedisAuthorityCorruptionError(f"stored receipt proof mismatch: {exc}") from exc
        return probe

    async def get_aggregate_snapshot(
        self,
        aggregate_identity: _core.CanonicalAggregateIdentity,
    ) -> PersistedAggregateSnapshot | None:
        if not isinstance(aggregate_identity, _core.CanonicalAggregateIdentity):
            raise TypeError("aggregate_identity must be CanonicalAggregateIdentity")
        try:
            aggregate_key = self.keyspace(aggregate_identity.sha256).aggregate
            aggregate_type = _as_text(await self._redis.type(aggregate_key))
            if aggregate_type not in {"none", "hash"}:
                raise RedisAuthorityCorruptionError("aggregate snapshot key type mismatch")
            raw = await self._redis.hgetall(aggregate_key)
        except RedisAuthorityCorruptionError:
            raise
        except Exception as exc:
            raise RedisAuthorityPersistenceError(f"aggregate snapshot Redis read failed: {exc}") from exc
        if not raw:
            return None
        data = {_as_text(key): _as_text(value) for key, value in raw.items()}
        expected_fields = _AGGREGATE_SNAPSHOT_FIELDS
        if set(data) != expected_fields:
            raise RedisAuthorityCorruptionError("aggregate snapshot fields are incomplete or unexpected")
        try:
            identity_sha = _sha256(data["canonical_aggregate_identity_sha256"], "snapshot identity")
            revision = _safe_int(int(data["revision"]), "snapshot revision", minimum=1)
            updated_at_ms = _safe_int(int(data["updated_at_ms"]), "snapshot updated_at_ms")
            if data["state_is_null"] not in {"0", "1"}:
                raise ValueError("state_is_null must be 0 or 1")
            state = None if data["state_is_null"] == "1" else _nonempty(data["state"], "snapshot state")
            if state is None and data["state"] != "":
                raise ValueError("null state must use an empty state value")
            transition_id = _nonempty(data["transition_id"], "snapshot transition_id")
            record_hash = _sha256(data["canonical_record_hash"], "snapshot canonical_record_hash")
            command_hash = _sha256(data["canonical_command_hash"], "snapshot canonical_command_hash")
            operation_id = _nonempty(data["operation_id"], "snapshot operation_id")
            operation_digest = _sha256(data["operation_digest"], "snapshot operation_digest")
            if operation_digest != _operation_digest(operation_id):
                raise ValueError("snapshot operation digest mismatch")
            projection_intents_json = data["projection_intents_json"]
            decoded_intents = json.loads(projection_intents_json)
            if not isinstance(decoded_intents, list):
                raise ValueError("projection_intents_json must decode to a list")
        except (ValueError, json.JSONDecodeError) as exc:
            raise RedisAuthorityCorruptionError(f"invalid aggregate snapshot: {exc}") from exc
        if identity_sha != aggregate_identity.sha256:
            raise RedisAuthorityCorruptionError("aggregate snapshot identity mismatch")
        return PersistedAggregateSnapshot(
            canonical_aggregate_identity_sha256=identity_sha,
            revision=revision,
            state=state,
            transition_id=transition_id,
            canonical_record_hash=record_hash,
            canonical_command_hash=command_hash,
            operation_id=operation_id,
            operation_digest=operation_digest,
            projection_intents_json=projection_intents_json,
            updated_at_ms=updated_at_ms,
        )

    @staticmethod
    def canonical_projection_intents_json(value: Any) -> str:
        return _canonical_text(value)

    async def validate_authority_head(
        self,
        aggregate_identity: _core.CanonicalAggregateIdentity,
        *,
        expected_operation_id: str,
        expected_operation_digest: str,
        expected_transition_id: str,
        expected_revision: int,
        expected_canonical_command_hash: str,
        expected_canonical_record_hash: str,
        expected_record: _core.CanonicalTransitionRecord,
        expected_receipt: _core.OperationReceipt,
        expected_state: str | None,
        expected_projection_intents_json: str,
        expected_updated_at_ms: int,
    ) -> PersistedAggregateSnapshot:
        """Atomically validate the exact evaluated authority head before projection."""
        if not isinstance(aggregate_identity, _core.CanonicalAggregateIdentity):
            raise TypeError("aggregate_identity must be CanonicalAggregateIdentity")
        expected_operation_id = _nonempty(expected_operation_id, "expected_operation_id")
        expected_operation_digest = _sha256(expected_operation_digest, "expected_operation_digest")
        if expected_operation_digest != _operation_digest(expected_operation_id):
            raise ValueError("expected_operation_digest does not bind expected_operation_id")
        expected_transition_id = _nonempty(expected_transition_id, "expected_transition_id")
        expected_revision = _safe_int(expected_revision, "expected_revision", minimum=1)
        expected_canonical_command_hash = _sha256(
            expected_canonical_command_hash, "expected_canonical_command_hash"
        )
        expected_canonical_record_hash = _sha256(
            expected_canonical_record_hash, "expected_canonical_record_hash"
        )
        if not isinstance(expected_record, _core.CanonicalTransitionRecord):
            raise TypeError("expected_record must be CanonicalTransitionRecord")
        if not isinstance(expected_receipt, _core.OperationReceipt):
            raise TypeError("expected_receipt must be OperationReceipt")
        expected_updated_at_ms = _safe_int(expected_updated_at_ms, "expected_updated_at_ms")
        expected_record_json = _canonical_text(_record_payload(expected_record))
        expected_receipt_json = _canonical_text(_receipt_payload(expected_receipt))
        expected_index_json = _canonical_text(_transition_index_payload(expected_record))
        expected_state_is_null = "1" if expected_state is None else "0"
        expected_state_text = "" if expected_state is None else _nonempty(expected_state, "expected_state")
        keyspace = self.keyspace(aggregate_identity.sha256)
        try:
            raw = await self._head_validate_loader.run(
                num_keys=6,
                keys=[
                    keyspace.aggregate, keyspace.transition_indexes, keyspace.receipts,
                    keyspace.operation_records, keyspace.transition_log, keyspace.outbox,
                ],
                args=[
                    aggregate_identity.sha256, expected_operation_id,
                    expected_operation_digest, expected_transition_id,
                    str(expected_revision), expected_canonical_command_hash,
                    expected_canonical_record_hash, expected_record_json,
                    expected_receipt_json, expected_index_json, expected_state_text,
                    expected_state_is_null, expected_projection_intents_json,
                    str(expected_updated_at_ms),
                ],
            )
        except Exception as exc:
            raise RedisAuthorityPersistenceError(
                f"authority head validation execution failed: {exc}"
            ) from exc
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)) or not raw:
            raise RedisAuthorityPersistenceError("authority head validation returned an invalid result")
        status = _as_text(raw[0])
        detail = _as_text(raw[1]) if len(raw) > 1 else ""
        if status != "VALID":
            raise RedisAuthorityCorruptionError(detail or "authority head validation failed")
        snapshot = await self.get_aggregate_snapshot(aggregate_identity)
        if snapshot is None:
            raise RedisAuthorityCorruptionError("authority head disappeared after validation")
        return snapshot

    async def record_authority_conflict(
        self,
        aggregate_identity: _core.CanonicalAggregateIdentity,
        *,
        status: RedisAuthorityCommitStatus,
        operation_id: str,
        incoming_command_hash: str,
        observed_at_ms: int,
        detail_code: str,
        stored_command_hash: str | None = None,
        existing_transition_id: str | None = None,
        aggregate_revision: int | None = None,
        detail: str = "",
    ) -> RedisAuthorityCommitResult:
        """Atomically persist policy-level conflict evidence in the authority pair.

        This is for evaluator decisions that intentionally have no commit plan. It
        writes only the deterministic conflict index/stream pair and never mutates
        aggregate lifecycle state.
        """
        if not isinstance(aggregate_identity, _core.CanonicalAggregateIdentity):
            raise TypeError("aggregate_identity must be CanonicalAggregateIdentity")
        if status not in {
            RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
            RedisAuthorityCommitStatus.AGGREGATE_ALREADY_EXISTS_CONFLICT,
            RedisAuthorityCommitStatus.STALE_REVISION_CONFLICT,
            RedisAuthorityCommitStatus.FUTURE_REVISION_CONFLICT,
            RedisAuthorityCommitStatus.ILLEGAL_STATE_TRANSITION,
            RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT,
        }:
            raise ValueError("status is not a durable authority conflict")
        operation_id = _nonempty(operation_id, "operation_id")
        incoming_command_hash = _sha256(incoming_command_hash, "incoming_command_hash")
        observed_at_ms = _safe_int(observed_at_ms, "observed_at_ms")
        detail_code = _nonempty(detail_code, "detail_code")
        if stored_command_hash is not None:
            stored_command_hash = _sha256(stored_command_hash, "stored_command_hash")
        if existing_transition_id is not None:
            existing_transition_id = _nonempty(existing_transition_id, "existing_transition_id")
        if aggregate_revision is not None:
            aggregate_revision = _safe_int(aggregate_revision, "aggregate_revision", minimum=1)
        keyspace = self.keyspace(aggregate_identity.sha256)
        try:
            raw = await self._conflict_loader.run(
                num_keys=2,
                keys=[keyspace.conflict_index, keyspace.conflicts],
                args=[
                    aggregate_identity.sha256,
                    operation_id,
                    keyspace.operation_field(operation_id),
                    incoming_command_hash,
                    stored_command_hash or "",
                    status.value,
                    detail_code,
                    str(observed_at_ms),
                    existing_transition_id or "",
                    "" if aggregate_revision is None else str(aggregate_revision),
                    detail,
                ],
            )
        except Exception as exc:
            raise RedisAuthorityPersistenceError(f"authority conflict script execution failed: {exc}") from exc
        return self._parse_commit_result(raw)

    async def record_conflict(
        self,
        aggregate_identity: _core.CanonicalAggregateIdentity,
        *,
        conflict_type: str,
        operation_id: str,
        observed_at_ms: int,
        detail: Mapping[str, Any] | None = None,
    ) -> str:
        """Append an operator-originated conflict note outside authority commit decisions.

        Authority commit conflicts are recorded atomically by the Lua script. This
        helper remains for explicit operator/audit notes and is not used to complete
        a commit result after the fact.
        """
        if not isinstance(aggregate_identity, _core.CanonicalAggregateIdentity):
            raise TypeError("aggregate_identity must be CanonicalAggregateIdentity")
        conflict_type = _nonempty(conflict_type, "conflict_type")
        operation_id = _nonempty(operation_id, "operation_id")
        observed_at_ms = _safe_int(observed_at_ms, "observed_at_ms")
        payload = _canonical_text(detail or {})
        return _as_text(
            await self._redis.xadd(
                self.keyspace(aggregate_identity.sha256).operator_audits,
                {
                    "conflict_type": conflict_type,
                    "operation_id": operation_id,
                    "observed_at_ms": str(observed_at_ms),
                    "detail_json": payload,
                    "origin": "OPERATOR_NOTE",
                },
            )
        )

    @staticmethod
    def _parse_commit_result(raw: Any) -> RedisAuthorityCommitResult:
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)) or not raw:
            raise RedisAuthorityPersistenceError("canonical authority Lua result must be a non-empty sequence")
        try:
            status = RedisAuthorityCommitStatus(_as_text(raw[0]))
        except (ValueError, IndexError) as exc:
            raise RedisAuthorityPersistenceError(f"unknown canonical authority Lua result: {raw!r}") from exc
        transition_id = _as_text(raw[1]) if len(raw) > 1 and raw[1] not in (None, "", b"") else None
        try:
            revision = int(_as_text(raw[2])) if len(raw) > 2 and raw[2] not in (None, "", b"") else None
        except ValueError as exc:
            raise RedisAuthorityPersistenceError("canonical authority Lua revision is invalid") from exc
        detail = _as_text(raw[3]) if len(raw) > 3 and raw[3] not in (None, b"") else ""
        return RedisAuthorityCommitResult(status, transition_id, revision, detail)


__all__ = [
    "PersistedAggregateSnapshot",
    "RedisAuthorityCommitResult",
    "RedisAuthorityCommitStatus",
    "RedisAuthorityKeyspace",
    "RedisAuthorityCorruptionError",
    "RedisAuthorityPersistenceError",
    "RedisCanonicalAuthorityStore",
]

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


class RedisAuthorityPersistenceError(RuntimeError):
    """Raised when persisted authority data cannot be safely decoded."""


class RedisAuthorityCommitStatus(str, Enum):
    COMMITTED = "COMMITTED"
    ALREADY_APPLIED = "ALREADY_APPLIED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    AGGREGATE_ALREADY_EXISTS_CONFLICT = "AGGREGATE_ALREADY_EXISTS_CONFLICT"
    STALE_REVISION_CONFLICT = "STALE_REVISION_CONFLICT"
    FUTURE_REVISION_CONFLICT = "FUTURE_REVISION_CONFLICT"
    ILLEGAL_STATE_TRANSITION = "ILLEGAL_STATE_TRANSITION"
    CANONICAL_RECORD_CORRUPTION_CONFLICT = "CANONICAL_RECORD_CORRUPTION_CONFLICT"
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

    async def initialise(self) -> None:
        await self._commit_loader.load()

    def keyspace(self, canonical_aggregate_identity_sha256: str) -> RedisAuthorityKeyspace:
        return RedisAuthorityKeyspace(canonical_aggregate_identity_sha256, self._namespace)

    async def commit(self, plan: _core.AuthorityCommitPlan) -> RedisAuthorityCommitResult:
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
            ],
        )
        return self._parse_commit_result(raw)

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
        raw_receipt = await self._redis.hget(keyspace.receipts, field)
        raw_record = await self._redis.hget(keyspace.operation_records, field)
        if raw_receipt is None and raw_record is None:
            return None
        if raw_receipt is None or raw_record is None:
            raise RedisAuthorityPersistenceError("stored receipt proof is incomplete")
        _, receipt_payload = _decode_storage_envelope(raw_receipt, field_name="operation receipt")
        _, record_payload = _decode_storage_envelope(raw_record, field_name="canonical operation record")
        receipt = _receipt_from_payload(receipt_payload)
        record = _record_from_payload(record_payload)
        raw_index = await self._redis.hget(keyspace.transition_indexes, keyspace.transition_field(record.transition_id))
        if raw_index is None:
            raise RedisAuthorityPersistenceError("canonical transition index is missing")
        _, index_payload = _decode_storage_envelope(raw_index, field_name="canonical transition index")
        _validate_transition_index(index_payload, record)
        probe = _core.ReceiptProbe(receipt=receipt, canonical_store_record=record)
        try:
            _core._validate_stored_duplicate_proof(
                probe,
                lookup_aggregate_identity_sha256=aggregate_identity.sha256,
                lookup_operation_id=operation_id,
            )
        except _core.AuthorityContractError as exc:
            raise RedisAuthorityPersistenceError(f"stored receipt proof mismatch: {exc}") from exc
        return probe

    async def get_aggregate_snapshot(
        self,
        aggregate_identity: _core.CanonicalAggregateIdentity,
    ) -> PersistedAggregateSnapshot | None:
        if not isinstance(aggregate_identity, _core.CanonicalAggregateIdentity):
            raise TypeError("aggregate_identity must be CanonicalAggregateIdentity")
        raw = await self._redis.hgetall(self.keyspace(aggregate_identity.sha256).aggregate)
        if not raw:
            return None
        data = {_as_text(key): _as_text(value) for key, value in raw.items()}
        expected_fields = {
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
        }
        if set(data) != expected_fields:
            raise RedisAuthorityPersistenceError("aggregate snapshot fields are incomplete or unexpected")
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
            raise RedisAuthorityPersistenceError(f"invalid aggregate snapshot: {exc}") from exc
        if identity_sha != aggregate_identity.sha256:
            raise RedisAuthorityPersistenceError("aggregate snapshot identity mismatch")
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
                self.keyspace(aggregate_identity.sha256).conflicts,
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
    "RedisAuthorityPersistenceError",
    "RedisCanonicalAuthorityStore",
]

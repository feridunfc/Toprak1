"""Redis/Lua persistence adapter for canonical authority commit plans.

Sprint 81.2 adds a persistence boundary only. It does not wire existing
scheduler/worker writers to this adapter, migrate historical keys, or authorize
runtime cutover.

All keys participating in one authority commit share the same Redis Cluster
hash tag. The Lua script performs receipt-first idempotency resolution, strict
revision CAS, immutable record/receipt insertion, aggregate state advancement,
and append-only log/outbox writes in one Redis script execution.
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
    updated_at_ms: int


@dataclass(frozen=True)
class RedisAuthorityKeyspace:
    """Cluster-slot-safe Redis keys for one aggregate authority."""

    canonical_aggregate_identity_sha256: str
    namespace: str = "hfa:authority:v1"

    def __post_init__(self) -> None:
        if type(self.canonical_aggregate_identity_sha256) is not str or _SHA256_RE.fullmatch(
            self.canonical_aggregate_identity_sha256
        ) is None:
            raise ValueError("canonical_aggregate_identity_sha256 must be lowercase SHA-256 hex")
        if type(self.namespace) is not str or not self.namespace or "{" in self.namespace or "}" in self.namespace:
            raise ValueError("namespace must be a non-empty Redis key prefix without hash-tag braces")

    @property
    def hash_tag(self) -> str:
        return "{" + self.canonical_aggregate_identity_sha256 + "}"

    @property
    def aggregate(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:aggregate"

    def transition(self, transition_id: str) -> str:
        return f"{self.namespace}:{self.hash_tag}:transition:{_nonempty(transition_id, 'transition_id')}"

    def receipt(self, operation_id: str) -> str:
        return f"{self.namespace}:{self.hash_tag}:receipt:{_component_digest(_nonempty(operation_id, 'operation_id'))}"

    @property
    def transition_log(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:log"

    @property
    def outbox(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:outbox"

    @property
    def conflicts(self) -> str:
        return f"{self.namespace}:{self.hash_tag}:conflicts"

    def commit_keys(self, *, transition_id: str, operation_id: str) -> list[str]:
        return [
            self.aggregate,
            self.transition(transition_id),
            self.receipt(operation_id),
            self.transition_log,
            self.outbox,
        ]


def _nonempty(value: Any, field_name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _component_digest(value: str) -> str:
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


def _record_from_payload(payload: Mapping[str, Any]) -> _core.CanonicalTransitionRecord:
    try:
        structured = payload["canonical_aggregate_identity"]
        if not isinstance(structured, Mapping):
            raise TypeError("canonical_aggregate_identity must be an object")
        aggregate_type = _core.AggregateType(payload["aggregate_type"])
        identity = _core.CanonicalAggregateIdentity(
            aggregate_type=aggregate_type,
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
        projection_intents = record_payload["durable_projection_intents"]
        is_create = record.operation_type in {
            _core.OperationType.TASK_ADMIT.value,
            _core.OperationType.RUN_CREATE.value,
        }
        raw = await self._commit_loader.run(
            num_keys=5,
            keys=keyspace.commit_keys(
                transition_id=record.transition_id,
                operation_id=record.operation_id,
            ),
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
                record.operation_type,
                str(record.committed_at_ms),
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
        raw_receipt = await self._redis.get(keyspace.receipt(operation_id))
        if raw_receipt is None:
            return None
        receipt_payload = _decode_object(raw_receipt, field_name="operation receipt")
        receipt = _receipt_from_payload(receipt_payload)
        raw_record = await self._redis.get(keyspace.transition(receipt.transition_id))
        record = None if raw_record is None else _record_from_payload(
            _decode_object(raw_record, field_name="canonical transition record")
        )
        return _core.ReceiptProbe(receipt=receipt, canonical_store_record=record)

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
        try:
            state = None if data["state_is_null"] == "1" else data["state"]
            snapshot = PersistedAggregateSnapshot(
                canonical_aggregate_identity_sha256=data["canonical_aggregate_identity_sha256"],
                revision=int(data["revision"]),
                state=state,
                transition_id=data["transition_id"],
                canonical_record_hash=data["canonical_record_hash"],
                updated_at_ms=int(data["updated_at_ms"]),
            )
        except (KeyError, ValueError) as exc:
            raise RedisAuthorityPersistenceError(f"invalid aggregate snapshot: {exc}") from exc
        if snapshot.canonical_aggregate_identity_sha256 != aggregate_identity.sha256:
            raise RedisAuthorityPersistenceError("aggregate snapshot identity mismatch")
        return snapshot

    async def record_conflict(
        self,
        aggregate_identity: _core.CanonicalAggregateIdentity,
        *,
        conflict_type: str,
        operation_id: str,
        observed_at_ms: int,
        detail: Mapping[str, Any] | None = None,
    ) -> str:
        if not isinstance(aggregate_identity, _core.CanonicalAggregateIdentity):
            raise TypeError("aggregate_identity must be CanonicalAggregateIdentity")
        conflict_type = _nonempty(conflict_type, "conflict_type")
        operation_id = _nonempty(operation_id, "operation_id")
        if type(observed_at_ms) is not int or observed_at_ms < 0:
            raise ValueError("observed_at_ms must be a non-negative exact int")
        payload = _canonical_text(detail or {})
        return _as_text(
            await self._redis.xadd(
                self.keyspace(aggregate_identity.sha256).conflicts,
                {
                    "conflict_type": conflict_type,
                    "operation_id": operation_id,
                    "observed_at_ms": str(observed_at_ms),
                    "detail_json": payload,
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
        revision = int(_as_text(raw[2])) if len(raw) > 2 and raw[2] not in (None, "", b"") else None
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

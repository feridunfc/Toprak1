"""Canonical TASK_REQUEUE authority binding and proof-bound projection.

Sprint 84.9 closes the stale-task retry lifecycle without introducing a new
aggregate or operation type.  The existing TASK_REQUEUE authority transition
is committed durably before the legacy requeue Lua is allowed to project the
canonical truth into mutable Redis state and retry-delivery effects.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisAuthorityCorruptionError,
    RedisCanonicalAuthorityStore,
    canonical_json_bytes,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey, RedisTTL
from hfa.dag.schema import DagRedisKey
from hfa.lua.loader import LuaScriptLoader

FEATURE_FLAG = "HFA_CANONICAL_TASK_REQUEUE_BINDING"
WRITER_ID = "hfa-control/task-requeue-recovery-writer:v1"

TASK_REQUEUE_PROJECTED_STATUS = "TASK_REQUEUED"
TASK_REQUEUE_DUPLICATE_STATUS = "TASK_ALREADY_REQUEUED"
TASK_REQUEUE_PROJECTION_PENDING_STATUS = "canonical_requeue_projection_pending"
TASK_REQUEUE_DELIVERY_PENDING_STATUS = "canonical_requeue_delivery_pending"
TASK_REQUEUE_DELIVERED_STATUS = "TASK_REQUEUE_DELIVERED"
TASK_REQUEUE_DELIVERY_DUPLICATE_STATUS = "TASK_REQUEUE_DELIVERY_ALREADY_APPLIED"
TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS = "canonical_requeue_evidence_conflict"
TASK_REQUEUE_AUTHORITY_CONFLICT_STATUS = "canonical_requeue_authority_conflict"

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"", "0", "false", "no", "off"})
_MAX_SAFE_INTEGER = 2**53 - 1
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def parse_task_requeue_binding_flag(value: str | None) -> bool:
    normalized = "" if value is None else value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"invalid {FEATURE_FLAG} value: {value!r}")


def _required_text(value: Any, field_name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _required_sha256(value: Any, field_name: str) -> str:
    normalized = _required_text(value, field_name)
    if _SHA256_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be lowercase SHA-256 hex")
    return normalized


def _safe_integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is bool:
        raise ValueError(f"{field_name} must not be bool")
    if type(value) is int:
        result = value
    elif type(value) is float and math.isfinite(value) and value.is_integer():
        result = int(value)
    elif type(value) is str and value.isdecimal():
        result = int(value)
    else:
        raise ValueError(f"{field_name} must be an exact integer")
    if result < minimum or result > _MAX_SAFE_INTEGER:
        raise ValueError(f"{field_name} is outside the safe integer domain")
    return result


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _lua_path() -> Path:
    here = Path(__file__).resolve()
    candidates = [
        here.parent.parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua" / "task_requeue.lua",
        here.parent.parent.parent / "hfa" / "lua" / "task_requeue.lua",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    for parent in here.parents:
        candidate = parent / "hfa-core" / "src" / "hfa" / "lua" / "task_requeue.lua"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("task_requeue.lua not found")


@dataclass(frozen=True)
class TaskRequeueAuthorityInput:
    task_id: str
    run_id: str
    tenant_id: str
    reason_code: str
    max_requeue_count: int
    worker_instance_id: str
    scheduler_epoch: str
    claim_epoch: int
    dispatch_attempt: int
    claim_transition_id: str
    claim_record_hash: str
    claim_command_hash: str
    claim_revision: int
    claim_operation_id: str

    @property
    def retry_attempt(self) -> int:
        # TASK_CLAIM already carries the scheduler execution attempt. The
        # mutable requeue_count is its projection predecessor (attempt - 1);
        # canonical authority does not invent another retry counter.
        return self.dispatch_attempt


def normalize_task_requeue_input(value: TaskRequeueAuthorityInput) -> TaskRequeueAuthorityInput:
    if not isinstance(value, TaskRequeueAuthorityInput):
        raise ValueError("TASK_REQUEUE input must be TaskRequeueAuthorityInput")
    normalized = replace(
        value,
        task_id=_required_text(value.task_id, "task_id"),
        run_id=_required_text(value.run_id, "run_id"),
        tenant_id=_required_text(value.tenant_id, "tenant_id"),
        reason_code=_required_text(value.reason_code, "reason_code"),
        max_requeue_count=_safe_integer(value.max_requeue_count, "max_requeue_count"),
        worker_instance_id=_required_text(value.worker_instance_id, "worker_instance_id"),
        scheduler_epoch=_required_text(value.scheduler_epoch, "scheduler_epoch"),
        claim_epoch=_safe_integer(value.claim_epoch, "claim_epoch", minimum=1),
        dispatch_attempt=_safe_integer(value.dispatch_attempt, "dispatch_attempt", minimum=1),
        claim_transition_id=_required_text(value.claim_transition_id, "claim_transition_id"),
        claim_record_hash=_required_sha256(value.claim_record_hash, "claim_record_hash"),
        claim_command_hash=_required_sha256(value.claim_command_hash, "claim_command_hash"),
        claim_revision=_safe_integer(value.claim_revision, "claim_revision", minimum=1),
        claim_operation_id=_required_text(value.claim_operation_id, "claim_operation_id"),
    )
    if normalized.scheduler_epoch == "0":
        raise ValueError("scheduler_epoch must represent acquired claim authority")
    if normalized.retry_attempt > normalized.max_requeue_count:
        raise ValueError("retry_attempt exceeds max_requeue_count")
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=normalized.run_id,
        task_id=normalized.task_id,
    )
    expected_claim_operation_id = (
        f"task-claim:v1:{identity.sha256}:attempt:{normalized.dispatch_attempt}"
    )
    if normalized.claim_operation_id != expected_claim_operation_id:
        raise ValueError("claim_operation_id does not match TASK identity and dispatch_attempt")
    return normalized


def task_requeue_identity(value: TaskRequeueAuthorityInput) -> CanonicalAggregateIdentity:
    requeue = normalize_task_requeue_input(value)
    return CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=requeue.run_id,
        task_id=requeue.task_id,
    )


def task_requeue_operation_id(value: TaskRequeueAuthorityInput) -> str:
    requeue = normalize_task_requeue_input(value)
    identity = task_requeue_identity(requeue)
    return f"task-requeue:v1:{identity.sha256}:claim:{requeue.claim_epoch}"


def build_task_requeue_command(value: TaskRequeueAuthorityInput) -> AuthorityCommand:
    requeue = normalize_task_requeue_input(value)
    identity = task_requeue_identity(requeue)
    metadata = {
        "task_id": requeue.task_id,
        "run_id": requeue.run_id,
        "tenant_id": requeue.tenant_id,
        "reason_code": requeue.reason_code,
        "max_requeue_count": requeue.max_requeue_count,
        "retry_attempt": requeue.retry_attempt,
        "worker_instance_id": requeue.worker_instance_id,
        "scheduler_epoch": requeue.scheduler_epoch,
        "claim_epoch": requeue.claim_epoch,
        "dispatch_attempt": requeue.dispatch_attempt,
        "claim_transition_id": requeue.claim_transition_id,
        "claim_record_hash": requeue.claim_record_hash,
        "claim_command_hash": requeue.claim_command_hash,
        "claim_revision": requeue.claim_revision,
        "claim_operation_id": requeue.claim_operation_id,
    }
    return AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_REQUEUE,
        operation_id=task_requeue_operation_id(requeue),
        expected_revision=requeue.claim_revision,
        intended_previous_state="running",
        intended_next_state="ready",
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={},
        requested_projection_intents=(
            {
                "kind": "READY_QUEUE",
                "tenant_id": requeue.tenant_id,
                "task_id": requeue.task_id,
                "retry_attempt": requeue.retry_attempt,
            },
            {
                "kind": "REQUEUE_NOTIFICATION",
                "tenant_id": requeue.tenant_id,
                "task_id": requeue.task_id,
                "reason_code": requeue.reason_code,
                "retry_attempt": requeue.retry_attempt,
            },
        ),
        causation_id=requeue.claim_transition_id,
    )


def build_task_requeue_context(command: AuthorityCommand) -> AuthorityEntryContext:
    if command.operation_type is not OperationType.TASK_REQUEUE:
        raise ValueError("TASK_REQUEUE binding rejects every other operation")
    return AuthorityEntryContext(
        authenticated_writer_id=WRITER_ID,
        allowed_operations=frozenset({OperationType.TASK_REQUEUE}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        # Recovery is an authority adapter, not a scheduler/worker lease.  Its
        # safety fence is the exact canonical TASK head and predecessor claim.
        fence_required=False,
        fence_valid=True,
    )


@dataclass(frozen=True)
class TaskRequeueCanonicalProjectionInput:
    task_id: str
    run_id: str
    tenant_id: str
    reason_code: str
    requeued_at_ms: int
    ready_score: int
    max_requeue_count: int
    worker_instance_id: str
    scheduler_epoch: str
    claim_epoch: int
    dispatch_attempt: int
    claim_transition_id: str
    claim_record_hash: str
    claim_command_hash: str
    claim_revision: int
    claim_operation_id: str
    canonical_transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    canonical_revision: int
    canonical_operation_id: str

    @property
    def retry_attempt(self) -> int:
        return self.dispatch_attempt


@dataclass(frozen=True)
class TaskRequeuePreparedProjection:
    projection: TaskRequeueCanonicalProjectionInput
    exact_retry: bool
    projection_required: bool = True


@dataclass(frozen=True)
class TaskRequeueProjectionResult:
    status: str
    projected: bool
    already_projected: bool
    requeue_count: int


@dataclass(frozen=True)
class TaskRequeueBindingResult:
    status: str
    task_id: str
    requeue_count: int
    canonical_transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    canonical_revision: int
    canonical_operation_id: str
    exact_retry: bool
    projection_applied: bool
    projection_already_applied: bool
    delivery_applied: bool
    delivery_already_applied: bool


@dataclass(frozen=True)
class TaskRequeueClaimContext:
    task_id: str
    run_id: str
    tenant_id: str
    worker_instance_id: str
    scheduler_epoch: str
    claim_epoch: int
    dispatch_attempt: int
    claim_transition_id: str
    claim_record_hash: str
    claim_command_hash: str
    claim_revision: int
    claim_operation_id: str


class TaskRequeueAuthorityError(RuntimeError):
    def __init__(
        self,
        *,
        status: str,
        detail: str = "",
        canonical_commit_durable: bool = False,
    ) -> None:
        super().__init__(f"{status}: {detail}".rstrip(": "))
        self.status = status
        self.detail = detail
        self.canonical_commit_durable = canonical_commit_durable


@dataclass
class TaskRequeueProjectionManager:
    redis: Any
    store: RedisCanonicalAuthorityStore | None = None

    def __post_init__(self) -> None:
        if self.store is None:
            self.store = RedisCanonicalAuthorityStore(self.redis)
        self._loader: LuaScriptLoader | None = None
        self._initialised = False

    async def initialise(self) -> None:
        if self._initialised:
            return
        assert self.store is not None
        await self.store.initialise()
        self._loader = LuaScriptLoader(self.redis, _lua_path())
        await self._loader.load()
        self._initialised = True

    @staticmethod
    def _validate_record_receipt(identity: CanonicalAggregateIdentity, probe: Any) -> tuple[Any, Any]:
        if probe is None or probe.canonical_store_record is None:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical operation record/receipt is missing",
            )
        record = probe.canonical_store_record
        receipt = probe.receipt
        checks = (
            record.aggregate_identity.sha256 == identity.sha256,
            record.aggregate_identity_sha256 == identity.sha256,
            receipt.operation_id == record.operation_id,
            receipt.transition_id == record.transition_id,
            receipt.canonical_command_hash == record.canonical_command_hash,
            receipt.canonical_record_hash == record.canonical_record_hash,
            receipt.aggregate_revision == record.to_revision,
            receipt.operation_type == record.operation_type,
            bool(record.verify_hash()),
        )
        if not all(checks):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical record/receipt continuity mismatch",
            )
        return record, receipt

    async def _validate_durable_authority(
        self,
        projection: TaskRequeueCanonicalProjectionInput,
        *,
        require_exact_head: bool,
    ) -> None:
        assert self.store is not None
        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=projection.run_id,
            task_id=projection.task_id,
        )
        try:
            requeue_probe = await self.store.load_receipt_probe(identity, projection.canonical_operation_id)
            claim_probe = await self.store.load_receipt_probe(identity, projection.claim_operation_id)
            snapshot = await self.store.get_aggregate_snapshot(identity)
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=(
                    "CANONICAL_RECORD_CORRUPTION_CONFLICT"
                    if isinstance(exc, RedisAuthorityCorruptionError)
                    else "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"
                ),
                detail=str(exc),
            ) from exc
        record, receipt = self._validate_record_receipt(identity, requeue_probe)
        claim_record, _claim_receipt = self._validate_record_receipt(identity, claim_probe)
        checks = {
            "operation_type": (record.operation_type, OperationType.TASK_REQUEUE.value),
            "operation_id": (record.operation_id, projection.canonical_operation_id),
            "transition_id": (record.transition_id, projection.canonical_transition_id),
            "record_hash": (record.canonical_record_hash, projection.canonical_record_hash),
            "command_hash": (record.canonical_command_hash, projection.canonical_command_hash),
            "from_revision": (record.from_revision, projection.claim_revision),
            "to_revision": (record.to_revision, projection.canonical_revision),
            "previous_state": (record.previous_state, "running"),
            "next_state": (record.next_state, "ready"),
            "causation_id": (record.causation_id, projection.claim_transition_id),
            "claim_operation_type": (claim_record.operation_type, OperationType.TASK_CLAIM.value),
            "claim_operation_id": (claim_record.operation_id, projection.claim_operation_id),
            "claim_transition_id": (claim_record.transition_id, projection.claim_transition_id),
            "claim_record_hash": (claim_record.canonical_record_hash, projection.claim_record_hash),
            "claim_command_hash": (claim_record.canonical_command_hash, projection.claim_command_hash),
            "claim_revision": (claim_record.to_revision, projection.claim_revision),
            "claim_next_state": (claim_record.next_state, "running"),
        }
        for field, (observed, expected) in checks.items():
            if observed != expected:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail=f"durable TASK_REQUEUE proof mismatch: {field}",
                )
        metadata = record.authoritative_metadata_changes
        if not isinstance(metadata, Mapping):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="durable TASK_REQUEUE metadata is invalid",
            )
        for field in TaskRequeueAuthorityInput.__dataclass_fields__:
            expected = getattr(projection, field)
            if metadata.get(field) != expected:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail=f"durable TASK_REQUEUE metadata.{field} mismatch",
                )
        effect_at_ms = _safe_integer(
            record.committed_at_ms,
            "durable TASK_REQUEUE committed_at_ms",
        )
        if (
            projection.requeued_at_ms != effect_at_ms
            or projection.ready_score != effect_at_ms
        ):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=(
                    "TASK_REQUEUE projection timing must be derived from "
                    "durable committed_at_ms"
                ),
                canonical_commit_durable=True,
            )
        if metadata.get("retry_attempt") != projection.retry_attempt:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="durable TASK_REQUEUE retry_attempt mismatch",
            )
        claim_metadata = claim_record.authoritative_metadata_changes
        if not isinstance(claim_metadata, Mapping):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="durable TASK_CLAIM metadata is invalid",
            )
        if claim_metadata.get("dispatch_attempt") != projection.dispatch_attempt:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="durable TASK_CLAIM dispatch_attempt mismatch",
            )
        if claim_metadata.get("claim_epoch") != projection.claim_epoch:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="durable TASK_CLAIM claim_epoch mismatch",
            )
        if snapshot is None or snapshot.revision < record.to_revision:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK head is behind TASK_REQUEUE receipt",
                canonical_commit_durable=True,
            )
        if require_exact_head:
            if snapshot.revision != record.to_revision:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail="TASK_REQUEUE projection requires exact canonical head",
                    canonical_commit_durable=True,
                )
            try:
                await self.store.validate_authority_head(
                    identity,
                    expected_operation_id=record.operation_id,
                    expected_operation_digest=self.store.keyspace(
                        record.aggregate_identity_sha256
                    ).operation_field(record.operation_id),
                    expected_transition_id=record.transition_id,
                    expected_revision=record.to_revision,
                    expected_canonical_command_hash=record.canonical_command_hash,
                    expected_canonical_record_hash=record.canonical_record_hash,
                    expected_record=record,
                    expected_receipt=receipt,
                    expected_state="ready",
                    expected_projection_intents_json=canonical_json_bytes(
                        record.durable_projection_intents
                    ).decode("utf-8"),
                    expected_updated_at_ms=record.committed_at_ms,
                )
            except Exception as exc:
                raise TaskRequeueAuthorityError(
                    status="CANONICAL_RECORD_CORRUPTION_CONFLICT"
                    if isinstance(exc, RedisAuthorityCorruptionError)
                    else "CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                    detail=str(exc),
                    canonical_commit_durable=True,
                ) from exc

    async def _run_lua(
        self,
        projection: TaskRequeueCanonicalProjectionInput,
        *,
        mode: str,
        require_exact_head: bool,
    ) -> tuple[str, int]:
        await self.initialise()
        await self._validate_durable_authority(
            projection, require_exact_head=require_exact_head
        )
        assert self._loader is not None
        raw = await self._loader.run(
            num_keys=9,
            keys=[
                DagRedisKey.task_state(projection.task_id),
                DagRedisKey.task_meta(projection.task_id),
                DagRedisKey.tenant_ready_queue(projection.tenant_id),
                DagRedisKey.task_running_zset(projection.tenant_id),
                DagRedisKey.completion_stream(projection.tenant_id),
                RedisKey.run_state(projection.run_id),
                DagRedisKey.run_tasks(projection.run_id),
                RedisKey.runtime_truth_conflict_index(),
                RedisKey.runtime_truth_conflict_stream(),
            ],
            args=[
                projection.task_id,
                projection.run_id,
                projection.tenant_id,
                "running",
                str(projection.requeued_at_ms),
                str(projection.ready_score),
                str(projection.max_requeue_count),
                projection.reason_code,
                str(int(getattr(RedisTTL, "STREAM_MAXLEN", 10000))),
                mode,
                projection.canonical_transition_id,
                projection.canonical_record_hash,
                projection.canonical_command_hash,
                str(projection.canonical_revision),
                projection.canonical_operation_id,
                OperationType.TASK_REQUEUE.value,
                projection.claim_transition_id,
                projection.claim_record_hash,
                projection.claim_command_hash,
                str(projection.claim_revision),
                projection.claim_operation_id,
                str(projection.claim_epoch),
                str(projection.retry_attempt),
            ],
        )
        if not isinstance(raw, (list, tuple)) or not raw:
            raise TaskRequeueAuthorityError(
                status=(
                    TASK_REQUEUE_PROJECTION_PENDING_STATUS
                    if mode == "canonical_project"
                    else TASK_REQUEUE_DELIVERY_PENDING_STATUS
                ),
                detail=f"invalid task requeue Lua result: {raw!r}",
                canonical_commit_durable=True,
            )
        status = _decode(raw[0])
        count = _safe_integer(
            _decode(raw[1]) if len(raw) > 1 else "0", "requeue_count"
        )
        return status, count

    async def project(
        self, projection: TaskRequeueCanonicalProjectionInput
    ) -> TaskRequeueProjectionResult:
        status, count = await self._run_lua(
            projection, mode="canonical_project", require_exact_head=True
        )
        if status == TASK_REQUEUE_PROJECTED_STATUS:
            return TaskRequeueProjectionResult(status, True, False, count)
        if status == TASK_REQUEUE_DUPLICATE_STATUS:
            return TaskRequeueProjectionResult(status, False, True, count)
        raise TaskRequeueAuthorityError(
            status=status or TASK_REQUEUE_PROJECTION_PENDING_STATUS,
            detail="canonical TASK_REQUEUE projection did not commit",
            canonical_commit_durable=True,
        )

    async def deliver(
        self, projection: TaskRequeueCanonicalProjectionInput
    ) -> tuple[bool, bool]:
        status, _count = await self._run_lua(
            projection, mode="canonical_deliver", require_exact_head=False
        )
        if status == TASK_REQUEUE_DELIVERED_STATUS:
            return True, False
        if status == TASK_REQUEUE_DELIVERY_DUPLICATE_STATUS:
            return False, True
        raise TaskRequeueAuthorityError(
            status=status or TASK_REQUEUE_DELIVERY_PENDING_STATUS,
            detail="canonical TASK_REQUEUE delivery effect did not commit",
            canonical_commit_durable=True,
        )


@dataclass
class TaskRequeueAuthorityBinding:
    redis: Any
    store: RedisCanonicalAuthorityStore | None = None
    projection_manager: TaskRequeueProjectionManager | None = None

    def __post_init__(self) -> None:
        if self.store is None:
            self.store = RedisCanonicalAuthorityStore(self.redis)
        if self.projection_manager is None:
            self.projection_manager = TaskRequeueProjectionManager(self.redis, store=self.store)
        self._initialised = False

    async def initialise(self) -> None:
        if self._initialised:
            return
        assert self.store is not None
        await self.store.initialise()
        assert self.projection_manager is not None
        await self.projection_manager.initialise()
        self._initialised = True

    @staticmethod
    def _persistence_status(exc: Exception) -> str:
        return (
            "CANONICAL_RECORD_CORRUPTION_CONFLICT"
            if isinstance(exc, RedisAuthorityCorruptionError)
            else "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"
        )

    @staticmethod
    def _validate_record_receipt(identity: CanonicalAggregateIdentity, probe: Any) -> tuple[Any, Any]:
        return TaskRequeueProjectionManager._validate_record_receipt(identity, probe)

    async def _validate_exact_head(self, record: Any, receipt: Any) -> None:
        assert self.store is not None
        try:
            await self.store.validate_authority_head(
                record.aggregate_identity,
                expected_operation_id=record.operation_id,
                expected_operation_digest=self.store.keyspace(
                    record.aggregate_identity_sha256
                ).operation_field(record.operation_id),
                expected_transition_id=record.transition_id,
                expected_revision=record.to_revision,
                expected_canonical_command_hash=record.canonical_command_hash,
                expected_canonical_record_hash=record.canonical_record_hash,
                expected_record=record,
                expected_receipt=receipt,
                expected_state=record.next_state,
                expected_projection_intents_json=canonical_json_bytes(
                    record.durable_projection_intents
                ).decode("utf-8"),
                expected_updated_at_ms=record.committed_at_ms,
            )
        except TaskRequeueAuthorityError:
            raise
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

    async def _durable_conflict(
        self,
        command: AuthorityCommand,
        *,
        status: RedisAuthorityCommitStatus,
        detail_code: str,
        detail: str,
        snapshot: Any,
        receipt_probe: Any,
        observed_at_ms: int,
    ) -> None:
        assert self.store is not None
        record = None if receipt_probe is None else receipt_probe.canonical_store_record
        persisted = await self.store.record_authority_conflict(
            command.aggregate_identity,
            status=status,
            operation_id=command.operation_id,
            incoming_command_hash=command.canonical_command_hash,
            stored_command_hash=None if record is None else record.canonical_command_hash,
            existing_transition_id=None if record is None else record.transition_id,
            aggregate_revision=None if snapshot is None else snapshot.revision,
            observed_at_ms=observed_at_ms,
            detail_code=detail_code,
            detail=detail,
        )
        raise TaskRequeueAuthorityError(
            status=persisted.status.value,
            detail=persisted.detail or detail,
            canonical_commit_durable=receipt_probe is not None,
        )

    @staticmethod
    def _requeue_from_record(record: Any) -> TaskRequeueAuthorityInput:
        if record.operation_type != OperationType.TASK_REQUEUE.value:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="stored operation is not TASK_REQUEUE",
            )
        data = record.authoritative_metadata_changes
        if not isinstance(data, Mapping):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="stored TASK_REQUEUE metadata is invalid",
            )
        try:
            fields = {key: data[key] for key in TaskRequeueAuthorityInput.__dataclass_fields__}
            requeue = normalize_task_requeue_input(TaskRequeueAuthorityInput(**fields))
        except (KeyError, TypeError, ValueError) as exc:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=f"stored TASK_REQUEUE metadata is invalid: {exc}",
            ) from exc
        if record.previous_state != "running" or record.next_state != "ready":
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="stored TASK_REQUEUE transition is invalid",
            )
        if record.from_revision != requeue.claim_revision or record.to_revision != requeue.claim_revision + 1:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="stored TASK_REQUEUE revision is invalid",
            )
        if record.operation_id != task_requeue_operation_id(requeue):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="stored TASK_REQUEUE operation identity mismatch",
            )
        if record.causation_id != requeue.claim_transition_id:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="stored TASK_REQUEUE causation mismatch",
            )
        return requeue

    @staticmethod
    def _projection(requeue: TaskRequeueAuthorityInput, record: Any) -> TaskRequeueCanonicalProjectionInput:
        # Projection/effect timing is not lifecycle command identity. The first
        # durable canonical commit timestamp is the stable source for both the
        # ready-queue score and TaskRequeued delivery timestamp on every replay.
        effect_at_ms = _safe_integer(
            record.committed_at_ms,
            "canonical TASK_REQUEUE committed_at_ms",
        )
        return TaskRequeueCanonicalProjectionInput(
            **requeue.__dict__,
            requeued_at_ms=effect_at_ms,
            ready_score=effect_at_ms,
            canonical_transition_id=record.transition_id,
            canonical_record_hash=record.canonical_record_hash,
            canonical_command_hash=record.canonical_command_hash,
            canonical_revision=record.to_revision,
            canonical_operation_id=record.operation_id,
        )

    async def _read_projection_evidence(self, *, task_id: str, run_id: str, tenant_id: str) -> dict[str, str]:
        try:
            raw = await _maybe_await(self.redis.hgetall(DagRedisKey.task_meta(task_id)))
            task_state = _decode(await _maybe_await(self.redis.get(DagRedisKey.task_state(task_id))))
            run_state = _decode(await _maybe_await(self.redis.get(RedisKey.run_state(run_id))))
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=str(exc),
            ) from exc
        meta = {_decode(k): _decode(v) for k, v in (raw or {}).items()}
        if not meta:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="task metadata is missing",
            )
        expected = {"task_id": task_id, "run_id": run_id, "tenant_id": tenant_id}
        for field, wanted in expected.items():
            if meta.get(field, "") != wanted:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail=f"task_meta.{field} mismatch",
                )
        if task_state != "running":
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=f"legacy task projection is not running: {task_state or 'missing'}",
            )
        if run_state not in {"admitted", "queued", "scheduled", "running", "rescheduled"}:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=f"RUN truth is not nonterminal: {run_state or 'missing'}",
            )
        return meta

    async def current_claim_context(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
    ) -> TaskRequeueClaimContext:
        """Return the exact durable running TASK_CLAIM used for retry policy."""
        await self.initialise()
        assert self.store is not None
        task_id = _required_text(task_id, "task_id")
        run_id = _required_text(run_id, "run_id")
        tenant_id = _required_text(tenant_id, "tenant_id")
        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=run_id,
            task_id=task_id,
        )
        try:
            snapshot = await self.store.get_aggregate_snapshot(identity)
            if snapshot is None or snapshot.state != "running":
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail="canonical TASK head is not running",
                )
            probe = await self.store.load_receipt_probe(identity, snapshot.operation_id)
        except TaskRequeueAuthorityError:
            raise
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc
        record, receipt = self._validate_record_receipt(identity, probe)
        if (
            record.operation_type != OperationType.TASK_CLAIM.value
            or record.previous_state != "scheduled"
            or record.next_state != "running"
            or record.to_revision != snapshot.revision
            or record.operation_id != snapshot.operation_id
            or record.transition_id != snapshot.transition_id
        ):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK head is not exact TASK_CLAIM",
            )
        try:
            await self._validate_exact_head(record, receipt)
        except Exception as exc:
            if isinstance(exc, TaskRequeueAuthorityError):
                raise
            raise TaskRequeueAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc
        data = record.authoritative_metadata_changes
        if not isinstance(data, Mapping):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK_CLAIM metadata is invalid",
            )
        try:
            worker_instance_id = _required_text(
                data["worker_instance_id"], "canonical TASK_CLAIM worker_instance_id"
            )
            scheduler_epoch = _required_text(
                data["scheduler_epoch"], "canonical TASK_CLAIM scheduler_epoch"
            )
            claim_epoch = _safe_integer(
                data["claim_epoch"], "canonical TASK_CLAIM claim_epoch", minimum=1
            )
            dispatch_attempt = _safe_integer(
                data["dispatch_attempt"],
                "canonical TASK_CLAIM dispatch_attempt",
                minimum=1,
            )
        except (KeyError, ValueError) as exc:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=f"canonical TASK_CLAIM metadata is invalid: {exc}",
            ) from exc
        if scheduler_epoch == "0":
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK_CLAIM scheduler_epoch is invalid",
            )
        for field, wanted in (
            ("task_id", task_id),
            ("run_id", run_id),
            ("tenant_id", tenant_id),
        ):
            if data.get(field) != wanted:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail=f"canonical TASK_CLAIM {field} mismatch",
                )
        expected_operation_id = (
            f"task-claim:v1:{identity.sha256}:attempt:{dispatch_attempt}"
        )
        if record.operation_id != expected_operation_id:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK_CLAIM operation identity mismatch",
            )
        return TaskRequeueClaimContext(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            scheduler_epoch=scheduler_epoch,
            claim_epoch=claim_epoch,
            dispatch_attempt=dispatch_attempt,
            claim_transition_id=record.transition_id,
            claim_record_hash=record.canonical_record_hash,
            claim_command_hash=record.canonical_command_hash,
            claim_revision=record.to_revision,
            claim_operation_id=record.operation_id,
        )

    async def prepare_requeue(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        observed_at_ms: int,
        max_requeue_count: int,
        reason_code: str,
        worker_instance_id: str,
        scheduler_epoch: str,
        claim_epoch: int | str,
    ) -> TaskRequeuePreparedProjection:
        await self.initialise()
        assert self.store is not None
        task_id = _required_text(task_id, "task_id")
        run_id = _required_text(run_id, "run_id")
        tenant_id = _required_text(tenant_id, "tenant_id")
        reason_code = _required_text(reason_code, "reason_code")
        worker_instance_id = _required_text(worker_instance_id, "worker_instance_id")
        scheduler_epoch = _required_text(scheduler_epoch, "scheduler_epoch")
        claim_epoch_int = _safe_integer(claim_epoch, "claim_epoch", minimum=1)
        max_count = _safe_integer(max_requeue_count, "max_requeue_count")
        observed_at_ms = _safe_integer(observed_at_ms, "observed_at_ms")
        identity = CanonicalAggregateIdentity(AggregateType.TASK, run_id, task_id)
        operation_id = f"task-requeue:v1:{identity.sha256}:claim:{claim_epoch_int}"
        try:
            snapshot = await self.store.get_aggregate_snapshot(identity)
            probe = await self.store.load_receipt_probe(identity, operation_id)
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

        if probe is not None:
            record, receipt = self._validate_record_receipt(identity, probe)
            stored = self._requeue_from_record(record)
            # Caller-local observation time is an effect input only. Exact
            # duplicate classification is command-hash based over semantic fields;
            # projection time/score are reconstructed from record.committed_at_ms.
            if max_count < stored.retry_attempt:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail="retry policy no longer allows the stored TASK_REQUEUE attempt",
                    canonical_commit_durable=True,
                )
            expected = {
                "task_id": task_id,
                "run_id": run_id,
                "tenant_id": tenant_id,
                "reason_code": reason_code,
                "max_requeue_count": max_count,
                "worker_instance_id": worker_instance_id,
                "scheduler_epoch": scheduler_epoch,
                "claim_epoch": claim_epoch_int,
            }
            for field, wanted in expected.items():
                if getattr(stored, field) != wanted:
                    incoming = replace(stored, **{field: wanted})
                    command = build_task_requeue_command(incoming)
                    await self._durable_conflict(
                        command,
                        status=RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
                        detail_code="requeue_operation_semantic_conflict",
                        detail=f"stored TASK_REQUEUE {field} mismatch",
                        snapshot=snapshot,
                        receipt_probe=probe,
                        observed_at_ms=observed_at_ms,
                    )
            if snapshot is None or snapshot.revision < record.to_revision:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail="canonical TASK head is behind TASK_REQUEUE receipt",
                    canonical_commit_durable=True,
                )
            if snapshot.revision == record.to_revision:
                if (
                    snapshot.operation_id != record.operation_id
                    or snapshot.transition_id != record.transition_id
                    or snapshot.canonical_record_hash != record.canonical_record_hash
                    or snapshot.canonical_command_hash != record.canonical_command_hash
                    or snapshot.state != "ready"
                ):
                    raise TaskRequeueAuthorityError(
                        status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                        detail="canonical TASK_REQUEUE head mismatch",
                        canonical_commit_durable=True,
                    )
                await self._validate_exact_head(record, receipt)
                return TaskRequeuePreparedProjection(
                    projection=self._projection(stored, record),
                    exact_retry=True,
                    projection_required=True,
                )
            # Later canonical progress proves the ready projection was already
            # consumable by TASK_DISPATCH/TASK_CLAIM; never replay old effects.
            return TaskRequeuePreparedProjection(
                projection=self._projection(stored, record),
                exact_retry=True,
                projection_required=False,
            )

        if snapshot is None or snapshot.state != "running":
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK head is not running",
            )
        try:
            claim_probe = await self.store.load_receipt_probe(identity, snapshot.operation_id)
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc
        claim_record, claim_receipt = self._validate_record_receipt(identity, claim_probe)
        if claim_record.operation_type != OperationType.TASK_CLAIM.value:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK head is not TASK_CLAIM",
            )
        if claim_record.to_revision != snapshot.revision or claim_record.next_state != "running":
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="canonical TASK_CLAIM head mismatch",
            )
        await self._validate_exact_head(claim_record, claim_receipt)
        claim_metadata = claim_record.authoritative_metadata_changes
        if not isinstance(claim_metadata, Mapping):
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="TASK_CLAIM metadata is invalid",
            )
        try:
            dispatch_attempt = _safe_integer(
                claim_metadata["dispatch_attempt"],
                "canonical TASK_CLAIM dispatch_attempt",
                minimum=1,
            )
        except (KeyError, ValueError) as exc:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail=f"canonical TASK_CLAIM dispatch_attempt is invalid: {exc}",
            ) from exc
        expected_claim = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_instance_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": claim_epoch_int,
        }
        for field, wanted in expected_claim.items():
            if claim_metadata.get(field) != wanted:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail=f"canonical TASK_CLAIM {field} mismatch",
                )

        meta = await self._read_projection_evidence(task_id=task_id, run_id=run_id, tenant_id=tenant_id)
        projection_claim = {
            "worker_instance_id": worker_instance_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": str(claim_epoch_int),
            "canonical_transition_id": claim_record.transition_id,
            "canonical_record_hash": claim_record.canonical_record_hash,
            "canonical_command_hash": claim_record.canonical_command_hash,
            "canonical_revision": str(claim_record.to_revision),
            "canonical_operation_id": claim_record.operation_id,
        }
        for field, wanted in projection_claim.items():
            if meta.get(field, "") != wanted:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                    detail=f"task_meta.{field} mismatch",
                )
        projected_count = _safe_integer(meta.get("requeue_count", "0"), "task_meta.requeue_count")
        if projected_count != dispatch_attempt - 1:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_EVIDENCE_CONFLICT_STATUS,
                detail="task_meta.requeue_count does not match dispatch_attempt-1",
            )

        requeue = normalize_task_requeue_input(
            TaskRequeueAuthorityInput(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                reason_code=reason_code,
                max_requeue_count=max_count,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
                claim_epoch=claim_epoch_int,
                dispatch_attempt=dispatch_attempt,
                claim_transition_id=claim_record.transition_id,
                claim_record_hash=claim_record.canonical_record_hash,
                claim_command_hash=claim_record.canonical_command_hash,
                claim_revision=claim_record.to_revision,
                claim_operation_id=claim_record.operation_id,
            )
        )
        command = build_task_requeue_command(requeue)
        evaluation = evaluate_authority_commit(
            context=build_task_requeue_context(command),
            command=command,
            current_revision=snapshot.revision,
            current_state=snapshot.state,
            receipt_probe=None,
            committed_at_ms=observed_at_ms,
            correlation_id=None,
        )
        if evaluation.decision.code is not AuthorityDecisionCode.ACCEPTED or evaluation.commit_plan is None:
            try:
                status = RedisAuthorityCommitStatus(evaluation.decision.code.value)
            except ValueError:
                raise TaskRequeueAuthorityError(
                    status=evaluation.decision.code.value,
                    detail="canonical policy rejected TASK_REQUEUE",
                )
            await self._durable_conflict(
                command,
                status=status,
                detail_code=f"policy_{evaluation.decision.code.value.lower()}",
                detail="canonical policy rejected TASK_REQUEUE",
                snapshot=snapshot,
                receipt_probe=None,
                observed_at_ms=observed_at_ms,
            )
        try:
            persisted = await self.store.commit(evaluation.commit_plan)
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status="CANONICAL_RECORD_CORRUPTION_CONFLICT"
                if isinstance(exc, RedisAuthorityCorruptionError)
                else "CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=str(exc),
            ) from exc
        if persisted.status is RedisAuthorityCommitStatus.COMMITTED:
            return TaskRequeuePreparedProjection(
                projection=self._projection(requeue, evaluation.commit_plan.record),
                exact_retry=False,
            )
        if persisted.status in {
            RedisAuthorityCommitStatus.ALREADY_APPLIED,
            RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
            RedisAuthorityCommitStatus.STALE_REVISION_CONFLICT,
        }:
            return await self.prepare_requeue(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                observed_at_ms=observed_at_ms,
                max_requeue_count=max_count,
                reason_code=reason_code,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
                claim_epoch=claim_epoch_int,
            )
        raise TaskRequeueAuthorityError(
            status=persisted.status.value,
            detail=persisted.detail,
        )

    async def requeue(self, **kwargs: Any) -> TaskRequeueBindingResult:
        prepared = await self.prepare_requeue(**kwargs)
        projection = prepared.projection
        assert self.projection_manager is not None

        projection_applied = False
        projection_already_applied = not prepared.projection_required
        if prepared.projection_required:
            try:
                projected = await self.projection_manager.project(projection)
            except TaskRequeueAuthorityError:
                raise
            except Exception as exc:
                raise TaskRequeueAuthorityError(
                    status=TASK_REQUEUE_PROJECTION_PENDING_STATUS,
                    detail=str(exc),
                    canonical_commit_durable=True,
                ) from exc
            projection_applied = projected.projected
            projection_already_applied = projected.already_projected

        try:
            delivery_applied, delivery_already_applied = (
                await self.projection_manager.deliver(projection)
            )
        except TaskRequeueAuthorityError:
            raise
        except Exception as exc:
            raise TaskRequeueAuthorityError(
                status=TASK_REQUEUE_DELIVERY_PENDING_STATUS,
                detail=str(exc),
                canonical_commit_durable=True,
            ) from exc

        status = (
            TASK_REQUEUE_PROJECTED_STATUS
            if projection_applied
            else TASK_REQUEUE_DUPLICATE_STATUS
        )
        return TaskRequeueBindingResult(
            status=status,
            task_id=projection.task_id,
            requeue_count=projection.retry_attempt,
            canonical_transition_id=projection.canonical_transition_id,
            canonical_record_hash=projection.canonical_record_hash,
            canonical_command_hash=projection.canonical_command_hash,
            canonical_revision=projection.canonical_revision,
            canonical_operation_id=projection.canonical_operation_id,
            exact_retry=prepared.exact_retry,
            projection_applied=projection_applied,
            projection_already_applied=projection_already_applied,
            delivery_applied=delivery_applied,
            delivery_already_applied=delivery_already_applied,
        )

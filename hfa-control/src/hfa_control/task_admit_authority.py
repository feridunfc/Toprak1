"""Feature-flagged TASK_ADMIT binding to canonical authority (Sprint 81.3)."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityDecisionCode,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
    RedisAuthorityCommitStatus,
    RedisAuthorityCorruptionError,
    RedisAuthorityPersistenceError,
    RedisCanonicalAuthorityStore,
    canonical_json_bytes,
    evaluate_authority_commit,
)
from hfa.dag.schema import DagRedisKey

FEATURE_FLAG = "HFA_CANONICAL_TASK_ADMIT_BINDING"
WRITER_ID = "hfa-control/task-admission-writer:v1"
_MAX_SAFE_INTEGER = 2**53 - 1
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"", "0", "false", "no", "off"})


class TaskAdmitAuthorityError(RuntimeError):
    """Base error for the canonical TASK_ADMIT runtime binding."""


class TaskAdmitLegacyStateConflictError(TaskAdmitAuthorityError):
    """Legacy task footprint exists without canonical evidence; migration is required."""


class TaskAdmitProjectionPendingError(TaskAdmitAuthorityError):
    def __init__(self, message: str = "canonical commit is durable; legacy projection is pending") -> None:
        super().__init__(message)
        self.canonical_commit_durable = True
        self.legacy_projection_completed = False
        self.retry_safe = True


class TaskAdmitAuthorityConflictError(TaskAdmitAuthorityError):
    def __init__(self, *, status: str, detail: str = "", transition_id: str | None = None,
                 aggregate_revision: int | None = None) -> None:
        super().__init__(f"TASK_ADMIT authority conflict: {status}: {detail}".rstrip(": "))
        self.status = status
        self.detail = detail
        self.transition_id = transition_id
        self.aggregate_revision = aggregate_revision


def parse_task_admit_binding_flag(value: str | None) -> bool:
    normalized = "" if value is None else value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"invalid {FEATURE_FLAG} value: {value!r}")


def _exact_safe_integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is bool:
        raise ValueError(f"{field_name} must not be bool")
    if type(value) is int:
        result = value
    elif type(value) is float and math.isfinite(value) and value.is_integer():
        result = int(value)
    else:
        raise ValueError(f"{field_name} must be an exact int or integral float")
    if result < minimum or result > _MAX_SAFE_INTEGER:
        raise ValueError(f"{field_name} is outside the safe integer domain")
    return result


def stable_committed_at_ms(value: Any) -> int:
    return _exact_safe_integer(value, "DagTaskSeed.admitted_at")


def normalize_task_admit_seed(seed: Any) -> Any:
    """Validate once and return the exact values used by policy and legacy Lua."""
    priority = _exact_safe_integer(getattr(seed, "priority", None), "DagTaskSeed.priority")
    dependency_count = _exact_safe_integer(
        getattr(seed, "dependency_count", None), "DagTaskSeed.dependency_count"
    )
    admitted_at = stable_committed_at_ms(getattr(seed, "admitted_at", None))
    try:
        return replace(seed, priority=priority, dependency_count=dependency_count, admitted_at=admitted_at)
    except TypeError as exc:
        raise ValueError("TASK_ADMIT seed must be a dataclass-compatible DagTaskSeed") from exc


def build_task_admit_command(seed: Any) -> AuthorityCommand:
    seed = normalize_task_admit_seed(seed)
    admitted_at_ms = seed.admitted_at
    dependency_count = seed.dependency_count
    child_task_ids = tuple(str(v) for v in (getattr(seed, "child_task_ids", ()) or ()))
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK, run_id=seed.run_id, task_id=seed.task_id
    )
    next_state = "ready" if dependency_count <= 0 else "pending"
    projection_intents = (
        ({"kind": "READY_QUEUE_IF_READY", "task_id": seed.task_id,
          "tenant_id": seed.tenant_id, "ready_score": admitted_at_ms},)
        if next_state == "ready" else ()
    )
    metadata = {
        "task_id": seed.task_id, "run_id": seed.run_id, "tenant_id": seed.tenant_id,
        "agent_type": getattr(seed, "agent_type", "") or "", "priority": seed.priority,
        "admitted_at_ms": admitted_at_ms, "payload_json": getattr(seed, "payload_json", "") or "",
        "trace_parent": getattr(seed, "trace_parent", "") or "",
        "trace_state": getattr(seed, "trace_state", "") or "",
        "dependency_count": dependency_count, "region": getattr(seed, "region", "") or "",
        "policy": getattr(seed, "policy", "") or "",
    }
    return AuthorityCommand(
        aggregate_identity=identity, operation_type=OperationType.TASK_ADMIT,
        operation_id=f"task-admit:v1:{identity.sha256}", expected_revision=0,
        intended_previous_state=None, intended_next_state=next_state,
        authoritative_payload=metadata, authoritative_metadata_changes=metadata,
        requested_child_effects={
            "child_task_ids": child_task_ids,
            "run_tasks_membership": {"run_id": seed.run_id, "task_id": seed.task_id},
        },
        requested_projection_intents=projection_intents, causation_id=None,
    )


def build_task_admit_context(command: AuthorityCommand) -> AuthorityEntryContext:
    if command.operation_type is not OperationType.TASK_ADMIT:
        raise ValueError("TASK_ADMIT binding rejects every other operation")
    return AuthorityEntryContext(
        authenticated_writer_id=WRITER_ID,
        allowed_operations=frozenset({OperationType.TASK_ADMIT}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False, fence_valid=True,
    )


@dataclass
class TaskAdmitAuthorityBinding:
    redis: Any
    legacy_admit: Callable[[Any], Awaitable[Any]]
    store: RedisCanonicalAuthorityStore | None = None

    def __post_init__(self) -> None:
        if self.store is None:
            self.store = RedisCanonicalAuthorityStore(self.redis)
        self._initialised = False

    async def initialise(self) -> None:
        if not self._initialised:
            assert self.store is not None
            try:
                await self.store.initialise()
            except Exception as exc:
                raise TaskAdmitAuthorityConflictError(
                    status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE", detail=str(exc)
                ) from exc
            self._initialised = True

    async def _legacy_footprint_exists(self, seed: Any) -> bool:
        direct_keys = [
            DagRedisKey.task_state(seed.task_id), DagRedisKey.task_meta(seed.task_id),
            DagRedisKey.task_remaining_deps(seed.task_id), DagRedisKey.task_children(seed.task_id),
            DagRedisKey.task_ready_emitted(seed.task_id),
        ]
        try:
            for key in direct_keys:
                if await self.redis.exists(key):
                    return True
            if await self.redis.sismember(DagRedisKey.run_tasks(seed.run_id), seed.task_id):
                return True
            return await self.redis.zscore(
                DagRedisKey.task_ready_queue(seed.tenant_id), seed.task_id
            ) is not None
        except Exception as exc:
            raise TaskAdmitAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=f"legacy footprint inspection failed: {exc}",
            ) from exc

    @staticmethod
    def _continuity_error(snapshot: Any, probe: Any) -> str | None:
        if (snapshot is None) != (probe is None):
            return "snapshot_receipt_presence_mismatch"
        if snapshot is None:
            return None
        record = probe.canonical_store_record
        receipt = probe.receipt
        if record is None:
            return "receipt_record_missing"
        checks = (
            snapshot.operation_id == receipt.operation_id == record.operation_id,
            snapshot.transition_id == receipt.transition_id == record.transition_id,
            snapshot.canonical_command_hash == receipt.canonical_command_hash == record.canonical_command_hash,
            snapshot.canonical_record_hash == receipt.canonical_record_hash == record.canonical_record_hash,
            snapshot.revision == receipt.aggregate_revision == record.to_revision,
            snapshot.state == record.next_state,
        )
        return None if all(checks) else "snapshot_receipt_continuity_mismatch"

    async def _durable_conflict(self, command: AuthorityCommand, *, status: RedisAuthorityCommitStatus,
                                detail_code: str, detail: str, snapshot: Any = None,
                                receipt_probe: Any = None) -> None:
        assert self.store is not None
        record = None if receipt_probe is None else receipt_probe.canonical_store_record
        stored_hash = None if record is None else record.canonical_command_hash
        transition_id = None if record is None else record.transition_id
        revision = None if snapshot is None else snapshot.revision
        try:
            persisted = await self.store.record_authority_conflict(
                command.aggregate_identity, status=status, operation_id=command.operation_id,
                incoming_command_hash=command.canonical_command_hash,
                stored_command_hash=stored_hash, existing_transition_id=transition_id,
                aggregate_revision=revision, observed_at_ms=command.authoritative_payload["admitted_at_ms"],
                detail_code=detail_code, detail=detail,
            )
        except Exception as exc:
            raise TaskAdmitAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE", detail=str(exc)
            ) from exc
        raise TaskAdmitAuthorityConflictError(
            status=persisted.status.value, detail=persisted.detail,
            transition_id=persisted.transition_id, aggregate_revision=persisted.aggregate_revision,
        )

    @staticmethod
    def _persistence_status(exc: Exception) -> str:
        if isinstance(exc, RedisAuthorityCorruptionError):
            return "CANONICAL_RECORD_CORRUPTION_CONFLICT"
        return "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"

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
        except Exception as exc:
            raise TaskAdmitAuthorityConflictError(
                status=self._persistence_status(exc), detail=str(exc)
            ) from exc

    async def admit(self, seed: Any) -> Any:
        normalized = normalize_task_admit_seed(seed)
        command = build_task_admit_command(normalized)
        context = build_task_admit_context(command)
        await self.initialise()
        assert self.store is not None
        try:
            snapshot = await self.store.get_aggregate_snapshot(command.aggregate_identity)
            receipt_probe = await self.store.load_receipt_probe(command.aggregate_identity, command.operation_id)
        except Exception as exc:
            raise TaskAdmitAuthorityConflictError(
                status=self._persistence_status(exc), detail=str(exc)
            ) from exc

        continuity = self._continuity_error(snapshot, receipt_probe)
        if continuity:
            await self._durable_conflict(
                command, status=RedisAuthorityCommitStatus.CANONICAL_RECORD_CORRUPTION_CONFLICT,
                detail_code=continuity, detail="canonical snapshot/receipt continuity failed",
                snapshot=snapshot, receipt_probe=receipt_probe,
            )
        if snapshot is None and receipt_probe is None and await self._legacy_footprint_exists(normalized):
            raise TaskAdmitLegacyStateConflictError(
                "legacy TASK_ADMIT footprint exists without canonical authority evidence"
            )

        evaluation = evaluate_authority_commit(
            context=context, command=command,
            current_revision=0 if snapshot is None else snapshot.revision,
            current_state=None if snapshot is None else snapshot.state,
            receipt_probe=receipt_probe, committed_at_ms=normalized.admitted_at, correlation_id=None,
        )
        duplicate_record = None
        duplicate_receipt = None
        if evaluation.decision.code is AuthorityDecisionCode.ALREADY_APPLIED:
            if receipt_probe is None or receipt_probe.canonical_store_record is None:
                raise TaskAdmitAuthorityConflictError(
                    status="CANONICAL_RECORD_CORRUPTION_CONFLICT",
                    detail="duplicate evaluation did not provide exact stored proof",
                )
            duplicate_record = receipt_probe.canonical_store_record
            duplicate_receipt = receipt_probe.receipt
        elif evaluation.decision.code is AuthorityDecisionCode.ACCEPTED:
            if evaluation.commit_plan is None:
                raise TaskAdmitAuthorityConflictError(
                    status="AUTHORITY_ENTRY_REJECTED",
                    detail="accepted authority evaluation did not provide a commit plan",
                )
            try:
                persisted = await self.store.commit(evaluation.commit_plan)
            except Exception as exc:
                raise TaskAdmitAuthorityConflictError(
                    status=self._persistence_status(exc), detail=str(exc)
                ) from exc
            if persisted.status is RedisAuthorityCommitStatus.ALREADY_APPLIED:
                duplicate_record = evaluation.commit_plan.record
                duplicate_receipt = evaluation.commit_plan.receipt
            elif persisted.status is not RedisAuthorityCommitStatus.COMMITTED:
                raise TaskAdmitAuthorityConflictError(
                    status=persisted.status.value, detail=persisted.detail,
                    transition_id=persisted.transition_id,
                    aggregate_revision=persisted.aggregate_revision,
                )
        else:
            try:
                status = RedisAuthorityCommitStatus(evaluation.decision.code.value)
            except ValueError:
                raise TaskAdmitAuthorityConflictError(
                    status=evaluation.decision.code.value,
                    detail="canonical policy evaluation blocked legacy projection",
                )
            await self._durable_conflict(
                command, status=status,
                detail_code=f"policy_{evaluation.decision.code.value.lower()}",
                detail="canonical policy evaluation blocked legacy projection",
                snapshot=snapshot, receipt_probe=receipt_probe,
            )

        if duplicate_record is not None:
            assert duplicate_receipt is not None
            await self._validate_exact_head(duplicate_record, duplicate_receipt)

        try:
            return await self.legacy_admit(normalized)
        except Exception as exc:
            raise TaskAdmitProjectionPendingError() from exc

"Canonical TASK_CLAIM authority command contract and manager binding."

from __future__ import annotations

import math
import re
from dataclasses import dataclass, replace
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
    RedisAuthorityPersistenceError,
    RedisCanonicalAuthorityStore,
    canonical_json_bytes,
    evaluate_authority_commit,
)
from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey

FEATURE_FLAG = "HFA_CANONICAL_TASK_CLAIM_BINDING"
WRITER_ID = "hfa-worker/task-claim-writer:v1"

TASK_CLAIM_PROJECTED_STATUS = "task_claimed"
TASK_CLAIM_DUPLICATE_STATUS = "canonical_claim_already_projected"
TASK_CLAIM_PROJECTION_PENDING_STATUS = "canonical_projection_pending"
TASK_CLAIM_EVIDENCE_CONFLICT_STATUS = "canonical_claim_evidence_conflict"
TASK_CLAIM_AUTHORITY_CONFLICT_STATUS = "canonical_claim_authority_conflict"
TASK_CLAIM_CONFIGURATION_CONFLICT_STATUS = "canonical_claim_configuration_conflict"

_MAX_SAFE_INTEGER = 2**53 - 1
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"", "0", "false", "no", "off"})
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def parse_task_claim_binding_flag(value: str | None) -> bool:
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
        raise ValueError(
            f"{field_name} must be a lowercase SHA-256 hex digest"
        )
    return normalized


def _exact_safe_integer(
    value: Any,
    field_name: str,
    *,
    minimum: int = 0,
) -> int:
    if type(value) is bool:
        raise ValueError(f"{field_name} must not be bool")

    if type(value) is int:
        result = value
    elif (
        type(value) is float
        and math.isfinite(value)
        and value.is_integer()
    ):
        result = int(value)
    else:
        raise ValueError(f"{field_name} must be an exact integer")

    if result < minimum or result > _MAX_SAFE_INTEGER:
        raise ValueError(
            f"{field_name} is outside the safe integer domain"
        )
    return result

def _redis_safe_integer(
    value: Any,
    field_name: str,
    *,
    minimum: int = 0,
) -> int:
    if type(value) is str and value and value.isdecimal():
        value = int(value)
    return _exact_safe_integer(value, field_name, minimum=minimum)


@dataclass(frozen=True)
class TaskClaimAuthorityInput:
    task_id: str
    run_id: str
    tenant_id: str
    worker_instance_id: str
    dispatch_worker_id: str
    scheduler_epoch: str
    claimed_at_ms: int
    dispatch_attempt: int
    dispatch_revision: int
    previous_claim_epoch: int
    dispatch_transition_id: str
    dispatch_record_hash: str
    dispatch_command_hash: str
    dispatch_operation_id: str

    @property
    def intended_claim_epoch(self) -> int:
        return self.previous_claim_epoch + 1


def normalize_task_claim_input(
    claim: TaskClaimAuthorityInput,
) -> TaskClaimAuthorityInput:
    if not isinstance(claim, TaskClaimAuthorityInput):
        raise ValueError(
            "TASK_CLAIM input must be TaskClaimAuthorityInput"
        )

    values = {
        "task_id": _required_text(claim.task_id, "task_id"),
        "run_id": _required_text(claim.run_id, "run_id"),
        "tenant_id": _required_text(claim.tenant_id, "tenant_id"),
        "worker_instance_id": _required_text(
            claim.worker_instance_id,
            "worker_instance_id",
        ),
        "dispatch_worker_id": _required_text(
            claim.dispatch_worker_id,
            "dispatch_worker_id",
        ),
        "scheduler_epoch": _required_text(
            claim.scheduler_epoch,
            "scheduler_epoch",
        ),
        "claimed_at_ms": _exact_safe_integer(
            claim.claimed_at_ms,
            "claimed_at_ms",
        ),
        "dispatch_attempt": _exact_safe_integer(
            claim.dispatch_attempt,
            "dispatch_attempt",
            minimum=1,
        ),
        "dispatch_revision": _exact_safe_integer(
            claim.dispatch_revision,
            "dispatch_revision",
            minimum=1,
        ),
        "previous_claim_epoch": _exact_safe_integer(
            claim.previous_claim_epoch,
            "previous_claim_epoch",
        ),
        "dispatch_transition_id": _required_text(
            claim.dispatch_transition_id,
            "dispatch_transition_id",
        ),
        "dispatch_record_hash": _required_sha256(
            claim.dispatch_record_hash,
            "dispatch_record_hash",
        ),
        "dispatch_command_hash": _required_sha256(
            claim.dispatch_command_hash,
            "dispatch_command_hash",
        ),
        "dispatch_operation_id": _required_text(
            claim.dispatch_operation_id,
            "dispatch_operation_id",
        ),
    }

    normalized = replace(claim, **values)

    if normalized.scheduler_epoch == "0":
        raise ValueError(
            "scheduler_epoch must represent acquired scheduling authority"
        )

    if (
        normalized.worker_instance_id
        != normalized.dispatch_worker_id
    ):
        raise ValueError(
            "claim worker must match the canonical dispatch worker"
        )

    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=normalized.run_id,
        task_id=normalized.task_id,
    )

    expected_dispatch_operation_id = (
        f"task-dispatch:v1:{identity.sha256}:"
        f"attempt:{normalized.dispatch_attempt}"
    )

    if (
        normalized.dispatch_operation_id
        != expected_dispatch_operation_id
    ):
        raise ValueError(
            "dispatch_operation_id does not match "
            "TASK identity and attempt"
        )

    return normalized


def task_claim_identity(
    claim: TaskClaimAuthorityInput,
) -> CanonicalAggregateIdentity:
    normalized = normalize_task_claim_input(claim)
    return CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=normalized.run_id,
        task_id=normalized.task_id,
    )


def task_claim_operation_id(
    claim: TaskClaimAuthorityInput,
) -> str:
    normalized = normalize_task_claim_input(claim)
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=normalized.run_id,
        task_id=normalized.task_id,
    )
    return (
        f"task-claim:v1:{identity.sha256}:"
        f"attempt:{normalized.dispatch_attempt}"
    )


def build_task_claim_command(
    claim: TaskClaimAuthorityInput,
) -> AuthorityCommand:
    normalized = normalize_task_claim_input(claim)
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=normalized.run_id,
        task_id=normalized.task_id,
    )

    metadata = {
        "task_id": normalized.task_id,
        "run_id": normalized.run_id,
        "tenant_id": normalized.tenant_id,
        "worker_instance_id": normalized.worker_instance_id,
        "scheduler_epoch": normalized.scheduler_epoch,
        "claimed_at_ms": normalized.claimed_at_ms,
        "dispatch_attempt": normalized.dispatch_attempt,
        "previous_claim_epoch": normalized.previous_claim_epoch,
        "claim_epoch": normalized.intended_claim_epoch,
        "dispatch_transition_id": normalized.dispatch_transition_id,
        "dispatch_record_hash": normalized.dispatch_record_hash,
        "dispatch_command_hash": normalized.dispatch_command_hash,
        "dispatch_operation_id": normalized.dispatch_operation_id,
        "dispatch_revision": normalized.dispatch_revision,
    }

    return AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_CLAIM,
        operation_id=(
            f"task-claim:v1:{identity.sha256}:"
            f"attempt:{normalized.dispatch_attempt}"
        ),
        expected_revision=normalized.dispatch_revision,
        intended_previous_state="scheduled",
        intended_next_state="running",
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={
            "worker_reservation": {
                "action": "CONSUME",
                "task_id": normalized.task_id,
                "worker_id": normalized.worker_instance_id,
                "scheduler_epoch": normalized.scheduler_epoch,
            }
        },
        requested_projection_intents=(
            {
                "kind": "RUNNING_SET",
                "tenant_id": normalized.tenant_id,
                "task_id": normalized.task_id,
            },
        ),
        causation_id=normalized.dispatch_transition_id,
    )


def build_task_claim_context(
    command: AuthorityCommand,
    *,
    scheduler_epoch: str,
) -> AuthorityEntryContext:
    if command.operation_type is not OperationType.TASK_CLAIM:
        raise ValueError(
            "TASK_CLAIM binding rejects every other operation"
        )

    epoch = _required_text(scheduler_epoch, "scheduler_epoch")

    return AuthorityEntryContext(
        authenticated_writer_id=WRITER_ID,
        allowed_operations=frozenset({OperationType.TASK_CLAIM}),
        target_aggregate_identity_sha256=(
            command.aggregate_identity.sha256
        ),
        fence_required=True,
        fence_valid=epoch != "0",
    )


def task_claim_status_allows_execution(status: str) -> bool:
    return status == TASK_CLAIM_PROJECTED_STATUS


@dataclass(frozen=True)
class TaskClaimCanonicalProjectionInput:
    task_id: str
    run_id: str
    tenant_id: str
    worker_instance_id: str
    scheduler_epoch: str
    claimed_at_ms: int
    dispatch_attempt: int
    dispatch_revision: int
    previous_claim_epoch: int
    dispatch_transition_id: str
    dispatch_record_hash: str
    dispatch_command_hash: str
    dispatch_operation_id: str
    canonical_transition_id: str
    canonical_record_hash: str
    canonical_command_hash: str
    canonical_revision: int
    canonical_operation_id: str
    claim_epoch: int


def normalize_task_claim_canonical_projection_input(
    projection: TaskClaimCanonicalProjectionInput,
) -> TaskClaimCanonicalProjectionInput:
    if not isinstance(
        projection,
        TaskClaimCanonicalProjectionInput,
    ):
        raise ValueError(
            "canonical TASK_CLAIM projection must be "
            "TaskClaimCanonicalProjectionInput"
        )

    claim = normalize_task_claim_input(
        TaskClaimAuthorityInput(
            task_id=projection.task_id,
            run_id=projection.run_id,
            tenant_id=projection.tenant_id,
            worker_instance_id=projection.worker_instance_id,
            dispatch_worker_id=projection.worker_instance_id,
            scheduler_epoch=projection.scheduler_epoch,
            claimed_at_ms=projection.claimed_at_ms,
            dispatch_attempt=projection.dispatch_attempt,
            dispatch_revision=projection.dispatch_revision,
            previous_claim_epoch=projection.previous_claim_epoch,
            dispatch_transition_id=projection.dispatch_transition_id,
            dispatch_record_hash=projection.dispatch_record_hash,
            dispatch_command_hash=projection.dispatch_command_hash,
            dispatch_operation_id=projection.dispatch_operation_id,
        )
    )

    normalized = replace(
        projection,
        task_id=claim.task_id,
        run_id=claim.run_id,
        tenant_id=claim.tenant_id,
        worker_instance_id=claim.worker_instance_id,
        scheduler_epoch=claim.scheduler_epoch,
        claimed_at_ms=claim.claimed_at_ms,
        dispatch_attempt=claim.dispatch_attempt,
        dispatch_revision=claim.dispatch_revision,
        previous_claim_epoch=claim.previous_claim_epoch,
        dispatch_transition_id=claim.dispatch_transition_id,
        dispatch_record_hash=_required_sha256(
            claim.dispatch_record_hash,
            "dispatch_record_hash",
        ),
        dispatch_command_hash=_required_sha256(
            claim.dispatch_command_hash,
            "dispatch_command_hash",
        ),
        dispatch_operation_id=claim.dispatch_operation_id,
        canonical_transition_id=_required_text(
            projection.canonical_transition_id,
            "canonical_transition_id",
        ),
        canonical_record_hash=_required_sha256(
            projection.canonical_record_hash,
            "canonical_record_hash",
        ),
        canonical_command_hash=_required_sha256(
            projection.canonical_command_hash,
            "canonical_command_hash",
        ),
        canonical_revision=_exact_safe_integer(
            projection.canonical_revision,
            "canonical_revision",
            minimum=1,
        ),
        canonical_operation_id=_required_text(
            projection.canonical_operation_id,
            "canonical_operation_id",
        ),
        claim_epoch=_exact_safe_integer(
            projection.claim_epoch,
            "claim_epoch",
            minimum=1,
        ),
    )

    if normalized.canonical_revision != normalized.dispatch_revision + 1:
        raise ValueError(
            "canonical_revision must be dispatch_revision + 1"
        )
    if normalized.claim_epoch != normalized.previous_claim_epoch + 1:
        raise ValueError(
            "claim_epoch must be previous_claim_epoch + 1"
        )
    expected_operation_id = task_claim_operation_id(claim)
    if normalized.canonical_operation_id != expected_operation_id:
        raise ValueError(
            "canonical_operation_id does not match TASK identity and attempt"
        )

    return normalized


class TaskClaimAuthorityError(RuntimeError):
    """Base fail-closed error for the manager-level claim binding."""

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
        self.execution_allowed = False
        self.release_reservation = False
        self.automatic_repair = False
        self.automatic_reassignment = False


class TaskClaimEvidenceConflictError(TaskClaimAuthorityError):
    def __init__(self, detail: str) -> None:
        super().__init__(
            status=TASK_CLAIM_EVIDENCE_CONFLICT_STATUS,
            detail=detail,
        )


class TaskClaimAuthorityConflictError(TaskClaimAuthorityError):
    def __init__(
        self,
        *,
        status: str = TASK_CLAIM_AUTHORITY_CONFLICT_STATUS,
        detail: str = "",
        transition_id: str | None = None,
        aggregate_revision: int | None = None,
    ) -> None:
        super().__init__(status=status, detail=detail)
        self.transition_id = transition_id
        self.aggregate_revision = aggregate_revision


@dataclass(frozen=True)
class TaskClaimPreparedProjection:
    projection: TaskClaimCanonicalProjectionInput
    exact_retry: bool


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _decode_redis(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


@dataclass
class TaskClaimAuthorityBinding:
    redis: Any
    store: RedisCanonicalAuthorityStore | None = None

    def __post_init__(self) -> None:
        if self.store is None:
            self.store = RedisCanonicalAuthorityStore(self.redis)
        self._initialised = False

    async def initialise(self) -> None:
        if self._initialised:
            return
        assert self.store is not None
        try:
            await self.store.initialise()
        except Exception as exc:
            raise TaskClaimAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=str(exc),
            ) from exc
        self._initialised = True

    @staticmethod
    def _persistence_status(exc: Exception) -> str:
        if isinstance(exc, RedisAuthorityCorruptionError):
            return "CANONICAL_RECORD_CORRUPTION_CONFLICT"
        return "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"

    async def _redis_type(self, key: str) -> str | None:
        method = getattr(self.redis, "type", None)
        if not callable(method):
            return None
        try:
            return _decode_redis(await _maybe_await(method(key)))
        except Exception as exc:
            raise TaskClaimEvidenceConflictError(
                f"Redis type inspection failed for {key}: {exc}"
            ) from exc

    async def _read_hash(self, key: str, label: str) -> dict[str, str]:
        kind = await self._redis_type(key)
        if kind not in {None, "hash"}:
            raise TaskClaimEvidenceConflictError(
                f"{label} must be a Redis hash, observed {kind}"
            )
        try:
            raw = await _maybe_await(self.redis.hgetall(key))
        except Exception as exc:
            raise TaskClaimEvidenceConflictError(
                f"{label} read failed: {exc}"
            ) from exc
        values = {
            _decode_redis(k): _decode_redis(v)
            for k, v in (raw or {}).items()
        }
        if not values:
            raise TaskClaimEvidenceConflictError(f"{label} is missing")
        return values

    async def _read_task_state(self, task_id: str) -> str:
        key = DagRedisKey.task_state(task_id)
        kind = await self._redis_type(key)
        if kind not in {None, "string"}:
            raise TaskClaimEvidenceConflictError(
                f"legacy task state must be a Redis string, observed {kind}"
            )
        try:
            return _decode_redis(await _maybe_await(self.redis.get(key)))
        except Exception as exc:
            raise TaskClaimEvidenceConflictError(
                f"legacy task state read failed: {exc}"
            ) from exc

    async def _validate_run_truth_for_first_application(
        self,
        run_id: str,
    ) -> None:
        key = RedisKey.run_state(run_id)
        kind = await self._redis_type(key)
        if kind not in {None, "string"}:
            raise TaskClaimEvidenceConflictError(
                f"RUN truth must be a Redis string, observed {kind}"
            )
        try:
            state = _decode_redis(
                await _maybe_await(self.redis.get(key))
            )
        except Exception as exc:
            raise TaskClaimEvidenceConflictError(
                f"RUN truth read failed: {exc}"
            ) from exc
        if state not in {
            "admitted",
            "queued",
            "scheduled",
            "running",
            "rescheduled",
        }:
            raise TaskClaimEvidenceConflictError(
                f"RUN truth is not nonterminal: {state or 'missing'}"
            )

    @staticmethod
    def _require_equal(
        values: Mapping[str, str],
        field: str,
        expected: str,
        label: str,
    ) -> str:
        actual = values.get(field, "")
        if actual != expected:
            raise TaskClaimEvidenceConflictError(
                f"{label}.{field} mismatch"
            )
        return actual

    @staticmethod
    def _metadata(record: Any, operation: OperationType) -> Mapping[str, Any]:
        if record is None:
            raise TaskClaimEvidenceConflictError(
                f"stored {operation.value} record is missing"
            )
        if record.operation_type != operation.value:
            raise TaskClaimEvidenceConflictError(
                f"stored operation is not {operation.value}"
            )
        metadata = record.authoritative_metadata_changes
        if not isinstance(metadata, Mapping):
            raise TaskClaimEvidenceConflictError(
                f"stored {operation.value} metadata is not a mapping"
            )
        return metadata

    @staticmethod
    def _validate_record_receipt(
        identity: CanonicalAggregateIdentity,
        probe: Any,
    ) -> tuple[Any, Any]:
        if probe is None or probe.canonical_store_record is None:
            raise TaskClaimEvidenceConflictError(
                "canonical operation record/receipt is missing"
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
            raise TaskClaimEvidenceConflictError(
                "canonical record/receipt continuity mismatch"
            )
        return record, receipt

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
                expected_canonical_command_hash=(
                    record.canonical_command_hash
                ),
                expected_canonical_record_hash=(
                    record.canonical_record_hash
                ),
                expected_record=record,
                expected_receipt=receipt,
                expected_state=record.next_state,
                expected_projection_intents_json=(
                    canonical_json_bytes(
                        record.durable_projection_intents
                    ).decode("utf-8")
                ),
                expected_updated_at_ms=record.committed_at_ms,
            )
        except Exception as exc:
            raise TaskClaimAuthorityConflictError(
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
        snapshot: Any = None,
        receipt_probe: Any = None,
    ) -> None:
        assert self.store is not None
        record = (
            None
            if receipt_probe is None
            else receipt_probe.canonical_store_record
        )
        try:
            persisted = await self.store.record_authority_conflict(
                command.aggregate_identity,
                status=status,
                operation_id=command.operation_id,
                incoming_command_hash=command.canonical_command_hash,
                stored_command_hash=(
                    None if record is None else record.canonical_command_hash
                ),
                existing_transition_id=(
                    None if record is None else record.transition_id
                ),
                aggregate_revision=(
                    None if snapshot is None else snapshot.revision
                ),
                observed_at_ms=int(
                    command.authoritative_metadata_changes["claimed_at_ms"]
                ),
                detail_code=detail_code,
                detail=detail,
            )
        except Exception as exc:
            raise TaskClaimAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=str(exc),
            ) from exc
        raise TaskClaimAuthorityConflictError(
            status=persisted.status.value,
            detail=persisted.detail,
            transition_id=persisted.transition_id,
            aggregate_revision=persisted.aggregate_revision,
        )

    @staticmethod
    def _projection_from_claim_record(
        claim: TaskClaimAuthorityInput,
        record: Any,
    ) -> TaskClaimCanonicalProjectionInput:
        return normalize_task_claim_canonical_projection_input(
            TaskClaimCanonicalProjectionInput(
                task_id=claim.task_id,
                run_id=claim.run_id,
                tenant_id=claim.tenant_id,
                worker_instance_id=claim.worker_instance_id,
                scheduler_epoch=claim.scheduler_epoch,
                claimed_at_ms=claim.claimed_at_ms,
                dispatch_attempt=claim.dispatch_attempt,
                dispatch_revision=claim.dispatch_revision,
                previous_claim_epoch=claim.previous_claim_epoch,
                dispatch_transition_id=claim.dispatch_transition_id,
                dispatch_record_hash=claim.dispatch_record_hash,
                dispatch_command_hash=claim.dispatch_command_hash,
                dispatch_operation_id=claim.dispatch_operation_id,
                canonical_transition_id=record.transition_id,
                canonical_record_hash=record.canonical_record_hash,
                canonical_command_hash=record.canonical_command_hash,
                canonical_revision=record.to_revision,
                canonical_operation_id=record.operation_id,
                claim_epoch=claim.intended_claim_epoch,
            )
        )

    @staticmethod
    def _claim_from_record(
        record: Any,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        worker_instance_id: str,
        scheduler_epoch: str,
    ) -> TaskClaimAuthorityInput:
        metadata = TaskClaimAuthorityBinding._metadata(
            record,
            OperationType.TASK_CLAIM,
        )
        try:
            claim = normalize_task_claim_input(
                TaskClaimAuthorityInput(
                    task_id=metadata["task_id"],
                    run_id=metadata["run_id"],
                    tenant_id=metadata["tenant_id"],
                    worker_instance_id=metadata["worker_instance_id"],
                    dispatch_worker_id=metadata["worker_instance_id"],
                    scheduler_epoch=metadata["scheduler_epoch"],
                    claimed_at_ms=metadata["claimed_at_ms"],
                    dispatch_attempt=metadata["dispatch_attempt"],
                    dispatch_revision=metadata["dispatch_revision"],
                    previous_claim_epoch=metadata["previous_claim_epoch"],
                    dispatch_transition_id=metadata[
                        "dispatch_transition_id"
                    ],
                    dispatch_record_hash=metadata[
                        "dispatch_record_hash"
                    ],
                    dispatch_command_hash=metadata[
                        "dispatch_command_hash"
                    ],
                    dispatch_operation_id=metadata[
                        "dispatch_operation_id"
                    ],
                )
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise TaskClaimEvidenceConflictError(
                f"stored TASK_CLAIM metadata is invalid: {exc}"
            ) from exc
        expected = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_instance_id": worker_instance_id,
            "scheduler_epoch": scheduler_epoch,
        }
        for field, value in expected.items():
            if getattr(claim, field) != value:
                raise TaskClaimEvidenceConflictError(
                    f"stored TASK_CLAIM {field} mismatch"
                )
        if record.previous_state != "scheduled" or record.next_state != "running":
            raise TaskClaimEvidenceConflictError(
                "stored TASK_CLAIM state transition is invalid"
            )
        if record.from_revision != claim.dispatch_revision:
            raise TaskClaimEvidenceConflictError(
                "stored TASK_CLAIM dispatch revision mismatch"
            )
        if record.to_revision != claim.dispatch_revision + 1:
            raise TaskClaimEvidenceConflictError(
                "stored TASK_CLAIM revision is invalid"
            )
        if record.operation_id != task_claim_operation_id(claim):
            raise TaskClaimEvidenceConflictError(
                "stored TASK_CLAIM operation identity mismatch"
            )
        return claim

    async def _prepare_exact_retry(
        self,
        *,
        identity: CanonicalAggregateIdentity,
        snapshot: Any,
        probe: Any,
        task_id: str,
        run_id: str,
        tenant_id: str,
        worker_instance_id: str,
        scheduler_epoch: str,
    ) -> TaskClaimPreparedProjection:
        record, receipt = self._validate_record_receipt(identity, probe)
        claim = self._claim_from_record(
            record,
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            scheduler_epoch=scheduler_epoch,
        )
        if snapshot is None or snapshot.revision < record.to_revision:
            raise TaskClaimEvidenceConflictError(
                "canonical aggregate head is missing or behind claim receipt"
            )
        if snapshot.revision == record.to_revision:
            if (
                snapshot.operation_id != record.operation_id
                or snapshot.transition_id != record.transition_id
                or snapshot.canonical_record_hash
                != record.canonical_record_hash
                or snapshot.canonical_command_hash
                != record.canonical_command_hash
                or snapshot.state != record.next_state
            ):
                raise TaskClaimEvidenceConflictError(
                    "canonical claim head does not match stored receipt"
                )
            await self._validate_exact_head(record, receipt)
        projection = self._projection_from_claim_record(claim, record)
        return TaskClaimPreparedProjection(
            projection=projection,
            exact_retry=True,
        )

    async def _validate_legacy_first_application(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        worker_instance_id: str,
        scheduler_epoch: str,
        dispatch_record: Any,
        dispatch_metadata: Mapping[str, Any],
        task_meta: Mapping[str, str],
    ) -> int:
        state = await self._read_task_state(task_id)
        if state != "scheduled":
            raise TaskClaimEvidenceConflictError(
                "legacy task state is not scheduled"
            )
        for field, expected in (
            ("task_id", task_id),
            ("run_id", run_id),
            ("tenant_id", tenant_id),
            ("scheduler_epoch", scheduler_epoch),
            ("dispatch_worker_id", worker_instance_id),
            ("canonical_transition_id", dispatch_record.transition_id),
            ("canonical_record_hash", dispatch_record.canonical_record_hash),
            ("canonical_command_hash", dispatch_record.canonical_command_hash),
            ("canonical_revision", str(dispatch_record.to_revision)),
            ("canonical_operation_id", dispatch_record.operation_id),
            ("dispatch_attempt", str(dispatch_metadata["dispatch_attempt"])),
        ):
            self._require_equal(task_meta, field, expected, "task_meta")

        reservation = await self._read_hash(
            DagRedisKey.worker_reservation(worker_instance_id),
            "worker reservation",
        )
        owner = await self._read_hash(
            DagRedisKey.task_reservation_owner(task_id),
            "task reservation-owner index",
        )
        for label, values in (
            ("worker reservation", reservation),
            ("task reservation-owner index", owner),
        ):
            self._require_equal(values, "task_id", task_id, label)
            self._require_equal(
                values, "worker_id", worker_instance_id, label
            )
            self._require_equal(
                values, "scheduler_epoch", scheduler_epoch, label
            )

        try:
            return _redis_safe_integer(
                task_meta.get("claim_epoch", "0"),
                "task_meta.claim_epoch",
            )
        except ValueError as exc:
            raise TaskClaimEvidenceConflictError(str(exc)) from exc

    async def prepare_claim(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        worker_instance_id: str,
        scheduler_epoch: str,
        claimed_at_ms: int,
    ) -> TaskClaimPreparedProjection:
        try:
            task_id = _required_text(task_id, "task_id")
            run_id = _required_text(run_id, "run_id")
            tenant_id = _required_text(tenant_id, "tenant_id")
            worker_instance_id = _required_text(
                worker_instance_id,
                "worker_instance_id",
            )
            scheduler_epoch = _required_text(
                scheduler_epoch,
                "scheduler_epoch",
            )
            claimed_at_ms = _exact_safe_integer(
                claimed_at_ms,
                "claimed_at_ms",
            )
            if scheduler_epoch == "0":
                raise ValueError(
                    "scheduler_epoch must represent acquired authority"
                )
        except ValueError as exc:
            raise TaskClaimEvidenceConflictError(str(exc)) from exc

        await self.initialise()
        assert self.store is not None

        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=run_id,
            task_id=task_id,
        )
        task_meta = await self._read_hash(
            DagRedisKey.task_meta(task_id),
            "task metadata",
        )
        for field, expected in (
            ("task_id", task_id),
            ("run_id", run_id),
            ("tenant_id", tenant_id),
        ):
            self._require_equal(task_meta, field, expected, "task_meta")
        try:
            dispatch_attempt = _redis_safe_integer(
                task_meta.get("dispatch_attempt", ""),
                "task_meta.dispatch_attempt",
                minimum=1,
            )
        except ValueError as exc:
            raise TaskClaimEvidenceConflictError(str(exc)) from exc
        claim_operation_id = (
            f"task-claim:v1:{identity.sha256}:"
            f"attempt:{dispatch_attempt}"
        )

        try:
            snapshot = await self.store.get_aggregate_snapshot(identity)
            claim_probe = await self.store.load_receipt_probe(
                identity,
                claim_operation_id,
            )
        except Exception as exc:
            raise TaskClaimAuthorityConflictError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

        if claim_probe is not None:
            return await self._prepare_exact_retry(
                identity=identity,
                snapshot=snapshot,
                probe=claim_probe,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
            )

        if snapshot is None:
            raise TaskClaimEvidenceConflictError(
                "canonical TASK aggregate is missing"
            )
        if snapshot.state != "scheduled":
            raise TaskClaimEvidenceConflictError(
                "canonical TASK head is not scheduled"
            )
        dispatch_operation_id = snapshot.operation_id
        try:
            dispatch_probe = await self.store.load_receipt_probe(
                identity,
                dispatch_operation_id,
            )
        except Exception as exc:
            raise TaskClaimAuthorityConflictError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc
        dispatch_record, dispatch_receipt = self._validate_record_receipt(
            identity,
            dispatch_probe,
        )
        dispatch_metadata = self._metadata(
            dispatch_record,
            OperationType.TASK_DISPATCH,
        )
        if (
            dispatch_record.next_state != "scheduled"
            or dispatch_record.to_revision != snapshot.revision
            or dispatch_record.operation_id != snapshot.operation_id
        ):
            raise TaskClaimEvidenceConflictError(
                "canonical TASK head is not the exact dispatch transition"
            )
        await self._validate_exact_head(
            dispatch_record,
            dispatch_receipt,
        )

        expected_dispatch = {
            "task_id": task_id,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "worker_id": worker_instance_id,
            "scheduler_epoch": scheduler_epoch,
        }
        for field, expected in expected_dispatch.items():
            if dispatch_metadata.get(field) != expected:
                raise TaskClaimEvidenceConflictError(
                    f"canonical TASK_DISPATCH {field} mismatch"
                )
        try:
            canonical_attempt = _exact_safe_integer(
                dispatch_metadata["dispatch_attempt"],
                "canonical dispatch_attempt",
                minimum=1,
            )
        except (KeyError, ValueError) as exc:
            raise TaskClaimEvidenceConflictError(
                f"canonical TASK_DISPATCH attempt is invalid: {exc}"
            ) from exc
        if canonical_attempt != dispatch_attempt:
            raise TaskClaimEvidenceConflictError(
                "legacy and canonical dispatch attempt mismatch"
            )
        expected_dispatch_operation_id = (
            f"task-dispatch:v1:{identity.sha256}:"
            f"attempt:{canonical_attempt}"
        )
        if dispatch_record.operation_id != expected_dispatch_operation_id:
            raise TaskClaimEvidenceConflictError(
                "canonical TASK_DISPATCH operation identity mismatch"
            )

        await self._validate_run_truth_for_first_application(run_id)
        previous_claim_epoch = await self._validate_legacy_first_application(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            scheduler_epoch=scheduler_epoch,
            dispatch_record=dispatch_record,
            dispatch_metadata=dispatch_metadata,
            task_meta=task_meta,
        )
        claim = normalize_task_claim_input(
            TaskClaimAuthorityInput(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                worker_instance_id=worker_instance_id,
                dispatch_worker_id=dispatch_metadata["worker_id"],
                scheduler_epoch=scheduler_epoch,
                claimed_at_ms=claimed_at_ms,
                dispatch_attempt=canonical_attempt,
                dispatch_revision=dispatch_record.to_revision,
                previous_claim_epoch=previous_claim_epoch,
                dispatch_transition_id=dispatch_record.transition_id,
                dispatch_record_hash=dispatch_record.canonical_record_hash,
                dispatch_command_hash=dispatch_record.canonical_command_hash,
                dispatch_operation_id=dispatch_record.operation_id,
            )
        )
        command = build_task_claim_command(claim)
        context = build_task_claim_context(
            command,
            scheduler_epoch=scheduler_epoch,
        )
        evaluation = evaluate_authority_commit(
            context=context,
            command=command,
            current_revision=snapshot.revision,
            current_state=snapshot.state,
            receipt_probe=None,
            committed_at_ms=claim.claimed_at_ms,
            correlation_id=None,
        )
        if evaluation.decision.code is not AuthorityDecisionCode.ACCEPTED:
            try:
                status = RedisAuthorityCommitStatus(
                    evaluation.decision.code.value
                )
            except ValueError:
                raise TaskClaimAuthorityConflictError(
                    status=evaluation.decision.code.value,
                    detail="canonical policy rejected TASK_CLAIM",
                )
            await self._durable_conflict(
                command,
                status=status,
                detail_code=(
                    "policy_"
                    f"{evaluation.decision.code.value.lower()}"
                ),
                detail="canonical policy rejected TASK_CLAIM",
                snapshot=snapshot,
            )
        if evaluation.commit_plan is None:
            raise TaskClaimAuthorityConflictError(
                status="AUTHORITY_ENTRY_REJECTED",
                detail="accepted TASK_CLAIM evaluation has no commit plan",
            )

        try:
            persisted = await self.store.commit(evaluation.commit_plan)
        except Exception as exc:
            raise TaskClaimAuthorityConflictError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

        if persisted.status is RedisAuthorityCommitStatus.COMMITTED:
            record = evaluation.commit_plan.record
            projection = self._projection_from_claim_record(claim, record)
            return TaskClaimPreparedProjection(
                projection=projection,
                exact_retry=False,
            )

        if persisted.status in {
            RedisAuthorityCommitStatus.ALREADY_APPLIED,
            RedisAuthorityCommitStatus.IDEMPOTENCY_CONFLICT,
        }:
            try:
                retry_snapshot = await self.store.get_aggregate_snapshot(
                    identity
                )
                retry_probe = await self.store.load_receipt_probe(
                    identity,
                    command.operation_id,
                )
            except Exception as exc:
                raise TaskClaimAuthorityConflictError(
                    status=self._persistence_status(exc),
                    detail=str(exc),
                ) from exc
            if retry_probe is not None:
                return await self._prepare_exact_retry(
                    identity=identity,
                    snapshot=retry_snapshot,
                    probe=retry_probe,
                    task_id=task_id,
                    run_id=run_id,
                    tenant_id=tenant_id,
                    worker_instance_id=worker_instance_id,
                    scheduler_epoch=scheduler_epoch,
                )

        raise TaskClaimAuthorityConflictError(
            status=persisted.status.value,
            detail=persisted.detail,
            transition_id=persisted.transition_id,
            aggregate_revision=persisted.aggregate_revision,
        )

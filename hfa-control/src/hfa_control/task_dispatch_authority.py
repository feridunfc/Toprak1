"""Feature-flagged TASK_DISPATCH binding to canonical authority."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable, Mapping

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

FEATURE_FLAG = "HFA_CANONICAL_TASK_DISPATCH_BINDING"
WRITER_ID = "hfa-control/task-dispatch-writer:v1"
_MAX_SAFE_INTEGER = 2**53 - 1
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"", "0", "false", "no", "off"})


class TaskDispatchAuthorityError(RuntimeError):
    """Base error for canonical TASK_DISPATCH binding."""


class TaskDispatchLegacyStateConflictError(TaskDispatchAuthorityError):
    """Legacy TASK state exists without canonical TASK authority evidence."""


class TaskDispatchProjectionPendingError(TaskDispatchAuthorityError):
    def __init__(
        self,
        message: str = (
            "canonical TASK_DISPATCH commit is durable; "
            "legacy projection is pending"
        ),
        *,
        status: str = "canonical_projection_pending",
        detail: str = "",
        retry_safe: bool = True,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.detail = detail
        self.canonical_commit_durable = True
        self.legacy_projection_completed = False
        self.release_reservation = False
        self.retry_safe = bool(retry_safe)


class TaskDispatchAuthorityConflictError(TaskDispatchAuthorityError):
    def __init__(
        self,
        *,
        status: str,
        detail: str = "",
        transition_id: str | None = None,
        aggregate_revision: int | None = None,
    ) -> None:
        super().__init__(
            f"TASK_DISPATCH authority conflict: {status}: {detail}".rstrip(
                ": "
            )
        )
        self.status = status
        self.detail = detail
        self.transition_id = transition_id
        self.aggregate_revision = aggregate_revision
        self.canonical_commit_durable = False
        self.release_reservation = True
        self.retry_safe = False


def parse_task_dispatch_binding_flag(value: str | None) -> bool:
    normalized = "" if value is None else value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"invalid {FEATURE_FLAG} value: {value!r}")


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
        raise ValueError(
            f"{field_name} must be an exact int or integral float"
        )
    if result < minimum or result > _MAX_SAFE_INTEGER:
        raise ValueError(
            f"{field_name} is outside the safe integer domain"
        )
    return result


def _required_text(value: Any, field_name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def normalize_task_dispatch_input(dispatch: Any) -> Any:
    """Return the exact values shared by policy and legacy projection."""
    values = {
        "task_id": _required_text(
            getattr(dispatch, "task_id", None),
            "DagTaskDispatchInput.task_id",
        ),
        "run_id": _required_text(
            getattr(dispatch, "run_id", None),
            "DagTaskDispatchInput.run_id",
        ),
        "tenant_id": _required_text(
            getattr(dispatch, "tenant_id", None),
            "DagTaskDispatchInput.tenant_id",
        ),
        "worker_id": _required_text(
            getattr(dispatch, "worker_id", None),
            "DagTaskDispatchInput.worker_id",
        ),
        "scheduler_epoch": _required_text(
            getattr(dispatch, "scheduler_epoch", None),
            "DagTaskDispatchInput.scheduler_epoch",
        ),
        "scheduled_zset": _required_text(
            getattr(dispatch, "scheduled_zset", None),
            "DagTaskDispatchInput.scheduled_zset",
        ),
        "running_zset": _required_text(
            getattr(dispatch, "running_zset", None),
            "DagTaskDispatchInput.running_zset",
        ),
        "control_stream": _required_text(
            getattr(dispatch, "control_stream", None),
            "DagTaskDispatchInput.control_stream",
        ),
        "shard_stream": _required_text(
            getattr(dispatch, "shard_stream", None),
            "DagTaskDispatchInput.shard_stream",
        ),
        "priority": _exact_safe_integer(
            getattr(dispatch, "priority", None),
            "DagTaskDispatchInput.priority",
        ),
        "shard": _exact_safe_integer(
            getattr(dispatch, "shard", None),
            "DagTaskDispatchInput.shard",
        ),
        "admitted_at": _exact_safe_integer(
            getattr(dispatch, "admitted_at", None),
            "DagTaskDispatchInput.admitted_at",
        ),
        "scheduled_at": _exact_safe_integer(
            getattr(dispatch, "scheduled_at", None),
            "DagTaskDispatchInput.scheduled_at",
        ),
        "attempt": _exact_safe_integer(
            getattr(dispatch, "attempt", None),
            "DagTaskDispatchInput.attempt",
            minimum=1,
        ),
    }
    if values["scheduler_epoch"] == "0":
        raise ValueError(
            "DagTaskDispatchInput.scheduler_epoch must represent "
            "acquired leadership authority"
        )
    try:
        return replace(dispatch, **values)
    except TypeError as exc:
        raise ValueError(
            "TASK_DISPATCH input must be a dataclass-compatible "
            "DagTaskDispatchInput"
        ) from exc


def task_dispatch_identity(dispatch: Any) -> CanonicalAggregateIdentity:
    normalized = normalize_task_dispatch_input(dispatch)
    return CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=normalized.run_id,
        task_id=normalized.task_id,
    )


def task_dispatch_operation_id(dispatch: Any) -> str:
    normalized = normalize_task_dispatch_input(dispatch)
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=normalized.run_id,
        task_id=normalized.task_id,
    )
    return (
        f"task-dispatch:v1:{identity.sha256}:"
        f"attempt:{normalized.attempt}"
    )


def _dispatch_metadata(dispatch: Any) -> dict[str, Any]:
    return {
        "task_id": dispatch.task_id,
        "run_id": dispatch.run_id,
        "tenant_id": dispatch.tenant_id,
        "worker_id": dispatch.worker_id,
        "worker_group": (
            str(getattr(dispatch, "worker_group", "") or "")
        ),
        "scheduler_epoch": dispatch.scheduler_epoch,
        "dispatch_attempt": dispatch.attempt,
        "agent_type": (
            str(getattr(dispatch, "agent_type", "") or "")
        ),
        "shard": dispatch.shard,
        "priority": dispatch.priority,
        "admitted_at_ms": dispatch.admitted_at,
        "scheduled_at_ms": dispatch.scheduled_at,
        "policy": str(
            getattr(dispatch, "policy", "") or "LEAST_LOADED"
        ),
        "region": str(getattr(dispatch, "region", "") or ""),
        "payload_json": str(
            getattr(dispatch, "payload_json", "") or "{}"
        ),
        "trace_parent": str(
            getattr(dispatch, "trace_parent", "") or ""
        ),
        "trace_state": str(
            getattr(dispatch, "trace_state", "") or ""
        ),
        "scheduled_zset": dispatch.scheduled_zset,
        "running_zset": dispatch.running_zset,
        "ready_queue": DagRedisKey.task_ready_queue(
            dispatch.tenant_id
        ),
        "control_stream": dispatch.control_stream,
        "shard_stream": dispatch.shard_stream,
    }


def build_task_dispatch_command(
    dispatch: Any,
    *,
    expected_revision: int = 1,
) -> AuthorityCommand:
    normalized = normalize_task_dispatch_input(dispatch)
    expected_revision = _exact_safe_integer(
        expected_revision,
        "expected_revision",
    )
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK,
        run_id=normalized.run_id,
        task_id=normalized.task_id,
    )
    metadata = _dispatch_metadata(normalized)
    return AuthorityCommand(
        aggregate_identity=identity,
        operation_type=OperationType.TASK_DISPATCH,
        operation_id=(
            f"task-dispatch:v1:{identity.sha256}:"
            f"attempt:{normalized.attempt}"
        ),
        expected_revision=expected_revision,
        intended_previous_state="ready",
        intended_next_state="scheduled",
        authoritative_payload=metadata,
        authoritative_metadata_changes=metadata,
        requested_child_effects={
            "worker_reservation": {
                "worker_id": normalized.worker_id,
                "task_id": normalized.task_id,
                "scheduler_epoch": normalized.scheduler_epoch,
            }
        },
        requested_projection_intents=(
            {
                "kind": "CONTROL_NOTIFICATION",
                "stream": normalized.control_stream,
            },
            {
                "kind": "TASK_REQUEST_MESSAGE",
                "stream": normalized.shard_stream,
            },
        ),
        causation_id=None,
    )


def build_task_dispatch_context(
    command: AuthorityCommand,
    *,
    scheduler_epoch: str,
) -> AuthorityEntryContext:
    if command.operation_type is not OperationType.TASK_DISPATCH:
        raise ValueError(
            "TASK_DISPATCH binding rejects every other operation"
        )
    epoch = _required_text(
        scheduler_epoch,
        "scheduler_epoch",
    )
    return AuthorityEntryContext(
        authenticated_writer_id=WRITER_ID,
        allowed_operations=frozenset(
            {OperationType.TASK_DISPATCH}
        ),
        target_aggregate_identity_sha256=(
            command.aggregate_identity.sha256
        ),
        fence_required=True,
        fence_valid=epoch != "0",
    )


@dataclass
class TaskDispatchAuthorityBinding:
    redis: Any
    legacy_dispatch: Callable[[Any], Awaitable[Any]]
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
            raise TaskDispatchAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=str(exc),
            ) from exc
        self._initialised = True

    async def _legacy_footprint_exists(
        self,
        dispatch: Any,
    ) -> bool:
        try:
            for key in (
                DagRedisKey.task_state(dispatch.task_id),
                DagRedisKey.task_meta(dispatch.task_id),
            ):
                if await self.redis.exists(key):
                    return True
            if (
                await self.redis.zscore(
                    DagRedisKey.task_ready_queue(
                        dispatch.tenant_id
                    ),
                    dispatch.task_id,
                )
                is not None
            ):
                return True
            return (
                await self.redis.zscore(
                    dispatch.scheduled_zset,
                    dispatch.task_id,
                )
                is not None
            )
        except Exception as exc:
            raise TaskDispatchAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=(
                    "legacy TASK_DISPATCH footprint inspection "
                    f"failed: {exc}"
                ),
            ) from exc

    @staticmethod
    def _continuity_error(
        snapshot: Any,
        probe: Any,
    ) -> str | None:
        # An existing aggregate head with no receipt for this operation is
        # the normal first-application path for non-create operations.
        if probe is None:
            return None
        if snapshot is None:
            return "snapshot_missing_for_receipt"
        record = probe.canonical_store_record
        receipt = probe.receipt
        if record is None:
            return "receipt_record_missing"
        checks = (
            (
                snapshot.operation_id
                == receipt.operation_id
                == record.operation_id
            ),
            (
                snapshot.transition_id
                == receipt.transition_id
                == record.transition_id
            ),
            (
                snapshot.canonical_command_hash
                == receipt.canonical_command_hash
                == record.canonical_command_hash
            ),
            (
                snapshot.canonical_record_hash
                == receipt.canonical_record_hash
                == record.canonical_record_hash
            ),
            (
                snapshot.revision
                == receipt.aggregate_revision
                == record.to_revision
            ),
            snapshot.state == record.next_state,
        )
        return (
            None
            if all(checks)
            else "snapshot_receipt_continuity_mismatch"
        )

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
        stored_hash = (
            None
            if record is None
            else record.canonical_command_hash
        )
        transition_id = (
            None if record is None else record.transition_id
        )
        revision = (
            None if snapshot is None else snapshot.revision
        )
        try:
            persisted = await self.store.record_authority_conflict(
                command.aggregate_identity,
                status=status,
                operation_id=command.operation_id,
                incoming_command_hash=(
                    command.canonical_command_hash
                ),
                stored_command_hash=stored_hash,
                existing_transition_id=transition_id,
                aggregate_revision=revision,
                observed_at_ms=int(
                    command.authoritative_metadata_changes[
                        "scheduled_at_ms"
                    ]
                ),
                detail_code=detail_code,
                detail=detail,
            )
        except Exception as exc:
            raise TaskDispatchAuthorityConflictError(
                status="CONFLICT_EVIDENCE_STORE_UNAVAILABLE",
                detail=str(exc),
            ) from exc
        raise TaskDispatchAuthorityConflictError(
            status=persisted.status.value,
            detail=persisted.detail,
            transition_id=persisted.transition_id,
            aggregate_revision=persisted.aggregate_revision,
        )

    @staticmethod
    def _persistence_status(exc: Exception) -> str:
        if isinstance(exc, RedisAuthorityCorruptionError):
            return "CANONICAL_RECORD_CORRUPTION_CONFLICT"
        return "CONFLICT_EVIDENCE_STORE_UNAVAILABLE"

    async def _validate_exact_head(
        self,
        record: Any,
        receipt: Any,
    ) -> None:
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
            raise TaskDispatchAuthorityConflictError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

    @staticmethod
    def _stored_scheduled_at(record: Any) -> int:
        metadata = record.authoritative_metadata_changes
        if not isinstance(metadata, Mapping):
            raise TaskDispatchAuthorityConflictError(
                status=(
                    "CANONICAL_RECORD_CORRUPTION_CONFLICT"
                ),
                detail=(
                    "stored TASK_DISPATCH metadata is not a mapping"
                ),
            )
        try:
            return _exact_safe_integer(
                metadata["scheduled_at_ms"],
                "stored scheduled_at_ms",
            )
        except (KeyError, ValueError) as exc:
            raise TaskDispatchAuthorityConflictError(
                status=(
                    "CANONICAL_RECORD_CORRUPTION_CONFLICT"
                ),
                detail=(
                    "stored TASK_DISPATCH scheduled_at_ms "
                    "is missing or invalid"
                ),
            ) from exc

    @staticmethod
    def _projection_input(
        dispatch: Any,
        record: Any,
    ) -> Any:
        return replace(
            dispatch,
            scheduled_at=(
                TaskDispatchAuthorityBinding
                ._stored_scheduled_at(record)
            ),
            canonical_transition_id=record.transition_id,
            canonical_record_hash=record.canonical_record_hash,
            canonical_command_hash=(
                record.canonical_command_hash
            ),
            canonical_revision=record.to_revision,
            canonical_operation_id=record.operation_id,
        )

    async def dispatch(self, dispatch: Any) -> Any:
        normalized = normalize_task_dispatch_input(dispatch)
        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=normalized.run_id,
            task_id=normalized.task_id,
        )
        operation_id = (
            f"task-dispatch:v1:{identity.sha256}:"
            f"attempt:{normalized.attempt}"
        )

        await self.initialise()
        assert self.store is not None
        try:
            snapshot = await self.store.get_aggregate_snapshot(
                identity
            )
            receipt_probe = await self.store.load_receipt_probe(
                identity,
                operation_id,
            )
        except Exception as exc:
            raise TaskDispatchAuthorityConflictError(
                status=self._persistence_status(exc),
                detail=str(exc),
            ) from exc

        continuity = self._continuity_error(
            snapshot,
            receipt_probe,
        )
        if continuity:
            provisional = build_task_dispatch_command(
                normalized,
                expected_revision=(
                    0
                    if snapshot is None
                    else snapshot.revision
                ),
            )
            await self._durable_conflict(
                provisional,
                status=(
                    RedisAuthorityCommitStatus
                    .CANONICAL_RECORD_CORRUPTION_CONFLICT
                ),
                detail_code=continuity,
                detail=(
                    "canonical snapshot/receipt continuity failed"
                ),
                snapshot=snapshot,
                receipt_probe=receipt_probe,
            )

        if (
            snapshot is None
            and receipt_probe is None
            and await self._legacy_footprint_exists(normalized)
        ):
            raise TaskDispatchLegacyStateConflictError(
                "legacy TASK_DISPATCH footprint exists without "
                "canonical TASK authority evidence"
            )

        stored_record = (
            None
            if receipt_probe is None
            else receipt_probe.canonical_store_record
        )
        if stored_record is not None:
            normalized = replace(
                normalized,
                scheduled_at=self._stored_scheduled_at(
                    stored_record
                ),
            )
            expected_revision = stored_record.from_revision
        else:
            expected_revision = (
                0
                if snapshot is None
                else snapshot.revision
            )

        command = build_task_dispatch_command(
            normalized,
            expected_revision=expected_revision,
        )
        context = build_task_dispatch_context(
            command,
            scheduler_epoch=normalized.scheduler_epoch,
        )
        evaluation = evaluate_authority_commit(
            context=context,
            command=command,
            current_revision=(
                0 if snapshot is None else snapshot.revision
            ),
            current_state=(
                None if snapshot is None else snapshot.state
            ),
            receipt_probe=receipt_probe,
            committed_at_ms=normalized.scheduled_at,
            correlation_id=None,
        )

        projection_record = None
        projection_receipt = None
        validate_exact_head = False

        if (
            evaluation.decision.code
            is AuthorityDecisionCode.ALREADY_APPLIED
        ):
            if (
                receipt_probe is None
                or receipt_probe.canonical_store_record is None
            ):
                raise TaskDispatchAuthorityConflictError(
                    status=(
                        "CANONICAL_RECORD_CORRUPTION_CONFLICT"
                    ),
                    detail=(
                        "duplicate evaluation did not provide "
                        "exact stored proof"
                    ),
                )
            projection_record = (
                receipt_probe.canonical_store_record
            )
            projection_receipt = receipt_probe.receipt
            validate_exact_head = True
        elif (
            evaluation.decision.code
            is AuthorityDecisionCode.ACCEPTED
        ):
            if evaluation.commit_plan is None:
                raise TaskDispatchAuthorityConflictError(
                    status="AUTHORITY_ENTRY_REJECTED",
                    detail=(
                        "accepted authority evaluation did not "
                        "provide a commit plan"
                    ),
                )
            try:
                persisted = await self.store.commit(
                    evaluation.commit_plan
                )
            except Exception as exc:
                raise TaskDispatchAuthorityConflictError(
                    status=self._persistence_status(exc),
                    detail=str(exc),
                ) from exc

            if (
                persisted.status
                is RedisAuthorityCommitStatus.ALREADY_APPLIED
            ):
                projection_record = (
                    evaluation.commit_plan.record
                )
                projection_receipt = (
                    evaluation.commit_plan.receipt
                )
                validate_exact_head = True
            elif (
                persisted.status
                is RedisAuthorityCommitStatus.COMMITTED
            ):
                projection_record = (
                    evaluation.commit_plan.record
                )
                projection_receipt = (
                    evaluation.commit_plan.receipt
                )
            else:
                raise TaskDispatchAuthorityConflictError(
                    status=persisted.status.value,
                    detail=persisted.detail,
                    transition_id=persisted.transition_id,
                    aggregate_revision=(
                        persisted.aggregate_revision
                    ),
                )
        else:
            try:
                status = RedisAuthorityCommitStatus(
                    evaluation.decision.code.value
                )
            except ValueError:
                raise TaskDispatchAuthorityConflictError(
                    status=evaluation.decision.code.value,
                    detail=(
                        "canonical policy evaluation blocked "
                        "legacy TASK_DISPATCH projection"
                    ),
                )
            await self._durable_conflict(
                command,
                status=status,
                detail_code=(
                    "policy_"
                    f"{evaluation.decision.code.value.lower()}"
                ),
                detail=(
                    "canonical policy evaluation blocked "
                    "legacy TASK_DISPATCH projection"
                ),
                snapshot=snapshot,
                receipt_probe=receipt_probe,
            )

        assert projection_record is not None
        assert projection_receipt is not None
        if validate_exact_head:
            await self._validate_exact_head(
                projection_record,
                projection_receipt,
            )

        projected_input = self._projection_input(
            normalized,
            projection_record,
        )
        try:
            result = await self.legacy_dispatch(
                projected_input
            )
        except Exception as exc:
            raise TaskDispatchProjectionPendingError(
                detail=str(exc),
            ) from exc

        if not bool(getattr(result, "committed", False)):
            status = str(
                getattr(result, "status", "") or ""
            )
            reason = str(
                getattr(result, "reason", "") or ""
            )
            raise TaskDispatchProjectionPendingError(
                status=status or "canonical_projection_pending",
                detail=reason,
                retry_safe=status not in {
                    "canonical_projection_conflict",
                    "canonical_projection_regressed",
                    "canonical_projection_input_invalid",
                },
            )
        return result

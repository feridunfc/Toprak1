"Canonical TASK_CLAIM authority command contract."

from __future__ import annotations

import math
import re
from dataclasses import dataclass, replace
from typing import Any

from hfa.authority import (
    AggregateType,
    AuthorityCommand,
    AuthorityEntryContext,
    CanonicalAggregateIdentity,
    OperationType,
)

FEATURE_FLAG = "HFA_CANONICAL_TASK_CLAIM_BINDING"
WRITER_ID = "hfa-worker/task-claim-writer:v1"

TASK_CLAIM_PROJECTED_STATUS = "task_claimed"
TASK_CLAIM_DUPLICATE_STATUS = "canonical_claim_already_projected"

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
        "dispatch_record_hash": _required_text(
            claim.dispatch_record_hash,
            "dispatch_record_hash",
        ),
        "dispatch_command_hash": _required_text(
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

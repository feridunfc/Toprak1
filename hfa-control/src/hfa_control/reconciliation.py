"""Read-only canonical/runtime reconciliation primitives for Sprint 85.0A.

The module intentionally owns no lifecycle authority and no repair capability.
It reads already-durable canonical TASK truth plus mutable runtime evidence and
returns deterministic findings.  The first implemented vertical slice is a
TASK aggregate whose stable current canonical head is TASK_REQUEUE.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping, Protocol, Sequence

from hfa.authority import (
    AggregateType,
    CanonicalAggregateIdentity,
    OperationType,
    RedisCanonicalAuthorityStore,
    canonical_json_bytes,
)
from hfa.authority.canonical_transition import OPERATION_CONTRACTS
from hfa.authority.redis_persistence import (
    RedisAuthorityCorruptionError,
    RedisAuthorityPersistenceError,
)
from hfa.dag.schema import DagRedisKey


TASK_REQUEUE_CURRENT_CONTRACT_ID = "TASK_REQUEUE_CURRENT_PROJECTION"
TASK_REQUEUE_HISTORICAL_CONTRACT_ID = "TASK_REQUEUE_HISTORICAL_DURABLE_EFFECT"
TASK_REQUEUE_CONTRACT_VERSION = 1


class ReconciliationStatus(str, Enum):
    CONSISTENT = "CONSISTENT"
    DRIFT = "DRIFT"
    BLOCKED_EVIDENCE = "BLOCKED_EVIDENCE"


class ReconciliationSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class ReconciliationCheckClass(str, Enum):
    CURRENT_PROJECTION = "CURRENT_PROJECTION"
    HISTORICAL_DURABLE_EFFECT = "HISTORICAL_DURABLE_EFFECT"
    CROSS_AGGREGATE_INVARIANT = "CROSS_AGGREGATE_INVARIANT"


class ReconciliationReason(str, Enum):
    CONSISTENT = "CONSISTENT"
    CANONICAL_EVIDENCE_UNAVAILABLE = "CANONICAL_EVIDENCE_UNAVAILABLE"
    CANONICAL_RECORD_CORRUPTION = "CANONICAL_RECORD_CORRUPTION"
    CANONICAL_CHANGED_DURING_OBSERVATION = "CANONICAL_CHANGED_DURING_OBSERVATION"
    CANONICAL_HISTORY_INCOMPLETE = "CANONICAL_HISTORY_INCOMPLETE"
    RESOURCE_PROOF_MISMATCH = "RESOURCE_PROOF_MISMATCH"
    RESOURCE_FINALIZATION_PENDING = "RESOURCE_FINALIZATION_PENDING"
    RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE = "RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE"
    DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION = "DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION"
    PROJECTION_EVIDENCE_UNAVAILABLE = "PROJECTION_EVIDENCE_UNAVAILABLE"
    PROJECTION_OBSERVATION_CHANGED = "PROJECTION_OBSERVATION_CHANGED"
    PROJECTION_SCHEMA_MISMATCH = "PROJECTION_SCHEMA_MISMATCH"
    TASK_STATE_MISMATCH = "TASK_STATE_MISMATCH"
    TASK_META_IDENTITY_MISMATCH = "TASK_META_IDENTITY_MISMATCH"
    CANONICAL_PROOF_MISMATCH = "CANONICAL_PROOF_MISMATCH"
    REQUEUE_PROOF_MISMATCH = "REQUEUE_PROOF_MISMATCH"
    CLAIM_PREDECESSOR_PROOF_MISMATCH = "CLAIM_PREDECESSOR_PROOF_MISMATCH"
    REQUEUE_COUNT_MISMATCH = "REQUEUE_COUNT_MISMATCH"
    REQUEUE_REASON_MISMATCH = "REQUEUE_REASON_MISMATCH"
    REQUEUE_TIMESTAMP_MISMATCH = "REQUEUE_TIMESTAMP_MISMATCH"
    WORKER_OWNERSHIP_NOT_CLEARED = "WORKER_OWNERSHIP_NOT_CLEARED"
    HEARTBEAT_NOT_CLEARED = "HEARTBEAT_NOT_CLEARED"
    RUNNING_INDEX_MEMBERSHIP_PRESENT = "RUNNING_INDEX_MEMBERSHIP_PRESENT"
    READY_QUEUE_MEMBERSHIP_MISSING = "READY_QUEUE_MEMBERSHIP_MISSING"
    READY_QUEUE_SCORE_MISMATCH = "READY_QUEUE_SCORE_MISMATCH"
    REQUEUE_DELIVERY_PROOF_MISSING = "REQUEUE_DELIVERY_PROOF_MISSING"
    REQUEUE_DELIVERY_PROOF_MISMATCH = "REQUEUE_DELIVERY_PROOF_MISMATCH"
    RUN_STATE_MISMATCH = "RUN_STATE_MISMATCH"
    PROJECTION_VALUE_MISMATCH = "PROJECTION_VALUE_MISMATCH"
    PROJECTION_MEMBERSHIP_MISMATCH = "PROJECTION_MEMBERSHIP_MISMATCH"
    PROJECTION_SCORE_MISMATCH = "PROJECTION_SCORE_MISMATCH"
    TASK_OUTPUT_MISMATCH = "TASK_OUTPUT_MISMATCH"
    RUN_META_MISMATCH = "RUN_META_MISMATCH"
    RUN_RESULT_MISMATCH = "RUN_RESULT_MISMATCH"
    OWNERSHIP_PROJECTION_MISMATCH = "OWNERSHIP_PROJECTION_MISMATCH"
    DEPENDENCY_FANOUT_EVIDENCE_REQUIRED = "DEPENDENCY_FANOUT_EVIDENCE_REQUIRED"


Scalar = str | int | float | bool | None
FrozenFields = tuple[tuple[str, Scalar], ...]


def _freeze_fields(values: Mapping[str, Scalar] | None = None) -> FrozenFields:
    if not values:
        return ()
    return tuple(sorted((str(key), value) for key, value in values.items()))


def _fields_dict(values: FrozenFields) -> dict[str, Scalar]:
    return {key: value for key, value in values}


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return "" if value is None else str(value)


def _optional_int(value: Any) -> int | None:
    text = _text(value)
    if not text or not text.isdecimal():
        return None
    return int(text)


def _intent_dicts(value: Sequence[Any]) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError("canonical projection intent must be a mapping")
        result.append({str(key): raw for key, raw in item.items()})
    return tuple(result)


@dataclass(frozen=True)
class ReconciliationCanonicalReference:
    revision: int | None
    operation_type: str | None
    state: str | None
    transition_id: str | None
    record_hash: str | None
    command_hash: str | None
    operation_id: str | None
    committed_at_ms: int | None

    def to_dict(self) -> dict[str, Scalar]:
        return {
            "revision": self.revision,
            "operation_type": self.operation_type,
            "state": self.state,
            "transition_id": self.transition_id,
            "record_hash": self.record_hash,
            "command_hash": self.command_hash,
            "operation_id": self.operation_id,
            "committed_at_ms": self.committed_at_ms,
        }


@dataclass(frozen=True)
class ReconciliationEvidenceState:
    canonical_proven: bool
    canonical_stable: bool
    projection_read_complete: bool
    projection_observation_stable: bool
    refs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "canonical_proven": self.canonical_proven,
            "canonical_stable": self.canonical_stable,
            "projection_read_complete": self.projection_read_complete,
            "projection_observation_stable": self.projection_observation_stable,
            "refs": list(self.refs),
        }


@dataclass(frozen=True)
class ReconciliationObservation:
    started_at_ms: int
    completed_at_ms: int

    def to_dict(self) -> dict[str, int]:
        return {
            "started_at_ms": self.started_at_ms,
            "completed_at_ms": self.completed_at_ms,
        }


@dataclass(frozen=True)
class ReconciliationFinding:
    aggregate_type: str
    run_id: str
    task_id: str | None
    check_class: ReconciliationCheckClass
    contract_id: str
    contract_version: int
    status: ReconciliationStatus
    severity: ReconciliationSeverity
    reason_code: ReconciliationReason
    canonical: ReconciliationCanonicalReference
    expected: FrozenFields
    observed: FrozenFields
    evidence: ReconciliationEvidenceState
    observation: ReconciliationObservation
    mutation_attempted: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregate_type": self.aggregate_type,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "check_class": self.check_class.value,
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "status": self.status.value,
            "severity": self.severity.value,
            "reason_code": self.reason_code.value,
            "canonical": self.canonical.to_dict(),
            "expected": _fields_dict(self.expected),
            "observed": _fields_dict(self.observed),
            "evidence": self.evidence.to_dict(),
            "observation": self.observation.to_dict(),
            "mutation_attempted": self.mutation_attempted,
        }

    def semantic_key(self) -> tuple[Any, ...]:
        """Deterministic equality key excluding informational wall-clock bounds."""
        return (
            self.aggregate_type,
            self.run_id,
            self.task_id,
            self.check_class.value,
            self.contract_id,
            self.contract_version,
            self.status.value,
            self.severity.value,
            self.reason_code.value,
            self.canonical.revision,
            self.canonical.operation_type,
            self.canonical.state,
            self.canonical.transition_id,
            self.canonical.record_hash,
            self.canonical.command_hash,
            self.canonical.operation_id,
            self.canonical.committed_at_ms,
            self.expected,
            self.observed,
            self.evidence,
            self.mutation_attempted,
        )


@dataclass(frozen=True)
class TaskRequeueCanonicalEvidence:
    task_id: str
    run_id: str
    tenant_id: str
    revision: int
    transition_id: str
    record_hash: str
    command_hash: str
    operation_id: str
    committed_at_ms: int
    reason_code: str
    retry_attempt: int
    claim_epoch: int
    dispatch_attempt: int
    claim_transition_id: str
    claim_record_hash: str
    claim_command_hash: str
    claim_revision: int
    claim_operation_id: str
    projection_intents_json: str

    @property
    def canonical_reference(self) -> ReconciliationCanonicalReference:
        return ReconciliationCanonicalReference(
            revision=self.revision,
            operation_type=OperationType.TASK_REQUEUE.value,
            state="ready",
            transition_id=self.transition_id,
            record_hash=self.record_hash,
            command_hash=self.command_hash,
            operation_id=self.operation_id,
            committed_at_ms=self.committed_at_ms,
        )

    @property
    def fingerprint(self) -> tuple[Any, ...]:
        return (
            self.revision,
            "ready",
            self.transition_id,
            self.record_hash,
            self.command_hash,
            self.operation_id,
        )


_RUNTIME_META_FIELDS = (
    "task_id",
    "run_id",
    "tenant_id",
    "requeue_count",
    "last_requeue_reason",
    "last_requeue_at_ms",
    "worker_instance_id",
    "scheduler_epoch",
    "last_heartbeat_at_ms",
    "canonical_transition_id",
    "canonical_record_hash",
    "canonical_command_hash",
    "canonical_revision",
    "canonical_operation_id",
    "requeue_canonical_transition_id",
    "requeue_canonical_record_hash",
    "requeue_canonical_command_hash",
    "requeue_canonical_revision",
    "requeue_canonical_operation_id",
    "requeue_notification_operation_id",
)


@dataclass(frozen=True)
class TaskRequeueRuntimeEvidence:
    task_state_type: str
    task_state: str | None
    task_meta_type: str
    task_meta: tuple[tuple[str, str | None], ...]
    ready_queue_type: str
    ready_score: float | None
    running_index_type: str
    running_score: float | None

    def meta(self, field: str) -> str | None:
        return dict(self.task_meta).get(field)

    @property
    def fingerprint(self) -> tuple[Any, ...]:
        return (
            self.task_state_type,
            self.task_state,
            self.task_meta_type,
            self.task_meta,
            self.ready_queue_type,
            self.ready_score,
            self.running_index_type,
            self.running_score,
        )


class TaskRequeueCanonicalReader(Protocol):
    async def read_task_requeue_head(
        self, *, task_id: str, run_id: str
    ) -> TaskRequeueCanonicalEvidence: ...


class TaskRequeueRuntimeReader(Protocol):
    async def read_task_requeue_projection(
        self, *, task_id: str, tenant_id: str
    ) -> TaskRequeueRuntimeEvidence: ...


class ReconciliationEvidenceError(RuntimeError):
    def __init__(
        self,
        *,
        reason: ReconciliationReason,
        detail: str,
        canonical_fingerprint: tuple[Any, ...] | None = None,
    ) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
        self.canonical_fingerprint = canonical_fingerprint


class RedisCanonicalReconciliationReader:
    """Read-only facade over accepted canonical storage parsing/validation.

    The wrapped store is commit-capable in the wider product, but this facade
    exposes no commit/initialise/conflict-recording methods to the reconciler.
    It uses only ``get_aggregate_snapshot`` and ``load_receipt_probe``.
    """

    def __init__(self, redis: Any, *, namespace: str = "hfa:authority:v1") -> None:
        self.__store = RedisCanonicalAuthorityStore(redis, namespace=namespace)

    @staticmethod
    def _record_receipt_checks(identity: CanonicalAggregateIdentity, probe: Any) -> tuple[Any, Any]:
        if probe is None or probe.canonical_store_record is None:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="canonical operation record/receipt is missing",
            )
        record = probe.canonical_store_record
        receipt = probe.receipt
        if not all(
            (
                record.aggregate_identity.sha256 == identity.sha256,
                record.aggregate_identity_sha256 == identity.sha256,
                receipt.operation_id == record.operation_id,
                receipt.transition_id == record.transition_id,
                receipt.canonical_command_hash == record.canonical_command_hash,
                receipt.canonical_record_hash == record.canonical_record_hash,
                receipt.aggregate_revision == record.to_revision,
                receipt.operation_type == record.operation_type,
                receipt.committed_at_ms == record.committed_at_ms,
                bool(record.verify_hash()),
            )
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="canonical record/receipt continuity mismatch",
            )
        return record, receipt

    async def _snapshot(self, identity: CanonicalAggregateIdentity) -> Any:
        try:
            return await self.__store.get_aggregate_snapshot(identity)
        except RedisAuthorityCorruptionError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=str(exc),
            ) from exc
        except RedisAuthorityPersistenceError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=str(exc),
            ) from exc
        except Exception as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=f"canonical read failed: {exc}",
            ) from exc

    async def _probe(self, identity: CanonicalAggregateIdentity, operation_id: str) -> Any:
        try:
            return await self.__store.load_receipt_probe(identity, operation_id)
        except RedisAuthorityCorruptionError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=str(exc),
            ) from exc
        except RedisAuthorityPersistenceError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=str(exc),
            ) from exc
        except Exception as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=f"canonical proof read failed: {exc}",
            ) from exc

    async def read_task_requeue_head(
        self, *, task_id: str, run_id: str
    ) -> TaskRequeueCanonicalEvidence:
        identity = CanonicalAggregateIdentity(
            aggregate_type=AggregateType.TASK,
            run_id=run_id,
            task_id=task_id,
        )
        snapshot_a = await self._snapshot(identity)
        if snapshot_a is None:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="canonical TASK aggregate is missing",
            )
        head_fingerprint = (
            snapshot_a.revision,
            snapshot_a.state,
            snapshot_a.transition_id,
            snapshot_a.canonical_record_hash,
            snapshot_a.canonical_command_hash,
            snapshot_a.operation_id,
        )
        probe = await self._probe(identity, snapshot_a.operation_id)
        try:
            record, receipt = self._record_receipt_checks(identity, probe)
        except ReconciliationEvidenceError as exc:
            exc.canonical_fingerprint = head_fingerprint
            raise
        if record.operation_type != OperationType.TASK_REQUEUE.value:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=f"current canonical TASK head is {record.operation_type}, not TASK_REQUEUE",
                canonical_fingerprint=head_fingerprint,
            )

        metadata = record.authoritative_metadata_changes
        if not isinstance(metadata, Mapping):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="TASK_REQUEUE authoritative metadata is not a mapping",
                canonical_fingerprint=head_fingerprint,
            )
        try:
            tenant_id = str(metadata["tenant_id"])
            reason_code = str(metadata["reason_code"])
            retry_attempt = int(metadata["retry_attempt"])
            claim_epoch = int(metadata["claim_epoch"])
            dispatch_attempt = int(metadata["dispatch_attempt"])
            claim_transition_id = str(metadata["claim_transition_id"])
            claim_record_hash = str(metadata["claim_record_hash"])
            claim_command_hash = str(metadata["claim_command_hash"])
            claim_revision = int(metadata["claim_revision"])
            claim_operation_id = str(metadata["claim_operation_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=f"TASK_REQUEUE authoritative metadata is incomplete: {exc}",
                canonical_fingerprint=head_fingerprint,
            ) from exc

        requeue_contract = OPERATION_CONTRACTS[OperationType.TASK_REQUEUE]
        try:
            intents = _intent_dicts(record.durable_projection_intents)
        except ValueError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=str(exc),
                canonical_fingerprint=head_fingerprint,
            ) from exc
        intent_kinds = frozenset(str(item.get("kind", "")) for item in intents)
        expected_intents = (
            {
                "kind": "READY_QUEUE",
                "tenant_id": tenant_id,
                "task_id": task_id,
                "retry_attempt": retry_attempt,
            },
            {
                "kind": "REQUEUE_NOTIFICATION",
                "tenant_id": tenant_id,
                "task_id": task_id,
                "reason_code": reason_code,
                "retry_attempt": retry_attempt,
            },
        )
        expected_claim_operation_id = (
            f"task-claim:v1:{identity.sha256}:attempt:{dispatch_attempt}"
        )
        expected_requeue_operation_id = (
            f"task-requeue:v1:{identity.sha256}:claim:{claim_epoch}"
        )
        expected_intents_json = canonical_json_bytes(record.durable_projection_intents).decode("utf-8")
        head_checks = (
            record.previous_state == "running",
            record.next_state == "ready",
            record.from_revision == claim_revision,
            record.to_revision == snapshot_a.revision,
            record.transition_id == snapshot_a.transition_id,
            record.canonical_record_hash == snapshot_a.canonical_record_hash,
            record.canonical_command_hash == snapshot_a.canonical_command_hash,
            record.operation_id == snapshot_a.operation_id == expected_requeue_operation_id,
            record.causation_id == claim_transition_id,
            snapshot_a.state == "ready",
            snapshot_a.projection_intents_json == expected_intents_json,
            snapshot_a.updated_at_ms == record.committed_at_ms,
            receipt.aggregate_revision == snapshot_a.revision,
            metadata.get("task_id") == task_id,
            metadata.get("run_id") == run_id,
            claim_operation_id == expected_claim_operation_id,
            retry_attempt == dispatch_attempt,
            requeue_contract.required_projection_intents.issubset(intent_kinds),
            intent_kinds == frozenset({"READY_QUEUE", "REQUEUE_NOTIFICATION"}),
            canonical_json_bytes(record.durable_projection_intents)
            == canonical_json_bytes(expected_intents),
        )
        if not all(head_checks):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="TASK_REQUEUE canonical head/record/receipt contract mismatch",
                canonical_fingerprint=head_fingerprint,
            )

        claim_probe = await self._probe(identity, claim_operation_id)
        try:
            claim_record, _claim_receipt = self._record_receipt_checks(identity, claim_probe)
        except ReconciliationEvidenceError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH,
                detail=exc.detail,
                canonical_fingerprint=head_fingerprint,
            ) from exc
        claim_metadata = claim_record.authoritative_metadata_changes
        predecessor_checks = (
            claim_record.operation_type == OperationType.TASK_CLAIM.value,
            claim_record.operation_id == claim_operation_id,
            claim_record.transition_id == claim_transition_id,
            claim_record.canonical_record_hash == claim_record_hash,
            claim_record.canonical_command_hash == claim_command_hash,
            claim_record.to_revision == claim_revision == record.from_revision,
            claim_record.next_state == "running",
            isinstance(claim_metadata, Mapping),
            isinstance(claim_metadata, Mapping)
            and claim_metadata.get("dispatch_attempt") == dispatch_attempt,
            isinstance(claim_metadata, Mapping)
            and claim_metadata.get("claim_epoch") == claim_epoch,
        )
        if not all(predecessor_checks):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH,
                detail="durable TASK_CLAIM predecessor proof mismatch",
                canonical_fingerprint=head_fingerprint,
            )

        snapshot_b = await self._snapshot(identity)
        if snapshot_b is None:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical TASK aggregate disappeared during evidence read",
                canonical_fingerprint=None,
            )
        fingerprint_b = (
            snapshot_b.revision,
            snapshot_b.state,
            snapshot_b.transition_id,
            snapshot_b.canonical_record_hash,
            snapshot_b.canonical_command_hash,
            snapshot_b.operation_id,
        )
        if fingerprint_b != head_fingerprint:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical TASK head changed during evidence read",
                canonical_fingerprint=fingerprint_b,
            )

        return TaskRequeueCanonicalEvidence(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            revision=record.to_revision,
            transition_id=record.transition_id,
            record_hash=record.canonical_record_hash,
            command_hash=record.canonical_command_hash,
            operation_id=record.operation_id,
            committed_at_ms=record.committed_at_ms,
            reason_code=reason_code,
            retry_attempt=retry_attempt,
            claim_epoch=claim_epoch,
            dispatch_attempt=dispatch_attempt,
            claim_transition_id=claim_transition_id,
            claim_record_hash=claim_record_hash,
            claim_command_hash=claim_command_hash,
            claim_revision=claim_revision,
            claim_operation_id=claim_operation_id,
            projection_intents_json=expected_intents_json,
        )


class ReconciliationRedisReader:
    """Small runtime read capability used by reconciliation only."""

    def __init__(self, redis: Any) -> None:
        self.__redis = redis

    async def _type(self, key: str) -> str:
        return _text(await self.__redis.type(key))

    async def read_task_requeue_projection(
        self, *, task_id: str, tenant_id: str
    ) -> TaskRequeueRuntimeEvidence:
        try:
            state_key = DagRedisKey.task_state(task_id)
            meta_key = DagRedisKey.task_meta(task_id)
            ready_key = DagRedisKey.tenant_ready_queue(tenant_id)
            running_key = DagRedisKey.task_running_zset(tenant_id)

            state_type = await self._type(state_key)
            state = _text(await self.__redis.get(state_key)) if state_type == "string" else None

            meta_type = await self._type(meta_key)
            if meta_type == "hash":
                raw_meta = await self.__redis.hmget(meta_key, *_RUNTIME_META_FIELDS)
                meta = tuple(
                    (field, None if raw is None else _text(raw))
                    for field, raw in zip(_RUNTIME_META_FIELDS, raw_meta)
                )
            else:
                meta = tuple((field, None) for field in _RUNTIME_META_FIELDS)

            ready_type = await self._type(ready_key)
            ready_score = (
                await self.__redis.zscore(ready_key, task_id)
                if ready_type == "zset"
                else None
            )
            running_type = await self._type(running_key)
            running_score = (
                await self.__redis.zscore(running_key, task_id)
                if running_type == "zset"
                else None
            )
            return TaskRequeueRuntimeEvidence(
                task_state_type=state_type,
                task_state=state,
                task_meta_type=meta_type,
                task_meta=meta,
                ready_queue_type=ready_type,
                ready_score=None if ready_score is None else float(ready_score),
                running_index_type=running_type,
                running_score=None if running_score is None else float(running_score),
            )
        except Exception as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.PROJECTION_EVIDENCE_UNAVAILABLE,
                detail=f"runtime projection read failed: {exc}",
            ) from exc


class TaskRequeueReconciler:
    """Read-only current-head + durable-effect verifier for TASK_REQUEUE."""

    def __init__(
        self,
        *,
        canonical_reader: TaskRequeueCanonicalReader,
        runtime_reader: TaskRequeueRuntimeReader,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.__canonical_reader = canonical_reader
        self.__runtime_reader = runtime_reader
        self.__clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    @staticmethod
    def _sort(findings: list[ReconciliationFinding]) -> tuple[ReconciliationFinding, ...]:
        return tuple(
            sorted(
                findings,
                key=lambda f: (
                    f.aggregate_type,
                    f.run_id,
                    f.task_id or "",
                    f.check_class.value,
                    f.reason_code.value,
                ),
            )
        )

    @staticmethod
    def _finding(
        *,
        canonical: TaskRequeueCanonicalEvidence | None,
        task_id: str,
        run_id: str,
        check_class: ReconciliationCheckClass,
        status: ReconciliationStatus,
        severity: ReconciliationSeverity,
        reason: ReconciliationReason,
        expected: Mapping[str, Scalar] | None,
        observed: Mapping[str, Scalar] | None,
        evidence: ReconciliationEvidenceState,
        observation: ReconciliationObservation,
    ) -> ReconciliationFinding:
        contract_id = (
            TASK_REQUEUE_CURRENT_CONTRACT_ID
            if check_class is ReconciliationCheckClass.CURRENT_PROJECTION
            else TASK_REQUEUE_HISTORICAL_CONTRACT_ID
        )
        reference = (
            canonical.canonical_reference
            if canonical is not None
            else ReconciliationCanonicalReference(
                revision=None,
                operation_type=None,
                state=None,
                transition_id=None,
                record_hash=None,
                command_hash=None,
                operation_id=None,
                committed_at_ms=None,
            )
        )
        return ReconciliationFinding(
            aggregate_type=AggregateType.TASK.value,
            run_id=run_id,
            task_id=task_id,
            check_class=check_class,
            contract_id=contract_id,
            contract_version=TASK_REQUEUE_CONTRACT_VERSION,
            status=status,
            severity=severity,
            reason_code=reason,
            canonical=reference,
            expected=_freeze_fields(expected),
            observed=_freeze_fields(observed),
            evidence=evidence,
            observation=observation,
            mutation_attempted=False,
        )

    def _blocked_pair(
        self,
        *,
        canonical: TaskRequeueCanonicalEvidence | None,
        task_id: str,
        run_id: str,
        reason: ReconciliationReason,
        detail: str,
        canonical_proven: bool,
        canonical_stable: bool,
        projection_read_complete: bool,
        projection_stable: bool,
        observation: ReconciliationObservation,
    ) -> tuple[ReconciliationFinding, ...]:
        evidence = ReconciliationEvidenceState(
            canonical_proven=canonical_proven,
            canonical_stable=canonical_stable,
            projection_read_complete=projection_read_complete,
            projection_observation_stable=projection_stable,
            refs=(detail,),
        )
        findings = [
            self._finding(
                canonical=canonical,
                task_id=task_id,
                run_id=run_id,
                check_class=check_class,
                status=ReconciliationStatus.BLOCKED_EVIDENCE,
                severity=ReconciliationSeverity.CRITICAL,
                reason=reason,
                expected=None,
                observed={"detail": detail},
                evidence=evidence,
                observation=observation,
            )
            for check_class in (
                ReconciliationCheckClass.CURRENT_PROJECTION,
                ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT,
            )
        ]
        return self._sort(findings)

    async def reconcile(
        self, *, task_id: str, run_id: str
    ) -> tuple[ReconciliationFinding, ...]:
        started = int(self.__clock_ms())
        try:
            canonical_a = await self.__canonical_reader.read_task_requeue_head(
                task_id=task_id, run_id=run_id
            )
        except ReconciliationEvidenceError as exc:
            observation = ReconciliationObservation(started, int(self.__clock_ms()))
            return self._blocked_pair(
                canonical=None,
                task_id=task_id,
                run_id=run_id,
                reason=exc.reason,
                detail=exc.detail,
                canonical_proven=False,
                canonical_stable=False,
                projection_read_complete=False,
                projection_stable=False,
                observation=observation,
            )

        try:
            runtime_a = await self.__runtime_reader.read_task_requeue_projection(
                task_id=task_id, tenant_id=canonical_a.tenant_id
            )
            runtime_b = await self.__runtime_reader.read_task_requeue_projection(
                task_id=task_id, tenant_id=canonical_a.tenant_id
            )
        except ReconciliationEvidenceError as exc:
            observation = ReconciliationObservation(started, int(self.__clock_ms()))
            return self._blocked_pair(
                canonical=canonical_a,
                task_id=task_id,
                run_id=run_id,
                reason=exc.reason,
                detail=exc.detail,
                canonical_proven=True,
                canonical_stable=False,
                projection_read_complete=False,
                projection_stable=False,
                observation=observation,
            )

        try:
            canonical_b = await self.__canonical_reader.read_task_requeue_head(
                task_id=task_id, run_id=run_id
            )
        except ReconciliationEvidenceError as exc:
            observation = ReconciliationObservation(started, int(self.__clock_ms()))
            changed = (
                exc.canonical_fingerprint is not None
                and exc.canonical_fingerprint != canonical_a.fingerprint
            )
            return self._blocked_pair(
                canonical=canonical_a,
                task_id=task_id,
                run_id=run_id,
                reason=(
                    ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION
                    if changed
                    else exc.reason
                ),
                detail=(
                    "canonical TASK head changed between observation boundaries"
                    if changed
                    else exc.detail
                ),
                canonical_proven=True,
                canonical_stable=False,
                projection_read_complete=True,
                projection_stable=runtime_a.fingerprint == runtime_b.fingerprint,
                observation=observation,
            )

        observation = ReconciliationObservation(started, int(self.__clock_ms()))
        if canonical_a.fingerprint != canonical_b.fingerprint:
            return self._blocked_pair(
                canonical=canonical_a,
                task_id=task_id,
                run_id=run_id,
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical TASK head changed between observation boundaries",
                canonical_proven=True,
                canonical_stable=False,
                projection_read_complete=True,
                projection_stable=runtime_a.fingerprint == runtime_b.fingerprint,
                observation=observation,
            )
        if runtime_a.fingerprint != runtime_b.fingerprint:
            return self._blocked_pair(
                canonical=canonical_b,
                task_id=task_id,
                run_id=run_id,
                reason=ReconciliationReason.PROJECTION_OBSERVATION_CHANGED,
                detail="runtime projection changed between read-only fingerprints",
                canonical_proven=True,
                canonical_stable=True,
                projection_read_complete=True,
                projection_stable=False,
                observation=observation,
            )

        evidence = ReconciliationEvidenceState(
            canonical_proven=True,
            canonical_stable=True,
            projection_read_complete=True,
            projection_observation_stable=True,
            refs=(
                f"canonical:{canonical_b.transition_id}",
                f"operation:{canonical_b.operation_id}",
            ),
        )
        findings = self._compare_current(
            canonical=canonical_b,
            runtime=runtime_b,
            evidence=evidence,
            observation=observation,
        )
        findings.extend(
            self._compare_historical(
                canonical=canonical_b,
                runtime=runtime_b,
                evidence=evidence,
                observation=observation,
            )
        )
        return self._sort(findings)

    def _compare_current(
        self,
        *,
        canonical: TaskRequeueCanonicalEvidence,
        runtime: TaskRequeueRuntimeEvidence,
        evidence: ReconciliationEvidenceState,
        observation: ReconciliationObservation,
    ) -> list[ReconciliationFinding]:
        drifts: list[ReconciliationFinding] = []

        def add(
            reason: ReconciliationReason,
            severity: ReconciliationSeverity,
            expected: Mapping[str, Scalar],
            observed: Mapping[str, Scalar],
        ) -> None:
            drifts.append(
                self._finding(
                    canonical=canonical,
                    task_id=canonical.task_id,
                    run_id=canonical.run_id,
                    check_class=ReconciliationCheckClass.CURRENT_PROJECTION,
                    status=ReconciliationStatus.DRIFT,
                    severity=severity,
                    reason=reason,
                    expected=expected,
                    observed=observed,
                    evidence=evidence,
                    observation=observation,
                )
            )

        schema_mismatches: list[tuple[str, str, str]] = []
        if runtime.task_state_type not in {"string"}:
            schema_mismatches.append(("task_state", "string", runtime.task_state_type))
        if runtime.task_meta_type not in {"hash"}:
            schema_mismatches.append(("task_meta", "hash", runtime.task_meta_type))
        if runtime.ready_queue_type not in {"none", "zset"}:
            schema_mismatches.append(("ready_queue", "zset", runtime.ready_queue_type))
        if runtime.running_index_type not in {"none", "zset"}:
            schema_mismatches.append(("running_index", "zset_or_absent", runtime.running_index_type))
        for surface, expected_type, actual_type in schema_mismatches:
            add(
                ReconciliationReason.PROJECTION_SCHEMA_MISMATCH,
                ReconciliationSeverity.CRITICAL,
                {"surface": surface, "redis_type": expected_type},
                {"surface": surface, "redis_type": actual_type},
            )

        if runtime.task_state_type == "string" and runtime.task_state != "ready":
            add(
                ReconciliationReason.TASK_STATE_MISMATCH,
                ReconciliationSeverity.CRITICAL,
                {"task_state": "ready"},
                {"task_state": runtime.task_state},
            )

        if runtime.task_meta_type == "hash":
            identity_expected = {
                "task_id": canonical.task_id,
                "run_id": canonical.run_id,
                "tenant_id": canonical.tenant_id,
            }
            identity_observed = {key: runtime.meta(key) for key in identity_expected}
            if identity_observed != identity_expected:
                add(
                    ReconciliationReason.TASK_META_IDENTITY_MISMATCH,
                    ReconciliationSeverity.CRITICAL,
                    identity_expected,
                    identity_observed,
                )

            generic_expected = {
                "canonical_transition_id": canonical.transition_id,
                "canonical_record_hash": canonical.record_hash,
                "canonical_command_hash": canonical.command_hash,
                "canonical_revision": str(canonical.revision),
                "canonical_operation_id": canonical.operation_id,
            }
            generic_observed = {key: runtime.meta(key) for key in generic_expected}
            if generic_observed != generic_expected:
                add(
                    ReconciliationReason.CANONICAL_PROOF_MISMATCH,
                    ReconciliationSeverity.CRITICAL,
                    generic_expected,
                    generic_observed,
                )

            requeue_expected = {
                "requeue_canonical_transition_id": canonical.transition_id,
                "requeue_canonical_record_hash": canonical.record_hash,
                "requeue_canonical_command_hash": canonical.command_hash,
                "requeue_canonical_revision": str(canonical.revision),
                "requeue_canonical_operation_id": canonical.operation_id,
            }
            requeue_observed = {key: runtime.meta(key) for key in requeue_expected}
            if requeue_observed != requeue_expected:
                add(
                    ReconciliationReason.REQUEUE_PROOF_MISMATCH,
                    ReconciliationSeverity.CRITICAL,
                    requeue_expected,
                    requeue_observed,
                )

            count = _optional_int(runtime.meta("requeue_count"))
            if count != canonical.retry_attempt:
                add(
                    ReconciliationReason.REQUEUE_COUNT_MISMATCH,
                    ReconciliationSeverity.CRITICAL,
                    {"requeue_count": canonical.retry_attempt},
                    {"requeue_count": runtime.meta("requeue_count")},
                )
            if runtime.meta("last_requeue_reason") != canonical.reason_code:
                add(
                    ReconciliationReason.REQUEUE_REASON_MISMATCH,
                    ReconciliationSeverity.WARNING,
                    {"last_requeue_reason": canonical.reason_code},
                    {"last_requeue_reason": runtime.meta("last_requeue_reason")},
                )
            if _optional_int(runtime.meta("last_requeue_at_ms")) != canonical.committed_at_ms:
                add(
                    ReconciliationReason.REQUEUE_TIMESTAMP_MISMATCH,
                    ReconciliationSeverity.WARNING,
                    {"last_requeue_at_ms": canonical.committed_at_ms},
                    {"last_requeue_at_ms": runtime.meta("last_requeue_at_ms")},
                )
            ownership_observed = {
                "worker_instance_id": runtime.meta("worker_instance_id"),
                "scheduler_epoch": runtime.meta("scheduler_epoch"),
            }
            if ownership_observed != {"worker_instance_id": "", "scheduler_epoch": ""}:
                add(
                    ReconciliationReason.WORKER_OWNERSHIP_NOT_CLEARED,
                    ReconciliationSeverity.CRITICAL,
                    {"worker_instance_id": "", "scheduler_epoch": ""},
                    ownership_observed,
                )
            if _optional_int(runtime.meta("last_heartbeat_at_ms")) != 0:
                add(
                    ReconciliationReason.HEARTBEAT_NOT_CLEARED,
                    ReconciliationSeverity.WARNING,
                    {"last_heartbeat_at_ms": 0},
                    {"last_heartbeat_at_ms": runtime.meta("last_heartbeat_at_ms")},
                )

        if runtime.running_index_type in {"none", "zset"} and runtime.running_score is not None:
            add(
                ReconciliationReason.RUNNING_INDEX_MEMBERSHIP_PRESENT,
                ReconciliationSeverity.CRITICAL,
                {"running_membership": "ABSENT"},
                {"running_score": runtime.running_score},
            )
        if runtime.ready_queue_type in {"none", "zset"}:
            if runtime.ready_score is None:
                add(
                    ReconciliationReason.READY_QUEUE_MEMBERSHIP_MISSING,
                    ReconciliationSeverity.CRITICAL,
                    {"ready_membership": "PRESENT"},
                    {"ready_score": None},
                )
            elif runtime.ready_score != float(canonical.committed_at_ms):
                add(
                    ReconciliationReason.READY_QUEUE_SCORE_MISMATCH,
                    ReconciliationSeverity.WARNING,
                    {"ready_score": canonical.committed_at_ms},
                    {"ready_score": runtime.ready_score},
                )

        if not drifts:
            drifts.append(
                self._finding(
                    canonical=canonical,
                    task_id=canonical.task_id,
                    run_id=canonical.run_id,
                    check_class=ReconciliationCheckClass.CURRENT_PROJECTION,
                    status=ReconciliationStatus.CONSISTENT,
                    severity=ReconciliationSeverity.INFO,
                    reason=ReconciliationReason.CONSISTENT,
                    expected={"task_state": "ready", "retry_attempt": canonical.retry_attempt},
                    observed={"task_state": runtime.task_state, "requeue_count": runtime.meta("requeue_count")},
                    evidence=evidence,
                    observation=observation,
                )
            )
        return drifts

    def _compare_historical(
        self,
        *,
        canonical: TaskRequeueCanonicalEvidence,
        runtime: TaskRequeueRuntimeEvidence,
        evidence: ReconciliationEvidenceState,
        observation: ReconciliationObservation,
    ) -> list[ReconciliationFinding]:
        if runtime.task_meta_type != "hash":
            return [
                self._finding(
                    canonical=canonical,
                    task_id=canonical.task_id,
                    run_id=canonical.run_id,
                    check_class=ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT,
                    status=ReconciliationStatus.DRIFT,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.PROJECTION_SCHEMA_MISMATCH,
                    expected={"task_meta_redis_type": "hash"},
                    observed={"task_meta_redis_type": runtime.task_meta_type},
                    evidence=evidence,
                    observation=observation,
                )
            ]
        durable = runtime.meta("requeue_notification_operation_id")
        if not durable:
            status = ReconciliationStatus.DRIFT
            severity = ReconciliationSeverity.WARNING
            reason = ReconciliationReason.REQUEUE_DELIVERY_PROOF_MISSING
        elif durable != canonical.operation_id:
            status = ReconciliationStatus.DRIFT
            severity = ReconciliationSeverity.WARNING
            reason = ReconciliationReason.REQUEUE_DELIVERY_PROOF_MISMATCH
        else:
            status = ReconciliationStatus.CONSISTENT
            severity = ReconciliationSeverity.INFO
            reason = ReconciliationReason.CONSISTENT
        return [
            self._finding(
                canonical=canonical,
                task_id=canonical.task_id,
                run_id=canonical.run_id,
                check_class=ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT,
                status=status,
                severity=severity,
                reason=reason,
                expected={"requeue_notification_operation_id": canonical.operation_id},
                observed={"requeue_notification_operation_id": durable},
                evidence=evidence,
                observation=observation,
            )
        ]

# Sprint 85.0B — explicit current-head reconciliation coverage.
# This block extends the accepted 85.0A primitives above.  It owns no
# authority, repair, replay, delivery or resource-settlement capability.

import hashlib as _reconciliation_hashlib
from dataclasses import dataclass as _reconciliation_dataclass

from hfa.config.keys import RedisKey as _ReconciliationRedisKey
from hfa.events.codec import serialize_event as _reconciliation_serialize_event
from hfa.events.schema import RunAdmittedEvent as _ReconciliationRunAdmittedEvent


CURRENT_HEAD_RECONCILIATION_CONTRACT_VERSION = 1
_CURRENT_HEAD_OPERATIONS = frozenset(
    {
        OperationType.RUN_CREATE,
        OperationType.TASK_ADMIT,
        OperationType.TASK_DISPATCH,
        OperationType.TASK_CLAIM,
        OperationType.TASK_COMPLETE,
        OperationType.TASK_FAIL,
        OperationType.RUN_TERMINATE,
    }
)


@_reconciliation_dataclass(frozen=True)
class ProjectionRule:
    field: str
    expected: Scalar
    reason: ReconciliationReason
    severity: ReconciliationSeverity


@_reconciliation_dataclass(frozen=True)
class CurrentHeadCanonicalEvidence:
    aggregate_type: str
    operation_type: OperationType
    run_id: str
    task_id: str | None
    tenant_id: str
    previous_state: str | None
    state: str
    revision: int
    transition_id: str
    record_hash: str
    command_hash: str
    operation_id: str
    committed_at_ms: int
    rules: tuple[ProjectionRule, ...]
    key_hints: FrozenFields = ()

    @property
    def canonical_reference(self) -> ReconciliationCanonicalReference:
        return ReconciliationCanonicalReference(
            revision=self.revision,
            operation_type=self.operation_type.value,
            state=self.state,
            transition_id=self.transition_id,
            record_hash=self.record_hash,
            command_hash=self.command_hash,
            operation_id=self.operation_id,
            committed_at_ms=self.committed_at_ms,
        )

    @property
    def fingerprint(self) -> tuple[Any, ...]:
        return (
            self.aggregate_type,
            self.operation_type.value,
            self.run_id,
            self.task_id,
            self.previous_state,
            self.state,
            self.revision,
            self.transition_id,
            self.record_hash,
            self.command_hash,
            self.operation_id,
        )

    def hint(self, name: str) -> str:
        raw = dict(self.key_hints).get(name)
        return "" if raw is None else str(raw)


@_reconciliation_dataclass(frozen=True)
class CurrentHeadRuntimeEvidence:
    operation_type: OperationType
    fields: FrozenFields

    def value(self, field: str) -> Scalar:
        return dict(self.fields).get(field)

    @property
    def fingerprint(self) -> tuple[Any, ...]:
        # Operation-specific readers intentionally omit coordination-only values
        # (notably TASK_CLAIM heartbeat timestamps and running ZSET score).
        return (self.operation_type.value, self.fields)


class CurrentHeadCanonicalReader(Protocol):
    async def read_current_head(
        self,
        *,
        operation_type: OperationType,
        run_id: str,
        task_id: str | None,
    ) -> CurrentHeadCanonicalEvidence: ...


class CurrentHeadRuntimeReader(Protocol):
    async def read_current_projection(
        self,
        *,
        canonical: CurrentHeadCanonicalEvidence,
    ) -> CurrentHeadRuntimeEvidence: ...


def _rule(
    field: str,
    expected: Scalar,
    reason: ReconciliationReason,
    severity: ReconciliationSeverity = ReconciliationSeverity.CRITICAL,
) -> ProjectionRule:
    return ProjectionRule(field=field, expected=expected, reason=reason, severity=severity)


def _type_rule(surface: str, expected: str) -> ProjectionRule:
    return _rule(
        f"type.{surface}",
        expected,
        ReconciliationReason.PROJECTION_SCHEMA_MISMATCH,
        ReconciliationSeverity.CRITICAL,
    )


def _proof_rules(
    *,
    transition_id: str,
    record_hash: str,
    command_hash: str,
    revision: int,
    operation_id: str,
    prefix: str = "meta.",
) -> tuple[ProjectionRule, ...]:
    return (
        _rule(f"{prefix}canonical_transition_id", transition_id, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
        _rule(f"{prefix}canonical_record_hash", record_hash, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
        _rule(f"{prefix}canonical_command_hash", command_hash, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
        _rule(f"{prefix}canonical_revision", str(revision), ReconciliationReason.CANONICAL_PROOF_MISMATCH),
        _rule(f"{prefix}canonical_operation_id", operation_id, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
    )


def _metadata(record: Any, *, operation: OperationType) -> Mapping[str, Any]:
    raw = record.authoritative_metadata_changes
    if not isinstance(raw, Mapping):
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail=f"{operation.value} authoritative metadata is not a mapping",
        )
    return raw


def _required_metadata_text(metadata: Mapping[str, Any], field: str, operation: OperationType) -> str:
    value = metadata.get(field)
    if type(value) is not str or not value:
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail=f"{operation.value} metadata field {field} is missing/invalid",
        )
    return value


def _metadata_int(metadata: Mapping[str, Any], field: str, operation: OperationType, minimum: int = 0) -> int:
    value = metadata.get(field)
    if type(value) is bool:
        value = None
    if type(value) is int:
        result = value
    elif type(value) is float and value.is_integer():
        result = int(value)
    elif type(value) is str and value.isdecimal():
        result = int(value)
    else:
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail=f"{operation.value} metadata field {field} is missing/invalid",
        )
    if result < minimum:
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail=f"{operation.value} metadata field {field} is below {minimum}",
        )
    return result


def _require_exact_projection_intents(
    record: Any,
    operation: OperationType,
    expected: Sequence[Mapping[str, Any]],
) -> None:
    try:
        observed = _intent_dicts(record.durable_projection_intents)
    except ValueError as exc:
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail=f"{operation.value} durable projection intents are corrupt: {exc}",
        ) from exc
    if canonical_json_bytes(observed) != canonical_json_bytes(tuple(expected)):
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail=f"{operation.value} durable projection intent contract mismatch",
        )


def _projection_receipt_key(kind: str, operation_id: str) -> str:
    digest = _reconciliation_hashlib.sha256(operation_id.encode("utf-8")).hexdigest()
    return f"{_ReconciliationRedisKey.PREFIX}:{kind}:projection:v1:{digest}"


def _event_id_for_run_terminate(run_id: str, final_state: str, task_count: int) -> str:
    material = f"RUN_TERMINATE\x1f{run_id}\x1f{final_state}\x1f{task_count}"
    return _reconciliation_hashlib.sha1(material.encode("utf-8")).hexdigest()


def _run_create_event_payload_hash(metadata: Mapping[str, Any], operation_id: str) -> str:
    operation = OperationType.RUN_CREATE
    payload = metadata.get("payload")
    if not isinstance(payload, Mapping):
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail="RUN_CREATE payload is not a mapping",
        )
    created_at_ms = _metadata_int(metadata, "created_at_ms", operation)
    event_digest = _reconciliation_hashlib.sha256(
        f"run-admitted-event:v1:{operation_id}".encode("utf-8")
    ).hexdigest()
    seconds = created_at_ms / 1000.0
    event = _ReconciliationRunAdmittedEvent(
        event_id=event_digest,
        timestamp=seconds,
        trace_parent=None,
        trace_state=None,
        run_id=_required_metadata_text(metadata, "run_id", operation),
        tenant_id=_required_metadata_text(metadata, "tenant_id", operation),
        agent_type=_required_metadata_text(metadata, "agent_type", operation),
        priority=_metadata_int(metadata, "priority", operation),
        preferred_region=str(metadata.get("preferred_region", "") or ""),
        preferred_placement=_required_metadata_text(metadata, "preferred_placement", operation),
        payload=dict(payload),
        estimated_cost_cents=_metadata_int(metadata, "estimated_cost_cents", operation),
        admitted_at=seconds,
    )
    fields = _reconciliation_serialize_event(event)
    encoded = canonical_json_bytes(fields)
    return _reconciliation_hashlib.sha256(encoded).hexdigest()


class CurrentHeadCanonicalReconciliationReader(RedisCanonicalReconciliationReader):
    """85.0B extension of the accepted read-only canonical facade.

    Only inherited ``_snapshot`` / ``_probe`` reads are used.  No commit,
    initialise, conflict-recording or head-repair surface is exposed.
    """

    async def _exact_record(
        self,
        *,
        identity: CanonicalAggregateIdentity,
        operation_type: OperationType,
    ) -> tuple[Any, Any]:
        if operation_type not in _CURRENT_HEAD_OPERATIONS or operation_type not in OPERATION_CONTRACTS:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=f"operation {operation_type.value} is outside Sprint 85.0B current-head coverage",
            )
        snapshot_a = await self._snapshot(identity)
        if snapshot_a is None:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="canonical aggregate is missing",
            )
        fingerprint_a = (
            snapshot_a.revision,
            snapshot_a.state,
            snapshot_a.transition_id,
            snapshot_a.canonical_record_hash,
            snapshot_a.canonical_command_hash,
            snapshot_a.operation_id,
        )
        probe = await self._probe(identity, snapshot_a.operation_id)
        try:
            record, _receipt = self._record_receipt_checks(identity, probe)
        except ReconciliationEvidenceError as exc:
            exc.canonical_fingerprint = fingerprint_a
            raise
        if record.operation_type != operation_type.value:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=f"current canonical head is {record.operation_type}, not {operation_type.value}",
                canonical_fingerprint=fingerprint_a,
            )
        checks = (
            record.to_revision == snapshot_a.revision,
            record.next_state == snapshot_a.state,
            record.transition_id == snapshot_a.transition_id,
            record.canonical_record_hash == snapshot_a.canonical_record_hash,
            record.canonical_command_hash == snapshot_a.canonical_command_hash,
            record.operation_id == snapshot_a.operation_id,
            snapshot_a.updated_at_ms == record.committed_at_ms,
            snapshot_a.projection_intents_json
            == canonical_json_bytes(record.durable_projection_intents).decode("utf-8"),
            bool(record.verify_hash()),
        )
        if not all(checks):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=f"{operation_type.value} snapshot/record head continuity mismatch",
                canonical_fingerprint=fingerprint_a,
            )
        snapshot_b = await self._snapshot(identity)
        if snapshot_b is None:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical aggregate disappeared during evidence read",
            )
        fingerprint_b = (
            snapshot_b.revision,
            snapshot_b.state,
            snapshot_b.transition_id,
            snapshot_b.canonical_record_hash,
            snapshot_b.canonical_command_hash,
            snapshot_b.operation_id,
        )
        if fingerprint_a != fingerprint_b:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical head changed during evidence read",
                canonical_fingerprint=fingerprint_b,
            )
        return record, snapshot_b

    @staticmethod
    def _reference_rules(record: Any) -> tuple[ProjectionRule, ...]:
        return _proof_rules(
            transition_id=record.transition_id,
            record_hash=record.canonical_record_hash,
            command_hash=record.canonical_command_hash,
            revision=record.to_revision,
            operation_id=record.operation_id,
        )

    async def _predecessor(
        self,
        *,
        identity: CanonicalAggregateIdentity,
        operation_id: str,
        operation_type: OperationType,
        transition_id: str,
        record_hash: str,
        command_hash: str,
        revision: int,
        next_state: str,
    ) -> Any:
        probe = await self._probe(identity, operation_id)
        try:
            record, _receipt = self._record_receipt_checks(identity, probe)
        except ReconciliationEvidenceError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=f"{operation_type.value} predecessor proof unreadable: {exc.detail}",
            ) from exc
        checks = (
            record.operation_type == operation_type.value,
            record.operation_id == operation_id,
            record.transition_id == transition_id,
            record.canonical_record_hash == record_hash,
            record.canonical_command_hash == command_hash,
            record.to_revision == revision,
            record.next_state == next_state,
        )
        if not all(checks):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=f"{operation_type.value} durable predecessor proof mismatch",
            )
        return record

    async def read_current_head(
        self,
        *,
        operation_type: OperationType,
        run_id: str,
        task_id: str | None,
    ) -> CurrentHeadCanonicalEvidence:
        if operation_type in {OperationType.RUN_CREATE, OperationType.RUN_TERMINATE}:
            identity = CanonicalAggregateIdentity(AggregateType.RUN, run_id, None)
        else:
            if not task_id:
                raise ReconciliationEvidenceError(
                    reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                    detail=f"{operation_type.value} requires task_id",
                )
            identity = CanonicalAggregateIdentity(AggregateType.TASK, run_id, task_id)
        record, _snapshot = await self._exact_record(identity=identity, operation_type=operation_type)
        if operation_type is OperationType.RUN_CREATE:
            return self._run_create(identity, record)
        if operation_type is OperationType.TASK_ADMIT:
            return self._task_admit(identity, record)
        if operation_type is OperationType.TASK_DISPATCH:
            return self._task_dispatch(identity, record)
        if operation_type is OperationType.TASK_CLAIM:
            return await self._task_claim(identity, record)
        if operation_type is OperationType.TASK_COMPLETE:
            return await self._task_terminal(identity, record, OperationType.TASK_COMPLETE)
        if operation_type is OperationType.TASK_FAIL:
            return await self._task_terminal(identity, record, OperationType.TASK_FAIL)
        if operation_type is OperationType.RUN_TERMINATE:
            return self._run_terminate(identity, record)
        raise AssertionError(operation_type)

    def _base(
        self,
        *,
        identity: CanonicalAggregateIdentity,
        record: Any,
        tenant_id: str,
        rules: Sequence[ProjectionRule],
        key_hints: Mapping[str, Scalar] | None = None,
    ) -> CurrentHeadCanonicalEvidence:
        return CurrentHeadCanonicalEvidence(
            aggregate_type=identity.aggregate_type.value,
            operation_type=OperationType(record.operation_type),
            run_id=identity.run_id,
            task_id=identity.task_id,
            tenant_id=tenant_id,
            previous_state=record.previous_state,
            state=record.next_state,
            revision=record.to_revision,
            transition_id=record.transition_id,
            record_hash=record.canonical_record_hash,
            command_hash=record.canonical_command_hash,
            operation_id=record.operation_id,
            committed_at_ms=record.committed_at_ms,
            rules=tuple(rules),
            key_hints=_freeze_fields(key_hints),
        )

    def _run_create(self, identity: CanonicalAggregateIdentity, record: Any) -> CurrentHeadCanonicalEvidence:
        op = OperationType.RUN_CREATE
        md = _metadata(record, operation=op)
        tenant_id = _required_metadata_text(md, "tenant_id", op)
        expected_operation_id = f"run-create:v1:{identity.sha256}"
        if not (
            record.from_revision == 0
            and record.to_revision == 1
            and record.previous_state is None
            and record.next_state == "pending"
            and record.operation_id == expected_operation_id
            and md.get("run_id") == identity.run_id
            and md.get("legacy_projection_state") == "admitted"
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_CREATE canonical contract mismatch",
            )
        _require_exact_projection_intents(
            record,
            op,
            (
                {
                    "kind": "RUN_STATUS_PROJECTION",
                    "legacy_state": "admitted",
                    "control_stream": _required_metadata_text(md, "control_stream", op),
                },
            ),
        )
        receipt_key = _projection_receipt_key("run-create", record.operation_id)
        rules = [
            _type_rule("run_state", "string"),
            _rule("run_state", "admitted", ReconciliationReason.RUN_STATE_MISMATCH),
            _type_rule("projection_receipt", "hash"),
            _rule("receipt.operation_id", record.operation_id, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("receipt.canonical_transition_id", record.transition_id, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("receipt.canonical_record_hash", record.canonical_record_hash, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("receipt.canonical_command_hash", record.canonical_command_hash, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("receipt.canonical_revision", str(record.to_revision), ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("receipt.run_id", identity.run_id, ReconciliationReason.PROJECTION_VALUE_MISMATCH),
            _rule("receipt.tenant_id", tenant_id, ReconciliationReason.PROJECTION_VALUE_MISMATCH),
            _rule("receipt.legacy_state", "admitted", ReconciliationReason.RUN_STATE_MISMATCH),
            _rule("receipt.event_payload_hash", _run_create_event_payload_hash(md, record.operation_id), ReconciliationReason.PROJECTION_VALUE_MISMATCH),
            _rule("receipt.stream_entry_id_present", True, ReconciliationReason.PROJECTION_VALUE_MISMATCH),
        ]
        return self._base(
            identity=identity,
            record=record,
            tenant_id=tenant_id,
            rules=rules,
            key_hints={"projection_receipt": receipt_key},
        )

    def _task_admit(self, identity: CanonicalAggregateIdentity, record: Any) -> CurrentHeadCanonicalEvidence:
        op = OperationType.TASK_ADMIT
        md = _metadata(record, operation=op)
        tenant_id = _required_metadata_text(md, "tenant_id", op)
        dependency_count = _metadata_int(md, "dependency_count", op)
        admitted_at = _metadata_int(md, "admitted_at_ms", op)
        expected_state = "ready" if dependency_count <= 0 else "pending"
        if not (
            record.from_revision == 0
            and record.to_revision == 1
            and record.previous_state is None
            and record.next_state == expected_state
            and record.operation_id == f"task-admit:v1:{identity.sha256}"
            and md.get("task_id") == identity.task_id
            and md.get("run_id") == identity.run_id
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="TASK_ADMIT canonical contract mismatch",
            )
        expected_intents: tuple[Mapping[str, Any], ...] = (
            (
                {
                    "kind": "READY_QUEUE_IF_READY",
                    "task_id": identity.task_id,
                    "tenant_id": tenant_id,
                    "ready_score": admitted_at,
                },
            )
            if expected_state == "ready"
            else ()
        )
        _require_exact_projection_intents(record, op, expected_intents)
        identity_rules = [
            _rule("meta.task_id", identity.task_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.run_id", identity.run_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.tenant_id", tenant_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
        ]
        value_fields = {
            "agent_type": str(md.get("agent_type", "") or ""),
            "priority": str(_metadata_int(md, "priority", op)),
            "admitted_at": str(admitted_at),
            "payload_json": str(md.get("payload_json", "") or ""),
            "trace_parent": str(md.get("trace_parent", "") or ""),
            "trace_state": str(md.get("trace_state", "") or ""),
            "region": str(md.get("region", "") or ""),
            "policy": str(md.get("policy", "") or ""),
        }
        rules: list[ProjectionRule] = [
            _type_rule("task_state", "string"),
            _rule("task_state", expected_state, ReconciliationReason.TASK_STATE_MISMATCH),
            _type_rule("task_meta", "hash"),
            _type_rule("remaining_deps", "string"),
            _rule("remaining_deps", str(dependency_count), ReconciliationReason.PROJECTION_VALUE_MISMATCH),
            _type_rule("run_tasks", "set"),
            _rule("run_tasks_member", True, ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH),
            _type_rule("ready_queue", "none|zset"),
        ]
        rules.extend(identity_rules)
        rules.extend(
            _rule(f"meta.{field}", value, ReconciliationReason.PROJECTION_VALUE_MISMATCH, ReconciliationSeverity.WARNING)
            for field, value in value_fields.items()
        )
        if expected_state == "ready":
            rules.extend(
                [
                    _rule("ready_member", True, ReconciliationReason.READY_QUEUE_MEMBERSHIP_MISSING),
                    _rule("ready_score", float(admitted_at), ReconciliationReason.READY_QUEUE_SCORE_MISMATCH, ReconciliationSeverity.WARNING),
                    _type_rule("ready_emitted", "string"),
                    _rule("ready_emitted", "1", ReconciliationReason.PROJECTION_VALUE_MISMATCH),
                ]
            )
        else:
            rules.extend(
                [
                    _rule("ready_member", False, ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH),
                    _type_rule("ready_emitted", "none|string"),
                ]
            )
        return self._base(
            identity=identity,
            record=record,
            tenant_id=tenant_id,
            rules=rules,
            key_hints={
                "admit_state": expected_state,
                "dependency_count": str(dependency_count),
            },
        )

    def _task_dispatch(self, identity: CanonicalAggregateIdentity, record: Any) -> CurrentHeadCanonicalEvidence:
        op = OperationType.TASK_DISPATCH
        md = _metadata(record, operation=op)
        tenant_id = _required_metadata_text(md, "tenant_id", op)
        attempt = _metadata_int(md, "dispatch_attempt", op, 1)
        scheduled_at = _metadata_int(md, "scheduled_at_ms", op)
        worker_id = _required_metadata_text(md, "worker_id", op)
        scheduler_epoch = _required_metadata_text(md, "scheduler_epoch", op)
        expected_operation_id = f"task-dispatch:v1:{identity.sha256}:attempt:{attempt}"
        if not (
            record.previous_state == "ready"
            and record.next_state == "scheduled"
            and record.operation_id == expected_operation_id
            and md.get("task_id") == identity.task_id
            and md.get("run_id") == identity.run_id
            and md.get("ready_queue") == DagRedisKey.task_ready_queue(tenant_id)
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="TASK_DISPATCH canonical contract mismatch",
            )
        _require_exact_projection_intents(
            record,
            op,
            (
                {"kind": "CONTROL_NOTIFICATION", "stream": _required_metadata_text(md, "control_stream", op)},
                {"kind": "TASK_REQUEST_MESSAGE", "stream": _required_metadata_text(md, "shard_stream", op)},
            ),
        )
        rules: list[ProjectionRule] = [
            _type_rule("task_state", "string"),
            _rule("task_state", "scheduled", ReconciliationReason.TASK_STATE_MISMATCH),
            _type_rule("task_meta", "hash"),
            _rule("meta.task_id", identity.task_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.run_id", identity.run_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.tenant_id", tenant_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.dispatch_attempt", str(attempt), ReconciliationReason.PROJECTION_VALUE_MISMATCH),
            _rule("meta.dispatch_worker_id", worker_id, ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            _rule("meta.scheduler_epoch", scheduler_epoch, ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            *_proof_rules(
                transition_id=record.transition_id,
                record_hash=record.canonical_record_hash,
                command_hash=record.canonical_command_hash,
                revision=record.to_revision,
                operation_id=record.operation_id,
            ),
            _type_rule("ready_queue", "none|zset"),
            _rule("ready_member", False, ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH),
            _type_rule("scheduled_index", "none|zset"),
            _rule("scheduled_member", True, ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH),
            _rule("scheduled_score", float(scheduled_at), ReconciliationReason.PROJECTION_SCORE_MISMATCH, ReconciliationSeverity.WARNING),
            _type_rule("running_index", "none|zset"),
            _rule("running_member", False, ReconciliationReason.RUNNING_INDEX_MEMBERSHIP_PRESENT),
        ]
        return self._base(
            identity=identity,
            record=record,
            tenant_id=tenant_id,
            rules=rules,
            key_hints={
                "ready_queue": str(md.get("ready_queue") or DagRedisKey.task_ready_queue(tenant_id)),
                "scheduled_index": _required_metadata_text(md, "scheduled_zset", op),
                "running_index": _required_metadata_text(md, "running_zset", op),
            },
        )

    async def _task_claim(self, identity: CanonicalAggregateIdentity, record: Any) -> CurrentHeadCanonicalEvidence:
        op = OperationType.TASK_CLAIM
        md = _metadata(record, operation=op)
        tenant_id = _required_metadata_text(md, "tenant_id", op)
        attempt = _metadata_int(md, "dispatch_attempt", op, 1)
        claim_epoch = _metadata_int(md, "claim_epoch", op, 1)
        previous_claim_epoch = _metadata_int(md, "previous_claim_epoch", op)
        dispatch_revision = _metadata_int(md, "dispatch_revision", op, 1)
        worker = _required_metadata_text(md, "worker_instance_id", op)
        scheduler_epoch = _required_metadata_text(md, "scheduler_epoch", op)
        dispatch_operation_id = _required_metadata_text(md, "dispatch_operation_id", op)
        dispatch_transition_id = _required_metadata_text(md, "dispatch_transition_id", op)
        dispatch_record_hash = _required_metadata_text(md, "dispatch_record_hash", op)
        dispatch_command_hash = _required_metadata_text(md, "dispatch_command_hash", op)
        expected_operation_id = f"task-claim:v1:{identity.sha256}:attempt:{attempt}"
        if not (
            record.previous_state == "scheduled"
            and record.next_state == "running"
            and record.operation_id == expected_operation_id
            and record.from_revision == dispatch_revision
            and record.to_revision == dispatch_revision + 1
            and record.causation_id == dispatch_transition_id
            and md.get("task_id") == identity.task_id
            and md.get("run_id") == identity.run_id
            and claim_epoch == previous_claim_epoch + 1
            and dispatch_operation_id
            == f"task-dispatch:v1:{identity.sha256}:attempt:{attempt}"
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="TASK_CLAIM canonical contract mismatch",
            )
        await self._predecessor(
            identity=identity,
            operation_id=dispatch_operation_id,
            operation_type=OperationType.TASK_DISPATCH,
            transition_id=dispatch_transition_id,
            record_hash=dispatch_record_hash,
            command_hash=dispatch_command_hash,
            revision=dispatch_revision,
            next_state="scheduled",
        )
        _require_exact_projection_intents(
            record,
            op,
            ({"kind": "RUNNING_SET", "tenant_id": tenant_id, "task_id": identity.task_id},),
        )
        rules: list[ProjectionRule] = [
            _type_rule("task_state", "string"),
            _rule("task_state", "running", ReconciliationReason.TASK_STATE_MISMATCH),
            _type_rule("task_meta", "hash"),
            _rule("meta.task_id", identity.task_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.run_id", identity.run_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.tenant_id", tenant_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.worker_instance_id", worker, ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            _rule("meta.scheduler_epoch", scheduler_epoch, ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            _rule("meta.claimed_at_ms", str(_metadata_int(md, "claimed_at_ms", op)), ReconciliationReason.PROJECTION_VALUE_MISMATCH, ReconciliationSeverity.WARNING),
            _rule("meta.claim_epoch", str(claim_epoch), ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            _rule("meta.dispatch_attempt", str(attempt), ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.dispatch_worker_id", worker, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            *_proof_rules(
                transition_id=record.transition_id,
                record_hash=record.canonical_record_hash,
                command_hash=record.canonical_command_hash,
                revision=record.to_revision,
                operation_id=record.operation_id,
            ),
            _rule("meta.claim_canonical_transition_id", record.transition_id, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.claim_canonical_record_hash", record.canonical_record_hash, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.claim_canonical_command_hash", record.canonical_command_hash, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.claim_canonical_revision", str(record.to_revision), ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.claim_canonical_operation_id", record.operation_id, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.dispatch_canonical_transition_id", dispatch_transition_id, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.dispatch_canonical_record_hash", dispatch_record_hash, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.dispatch_canonical_command_hash", dispatch_command_hash, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.dispatch_canonical_revision", str(dispatch_revision), ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.dispatch_canonical_operation_id", dispatch_operation_id, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _type_rule("scheduled_index", "none|zset"),
            _rule("scheduled_member", False, ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH),
            _type_rule("running_index", "none|zset"),
            _rule("running_member", True, ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH),
            _rule("worker_reservation_present", False, ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            _rule("task_owner_present", False, ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
        ]
        return self._base(
            identity=identity,
            record=record,
            tenant_id=tenant_id,
            rules=rules,
            key_hints={
                "scheduled_index": DagRedisKey.task_scheduled_zset(tenant_id),
                "running_index": DagRedisKey.task_running_zset(tenant_id),
                "worker_reservation": DagRedisKey.worker_reservation(worker),
                "task_owner": DagRedisKey.task_reservation_owner(identity.task_id or ""),
            },
        )

    async def _task_terminal(
        self,
        identity: CanonicalAggregateIdentity,
        record: Any,
        operation_type: OperationType,
    ) -> CurrentHeadCanonicalEvidence:
        md = _metadata(record, operation=operation_type)
        tenant_id = _required_metadata_text(md, "tenant_id", operation_type)
        claim_epoch = _metadata_int(md, "claim_epoch", operation_type, 1)
        claim_revision = _metadata_int(md, "claim_revision", operation_type, 1)
        claim_operation_id = _required_metadata_text(md, "claim_operation_id", operation_type)
        claim_transition_id = _required_metadata_text(md, "claim_transition_id", operation_type)
        claim_record_hash = _required_metadata_text(md, "claim_record_hash", operation_type)
        claim_command_hash = _required_metadata_text(md, "claim_command_hash", operation_type)
        terminal_state = "done" if operation_type is OperationType.TASK_COMPLETE else "failed"
        expected_operation_id = f"task-terminal:v1:{identity.sha256}:claim:{claim_epoch}"
        if not (
            record.previous_state == "running"
            and record.next_state == terminal_state
            and record.operation_id == expected_operation_id
            and record.from_revision == claim_revision
            and record.to_revision == claim_revision + 1
            and record.causation_id == claim_transition_id
            and md.get("task_id") == identity.task_id
            and md.get("run_id") == identity.run_id
            and md.get("terminal_state") == terminal_state
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=f"{operation_type.value} canonical contract mismatch",
            )
        claim_record = await self._predecessor(
            identity=identity,
            operation_id=claim_operation_id,
            operation_type=OperationType.TASK_CLAIM,
            transition_id=claim_transition_id,
            record_hash=claim_record_hash,
            command_hash=claim_command_hash,
            revision=claim_revision,
            next_state="running",
        )
        claim_md = _metadata(claim_record, operation=OperationType.TASK_CLAIM)
        terminal_intents: tuple[Mapping[str, Any], ...] = (
            (
                {"kind": "OUTPUT_PROJECTION"},
                {"kind": "DEPENDENCY_FANOUT_INTENT"},
            )
            if operation_type is OperationType.TASK_COMPLETE
            else ({"kind": "DEPENDENCY_FAILURE_FANOUT_INTENT"},)
        )
        _require_exact_projection_intents(record, operation_type, terminal_intents)
        worker = _required_metadata_text(md, "worker_instance_id", operation_type)
        scheduler_epoch = _required_metadata_text(md, "scheduler_epoch", operation_type)
        if not (
            claim_md.get("task_id") == identity.task_id
            and claim_md.get("run_id") == identity.run_id
            and claim_md.get("tenant_id") == tenant_id
            and claim_md.get("claim_epoch") == claim_epoch
            and claim_md.get("worker_instance_id") == worker
            and claim_md.get("scheduler_epoch") == scheduler_epoch
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH,
                detail=f"{operation_type.value} durable TASK_CLAIM predecessor metadata mismatch",
            )
        output_sha = str(md.get("output_sha256", "") or "")
        rules: list[ProjectionRule] = [
            _type_rule("task_state", "string"),
            _rule("task_state", terminal_state, ReconciliationReason.TASK_STATE_MISMATCH),
            _type_rule("task_meta", "hash"),
            _rule("meta.task_id", identity.task_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.run_id", identity.run_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.tenant_id", tenant_id, ReconciliationReason.TASK_META_IDENTITY_MISMATCH),
            _rule("meta.completed_at_ms", str(_metadata_int(md, "finished_at_ms", operation_type)), ReconciliationReason.PROJECTION_VALUE_MISMATCH, ReconciliationSeverity.WARNING),
            _rule("meta.terminal_state", terminal_state, ReconciliationReason.PROJECTION_VALUE_MISMATCH),
            _rule("meta.completion_reason", _required_metadata_text(md, "reason_code", operation_type), ReconciliationReason.PROJECTION_VALUE_MISMATCH, ReconciliationSeverity.WARNING),
            # Accepted terminal source RETAINS owner/fence proof.
            _rule("meta.worker_instance_id", worker, ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            _rule("meta.scheduler_epoch", scheduler_epoch, ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            _rule("meta.claim_epoch", str(claim_epoch), ReconciliationReason.OWNERSHIP_PROJECTION_MISMATCH),
            *_proof_rules(
                transition_id=record.transition_id,
                record_hash=record.canonical_record_hash,
                command_hash=record.canonical_command_hash,
                revision=record.to_revision,
                operation_id=record.operation_id,
            ),
            _rule("meta.terminal_canonical_transition_id", record.transition_id, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.terminal_canonical_record_hash", record.canonical_record_hash, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.terminal_canonical_command_hash", record.canonical_command_hash, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.terminal_canonical_revision", str(record.to_revision), ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.terminal_canonical_operation_id", record.operation_id, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.terminal_canonical_operation_type", operation_type.value, ReconciliationReason.CANONICAL_PROOF_MISMATCH),
            _rule("meta.terminal_output_sha256", output_sha, ReconciliationReason.TASK_OUTPUT_MISMATCH if operation_type is OperationType.TASK_COMPLETE else ReconciliationReason.PROJECTION_VALUE_MISMATCH),
            _rule("meta.claim_canonical_transition_id", claim_transition_id, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.claim_canonical_record_hash", claim_record_hash, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.claim_canonical_command_hash", claim_command_hash, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.claim_canonical_revision", str(claim_revision), ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _rule("meta.claim_canonical_operation_id", claim_operation_id, ReconciliationReason.CLAIM_PREDECESSOR_PROOF_MISMATCH),
            _type_rule("running_index", "none|zset"),
            _rule("running_member", False, ReconciliationReason.RUNNING_INDEX_MEMBERSHIP_PRESENT),
        ]
        if operation_type is OperationType.TASK_COMPLETE:
            rules.extend(
                [
                    _type_rule("task_output", "string"),
                    _rule("task_output", str(md.get("output_data", "")), ReconciliationReason.TASK_OUTPUT_MISMATCH),
                ]
            )
        # TASK_FAIL deliberately has NO output-key absence rule.
        return self._base(identity=identity, record=record, tenant_id=tenant_id, rules=rules)

    def _run_terminate(self, identity: CanonicalAggregateIdentity, record: Any) -> CurrentHeadCanonicalEvidence:
        op = OperationType.RUN_TERMINATE
        md = _metadata(record, operation=op)
        tenant_id = _required_metadata_text(md, "tenant_id", op)
        terminal = md.get("terminal_evidence")
        if not isinstance(terminal, Mapping):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE terminal_evidence is missing/corrupt",
            )
        final_state = str(terminal.get("final_state") or "")
        proof_sha = str(terminal.get("terminal_proof_sha256") or "")
        if final_state not in {"done", "failed"} or len(proof_sha) != 64:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE terminal evidence contract invalid",
            )
        if record.previous_state not in {"pending", "running"} or record.next_state != final_state:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE state transition invalid",
            )
        expected_digest = _reconciliation_hashlib.sha256(
            b"RUN_TERMINATE\x00" + identity.run_id.encode("utf-8") + b"\x00" + proof_sha.encode("ascii")
        ).hexdigest()
        if record.operation_id != f"run-terminate:v1:{expected_digest}":
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE operation identity mismatch",
            )
        _require_exact_projection_intents(
            record,
            op,
            (
                {
                    "kind": "RUN_RESULT_PROJECTION",
                    "terminal_proof_sha256": proof_sha,
                    "terminal_state": final_state,
                },
            ),
        )
        try:
            task_count = int(terminal.get("task_count", 0))
            done_count = int(terminal.get("done_count", 0))
            failed_count = int(terminal.get("failed_count", 0))
            skipped_count = int(terminal.get("skipped_count", 0))
        except (TypeError, ValueError) as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE terminal counts are invalid",
            ) from exc
        if (
            task_count < 1
            or min(done_count, failed_count, skipped_count) < 0
            or done_count + failed_count + skipped_count != task_count
            or (final_state == "done" and failed_count != 0)
            or (final_state == "failed" and failed_count < 1)
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE terminal count/final-state contract invalid",
            )
        tasks_raw = terminal.get("tasks")
        if not isinstance(tasks_raw, Sequence) or isinstance(tasks_raw, (str, bytes, bytearray)):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE terminal task evidence is invalid",
            )
        normalized_tasks: list[dict[str, str]] = []
        observed_done = observed_failed = observed_skipped = 0
        previous_task_id: str | None = None
        failure_states = {"failed", "blocked_by_failure", "dead_lettered", "rejected", "cancelled"}
        for row in tasks_raw:
            if not isinstance(row, Mapping) or set(row) != {"task_id", "state"}:
                raise ReconciliationEvidenceError(
                    reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                    detail="RUN_TERMINATE terminal task row is invalid",
                )
            task_id = str(row.get("task_id") or "")
            task_state = str(row.get("state") or "")
            if not task_id or (previous_task_id is not None and task_id <= previous_task_id):
                raise ReconciliationEvidenceError(
                    reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                    detail="RUN_TERMINATE terminal task ordering is invalid",
                )
            previous_task_id = task_id
            if task_state == "done":
                observed_done += 1
            elif task_state == "skipped":
                observed_skipped += 1
            elif task_state in failure_states:
                observed_failed += 1
            else:
                raise ReconciliationEvidenceError(
                    reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                    detail="RUN_TERMINATE terminal task state is invalid",
                )
            normalized_tasks.append({"task_id": task_id, "state": task_state})
        if (
            len(normalized_tasks) != task_count
            or observed_done != done_count
            or observed_failed != failed_count
            or observed_skipped != skipped_count
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE terminal task/count evidence mismatch",
            )
        proof_payload = {
            "schema_version": int(terminal.get("schema_version", 0)),
            "run_id": identity.run_id,
            "tenant_id": tenant_id,
            "tasks": normalized_tasks,
            "task_count": task_count,
            "done_count": done_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count,
            "final_state": final_state,
        }
        if (
            proof_payload["schema_version"] != 1
            or _reconciliation_hashlib.sha256(canonical_json_bytes(proof_payload)).hexdigest()
            != proof_sha
        ):
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail="RUN_TERMINATE terminal proof digest mismatch",
            )
        payload = {
            "task_count": task_count,
            "done_count": done_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count,
            "trigger_task_id": str(md.get("trigger_task_id", "") or ""),
            "trigger_terminal_state": str(md.get("trigger_terminal_state", "") or ""),
        }
        payload_json = canonical_json_bytes(payload).decode("utf-8")
        payload_sha = _reconciliation_hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        event_id = _event_id_for_run_terminate(identity.run_id, final_state, task_count)
        finalized_at = _metadata_int(md, "finalized_at_ms", op)
        receipt_key = _projection_receipt_key("run-terminate", record.operation_id)
        rules: list[ProjectionRule] = [
            _type_rule("run_state", "string"),
            _rule("run_state", final_state, ReconciliationReason.RUN_STATE_MISMATCH),
            _type_rule("run_meta", "hash"),
            _type_rule("run_result", "hash"),
            _type_rule("projection_receipt", "hash"),
            _type_rule("cp_running", "none|zset"),
            _rule("cp_running_member", False, ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH),
        ]
        meta_expected = {
            "run_id": identity.run_id,
            "tenant_id": tenant_id,
            "state": final_state,
            "finalized_at_ms": str(finalized_at),
            "finalization_operation": "RUN_TERMINATE",
            "finalization_source": "terminal_task_aggregate",
            "terminal_proof_sha256": proof_sha,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_revision": str(record.to_revision),
            "result_event_id": event_id,
        }
        result_expected = dict(meta_expected)
        result_expected.pop("state")
        result_expected["status"] = final_state
        result_expected["payload"] = payload_json
        for field, value in meta_expected.items():
            reason = ReconciliationReason.CANONICAL_PROOF_MISMATCH if field.startswith("canonical_") else ReconciliationReason.RUN_META_MISMATCH
            rules.append(_rule(f"run_meta.{field}", value, reason))
        for field, value in result_expected.items():
            reason = ReconciliationReason.CANONICAL_PROOF_MISMATCH if field.startswith("canonical_") else ReconciliationReason.RUN_RESULT_MISMATCH
            rules.append(_rule(f"run_result.{field}", value, reason))
        receipt_expected = {
            "operation_id": record.operation_id,
            "terminal_proof_sha256": proof_sha,
            "canonical_transition_id": record.transition_id,
            "canonical_record_hash": record.canonical_record_hash,
            "canonical_command_hash": record.canonical_command_hash,
            "canonical_revision": str(record.to_revision),
            "run_id": identity.run_id,
            "tenant_id": tenant_id,
            "final_state": final_state,
            "finalized_at_ms": str(finalized_at),
            "result_payload_sha256": payload_sha,
            "event_id": event_id,
        }
        for field, value in receipt_expected.items():
            reason = ReconciliationReason.CANONICAL_PROOF_MISMATCH if field.startswith("canonical_") or field == "operation_id" else ReconciliationReason.PROJECTION_VALUE_MISMATCH
            rules.append(_rule(f"receipt.{field}", value, reason))
        rules.append(_rule("receipt.stream_entry_id_present", True, ReconciliationReason.PROJECTION_VALUE_MISMATCH))
        return self._base(
            identity=identity,
            record=record,
            tenant_id=tenant_id,
            rules=rules,
            key_hints={"projection_receipt": receipt_key},
        )


class CurrentHeadReconciliationRedisReader(ReconciliationRedisReader):
    """85.0B runtime reader exposing only Redis read operations."""

    def __init__(self, redis: Any) -> None:
        super().__init__(redis)
        self.__read_redis = redis

    async def _kind(self, key: str) -> str:
        return _text(await self.__read_redis.type(key))

    async def _string(self, key: str, kind: str) -> str | None:
        if kind != "string":
            return None
        raw = await self.__read_redis.get(key)
        return None if raw is None else _text(raw)

    async def _hash(self, key: str, kind: str, fields: Sequence[str]) -> dict[str, str | None]:
        if kind != "hash":
            return {field: None for field in fields}
        raw = await self.__read_redis.hmget(key, *fields)
        return {
            field: None if value is None else _text(value)
            for field, value in zip(fields, raw)
        }

    async def _zmember(self, key: str, kind: str, member: str) -> tuple[bool, float | None]:
        if kind != "zset":
            return False, None
        raw = await self.__read_redis.zscore(key, member)
        return raw is not None, None if raw is None else float(raw)

    async def read_current_projection(
        self,
        *,
        canonical: CurrentHeadCanonicalEvidence,
    ) -> CurrentHeadRuntimeEvidence:
        try:
            operation = canonical.operation_type
            if operation is OperationType.RUN_CREATE:
                values = await self._read_run_create(canonical)
            elif operation is OperationType.TASK_ADMIT:
                values = await self._read_task_admit(canonical)
            elif operation is OperationType.TASK_DISPATCH:
                values = await self._read_task_dispatch(canonical)
            elif operation is OperationType.TASK_CLAIM:
                values = await self._read_task_claim(canonical)
            elif operation in {OperationType.TASK_COMPLETE, OperationType.TASK_FAIL}:
                values = await self._read_task_terminal(canonical)
            elif operation is OperationType.RUN_TERMINATE:
                values = await self._read_run_terminate(canonical)
            else:
                raise ValueError(f"unsupported current-head operation {operation.value}")
            return CurrentHeadRuntimeEvidence(operation_type=operation, fields=_freeze_fields(values))
        except ReconciliationEvidenceError:
            raise
        except Exception as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.PROJECTION_EVIDENCE_UNAVAILABLE,
                detail=f"runtime current-head projection read failed: {exc}",
            ) from exc

    @staticmethod
    def _meta_fields(canonical: CurrentHeadCanonicalEvidence) -> tuple[str, ...]:
        return tuple(
            sorted(
                field.removeprefix("meta.")
                for field in {rule.field for rule in canonical.rules}
                if field.startswith("meta.")
            )
        )

    async def _read_run_create(self, c: CurrentHeadCanonicalEvidence) -> dict[str, Scalar]:
        state_key = _ReconciliationRedisKey.run_state(c.run_id)
        receipt_key = c.hint("projection_receipt")
        state_type = await self._kind(state_key)
        receipt_type = await self._kind(receipt_key)
        fields = [
            rule.field.removeprefix("receipt.")
            for rule in c.rules
            if rule.field.startswith("receipt.") and rule.field != "receipt.stream_entry_id_present"
        ]
        receipt = await self._hash(receipt_key, receipt_type, fields + ["stream_entry_id"])
        return {
            "type.run_state": state_type,
            "run_state": await self._string(state_key, state_type),
            "type.projection_receipt": receipt_type,
            **{f"receipt.{field}": receipt.get(field) for field in fields},
            "receipt.stream_entry_id_present": bool(receipt.get("stream_entry_id")),
        }

    async def _read_task_admit(self, c: CurrentHeadCanonicalEvidence) -> dict[str, Scalar]:
        assert c.task_id is not None
        state_key = DagRedisKey.task_state(c.task_id)
        meta_key = DagRedisKey.task_meta(c.task_id)
        remaining_key = DagRedisKey.task_remaining_deps(c.task_id)
        run_tasks_key = DagRedisKey.run_tasks(c.run_id)
        ready_key = DagRedisKey.task_ready_queue(c.tenant_id)
        emitted_key = DagRedisKey.task_ready_emitted(c.task_id)
        state_type = await self._kind(state_key)
        meta_type = await self._kind(meta_key)
        remaining_type = await self._kind(remaining_key)
        run_tasks_type = await self._kind(run_tasks_key)
        ready_type = await self._kind(ready_key)
        emitted_type = await self._kind(emitted_key)
        meta = await self._hash(meta_key, meta_type, self._meta_fields(c))
        ready_member, ready_score = await self._zmember(ready_key, ready_type, c.task_id)
        run_member = bool(await self.__read_redis.sismember(run_tasks_key, c.task_id)) if run_tasks_type == "set" else False
        result: dict[str, Scalar] = {
            "type.task_state": state_type,
            "task_state": await self._string(state_key, state_type),
            "type.task_meta": meta_type,
            "type.remaining_deps": remaining_type,
            "remaining_deps": await self._string(remaining_key, remaining_type),
            "type.run_tasks": run_tasks_type,
            "run_tasks_member": run_member,
            "type.ready_queue": ready_type,
            "ready_member": ready_member,
            "ready_score": ready_score,
            "type.ready_emitted": emitted_type,
            "ready_emitted": await self._string(emitted_key, emitted_type),
        }
        result.update({f"meta.{field}": value for field, value in meta.items()})
        return result

    async def _read_task_dispatch(self, c: CurrentHeadCanonicalEvidence) -> dict[str, Scalar]:
        assert c.task_id is not None
        state_key = DagRedisKey.task_state(c.task_id)
        meta_key = DagRedisKey.task_meta(c.task_id)
        ready_key = c.hint("ready_queue")
        scheduled_key = c.hint("scheduled_index")
        running_key = c.hint("running_index")
        state_type = await self._kind(state_key)
        meta_type = await self._kind(meta_key)
        ready_type = await self._kind(ready_key)
        scheduled_type = await self._kind(scheduled_key)
        running_type = await self._kind(running_key)
        meta = await self._hash(meta_key, meta_type, self._meta_fields(c))
        ready_member, _ready_score = await self._zmember(ready_key, ready_type, c.task_id)
        scheduled_member, scheduled_score = await self._zmember(scheduled_key, scheduled_type, c.task_id)
        running_member, _running_score = await self._zmember(running_key, running_type, c.task_id)
        result: dict[str, Scalar] = {
            "type.task_state": state_type,
            "task_state": await self._string(state_key, state_type),
            "type.task_meta": meta_type,
            "type.ready_queue": ready_type,
            "ready_member": ready_member,
            "type.scheduled_index": scheduled_type,
            "scheduled_member": scheduled_member,
            "scheduled_score": scheduled_score,
            "type.running_index": running_type,
            "running_member": running_member,
        }
        result.update({f"meta.{field}": value for field, value in meta.items()})
        return result

    async def _read_task_claim(self, c: CurrentHeadCanonicalEvidence) -> dict[str, Scalar]:
        assert c.task_id is not None
        state_key = DagRedisKey.task_state(c.task_id)
        meta_key = DagRedisKey.task_meta(c.task_id)
        scheduled_key = c.hint("scheduled_index")
        running_key = c.hint("running_index")
        reservation_key = c.hint("worker_reservation")
        owner_key = c.hint("task_owner")
        state_type = await self._kind(state_key)
        meta_type = await self._kind(meta_key)
        scheduled_type = await self._kind(scheduled_key)
        running_type = await self._kind(running_key)
        reservation_type = await self._kind(reservation_key)
        owner_type = await self._kind(owner_key)
        meta = await self._hash(meta_key, meta_type, self._meta_fields(c))
        scheduled_member, _ = await self._zmember(scheduled_key, scheduled_type, c.task_id)
        running_member, _heartbeat_score = await self._zmember(running_key, running_type, c.task_id)
        # _heartbeat_score is intentionally discarded.  TASK_HEARTBEAT may
        # legitimately advance it between runtime A and runtime B.
        result: dict[str, Scalar] = {
            "type.task_state": state_type,
            "task_state": await self._string(state_key, state_type),
            "type.task_meta": meta_type,
            "type.scheduled_index": scheduled_type,
            "scheduled_member": scheduled_member,
            "type.running_index": running_type,
            "running_member": running_member,
            "worker_reservation_present": reservation_type != "none",
            "task_owner_present": owner_type != "none",
        }
        result.update({f"meta.{field}": value for field, value in meta.items()})
        return result

    async def _read_task_terminal(self, c: CurrentHeadCanonicalEvidence) -> dict[str, Scalar]:
        assert c.task_id is not None
        state_key = DagRedisKey.task_state(c.task_id)
        meta_key = DagRedisKey.task_meta(c.task_id)
        running_key = DagRedisKey.task_running_zset(c.tenant_id)
        state_type = await self._kind(state_key)
        meta_type = await self._kind(meta_key)
        running_type = await self._kind(running_key)
        meta = await self._hash(meta_key, meta_type, self._meta_fields(c))
        running_member, _ = await self._zmember(running_key, running_type, c.task_id)
        result: dict[str, Scalar] = {
            "type.task_state": state_type,
            "task_state": await self._string(state_key, state_type),
            "type.task_meta": meta_type,
            "type.running_index": running_type,
            "running_member": running_member,
        }
        result.update({f"meta.{field}": value for field, value in meta.items()})
        # TASK_FAIL intentionally never reads task_output as an invariant.
        if c.operation_type is OperationType.TASK_COMPLETE:
            output_key = DagRedisKey.task_output(c.task_id)
            output_type = await self._kind(output_key)
            result["type.task_output"] = output_type
            result["task_output"] = await self._string(output_key, output_type)
        return result

    async def _read_run_terminate(self, c: CurrentHeadCanonicalEvidence) -> dict[str, Scalar]:
        state_key = _ReconciliationRedisKey.run_state(c.run_id)
        meta_key = _ReconciliationRedisKey.run_meta(c.run_id)
        result_key = _ReconciliationRedisKey.run_result(c.run_id)
        receipt_key = c.hint("projection_receipt")
        running_key = _ReconciliationRedisKey.cp_running()
        state_type = await self._kind(state_key)
        meta_type = await self._kind(meta_key)
        result_type = await self._kind(result_key)
        receipt_type = await self._kind(receipt_key)
        running_type = await self._kind(running_key)
        meta_fields = sorted(
            rule.field.removeprefix("run_meta.")
            for rule in c.rules
            if rule.field.startswith("run_meta.")
        )
        result_fields = sorted(
            rule.field.removeprefix("run_result.")
            for rule in c.rules
            if rule.field.startswith("run_result.")
        )
        receipt_fields = sorted(
            rule.field.removeprefix("receipt.")
            for rule in c.rules
            if rule.field.startswith("receipt.") and rule.field != "receipt.stream_entry_id_present"
        )
        meta = await self._hash(meta_key, meta_type, meta_fields)
        run_result = await self._hash(result_key, result_type, result_fields)
        receipt = await self._hash(receipt_key, receipt_type, receipt_fields + ["stream_entry_id"])
        running_member, _ = await self._zmember(running_key, running_type, c.run_id)
        values: dict[str, Scalar] = {
            "type.run_state": state_type,
            "run_state": await self._string(state_key, state_type),
            "type.run_meta": meta_type,
            "type.run_result": result_type,
            "type.projection_receipt": receipt_type,
            "type.cp_running": running_type,
            "cp_running_member": running_member,
            "receipt.stream_entry_id_present": bool(receipt.get("stream_entry_id")),
        }
        values.update({f"run_meta.{field}": value for field, value in meta.items()})
        values.update({f"run_result.{field}": value for field, value in run_result.items()})
        values.update({f"receipt.{field}": value for field, value in receipt.items() if field != "stream_entry_id"})
        return values


def _rule_matches(expected: Scalar, observed: Scalar) -> bool:
    if isinstance(expected, str) and "|" in expected:
        return str(observed) in expected.split("|")
    return expected == observed


_TASK_ADMIT_DEPENDENCY_OWNED_MUTABLE_FIELDS = frozenset(
    {
        "task_state",
        "remaining_deps",
        "ready_member",
        "ready_emitted",
    }
)


def _task_admit_dependency_ambiguity_applies(
    canonical: CurrentHeadCanonicalEvidence,
) -> bool:
    """Return whether the child head has the narrow 85.0B fanout ambiguity."""
    if (
        canonical.operation_type is not OperationType.TASK_ADMIT
        or canonical.state != "pending"
        or canonical.hint("admit_state") != "pending"
    ):
        return False
    raw_original = canonical.hint("dependency_count")
    return raw_original.isdecimal() and int(raw_original) > 0


def _task_admit_dependency_projection_shape(
    canonical: CurrentHeadCanonicalEvidence,
    observed: Mapping[str, Scalar],
) -> str | None:
    """Classify only the dependency-owned mutable TASK_ADMIT projection shape.

    Independent child invariants are deliberately not interpreted here. The
    reconciler validates identity, immutable metadata, RUN membership, and
    Redis schema/type evidence first. Only after those checks are clean may
    this local classifier decide whether the dependency-owned values are the
    untouched postimage, a source-reachable cross-TASK evolution requiring
    85.0C provenance, or a source-impossible projection shape.
    """
    if not _task_admit_dependency_ambiguity_applies(canonical):
        return None

    original = int(canonical.hint("dependency_count"))
    raw_remaining = observed.get("remaining_deps")
    if raw_remaining is None or not str(raw_remaining).isdecimal():
        return "IMPOSSIBLE"
    remaining = int(str(raw_remaining))
    state = observed.get("task_state")
    ready_member = observed.get("ready_member") is True
    ready_emitted = observed.get("ready_emitted")

    untouched = (
        state == "pending"
        and remaining == original
        and not ready_member
        and ready_emitted is None
    )
    if untouched:
        return "UNTOUCHED"

    partial_progress = (
        state == "pending"
        and 0 < remaining < original
        and not ready_member
        and ready_emitted is None
    )
    dependency_unlock = (
        state == "ready"
        and remaining == 0
        and ready_member
        and ready_emitted == "1"
    )
    failure_while_pending = (
        state == "blocked_by_failure"
        and 1 <= remaining <= original
        and not ready_member
        and ready_emitted is None
    )
    failure_after_ready = (
        state == "blocked_by_failure"
        and remaining == 0
        and not ready_member
        and ready_emitted == "1"
    )
    if partial_progress or dependency_unlock or failure_while_pending or failure_after_ready:
        return "EVIDENCE_REQUIRED"
    return "IMPOSSIBLE"


class CurrentHeadReconciler:
    """Read-only explicit-operation reconciler for Sprint 85.0B."""

    def __init__(
        self,
        *,
        canonical_reader: CurrentHeadCanonicalReader,
        runtime_reader: CurrentHeadRuntimeReader,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.__canonical_reader = canonical_reader
        self.__runtime_reader = runtime_reader
        self.__clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    @staticmethod
    def _contract_id(operation_type: OperationType) -> str:
        return f"{operation_type.value}_CURRENT_PROJECTION"

    @staticmethod
    def _empty_reference() -> ReconciliationCanonicalReference:
        return ReconciliationCanonicalReference(None, None, None, None, None, None, None, None)

    def _finding(
        self,
        *,
        canonical: CurrentHeadCanonicalEvidence | None,
        operation_type: OperationType,
        run_id: str,
        task_id: str | None,
        status: ReconciliationStatus,
        severity: ReconciliationSeverity,
        reason: ReconciliationReason,
        expected: Mapping[str, Scalar] | None,
        observed: Mapping[str, Scalar] | None,
        evidence: ReconciliationEvidenceState,
        observation: ReconciliationObservation,
    ) -> ReconciliationFinding:
        aggregate_type = (
            canonical.aggregate_type
            if canonical is not None
            else (AggregateType.RUN.value if operation_type in {OperationType.RUN_CREATE, OperationType.RUN_TERMINATE} else AggregateType.TASK.value)
        )
        return ReconciliationFinding(
            aggregate_type=aggregate_type,
            run_id=run_id,
            task_id=task_id,
            check_class=ReconciliationCheckClass.CURRENT_PROJECTION,
            contract_id=self._contract_id(operation_type),
            contract_version=CURRENT_HEAD_RECONCILIATION_CONTRACT_VERSION,
            status=status,
            severity=severity,
            reason_code=reason,
            canonical=canonical.canonical_reference if canonical is not None else self._empty_reference(),
            expected=_freeze_fields(expected),
            observed=_freeze_fields(observed),
            evidence=evidence,
            observation=observation,
            mutation_attempted=False,
        )

    def _blocked(
        self,
        *,
        canonical: CurrentHeadCanonicalEvidence | None,
        operation_type: OperationType,
        run_id: str,
        task_id: str | None,
        reason: ReconciliationReason,
        detail: str,
        canonical_proven: bool,
        canonical_stable: bool,
        projection_read_complete: bool,
        projection_stable: bool,
        observation: ReconciliationObservation,
        severity: ReconciliationSeverity = ReconciliationSeverity.CRITICAL,
    ) -> tuple[ReconciliationFinding, ...]:
        return (
            self._finding(
                canonical=canonical,
                operation_type=operation_type,
                run_id=run_id,
                task_id=task_id,
                status=ReconciliationStatus.BLOCKED_EVIDENCE,
                severity=severity,
                reason=reason,
                expected=None,
                observed={"detail": detail},
                evidence=ReconciliationEvidenceState(
                    canonical_proven=canonical_proven,
                    canonical_stable=canonical_stable,
                    projection_read_complete=projection_read_complete,
                    projection_observation_stable=projection_stable,
                    refs=(detail,),
                ),
                observation=observation,
            ),
        )

    async def reconcile(
        self,
        *,
        operation_type: OperationType,
        run_id: str,
        task_id: str | None = None,
    ) -> tuple[ReconciliationFinding, ...]:
        if operation_type not in _CURRENT_HEAD_OPERATIONS:
            raise ValueError(f"{operation_type.value} is outside Sprint 85.0B coverage")
        started = int(self.__clock_ms())
        try:
            canonical_a = await self.__canonical_reader.read_current_head(
                operation_type=operation_type,
                run_id=run_id,
                task_id=task_id,
            )
        except ReconciliationEvidenceError as exc:
            return self._blocked(
                canonical=None,
                operation_type=operation_type,
                run_id=run_id,
                task_id=task_id,
                reason=exc.reason,
                detail=exc.detail,
                canonical_proven=False,
                canonical_stable=False,
                projection_read_complete=False,
                projection_stable=False,
                observation=ReconciliationObservation(started, int(self.__clock_ms())),
            )
        try:
            runtime_a = await self.__runtime_reader.read_current_projection(canonical=canonical_a)
            runtime_b = await self.__runtime_reader.read_current_projection(canonical=canonical_a)
        except ReconciliationEvidenceError as exc:
            return self._blocked(
                canonical=canonical_a,
                operation_type=operation_type,
                run_id=run_id,
                task_id=task_id,
                reason=exc.reason,
                detail=exc.detail,
                canonical_proven=True,
                canonical_stable=False,
                projection_read_complete=False,
                projection_stable=False,
                observation=ReconciliationObservation(started, int(self.__clock_ms())),
            )
        try:
            canonical_b = await self.__canonical_reader.read_current_head(
                operation_type=operation_type,
                run_id=run_id,
                task_id=task_id,
            )
        except ReconciliationEvidenceError as exc:
            canonical_a_head = (
                canonical_a.revision,
                canonical_a.state,
                canonical_a.transition_id,
                canonical_a.record_hash,
                canonical_a.command_hash,
                canonical_a.operation_id,
            )
            changed = (
                exc.canonical_fingerprint is not None
                and exc.canonical_fingerprint != canonical_a_head
            )
            return self._blocked(
                canonical=canonical_a,
                operation_type=operation_type,
                run_id=run_id,
                task_id=task_id,
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION if changed else exc.reason,
                detail="canonical head changed between observation boundaries" if changed else exc.detail,
                canonical_proven=True,
                canonical_stable=False,
                projection_read_complete=True,
                projection_stable=runtime_a.fingerprint == runtime_b.fingerprint,
                observation=ReconciliationObservation(started, int(self.__clock_ms())),
            )
        observation = ReconciliationObservation(started, int(self.__clock_ms()))
        if canonical_a.fingerprint != canonical_b.fingerprint:
            return self._blocked(
                canonical=canonical_a,
                operation_type=operation_type,
                run_id=run_id,
                task_id=task_id,
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical head changed between observation boundaries",
                canonical_proven=True,
                canonical_stable=False,
                projection_read_complete=True,
                projection_stable=runtime_a.fingerprint == runtime_b.fingerprint,
                observation=observation,
            )
        if runtime_a.fingerprint != runtime_b.fingerprint:
            return self._blocked(
                canonical=canonical_b,
                operation_type=operation_type,
                run_id=run_id,
                task_id=task_id,
                reason=ReconciliationReason.PROJECTION_OBSERVATION_CHANGED,
                detail="immutable operation-owned runtime projection changed between reads",
                canonical_proven=True,
                canonical_stable=True,
                projection_read_complete=True,
                projection_stable=False,
                observation=observation,
            )
        evidence = ReconciliationEvidenceState(
            canonical_proven=True,
            canonical_stable=True,
            projection_read_complete=True,
            projection_observation_stable=True,
            refs=(f"canonical:{canonical_b.transition_id}", f"operation:{canonical_b.operation_id}"),
        )
        observed = dict(runtime_b.fields)
        findings: list[ReconciliationFinding] = []
        dependency_ambiguity = _task_admit_dependency_ambiguity_applies(canonical_b)
        independent_rules = (
            tuple(
                rule
                for rule in canonical_b.rules
                if rule.field not in _TASK_ADMIT_DEPENDENCY_OWNED_MUTABLE_FIELDS
            )
            if dependency_ambiguity
            else canonical_b.rules
        )
        for rule in independent_rules:
            actual = observed.get(rule.field)
            if _rule_matches(rule.expected, actual):
                continue
            findings.append(
                self._finding(
                    canonical=canonical_b,
                    operation_type=operation_type,
                    run_id=run_id,
                    task_id=task_id,
                    status=ReconciliationStatus.DRIFT,
                    severity=rule.severity,
                    reason=rule.reason,
                    expected={"field": rule.field, "value": rule.expected},
                    observed={"field": rule.field, "value": actual},
                    evidence=evidence,
                    observation=observation,
                )
            )

        # Cross-TASK ambiguity is subordinate to independently provable child
        # invariants. A valid-looking fanout shape must never hide identity,
        # immutable metadata, membership, or Redis schema/type drift.
        if dependency_ambiguity and not findings:
            dependency_shape = _task_admit_dependency_projection_shape(canonical_b, observed)
            if dependency_shape == "EVIDENCE_REQUIRED":
                return self._blocked(
                    canonical=canonical_b,
                    operation_type=operation_type,
                    run_id=run_id,
                    task_id=task_id,
                    reason=ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED,
                    detail=(
                        "TASK_ADMIT dependency-owned projection matches an exact "
                        "source-reachable cross-TASK terminal fanout shape; Sprint "
                        "85.0B requires cross-aggregate provenance before classifying it"
                    ),
                    canonical_proven=True,
                    canonical_stable=True,
                    projection_read_complete=True,
                    projection_stable=True,
                    observation=observation,
                    severity=ReconciliationSeverity.WARNING,
                )
            if dependency_shape == "IMPOSSIBLE":
                # Ordinary accepted rules still own the narrowest reason code
                # for dependency-owned mismatches. They are evaluated only now
                # so source-reachable fanout evolution is not mislabeled DRIFT.
                for rule in canonical_b.rules:
                    if rule.field not in _TASK_ADMIT_DEPENDENCY_OWNED_MUTABLE_FIELDS:
                        continue
                    actual = observed.get(rule.field)
                    if _rule_matches(rule.expected, actual):
                        continue
                    findings.append(
                        self._finding(
                            canonical=canonical_b,
                            operation_type=operation_type,
                            run_id=run_id,
                            task_id=task_id,
                            status=ReconciliationStatus.DRIFT,
                            severity=rule.severity,
                            reason=rule.reason,
                            expected={"field": rule.field, "value": rule.expected},
                            observed={"field": rule.field, "value": actual},
                            evidence=evidence,
                            observation=observation,
                        )
                    )
                if not findings:
                    # Pending TASK_ADMIT intentionally allows ready_emitted's
                    # Redis type to be none|string, because other accepted
                    # operations may create the marker. If the overall shape is
                    # nevertheless source-impossible, the marker value itself
                    # is positive projection drift rather than evidence lack.
                    findings.append(
                        self._finding(
                            canonical=canonical_b,
                            operation_type=operation_type,
                            run_id=run_id,
                            task_id=task_id,
                            status=ReconciliationStatus.DRIFT,
                            severity=ReconciliationSeverity.CRITICAL,
                            reason=ReconciliationReason.PROJECTION_VALUE_MISMATCH,
                            expected={"field": "ready_emitted", "value": None},
                            observed={"field": "ready_emitted", "value": observed.get("ready_emitted")},
                            evidence=evidence,
                            observation=observation,
                        )
                    )
        if not findings:
            findings.append(
                self._finding(
                    canonical=canonical_b,
                    operation_type=operation_type,
                    run_id=run_id,
                    task_id=task_id,
                    status=ReconciliationStatus.CONSISTENT,
                    severity=ReconciliationSeverity.INFO,
                    reason=ReconciliationReason.CONSISTENT,
                    expected={"operation_type": operation_type.value, "state": canonical_b.state},
                    observed={"operation_type": operation_type.value, "state": canonical_b.state},
                    evidence=evidence,
                    observation=observation,
                )
            )
        return tuple(
            sorted(
                findings,
                key=lambda f: (
                    f.aggregate_type,
                    f.run_id,
                    f.task_id or "",
                    f.check_class.value,
                    f.reason_code.value,
                    f.expected,
                ),
            )
        )


# Sprint 85.0C — read-only historical durable effects + cross-aggregate proof.
from dataclasses import dataclass as _history_dataclass
from enum import Enum as _HistoryEnum

from hfa.authority.redis_persistence import (
    PersistedAggregateHistory as _PersistedAggregateHistory,
    RedisAuthorityHistoryIncompleteError as _RedisAuthorityHistoryIncompleteError,
    RedisAuthorityObservationChangedError as _RedisAuthorityObservationChangedError,
)
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationConflictError as _AdmissionResourceReservationConflictError,
    AdmissionResourceReservationInput as _AdmissionResourceReservationInput,
    AdmissionResourceReservationManager as _AdmissionResourceReservationManager,
    AdmissionResourceReservationReceipt as _AdmissionResourceReservationReceipt,
    AdmissionResourceSettlementInput as _AdmissionResourceSettlementInput,
    RESERVATION_STATE_FINALIZED as _RESERVATION_STATE_FINALIZED,
    RESERVATION_STATE_RELEASED as _RESERVATION_STATE_RELEASED,
    RESERVATION_STATE_RESERVED as _RESERVATION_STATE_RESERVED,
    RESERVATION_STATE_SETTLED as _RESERVATION_STATE_SETTLED,
)
from hfa_control.run_create_authority import (
    resource_reservation_from_run_create_record as _resource_reservation_from_run_create_record,
)
from hfa_control.run_terminate_authority import (
    RunTerminateAuthorityError as _RunTerminateAuthorityError,
    TerminalAggregateProof as _TerminalAggregateProof,
    TerminalAggregateProofManager as _TerminalAggregateProofManager,
    run_terminate_operation_id as _run_terminate_operation_id,
)


HISTORICAL_RECONCILIATION_CONTRACT_VERSION = 1
HISTORICAL_CANONICAL_CONTRACT_ID = "CANONICAL_HISTORY"
RUN_TERMINATE_PROOF_CONTRACT_ID = "RUN_TERMINATE_TERMINAL_PROOF"
RUN_CREATE_RESOURCE_CONTRACT_ID = "RUN_CREATE_RESOURCE_RESERVATION"
RUN_TERMINATE_RESOURCE_CONTRACT_ID = "RUN_TERMINATE_RESOURCE_SETTLEMENT"


class OperationReachability(str, _HistoryEnum):
    DISCOVERABLE_BY_EXISTING_INDEX = "DISCOVERABLE_BY_EXISTING_INDEX"
    DERIVABLE_FROM_CURRENT_TRUTH = "DERIVABLE_FROM_CURRENT_TRUTH"
    REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID = "REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID"
    NOT_RELIABLY_REACHABLE = "NOT_RELIABLY_REACHABLE"


@_history_dataclass(frozen=True)
class OperationReachabilitySpec:
    durable_data: OperationReachability
    current_facade: OperationReachability
    operation_id_dependency: str
    current_facade_with_terminal_proof: OperationReachability | None = None


_HISTORICAL_REACHABILITY = {
    OperationType.RUN_CREATE: OperationReachabilitySpec(
        OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX,
        OperationReachability.DERIVABLE_FROM_CURRENT_TRUTH,
        "run_id",
    ),
    OperationType.TASK_ADMIT: OperationReachabilitySpec(
        OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX,
        OperationReachability.DERIVABLE_FROM_CURRENT_TRUTH,
        "run_id+task_id",
    ),
    OperationType.TASK_DISPATCH: OperationReachabilitySpec(
        OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX,
        OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        "task_identity+dispatch_attempt",
    ),
    OperationType.TASK_CLAIM: OperationReachabilitySpec(
        OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX,
        OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        "task_identity+dispatch_attempt",
    ),
    OperationType.TASK_COMPLETE: OperationReachabilitySpec(
        OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX,
        OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        "task_identity+claim_epoch",
    ),
    OperationType.TASK_FAIL: OperationReachabilitySpec(
        OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX,
        OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        "task_identity+claim_epoch",
    ),
    OperationType.TASK_REQUEUE: OperationReachabilitySpec(
        OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX,
        OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        "task_identity+claim_epoch",
    ),
    OperationType.RUN_TERMINATE: OperationReachabilitySpec(
        OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX,
        OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        "run_id+terminal_proof_sha256",
        OperationReachability.DERIVABLE_FROM_CURRENT_TRUTH,
    ),
}


def operation_reachability(
    operation_type: OperationType,
    *,
    terminal_proof_available: bool = False,
) -> OperationReachabilitySpec:
    try:
        spec = _HISTORICAL_REACHABILITY[operation_type]
    except KeyError as exc:
        raise ValueError(f"{operation_type.value} is outside Sprint 85.0C reachability coverage") from exc
    if (
        operation_type is OperationType.RUN_TERMINATE
        and terminal_proof_available
        and spec.current_facade_with_terminal_proof is not None
    ):
        return OperationReachabilitySpec(
            durable_data=spec.durable_data,
            current_facade=spec.current_facade_with_terminal_proof,
            operation_id_dependency=spec.operation_id_dependency,
            current_facade_with_terminal_proof=spec.current_facade_with_terminal_proof,
        )
    return spec


class HistoricalCanonicalReader(Protocol):
    async def read_history(
        self,
        identity: CanonicalAggregateIdentity,
    ) -> _PersistedAggregateHistory: ...


class TerminalProofReader(Protocol):
    async def read_terminal_proof(self, *, run_id: str) -> _TerminalAggregateProof | None: ...


@_history_dataclass(frozen=True)
class ResourceReceiptEvidence:
    redis_type: str
    raw_fingerprint: tuple[tuple[str, str], ...]
    receipt: _AdmissionResourceReservationReceipt | None
    missing_immutable_fields: tuple[str, ...] = ()
    immutable_mismatch: bool = False
    corrupt_detail: str = ""

    @property
    def fingerprint(self) -> tuple[Any, ...]:
        return (
            self.redis_type,
            self.raw_fingerprint,
            self.missing_immutable_fields,
            self.immutable_mismatch,
            self.corrupt_detail,
        )


class ResourceReceiptReader(Protocol):
    async def read_reservation_receipt(
        self,
        reservation: _AdmissionResourceReservationInput,
    ) -> ResourceReceiptEvidence: ...


class HistoricalCanonicalReconciliationReader:
    """Read-only facade over the 85.0C whole-history store API."""

    def __init__(self, redis: Any, *, namespace: str = "hfa:authority:v1") -> None:
        self.__store = RedisCanonicalAuthorityStore(redis, namespace=namespace)

    async def read_history(
        self,
        identity: CanonicalAggregateIdentity,
    ) -> _PersistedAggregateHistory:
        try:
            history = await self.__store.load_aggregate_history(identity)
        except _RedisAuthorityObservationChangedError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail=str(exc),
            ) from exc
        except _RedisAuthorityHistoryIncompleteError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_HISTORY_INCOMPLETE,
                detail=str(exc),
            ) from exc
        except RedisAuthorityCorruptionError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                detail=str(exc),
            ) from exc
        except RedisAuthorityPersistenceError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=str(exc),
            ) from exc
        except Exception as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=f"canonical history read failed: {exc}",
            ) from exc
        if history is None:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="canonical aggregate history is missing",
            )
        return history


class ReadOnlyTerminalProofReader:
    """Expose only immutable terminal-proof reads to reconciliation."""

    def __init__(self, redis: Any) -> None:
        self.__manager = _TerminalAggregateProofManager(redis)

    async def read_terminal_proof(self, *, run_id: str) -> _TerminalAggregateProof | None:
        try:
            return await self.__manager.load_existing(run_id)
        except _RunTerminateAuthorityError as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=f"terminal proof unavailable: {exc}",
            ) from exc
        except Exception as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail=f"terminal proof read failed: {exc}",
            ) from exc


class ReadOnlyResourceReceiptReader:
    """Read-only reservation/settlement receipt adapter with stable raw proof."""

    def __init__(self, redis: Any) -> None:
        self.__redis = redis
        self.__manager = _AdmissionResourceReservationManager(redis)

    @staticmethod
    def _decode_hash(raw: Mapping[Any, Any]) -> tuple[tuple[str, str], ...]:
        return tuple(sorted((_text(k), _text(v)) for k, v in raw.items()))

    async def read_reservation_receipt(
        self,
        reservation: _AdmissionResourceReservationInput,
    ) -> ResourceReceiptEvidence:
        key = self.__manager.reservation_receipt_key(reservation.operation_id)
        try:
            kind = _text(await self.__redis.type(key))
            if kind == "none":
                return ResourceReceiptEvidence("none", (), None)
            if kind != "hash":
                return ResourceReceiptEvidence(
                    kind,
                    (),
                    None,
                    corrupt_detail="resource receipt key has wrong Redis type",
                )
            raw_before = await self.__redis.hgetall(key)
            before = self._decode_hash(raw_before)
            values = dict(before)
            immutable_expected = {
                "operation_id": reservation.operation_id,
                "run_id": reservation.run_id,
                "tenant_id": reservation.tenant_id,
                "estimated_cost_cents": str(reservation.estimated_cost_cents),
                "reservation_version": str(reservation.reservation_version),
                "proof_sha256": reservation.proof_sha256,
            }
            missing_immutable_fields = tuple(
                sorted(field for field in immutable_expected if field not in values)
            )
            immutable_mismatch = any(
                values[field] != expected
                for field, expected in immutable_expected.items()
                if field in values
            )
            receipt: _AdmissionResourceReservationReceipt | None = None
            corrupt_detail = ""
            if not missing_immutable_fields and not immutable_mismatch:
                try:
                    receipt = await self.__manager.get_receipt(reservation)
                except _AdmissionResourceReservationConflictError as exc:
                    corrupt_detail = str(exc)
            raw_after = await self.__redis.hgetall(key)
            after = self._decode_hash(raw_after)
        except ReconciliationEvidenceError:
            raise
        except Exception as exc:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                detail=f"resource receipt read failed: {exc}",
            ) from exc
        if before != after:
            raise ReconciliationEvidenceError(
                reason=ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION,
                detail="resource receipt changed during one read-only observation",
            )
        return ResourceReceiptEvidence(
            redis_type="hash",
            raw_fingerprint=after,
            receipt=receipt,
            missing_immutable_fields=missing_immutable_fields,
            immutable_mismatch=immutable_mismatch,
            corrupt_detail=corrupt_detail,
        )


def _history_record(
    history: _PersistedAggregateHistory,
    operation_type: OperationType,
) -> Any:
    rows = [item.record for item in history.operations if item.record.operation_type == operation_type.value]
    if not rows:
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
            detail=f"canonical history has no {operation_type.value} operation",
        )
    if operation_type in {OperationType.RUN_CREATE, OperationType.RUN_TERMINATE} and len(rows) != 1:
        raise ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail=f"canonical history has multiple {operation_type.value} operations",
        )
    return rows[-1]


def _record_reference(record: Any) -> ReconciliationCanonicalReference:
    return ReconciliationCanonicalReference(
        revision=record.to_revision,
        operation_type=record.operation_type,
        state=record.next_state,
        transition_id=record.transition_id,
        record_hash=record.canonical_record_hash,
        command_hash=record.canonical_command_hash,
        operation_id=record.operation_id,
        committed_at_ms=record.committed_at_ms,
    )


def _proof_fingerprint(value: _TerminalAggregateProof | None) -> tuple[Any, ...]:
    if value is None:
        return ("ABSENT",)
    return (
        value.schema_version,
        value.run_id,
        value.tenant_id,
        tuple((row.task_id, row.state) for row in value.tasks),
        value.task_count,
        value.done_count,
        value.failed_count,
        value.skipped_count,
        value.final_state,
        value.proof_sha256,
        value.proof_payload_json,
        value.finalized_at_ms,
        value.worker_instance_id,
        value.trigger_task_id,
        value.trigger_terminal_state,
        value.canonical_expected_revision,
        value.canonical_previous_state,
    )


def _terminal_tasks_json(proof: _TerminalAggregateProof) -> str:
    return canonical_json_bytes(
        [{"task_id": row.task_id, "state": row.state} for row in proof.tasks]
    ).decode("utf-8")


def _terminal_record_observed(record: Any) -> dict[str, Scalar]:
    metadata = record.authoritative_metadata_changes
    terminal = metadata.get("terminal_evidence") if isinstance(metadata, Mapping) else None
    if not isinstance(terminal, Mapping):
        return {"terminal_evidence": "MISSING_OR_INVALID"}
    tasks = terminal.get("tasks")
    try:
        tasks_json = canonical_json_bytes(tasks).decode("utf-8")
    except Exception:
        tasks_json = "INVALID"
    return {
        "run_id": str(metadata.get("run_id") or ""),
        "tenant_id": str(metadata.get("tenant_id") or ""),
        "terminal_proof_sha256": str(terminal.get("terminal_proof_sha256") or ""),
        "task_count": terminal.get("task_count") if type(terminal.get("task_count")) is int else None,
        "done_count": terminal.get("done_count") if type(terminal.get("done_count")) is int else None,
        "failed_count": terminal.get("failed_count") if type(terminal.get("failed_count")) is int else None,
        "skipped_count": terminal.get("skipped_count") if type(terminal.get("skipped_count")) is int else None,
        "tasks_json": tasks_json,
        "final_state": str(terminal.get("final_state") or ""),
        "finalized_at_ms": metadata.get("finalized_at_ms") if type(metadata.get("finalized_at_ms")) is int else None,
        "worker_instance_id": str(metadata.get("worker_instance_id") or ""),
        "trigger_task_id": str(metadata.get("trigger_task_id") or ""),
        "trigger_terminal_state": str(metadata.get("trigger_terminal_state") or ""),
        "from_revision": record.from_revision,
        "to_revision": record.to_revision,
        "previous_state": record.previous_state,
        "next_state": record.next_state,
    }


def _terminal_proof_expected(proof: _TerminalAggregateProof) -> dict[str, Scalar]:
    return {
        "run_id": proof.run_id,
        "tenant_id": proof.tenant_id,
        "terminal_proof_sha256": proof.proof_sha256,
        "task_count": proof.task_count,
        "done_count": proof.done_count,
        "failed_count": proof.failed_count,
        "skipped_count": proof.skipped_count,
        "tasks_json": _terminal_tasks_json(proof),
        "final_state": proof.final_state,
        "finalized_at_ms": proof.finalized_at_ms,
        "worker_instance_id": proof.worker_instance_id,
        "trigger_task_id": proof.trigger_task_id,
        "trigger_terminal_state": proof.trigger_terminal_state,
        "from_revision": proof.canonical_expected_revision,
        "to_revision": proof.canonical_expected_revision + 1,
        "previous_state": proof.canonical_previous_state,
        "next_state": proof.final_state,
    }


def _run_terminate_proof_matches(record: Any, proof: _TerminalAggregateProof) -> bool:
    metadata = record.authoritative_metadata_changes
    terminal = metadata.get("terminal_evidence") if isinstance(metadata, Mapping) else None
    if not isinstance(metadata, Mapping) or not isinstance(terminal, Mapping):
        return False
    expected_tasks = [{"task_id": row.task_id, "state": row.state} for row in proof.tasks]
    return all(
        (
            record.operation_type == OperationType.RUN_TERMINATE.value,
            record.aggregate_identity.aggregate_type is AggregateType.RUN,
            record.aggregate_identity.run_id == proof.run_id,
            record.operation_id == _run_terminate_operation_id(proof.run_id, proof.proof_sha256),
            record.from_revision == proof.canonical_expected_revision,
            record.to_revision == proof.canonical_expected_revision + 1,
            record.previous_state == proof.canonical_previous_state,
            record.next_state == proof.final_state,
            metadata.get("run_id") == proof.run_id,
            metadata.get("tenant_id") == proof.tenant_id,
            metadata.get("finalized_at_ms") == proof.finalized_at_ms,
            metadata.get("worker_instance_id") == proof.worker_instance_id,
            metadata.get("trigger_task_id") == proof.trigger_task_id,
            metadata.get("trigger_terminal_state") == proof.trigger_terminal_state,
            terminal.get("schema_version") == proof.schema_version,
            terminal.get("terminal_proof_sha256") == proof.proof_sha256,
            terminal.get("task_count") == proof.task_count,
            terminal.get("done_count") == proof.done_count,
            terminal.get("failed_count") == proof.failed_count,
            terminal.get("skipped_count") == proof.skipped_count,
            terminal.get("final_state") == proof.final_state,
            canonical_json_bytes(terminal.get("tasks")) == canonical_json_bytes(expected_tasks),
        )
    )


class HistoricalCrossAggregateReconciler:
    """85.0C read-only historical/cross-aggregate reconciler."""

    def __init__(
        self,
        *,
        canonical_reader: HistoricalCanonicalReader,
        terminal_proof_reader: TerminalProofReader | None = None,
        resource_reader: ResourceReceiptReader | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.__canonical_reader = canonical_reader
        self.__terminal_proof_reader = terminal_proof_reader
        self.__resource_reader = resource_reader
        self.__clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    @staticmethod
    def _empty_reference() -> ReconciliationCanonicalReference:
        return ReconciliationCanonicalReference(None, None, None, None, None, None, None, None)

    def _finding(
        self,
        *,
        aggregate_type: str,
        run_id: str,
        task_id: str | None,
        contract_id: str,
        check_class: ReconciliationCheckClass,
        status: ReconciliationStatus,
        severity: ReconciliationSeverity,
        reason: ReconciliationReason,
        canonical_record: Any | None,
        expected: Mapping[str, Scalar] | None,
        observed: Mapping[str, Scalar] | None,
        refs: Sequence[str],
        started_at_ms: int,
    ) -> ReconciliationFinding:
        return ReconciliationFinding(
            aggregate_type=aggregate_type,
            run_id=run_id,
            task_id=task_id,
            check_class=check_class,
            contract_id=contract_id,
            contract_version=HISTORICAL_RECONCILIATION_CONTRACT_VERSION,
            status=status,
            severity=severity,
            reason_code=reason,
            canonical=(
                _record_reference(canonical_record)
                if canonical_record is not None
                else self._empty_reference()
            ),
            expected=_freeze_fields(expected),
            observed=_freeze_fields(observed),
            evidence=ReconciliationEvidenceState(
                canonical_proven=canonical_record is not None,
                canonical_stable=status is not ReconciliationStatus.BLOCKED_EVIDENCE
                or reason
                not in {
                    ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                    ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                    ReconciliationReason.CANONICAL_HISTORY_INCOMPLETE,
                    ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                },
                projection_read_complete=status is not ReconciliationStatus.BLOCKED_EVIDENCE
                or reason
                not in {
                    ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                    ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                },
                projection_observation_stable=reason
                not in {
                    ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                    ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION,
                },
                refs=tuple(refs),
            ),
            observation=ReconciliationObservation(started_at_ms, int(self.__clock_ms())),
            mutation_attempted=False,
        )

    async def reconcile_history(
        self,
        identity: CanonicalAggregateIdentity,
    ) -> tuple[ReconciliationFinding, ...]:
        started = int(self.__clock_ms())
        try:
            history = await self.__canonical_reader.read_history(identity)
        except ReconciliationEvidenceError as exc:
            return (
                self._finding(
                    aggregate_type=identity.aggregate_type.value,
                    run_id=identity.run_id,
                    task_id=identity.task_id,
                    contract_id=HISTORICAL_CANONICAL_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=(
                        ReconciliationSeverity.WARNING
                        if exc.reason is ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION
                        else ReconciliationSeverity.CRITICAL
                    ),
                    reason=exc.reason,
                    canonical_record=None,
                    expected={"history": "COMPLETE_AND_STABLE"},
                    observed={"detail": exc.detail},
                    refs=(exc.detail,),
                    started_at_ms=started,
                ),
            )
        highest = history.operations[-1].record
        return (
            self._finding(
                aggregate_type=identity.aggregate_type.value,
                run_id=identity.run_id,
                task_id=identity.task_id,
                contract_id=HISTORICAL_CANONICAL_CONTRACT_ID,
                check_class=ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT,
                status=ReconciliationStatus.CONSISTENT,
                severity=ReconciliationSeverity.INFO,
                reason=ReconciliationReason.CONSISTENT,
                canonical_record=highest,
                expected={"revision_count": history.snapshot.revision},
                observed={"revision_count": len(history.operations)},
                refs=(f"canonical:{highest.transition_id}",),
                started_at_ms=started,
            ),
        )

    async def _two_proofs(
        self,
        *,
        run_id: str,
    ) -> tuple[_TerminalAggregateProof | None, _TerminalAggregateProof | None, ReconciliationEvidenceError | None]:
        if self.__terminal_proof_reader is None:
            return None, None, ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="terminal proof reader is unavailable",
            )
        first: _TerminalAggregateProof | None = None
        second: _TerminalAggregateProof | None = None
        first_error: ReconciliationEvidenceError | None = None
        second_error: ReconciliationEvidenceError | None = None
        try:
            first = await self.__terminal_proof_reader.read_terminal_proof(run_id=run_id)
        except ReconciliationEvidenceError as exc:
            first_error = exc
        try:
            second = await self.__terminal_proof_reader.read_terminal_proof(run_id=run_id)
        except ReconciliationEvidenceError as exc:
            second_error = exc
        first_fp = ("ERROR", first_error.reason.value, first_error.detail) if first_error else _proof_fingerprint(first)
        second_fp = ("ERROR", second_error.reason.value, second_error.detail) if second_error else _proof_fingerprint(second)
        if first_fp != second_fp:
            return first, second, ReconciliationEvidenceError(
                reason=ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION,
                detail="terminal proof changed between read-only observations",
            )
        if first_error is not None:
            return None, None, first_error
        if first is None:
            return None, None, ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="immutable terminal proof is missing",
            )
        return first, second, None

    async def reconcile_run_terminate_proof(
        self,
        *,
        run_id: str,
    ) -> tuple[ReconciliationFinding, ...]:
        started = int(self.__clock_ms())
        identity = CanonicalAggregateIdentity(AggregateType.RUN, run_id, None)
        try:
            history_a = await self.__canonical_reader.read_history(identity)
            record_a = _history_record(history_a, OperationType.RUN_TERMINATE)
        except ReconciliationEvidenceError as exc:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_PROOF_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=exc.reason,
                    canonical_record=None,
                    expected={"terminal_proof": "READABLE"},
                    observed={"detail": exc.detail},
                    refs=(exc.detail,),
                    started_at_ms=started,
                ),
            )
        proof_a, _proof_b, proof_error = await self._two_proofs(run_id=run_id)
        try:
            history_b = await self.__canonical_reader.read_history(identity)
        except ReconciliationEvidenceError as exc:
            proof_error = exc
            history_b = history_a
        if history_a.fingerprint != history_b.fingerprint:
            proof_error = ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical RUN history changed around terminal-proof observation",
            )
        if proof_error is not None or proof_a is None:
            error = proof_error or ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="immutable terminal proof is unavailable",
            )
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_PROOF_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=(
                        ReconciliationSeverity.WARNING
                        if error.reason in {
                            ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                            ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION,
                        }
                        else ReconciliationSeverity.CRITICAL
                    ),
                    reason=error.reason,
                    canonical_record=record_a,
                    expected={"terminal_proof": "STABLE_AND_VALID"},
                    observed={"detail": error.detail},
                    refs=(error.detail,),
                    started_at_ms=started,
                ),
            )
        record_b = _history_record(history_b, OperationType.RUN_TERMINATE)
        expected = _terminal_proof_expected(proof_a)
        observed = _terminal_record_observed(record_b)
        if not _run_terminate_proof_matches(record_b, proof_a):
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_PROOF_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.DRIFT,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.CANONICAL_PROOF_MISMATCH,
                    canonical_record=record_b,
                    expected=expected,
                    observed=observed,
                    refs=(f"proof:{proof_a.proof_sha256}",),
                    started_at_ms=started,
                ),
            )
        return (
            self._finding(
                aggregate_type=AggregateType.RUN.value,
                run_id=run_id,
                task_id=None,
                contract_id=RUN_TERMINATE_PROOF_CONTRACT_ID,
                check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                status=ReconciliationStatus.CONSISTENT,
                severity=ReconciliationSeverity.INFO,
                reason=ReconciliationReason.CONSISTENT,
                canonical_record=record_b,
                expected=expected,
                observed=observed,
                refs=(f"proof:{proof_a.proof_sha256}",),
                started_at_ms=started,
            ),
        )

    async def _two_resources(
        self,
        reservation: _AdmissionResourceReservationInput,
    ) -> tuple[ResourceReceiptEvidence | None, ResourceReceiptEvidence | None, ReconciliationEvidenceError | None]:
        if self.__resource_reader is None:
            return None, None, ReconciliationEvidenceError(
                reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                detail="resource receipt reader is unavailable",
            )
        try:
            first = await self.__resource_reader.read_reservation_receipt(reservation)
            second = await self.__resource_reader.read_reservation_receipt(reservation)
        except ReconciliationEvidenceError as exc:
            return None, None, exc
        if first.fingerprint != second.fingerprint:
            return first, second, ReconciliationEvidenceError(
                reason=ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION,
                detail="resource receipt changed between read-only observations",
            )
        return first, second, None

    async def reconcile_run_create_resource(
        self,
        *,
        run_id: str,
    ) -> tuple[ReconciliationFinding, ...]:
        started = int(self.__clock_ms())
        identity = CanonicalAggregateIdentity(AggregateType.RUN, run_id, None)
        try:
            history_a = await self.__canonical_reader.read_history(identity)
            create_a = _history_record(history_a, OperationType.RUN_CREATE)
            reservation = _resource_reservation_from_run_create_record(create_a)
        except (ReconciliationEvidenceError, ValueError) as exc:
            reason = exc.reason if isinstance(exc, ReconciliationEvidenceError) else ReconciliationReason.CANONICAL_RECORD_CORRUPTION
            detail = exc.detail if isinstance(exc, ReconciliationEvidenceError) else str(exc)
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_CREATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=reason,
                    canonical_record=None,
                    expected={"reservation": "READABLE"},
                    observed={"detail": detail},
                    refs=(detail,),
                    started_at_ms=started,
                ),
            )
        resource_a, _resource_b, resource_error = await self._two_resources(reservation)
        try:
            history_b = await self.__canonical_reader.read_history(identity)
        except ReconciliationEvidenceError as exc:
            resource_error = exc
            history_b = history_a
        if history_a.fingerprint != history_b.fingerprint:
            resource_error = ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical RUN history changed around resource observation",
            )
        create_b = _history_record(history_b, OperationType.RUN_CREATE)
        if resource_error is not None or resource_a is None:
            error = resource_error or ReconciliationEvidenceError(
                reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                detail="resource receipt is unavailable",
            )
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_CREATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.WARNING,
                    reason=error.reason,
                    canonical_record=create_b,
                    expected={"resource_receipt": "STABLE_AND_VALID"},
                    observed={"detail": error.detail},
                    refs=(error.detail,),
                    started_at_ms=started,
                ),
            )
        if resource_a.missing_immutable_fields:
            detail = "missing required immutable resource fields: " + ",".join(resource_a.missing_immutable_fields)
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_CREATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                    canonical_record=create_b,
                    expected={"resource_receipt": "COMPLETE_IMMUTABLE_IDENTITY"},
                    observed={"detail": detail},
                    refs=(detail,),
                    started_at_ms=started,
                ),
            )
        if resource_a.immutable_mismatch:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_CREATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.DRIFT,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.RESOURCE_PROOF_MISMATCH,
                    canonical_record=create_b,
                    expected={"reservation_proof_sha256": reservation.proof_sha256},
                    observed={"resource_receipt": "IMMUTABLE_MISMATCH"},
                    refs=("resource:immutable-mismatch",),
                    started_at_ms=started,
                ),
            )
        if resource_a.corrupt_detail:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_CREATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                    canonical_record=create_b,
                    expected={"resource_receipt": "VALID"},
                    observed={"detail": resource_a.corrupt_detail},
                    refs=(resource_a.corrupt_detail,),
                    started_at_ms=started,
                ),
            )
        receipt = resource_a.receipt
        if receipt is None:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_CREATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.WARNING,
                    reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                    canonical_record=create_b,
                    expected={"resource_receipt": "PRESENT"},
                    observed={"resource_receipt": "ABSENT"},
                    refs=("resource:absent",),
                    started_at_ms=started,
                ),
            )
        if receipt.state == _RESERVATION_STATE_RESERVED:
            status = ReconciliationStatus.BLOCKED_EVIDENCE
            reason = ReconciliationReason.RESOURCE_FINALIZATION_PENDING
            severity = ReconciliationSeverity.WARNING
        elif receipt.state in {_RESERVATION_STATE_FINALIZED, _RESERVATION_STATE_SETTLED}:
            status = ReconciliationStatus.CONSISTENT
            reason = ReconciliationReason.CONSISTENT
            severity = ReconciliationSeverity.INFO
        elif receipt.state == _RESERVATION_STATE_RELEASED:
            status = ReconciliationStatus.DRIFT
            reason = ReconciliationReason.RESOURCE_PROOF_MISMATCH
            severity = ReconciliationSeverity.CRITICAL
        else:
            status = ReconciliationStatus.BLOCKED_EVIDENCE
            reason = ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE
            severity = ReconciliationSeverity.CRITICAL
        return (
            self._finding(
                aggregate_type=AggregateType.RUN.value,
                run_id=run_id,
                task_id=None,
                contract_id=RUN_CREATE_RESOURCE_CONTRACT_ID,
                check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                status=status,
                severity=severity,
                reason=reason,
                canonical_record=create_b,
                expected={"reservation_proof_sha256": reservation.proof_sha256},
                observed={
                    "reservation_proof_sha256": receipt.proof_sha256,
                    "resource_state": receipt.state,
                },
                refs=(f"resource:{receipt.proof_sha256}",),
                started_at_ms=started,
            ),
        )

    async def reconcile_run_terminate_settlement(
        self,
        *,
        run_id: str,
    ) -> tuple[ReconciliationFinding, ...]:
        started = int(self.__clock_ms())
        identity = CanonicalAggregateIdentity(AggregateType.RUN, run_id, None)
        try:
            history_a = await self.__canonical_reader.read_history(identity)
            create_a = _history_record(history_a, OperationType.RUN_CREATE)
            terminate_a = _history_record(history_a, OperationType.RUN_TERMINATE)
            reservation = _resource_reservation_from_run_create_record(create_a)
        except (ReconciliationEvidenceError, ValueError) as exc:
            reason = exc.reason if isinstance(exc, ReconciliationEvidenceError) else ReconciliationReason.CANONICAL_RECORD_CORRUPTION
            detail = exc.detail if isinstance(exc, ReconciliationEvidenceError) else str(exc)
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=reason,
                    canonical_record=None,
                    expected={"settlement": "READABLE"},
                    observed={"detail": detail},
                    refs=(detail,),
                    started_at_ms=started,
                ),
            )

        proof_a: _TerminalAggregateProof | None = None
        proof_b: _TerminalAggregateProof | None = None
        resource_a: ResourceReceiptEvidence | None = None
        resource_b: ResourceReceiptEvidence | None = None
        proof_error: ReconciliationEvidenceError | None = None
        resource_error: ReconciliationEvidenceError | None = None

        # Frozen cross-aggregate observation order:
        # RUN A -> terminal proof A -> settlement A -> settlement B
        # -> terminal proof B -> RUN B.
        if self.__terminal_proof_reader is None:
            proof_error = ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="terminal proof reader is unavailable",
            )
        else:
            try:
                proof_a = await self.__terminal_proof_reader.read_terminal_proof(run_id=run_id)
            except ReconciliationEvidenceError as exc:
                proof_error = exc

        if proof_error is None:
            if self.__resource_reader is None:
                resource_error = ReconciliationEvidenceError(
                    reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                    detail="resource receipt reader is unavailable",
                )
            else:
                try:
                    resource_a = await self.__resource_reader.read_reservation_receipt(reservation)
                    resource_b = await self.__resource_reader.read_reservation_receipt(reservation)
                except ReconciliationEvidenceError as exc:
                    resource_error = exc
                if (
                    resource_error is None
                    and resource_a is not None
                    and resource_b is not None
                    and resource_a.fingerprint != resource_b.fingerprint
                ):
                    resource_error = ReconciliationEvidenceError(
                        reason=ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION,
                        detail="resource receipt changed between read-only observations",
                    )

            try:
                proof_b = await self.__terminal_proof_reader.read_terminal_proof(run_id=run_id)
            except ReconciliationEvidenceError as exc:
                proof_error = exc
            if (
                proof_error is None
                and proof_a is not None
                and proof_b is not None
                and _proof_fingerprint(proof_a) != _proof_fingerprint(proof_b)
            ):
                proof_error = ReconciliationEvidenceError(
                    reason=ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION,
                    detail="terminal proof changed between read-only observations",
                )

        try:
            history_b = await self.__canonical_reader.read_history(identity)
        except ReconciliationEvidenceError as exc:
            history_b = history_a
            proof_error = exc
        if history_a.fingerprint != history_b.fingerprint:
            proof_error = ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION,
                detail="canonical RUN history changed around settlement observation",
            )
        terminate_b = _history_record(history_b, OperationType.RUN_TERMINATE)
        if proof_error is not None or proof_a is None:
            error = proof_error or ReconciliationEvidenceError(
                reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
                detail="terminal proof is unavailable",
            )
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.WARNING,
                    reason=error.reason,
                    canonical_record=terminate_b,
                    expected={"terminal_proof": "STABLE_AND_VALID"},
                    observed={"detail": error.detail},
                    refs=(error.detail,),
                    started_at_ms=started,
                ),
            )
        if not _run_terminate_proof_matches(terminate_b, proof_a):
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.DRIFT,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.CANONICAL_PROOF_MISMATCH,
                    canonical_record=terminate_b,
                    expected=_terminal_proof_expected(proof_a),
                    observed=_terminal_record_observed(terminate_b),
                    refs=(f"proof:{proof_a.proof_sha256}",),
                    started_at_ms=started,
                ),
            )
        if resource_error is not None or resource_a is None:
            error = resource_error or ReconciliationEvidenceError(
                reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                detail="settlement evidence is unavailable",
            )
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.WARNING,
                    reason=error.reason,
                    canonical_record=terminate_b,
                    expected={"settlement": "STABLE_AND_VALID"},
                    observed={"detail": error.detail},
                    refs=(error.detail,),
                    started_at_ms=started,
                ),
            )
        if resource_a.missing_immutable_fields:
            detail = "missing required immutable resource fields: " + ",".join(resource_a.missing_immutable_fields)
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                    canonical_record=terminate_b,
                    expected={"settlement": "COMPLETE_IMMUTABLE_RESERVATION_IDENTITY"},
                    observed={"detail": detail},
                    refs=(detail,),
                    started_at_ms=started,
                ),
            )
        if resource_a.immutable_mismatch:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.DRIFT,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.RESOURCE_PROOF_MISMATCH,
                    canonical_record=terminate_b,
                    expected={"reservation_proof_sha256": reservation.proof_sha256},
                    observed={"resource_receipt": "IMMUTABLE_MISMATCH"},
                    refs=("resource:immutable-mismatch",),
                    started_at_ms=started,
                ),
            )
        if resource_a.corrupt_detail:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                    canonical_record=terminate_b,
                    expected={"settlement": "VALID"},
                    observed={"detail": resource_a.corrupt_detail},
                    refs=(resource_a.corrupt_detail,),
                    started_at_ms=started,
                ),
            )
        receipt = resource_a.receipt
        if receipt is None or receipt.state != _RESERVATION_STATE_SETTLED:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.WARNING,
                    reason=ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE,
                    canonical_record=terminate_b,
                    expected={"settlement_state": _RESERVATION_STATE_SETTLED},
                    observed={"settlement_state": None if receipt is None else receipt.state},
                    refs=("settlement:applicability-unresolved",),
                    started_at_ms=started,
                ),
            )
        try:
            expected_settlement = _AdmissionResourceSettlementInput(
                run_create_operation_id=reservation.operation_id,
                run_id=reservation.run_id,
                tenant_id=reservation.tenant_id,
                estimated_cost_cents=reservation.estimated_cost_cents,
                run_create_reservation_proof_sha256=reservation.proof_sha256,
                run_terminate_operation_id=terminate_b.operation_id,
                terminal_proof_sha256=proof_a.proof_sha256,
                canonical_transition_id=terminate_b.transition_id,
                canonical_record_hash=terminate_b.canonical_record_hash,
                canonical_command_hash=terminate_b.canonical_command_hash,
                canonical_revision=terminate_b.to_revision,
                final_state=terminate_b.next_state,
            )
        except ValueError as exc:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.BLOCKED_EVIDENCE,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
                    canonical_record=terminate_b,
                    expected={"settlement_input": "VALID"},
                    observed={"detail": str(exc)},
                    refs=(str(exc),),
                    started_at_ms=started,
                ),
            )
        settlement_matches = all(
            (
                receipt.operation_id == expected_settlement.run_create_operation_id,
                receipt.run_id == expected_settlement.run_id,
                receipt.tenant_id == expected_settlement.tenant_id,
                receipt.estimated_cost_cents == expected_settlement.estimated_cost_cents,
                receipt.proof_sha256 == expected_settlement.run_create_reservation_proof_sha256,
                receipt.run_terminate_operation_id == expected_settlement.run_terminate_operation_id,
                receipt.terminal_proof_sha256 == expected_settlement.terminal_proof_sha256,
                receipt.canonical_transition_id == expected_settlement.canonical_transition_id,
                receipt.canonical_record_hash == expected_settlement.canonical_record_hash,
                receipt.canonical_command_hash == expected_settlement.canonical_command_hash,
                receipt.canonical_revision == expected_settlement.canonical_revision,
                receipt.final_state == expected_settlement.final_state,
                receipt.settlement_proof_sha256 == expected_settlement.proof_sha256,
            )
        )
        if not settlement_matches:
            return (
                self._finding(
                    aggregate_type=AggregateType.RUN.value,
                    run_id=run_id,
                    task_id=None,
                    contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                    check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                    status=ReconciliationStatus.DRIFT,
                    severity=ReconciliationSeverity.CRITICAL,
                    reason=ReconciliationReason.RESOURCE_PROOF_MISMATCH,
                    canonical_record=terminate_b,
                    expected={
                        "run_terminate_operation_id": expected_settlement.run_terminate_operation_id,
                        "terminal_proof_sha256": expected_settlement.terminal_proof_sha256,
                        "canonical_record_hash": expected_settlement.canonical_record_hash,
                        "settlement_proof_sha256": expected_settlement.proof_sha256,
                    },
                    observed={
                        "run_terminate_operation_id": receipt.run_terminate_operation_id,
                        "terminal_proof_sha256": receipt.terminal_proof_sha256,
                        "canonical_record_hash": receipt.canonical_record_hash,
                        "settlement_proof_sha256": receipt.settlement_proof_sha256,
                    },
                    refs=(f"settlement:{receipt.settlement_proof_sha256}",),
                    started_at_ms=started,
                ),
            )
        return (
            self._finding(
                aggregate_type=AggregateType.RUN.value,
                run_id=run_id,
                task_id=None,
                contract_id=RUN_TERMINATE_RESOURCE_CONTRACT_ID,
                check_class=ReconciliationCheckClass.CROSS_AGGREGATE_INVARIANT,
                status=ReconciliationStatus.CONSISTENT,
                severity=ReconciliationSeverity.INFO,
                reason=ReconciliationReason.CONSISTENT,
                canonical_record=terminate_b,
                expected={
                    "run_terminate_operation_id": expected_settlement.run_terminate_operation_id,
                    "terminal_proof_sha256": expected_settlement.terminal_proof_sha256,
                    "canonical_record_hash": expected_settlement.canonical_record_hash,
                    "settlement_proof_sha256": expected_settlement.proof_sha256,
                },
                observed={
                    "run_terminate_operation_id": receipt.run_terminate_operation_id,
                    "terminal_proof_sha256": receipt.terminal_proof_sha256,
                    "canonical_record_hash": receipt.canonical_record_hash,
                    "settlement_proof_sha256": receipt.settlement_proof_sha256,
                },
                refs=(f"settlement:{receipt.settlement_proof_sha256}",),
                started_at_ms=started,
            ),
        )

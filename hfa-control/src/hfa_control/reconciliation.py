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

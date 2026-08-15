from __future__ import annotations

import inspect

import pytest

from hfa_control.reconciliation import (
    ReconciliationCanonicalReference,
    ReconciliationCheckClass,
    ReconciliationEvidenceError,
    ReconciliationEvidenceState,
    ReconciliationFinding,
    ReconciliationObservation,
    ReconciliationReason,
    ReconciliationRedisReader,
    ReconciliationSeverity,
    ReconciliationStatus,
    TASK_REQUEUE_CONTRACT_VERSION,
    TASK_REQUEUE_CURRENT_CONTRACT_ID,
    TASK_REQUEUE_HISTORICAL_CONTRACT_ID,
    TaskRequeueCanonicalEvidence,
    TaskRequeueCanonicalReader,
    TaskRequeueReconciler,
    TaskRequeueRuntimeEvidence,
    TaskRequeueRuntimeReader,
)



def _canonical() -> TaskRequeueCanonicalEvidence:
    return TaskRequeueCanonicalEvidence(
        task_id="task-85",
        run_id="run-85",
        tenant_id="tenant-85",
        revision=4,
        transition_id="transition-4",
        record_hash="a" * 64,
        command_hash="b" * 64,
        operation_id="task-requeue:v1:identity:claim:1",
        committed_at_ms=5000,
        reason_code="TASK_STALE_DETECTED",
        retry_attempt=1,
        claim_epoch=1,
        dispatch_attempt=1,
        claim_transition_id="transition-3",
        claim_record_hash="c" * 64,
        claim_command_hash="d" * 64,
        claim_revision=3,
        claim_operation_id="task-claim:v1:identity:attempt:1",
        projection_intents_json="[]",
    )


def _runtime(**overrides) -> TaskRequeueRuntimeEvidence:
    c = _canonical()
    meta = {
        "task_id": c.task_id,
        "run_id": c.run_id,
        "tenant_id": c.tenant_id,
        "requeue_count": "1",
        "last_requeue_reason": c.reason_code,
        "last_requeue_at_ms": "5000",
        "worker_instance_id": "",
        "scheduler_epoch": "",
        "last_heartbeat_at_ms": "0",
        "canonical_transition_id": c.transition_id,
        "canonical_record_hash": c.record_hash,
        "canonical_command_hash": c.command_hash,
        "canonical_revision": "4",
        "canonical_operation_id": c.operation_id,
        "requeue_canonical_transition_id": c.transition_id,
        "requeue_canonical_record_hash": c.record_hash,
        "requeue_canonical_command_hash": c.command_hash,
        "requeue_canonical_revision": "4",
        "requeue_canonical_operation_id": c.operation_id,
        "requeue_notification_operation_id": c.operation_id,
    }
    values = dict(
        task_state_type="string",
        task_state="ready",
        task_meta_type="hash",
        task_meta=tuple(sorted(meta.items())),
        ready_queue_type="zset",
        ready_score=5000.0,
        running_index_type="none",
        running_score=None,
    )
    values.update(overrides)
    return TaskRequeueRuntimeEvidence(**values)


class _CanonicalReader:
    def __init__(self, value=None, error: ReconciliationEvidenceError | None = None):
        self.value = value or _canonical()
        self.error = error

    async def read_task_requeue_head(self, *, task_id: str, run_id: str):
        if self.error is not None:
            raise self.error
        return self.value


class _RuntimeReader:
    def __init__(self, value=None):
        self.value = value or _runtime()

    async def read_task_requeue_projection(self, *, task_id: str, tenant_id: str):
        return self.value


def _clock():
    values = iter((100, 101, 102, 103, 104))
    return lambda: next(values)


def test_status_vocabulary_is_exact():
    assert [x.value for x in ReconciliationStatus] == [
        "CONSISTENT",
        "DRIFT",
        "BLOCKED_EVIDENCE",
    ]


def test_severity_is_separate_exact_vocabulary():
    assert [x.value for x in ReconciliationSeverity] == ["INFO", "WARNING", "CRITICAL"]
    assert set(ReconciliationSeverity) != set(ReconciliationStatus)


def test_check_class_vocabulary_is_exact():
    assert [x.value for x in ReconciliationCheckClass] == [
        "CURRENT_PROJECTION",
        "HISTORICAL_DURABLE_EFFECT",
        "CROSS_AGGREGATE_INVARIANT",
    ]


def test_task_requeue_contract_ids_and_version_are_stable():
    assert TASK_REQUEUE_CURRENT_CONTRACT_ID == "TASK_REQUEUE_CURRENT_PROJECTION"
    assert TASK_REQUEUE_HISTORICAL_CONTRACT_ID == "TASK_REQUEUE_HISTORICAL_DURABLE_EFFECT"
    assert TASK_REQUEUE_CONTRACT_VERSION == 1


def test_finding_semantic_key_excludes_observation_timestamps():
    common = dict(
        aggregate_type="task",
        run_id="run-1",
        task_id="task-1",
        check_class=ReconciliationCheckClass.CURRENT_PROJECTION,
        contract_id=TASK_REQUEUE_CURRENT_CONTRACT_ID,
        contract_version=1,
        status=ReconciliationStatus.CONSISTENT,
        severity=ReconciliationSeverity.INFO,
        reason_code=ReconciliationReason.CONSISTENT,
        canonical=ReconciliationCanonicalReference(4, "TASK_REQUEUE", "ready", "t", "a" * 64, "b" * 64, "op", 5000),
        expected=(("task_state", "ready"),),
        observed=(("task_state", "ready"),),
        evidence=ReconciliationEvidenceState(True, True, True, True),
        mutation_attempted=False,
    )
    first = ReconciliationFinding(**common, observation=ReconciliationObservation(1, 2))
    second = ReconciliationFinding(**common, observation=ReconciliationObservation(100, 200))
    assert first.semantic_key() == second.semantic_key()
    assert first.to_dict()["mutation_attempted"] is False


def test_read_only_protocols_and_adapter_expose_no_known_mutation_methods():
    forbidden = {"set", "hset", "delete", "zadd", "zrem", "xadd", "expire", "eval", "evalsha", "commit"}
    for cls in (TaskRequeueCanonicalReader, TaskRequeueRuntimeReader, ReconciliationRedisReader):
        public = {name for name, _ in inspect.getmembers(cls) if not name.startswith("_")}
        assert public.isdisjoint(forbidden), (cls.__name__, public & forbidden)


@pytest.mark.asyncio
async def test_unknown_canonical_evidence_never_becomes_drift():
    reader = _CanonicalReader(
        error=ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE,
            detail="missing",
        )
    )
    reconciler = TaskRequeueReconciler(
        canonical_reader=reader,
        runtime_reader=_RuntimeReader(),
        clock_ms=_clock(),
    )
    findings = await reconciler.reconcile(task_id="task-85", run_id="run-85")
    assert {f.status for f in findings} == {ReconciliationStatus.BLOCKED_EVIDENCE}
    assert {f.reason_code for f in findings} == {ReconciliationReason.CANONICAL_EVIDENCE_UNAVAILABLE}


@pytest.mark.asyncio
async def test_proven_schema_mismatch_is_drift_not_blocked():
    runtime = _runtime(ready_queue_type="hash", ready_score=None)
    reconciler = TaskRequeueReconciler(
        canonical_reader=_CanonicalReader(),
        runtime_reader=_RuntimeReader(runtime),
        clock_ms=_clock(),
    )
    findings = await reconciler.reconcile(task_id="task-85", run_id="run-85")
    schema = [f for f in findings if f.reason_code is ReconciliationReason.PROJECTION_SCHEMA_MISMATCH]
    assert schema
    assert all(f.status is ReconciliationStatus.DRIFT for f in schema)
    assert all(f.severity is ReconciliationSeverity.CRITICAL for f in schema)


@pytest.mark.asyncio
async def test_canonical_corruption_is_blocked_evidence():
    reader = _CanonicalReader(
        error=ReconciliationEvidenceError(
            reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
            detail="bad hash",
        )
    )
    reconciler = TaskRequeueReconciler(
        canonical_reader=reader,
        runtime_reader=_RuntimeReader(),
        clock_ms=_clock(),
    )
    findings = await reconciler.reconcile(task_id="task-85", run_id="run-85")
    assert all(f.status is ReconciliationStatus.BLOCKED_EVIDENCE for f in findings)
    assert all(f.severity is ReconciliationSeverity.CRITICAL for f in findings)


@pytest.mark.asyncio
async def test_current_projection_and_historical_effect_are_separate_findings():
    reconciler = TaskRequeueReconciler(
        canonical_reader=_CanonicalReader(),
        runtime_reader=_RuntimeReader(),
        clock_ms=_clock(),
    )
    findings = await reconciler.reconcile(task_id="task-85", run_id="run-85")
    assert len(findings) == 2
    assert {f.check_class for f in findings} == {
        ReconciliationCheckClass.CURRENT_PROJECTION,
        ReconciliationCheckClass.HISTORICAL_DURABLE_EFFECT,
    }
    assert all(f.status is ReconciliationStatus.CONSISTENT for f in findings)
    assert all(f.mutation_attempted is False for f in findings)


# Sprint 85.0B current-head reconciliation contracts.
from dataclasses import replace as _dc_replace

from hfa.authority import OperationType
from hfa_control.reconciliation import (
    CURRENT_HEAD_RECONCILIATION_CONTRACT_VERSION,
    CurrentHeadCanonicalEvidence,
    CurrentHeadCanonicalReader,
    CurrentHeadReconciler,
    CurrentHeadRuntimeEvidence,
    CurrentHeadRuntimeReader,
    ProjectionRule,
    ReconciliationReason,
    _CURRENT_HEAD_OPERATIONS,
    _task_admit_dependency_projection_shape,
)


def _head_canonical(operation: OperationType = OperationType.TASK_CLAIM):
    return CurrentHeadCanonicalEvidence(
        aggregate_type="TASK",
        operation_type=operation,
        run_id="run-85b",
        task_id="task-85b",
        tenant_id="tenant-85b",
        previous_state="scheduled",
        state="running",
        revision=3,
        transition_id="transition-85b",
        record_hash="a" * 64,
        command_hash="b" * 64,
        operation_id="operation-85b",
        committed_at_ms=3000,
        rules=(
            ProjectionRule(
                field="task_state",
                expected="running",
                reason=ReconciliationReason.TASK_STATE_MISMATCH,
                severity=ReconciliationSeverity.CRITICAL,
            ),
        ),
    )


def _head_runtime(state: str = "running"):
    return CurrentHeadRuntimeEvidence(
        operation_type=OperationType.TASK_CLAIM,
        fields=(("task_state", state),),
    )


class _CurrentCanonicalReader:
    def __init__(self, values):
        self.values = list(values)
        self.index = 0

    async def read_current_head(self, *, operation_type, run_id, task_id):
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        if isinstance(value, Exception):
            raise value
        return value


class _CurrentRuntimeReader:
    def __init__(self, values):
        self.values = list(values)
        self.index = 0

    async def read_current_projection(self, *, canonical):
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        if isinstance(value, Exception):
            raise value
        return value


def test_85_0b_current_head_operation_set_is_exact():
    assert _CURRENT_HEAD_OPERATIONS == frozenset(
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
    assert OperationType.TASK_REQUEUE not in _CURRENT_HEAD_OPERATIONS
    assert OperationType.TASK_HEARTBEAT not in _CURRENT_HEAD_OPERATIONS


def test_85_0b_reason_families_are_exactly_available_without_operation_aliases():
    approved = {
        "RUN_STATE_MISMATCH",
        "PROJECTION_VALUE_MISMATCH",
        "PROJECTION_MEMBERSHIP_MISMATCH",
        "PROJECTION_SCORE_MISMATCH",
        "TASK_OUTPUT_MISMATCH",
        "RUN_META_MISMATCH",
        "RUN_RESULT_MISMATCH",
        "OWNERSHIP_PROJECTION_MISMATCH",
        "DEPENDENCY_FANOUT_EVIDENCE_REQUIRED",
    }
    assert approved <= {item.value for item in ReconciliationReason}
    assert not any(name.endswith("_CANONICAL_PROOF_MISMATCH") for name in approved)


def test_85_0b_contract_version_is_frozen_at_one():
    assert CURRENT_HEAD_RECONCILIATION_CONTRACT_VERSION == 1


def test_85_0b_current_head_protocols_expose_no_mutation_methods():
    forbidden = {
        "set", "hset", "delete", "zadd", "zrem", "xadd", "expire", "eval", "evalsha",
        "commit", "initialise", "record_authority_conflict", "validate_authority_head",
        "project", "deliver", "requeue", "terminate", "settle_once",
    }
    for cls in (CurrentHeadCanonicalReader, CurrentHeadRuntimeReader):
        public = {name for name, _ in inspect.getmembers(cls) if not name.startswith("_")}
        assert public.isdisjoint(forbidden), (cls.__name__, public & forbidden)


@pytest.mark.asyncio
async def test_85_0b_consistent_current_head_returns_one_consistent_finding():
    canonical = _head_canonical()
    runtime = _head_runtime()
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([canonical, canonical]),
        runtime_reader=_CurrentRuntimeReader([runtime, runtime]),
        clock_ms=lambda: 10,
    ).reconcile(operation_type=OperationType.TASK_CLAIM, run_id=canonical.run_id, task_id=canonical.task_id)
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.CONSISTENT
    assert findings[0].check_class is ReconciliationCheckClass.CURRENT_PROJECTION
    assert findings[0].mutation_attempted is False


@pytest.mark.asyncio
async def test_85_0b_proven_current_projection_mismatch_is_drift():
    canonical = _head_canonical()
    runtime = _head_runtime("scheduled")
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([canonical, canonical]),
        runtime_reader=_CurrentRuntimeReader([runtime, runtime]),
        clock_ms=lambda: 10,
    ).reconcile(operation_type=OperationType.TASK_CLAIM, run_id=canonical.run_id, task_id=canonical.task_id)
    assert {f.status for f in findings} == {ReconciliationStatus.DRIFT}
    assert {f.reason_code for f in findings} == {ReconciliationReason.TASK_STATE_MISMATCH}


@pytest.mark.asyncio
async def test_85_0b_runtime_immutable_change_is_blocked_not_drift():
    canonical = _head_canonical()
    first = _head_runtime("running")
    second = _head_runtime("scheduled")
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([canonical, canonical]),
        runtime_reader=_CurrentRuntimeReader([first, second]),
        clock_ms=lambda: 10,
    ).reconcile(operation_type=OperationType.TASK_CLAIM, run_id=canonical.run_id, task_id=canonical.task_id)
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.PROJECTION_OBSERVATION_CHANGED


@pytest.mark.asyncio
async def test_85_0b_canonical_change_is_blocked_not_drift():
    canonical = _head_canonical()
    changed = _dc_replace(canonical, revision=4, transition_id="transition-next")
    runtime = _head_runtime()
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([canonical, changed]),
        runtime_reader=_CurrentRuntimeReader([runtime, runtime]),
        clock_ms=lambda: 10,
    ).reconcile(operation_type=OperationType.TASK_CLAIM, run_id=canonical.run_id, task_id=canonical.task_id)
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.CANONICAL_CHANGED_DURING_OBSERVATION


@pytest.mark.asyncio
async def test_85_0b_canonical_read_failure_is_blocked_evidence():
    error = ReconciliationEvidenceError(
        reason=ReconciliationReason.CANONICAL_RECORD_CORRUPTION,
        detail="corrupt",
    )
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([error]),
        runtime_reader=_CurrentRuntimeReader([_head_runtime()]),
        clock_ms=lambda: 10,
    ).reconcile(operation_type=OperationType.TASK_CLAIM, run_id="run-85b", task_id="task-85b")
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.CANONICAL_RECORD_CORRUPTION


@pytest.mark.asyncio
async def test_85_0b_unsupported_operation_is_rejected_explicitly():
    with pytest.raises(ValueError):
        await CurrentHeadReconciler(
            canonical_reader=_CurrentCanonicalReader([_head_canonical()]),
            runtime_reader=_CurrentRuntimeReader([_head_runtime()]),
        ).reconcile(
            operation_type=OperationType.TASK_REQUEUE,
            run_id="run-85b",
            task_id="task-85b",
        )


def _pending_admit_canonical():
    return CurrentHeadCanonicalEvidence(
        aggregate_type="TASK",
        operation_type=OperationType.TASK_ADMIT,
        run_id="run-admit-85b",
        task_id="task-admit-85b",
        tenant_id="tenant-admit-85b",
        previous_state=None,
        state="pending",
        revision=1,
        transition_id="admit-transition-85b",
        record_hash="c" * 64,
        command_hash="d" * 64,
        operation_id="task-admit-operation-85b",
        committed_at_ms=2000,
        rules=(
            ProjectionRule(
                field="task_state",
                expected="pending",
                reason=ReconciliationReason.TASK_STATE_MISMATCH,
                severity=ReconciliationSeverity.CRITICAL,
            ),
            ProjectionRule(
                field="remaining_deps",
                expected="2",
                reason=ReconciliationReason.PROJECTION_VALUE_MISMATCH,
                severity=ReconciliationSeverity.CRITICAL,
            ),
            ProjectionRule(
                field="ready_member",
                expected=False,
                reason=ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH,
                severity=ReconciliationSeverity.CRITICAL,
            ),
            ProjectionRule(
                field="meta.tenant_id",
                expected="tenant-admit-85b",
                reason=ReconciliationReason.TASK_META_IDENTITY_MISMATCH,
                severity=ReconciliationSeverity.CRITICAL,
            ),
            ProjectionRule(
                field="run_tasks_member",
                expected=True,
                reason=ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH,
                severity=ReconciliationSeverity.CRITICAL,
            ),
        ),
        key_hints=(("admit_state", "pending"), ("dependency_count", "2")),
    )


def _pending_admit_runtime(
    *,
    remaining: str,
    state: str = "pending",
    ready_member=False,
    ready_emitted=None,
    tenant_id: str = "tenant-admit-85b",
    run_tasks_member: bool = True,
):
    return CurrentHeadRuntimeEvidence(
        operation_type=OperationType.TASK_ADMIT,
        fields=(
            ("type.task_state", "string"),
            ("task_state", state),
            ("type.remaining_deps", "string"),
            ("remaining_deps", remaining),
            ("type.ready_queue", "zset" if ready_member else "none"),
            ("ready_member", ready_member),
            ("type.ready_emitted", "string" if ready_emitted is not None else "none"),
            ("ready_emitted", ready_emitted),
            ("meta.tenant_id", tenant_id),
            ("run_tasks_member", run_tasks_member),
        ),
    )


@pytest.mark.asyncio
async def test_85_0b_pending_task_admit_untouched_initial_postimage_is_consistent():
    canonical = _pending_admit_canonical()
    runtime = _pending_admit_runtime(remaining="2")
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([canonical, canonical]),
        runtime_reader=_CurrentRuntimeReader([runtime, runtime]),
        clock_ms=lambda: 10,
    ).reconcile(
        operation_type=OperationType.TASK_ADMIT,
        run_id=canonical.run_id,
        task_id=canonical.task_id,
    )
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.CONSISTENT


@pytest.mark.asyncio
async def test_85_0b_pending_task_admit_partial_fanout_fails_closed_warning():
    canonical = _pending_admit_canonical()
    runtime = _pending_admit_runtime(remaining="1")
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([canonical, canonical]),
        runtime_reader=_CurrentRuntimeReader([runtime, runtime]),
        clock_ms=lambda: 10,
    ).reconcile(
        operation_type=OperationType.TASK_ADMIT,
        run_id=canonical.run_id,
        task_id=canonical.task_id,
    )
    assert len(findings) == 1
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].severity is ReconciliationSeverity.WARNING
    assert (
        findings[0].reason_code
        is ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED
    )


# Sprint 85.0B R1.2 — narrow TASK_ADMIT fanout anti-masking correction.

def test_85_0b_r1_2_dependency_unlock_shape_requires_evidence():
    c = _pending_admit_canonical()
    r = _pending_admit_runtime(remaining="0", state="ready", ready_member=True, ready_emitted="1")
    assert _task_admit_dependency_projection_shape(c, dict(r.fields)) == "EVIDENCE_REQUIRED"


def test_85_0b_r1_2_failure_while_pending_shape_requires_evidence():
    c = _pending_admit_canonical()
    r = _pending_admit_runtime(remaining="2", state="blocked_by_failure")
    assert _task_admit_dependency_projection_shape(c, dict(r.fields)) == "EVIDENCE_REQUIRED"


def test_85_0b_r1_2_failure_after_ready_shape_requires_evidence():
    c = _pending_admit_canonical()
    r = _pending_admit_runtime(remaining="0", state="blocked_by_failure", ready_emitted="1")
    assert _task_admit_dependency_projection_shape(c, dict(r.fields)) == "EVIDENCE_REQUIRED"


def test_85_0b_r1_2_failure_shape_with_positive_remaining_and_ready_marker_is_impossible():
    c = _pending_admit_canonical()
    r = _pending_admit_runtime(remaining="1", state="blocked_by_failure", ready_emitted="1")
    assert _task_admit_dependency_projection_shape(c, dict(r.fields)) == "IMPOSSIBLE"


def test_85_0b_r1_2_failure_shape_with_zero_remaining_and_missing_marker_is_impossible():
    c = _pending_admit_canonical()
    r = _pending_admit_runtime(remaining="0", state="blocked_by_failure")
    assert _task_admit_dependency_projection_shape(c, dict(r.fields)) == "IMPOSSIBLE"


def test_85_0b_r1_2_pending_original_remaining_with_ready_marker_is_impossible():
    c = _pending_admit_canonical()
    r = _pending_admit_runtime(remaining="2", ready_emitted="1")
    assert _task_admit_dependency_projection_shape(c, dict(r.fields)) == "IMPOSSIBLE"


def test_85_0b_r1_2_ready_with_positive_remaining_is_impossible():
    c = _pending_admit_canonical()
    r = _pending_admit_runtime(remaining="1", state="ready", ready_member=True, ready_emitted="1")
    assert _task_admit_dependency_projection_shape(c, dict(r.fields)) == "IMPOSSIBLE"


def test_85_0b_r1_2_ready_without_ready_membership_is_impossible():
    c = _pending_admit_canonical()
    r = _pending_admit_runtime(remaining="0", state="ready", ready_member=False, ready_emitted="1")
    assert _task_admit_dependency_projection_shape(c, dict(r.fields)) == "IMPOSSIBLE"


@pytest.mark.asyncio
async def test_85_0b_r1_2_fanout_ambiguity_does_not_mask_identity_drift():
    canonical = _pending_admit_canonical()
    runtime = _pending_admit_runtime(remaining="1", tenant_id="corrupt-tenant")
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([canonical, canonical]),
        runtime_reader=_CurrentRuntimeReader([runtime, runtime]),
        clock_ms=lambda: 10,
    ).reconcile(
        operation_type=OperationType.TASK_ADMIT,
        run_id=canonical.run_id,
        task_id=canonical.task_id,
    )
    assert {f.status for f in findings} == {ReconciliationStatus.DRIFT}
    assert ReconciliationReason.TASK_META_IDENTITY_MISMATCH in {f.reason_code for f in findings}
    assert ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED not in {f.reason_code for f in findings}


@pytest.mark.asyncio
async def test_85_0b_r1_2_fanout_ambiguity_does_not_mask_run_membership_drift():
    canonical = _pending_admit_canonical()
    runtime = _pending_admit_runtime(
        remaining="0",
        state="ready",
        ready_member=True,
        ready_emitted="1",
        run_tasks_member=False,
    )
    findings = await CurrentHeadReconciler(
        canonical_reader=_CurrentCanonicalReader([canonical, canonical]),
        runtime_reader=_CurrentRuntimeReader([runtime, runtime]),
        clock_ms=lambda: 10,
    ).reconcile(
        operation_type=OperationType.TASK_ADMIT,
        run_id=canonical.run_id,
        task_id=canonical.task_id,
    )
    assert {f.status for f in findings} == {ReconciliationStatus.DRIFT}
    assert ReconciliationReason.PROJECTION_MEMBERSHIP_MISMATCH in {f.reason_code for f in findings}
    assert ReconciliationReason.DEPENDENCY_FANOUT_EVIDENCE_REQUIRED not in {f.reason_code for f in findings}

# Sprint 85.0C — read-only historical/cross-aggregate contracts.
from types import SimpleNamespace as _SimpleNamespace

from hfa.authority import (
    AggregateType as _85cAggregateType,
    AuthorityDecisionCode as _85cAuthorityDecisionCode,
    AuthorityEntryContext as _85cAuthorityEntryContext,
    CanonicalAggregateIdentity as _85cCanonicalAggregateIdentity,
    OperationType as _85cOperationType,
    evaluate_authority_commit as _85c_evaluate_authority_commit,
)
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationReceipt as _85cReservationReceipt,
    RESERVATION_STATE_FINALIZED as _85c_FINALIZED,
    RESERVATION_STATE_RELEASED as _85c_RELEASED,
    RESERVATION_STATE_RESERVED as _85c_RESERVED,
    RESERVATION_STATE_SETTLED as _85c_SETTLED,
)
from hfa_control.reconciliation import (
    HISTORICAL_RECONCILIATION_CONTRACT_VERSION,
    HistoricalCrossAggregateReconciler,
    OperationReachability,
    ResourceReceiptEvidence,
    RUN_CREATE_RESOURCE_CONTRACT_ID,
    RUN_TERMINATE_PROOF_CONTRACT_ID,
    RUN_TERMINATE_RESOURCE_CONTRACT_ID,
    operation_reachability,
)
from hfa_control.run_create_authority import (
    WRITER_ID as _85c_RUN_CREATE_WRITER_ID,
    RunCreateAuthorityInput as _85cRunCreateAuthorityInput,
    build_run_create_command as _85c_build_run_create_command,
    resource_reservation_from_run_create_record as _85c_resource_from_record,
)
from hfa_control.run_terminate_authority import (
    WRITER_ID as _85c_RUN_TERMINATE_WRITER_ID,
    TerminalAggregateProof as _85cTerminalAggregateProof,
    TerminalTaskEvidence as _85cTerminalTaskEvidence,
    build_run_terminate_command as _85c_build_run_terminate_command,
    run_terminate_operation_id as _85c_run_terminate_operation_id,
)


def _85c_record(command, *, revision: int, state: str | None, at_ms: int):
    writer_id = (
        _85c_RUN_CREATE_WRITER_ID
        if command.operation_type is _85cOperationType.RUN_CREATE
        else _85c_RUN_TERMINATE_WRITER_ID
        if command.operation_type is _85cOperationType.RUN_TERMINATE
        else f"test/{command.operation_type.value}"
    )
    context = _85cAuthorityEntryContext(
        authenticated_writer_id=writer_id,
        allowed_operations=frozenset({command.operation_type}),
        target_aggregate_identity_sha256=command.aggregate_identity.sha256,
        fence_required=False,
        fence_valid=True,
    )
    evaluation = _85c_evaluate_authority_commit(
        context=context,
        command=command,
        current_revision=revision,
        current_state=state,
        receipt_probe=None,
        committed_at_ms=at_ms,
        correlation_id=None,
    )
    assert evaluation.decision.code is _85cAuthorityDecisionCode.ACCEPTED
    assert evaluation.commit_plan is not None
    return evaluation.commit_plan.record


def _85c_create_record(run_id: str = "run-85c"):
    value = _85cRunCreateAuthorityInput(
        run_id=run_id,
        tenant_id="tenant-85c",
        agent_type="agent",
        priority=1,
        payload={"sprint": "85.0C"},
        estimated_cost_cents=7,
        preferred_region="",
        preferred_placement="LEAST_LOADED",
        created_at_ms=1000,
        control_stream="hfa:stream:control",
    )
    return _85c_record(
        _85c_build_run_create_command(value), revision=0, state=None, at_ms=1000
    )


def _85c_proof(
    run_id: str = "run-85c",
    *,
    states=("done", "blocked_by_failure", "skipped"),
):
    tasks = tuple(
        _85cTerminalTaskEvidence(task_id=f"task-{index:02d}", state=state)
        for index, state in enumerate(states, start=1)
    )
    done = sum(row.state == "done" for row in tasks)
    skipped = sum(row.state == "skipped" for row in tasks)
    failed = len(tasks) - done - skipped
    final_state = "failed" if failed else "done"
    return _85cTerminalAggregateProof(
        schema_version=1,
        run_id=run_id,
        tenant_id="tenant-85c",
        tasks=tasks,
        task_count=len(tasks),
        done_count=done,
        failed_count=failed,
        skipped_count=skipped,
        final_state=final_state,
        proof_sha256="a" * 64,
        proof_payload_json="{}",
        finalized_at_ms=2000,
        worker_instance_id="worker-85c",
        trigger_task_id=tasks[0].task_id,
        trigger_terminal_state=tasks[0].state,
        canonical_expected_revision=1,
        canonical_previous_state="pending",
    )


def _85c_terminate_record(proof=None):
    proof = proof or _85c_proof()
    return _85c_record(
        _85c_build_run_terminate_command(proof),
        revision=1,
        state="pending",
        at_ms=2000,
    )


class _85cHistory:
    def __init__(self, *records, fingerprint="stable"):
        self.operations = tuple(
            _SimpleNamespace(revision=r.to_revision, operation_digest=f"digest-{i}", record=r, receipt=None)
            for i, r in enumerate(records, start=1)
        )
        self.fingerprint = (fingerprint, tuple(r.record.operation_id for r in self.operations))


class _85cHistoryReader:
    def __init__(self, values):
        self.values = list(values)
        self.index = 0

    async def read_history(self, identity):
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        if isinstance(value, Exception):
            raise value
        return value


class _85cProofReader:
    def __init__(self, values):
        self.values = list(values)
        self.index = 0

    async def read_terminal_proof(self, *, run_id):
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        if isinstance(value, Exception):
            raise value
        return value


class _85cResourceReader:
    def __init__(self, values):
        self.values = list(values)
        self.index = 0

    async def read_reservation_receipt(self, reservation):
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        if isinstance(value, Exception):
            raise value
        return value


def _85c_resource_evidence(record, state: str, **overrides):
    reservation = _85c_resource_from_record(record)
    values = dict(
        operation_id=reservation.operation_id,
        run_id=reservation.run_id,
        tenant_id=reservation.tenant_id,
        estimated_cost_cents=reservation.estimated_cost_cents,
        proof_sha256=reservation.proof_sha256,
        reservation_version=reservation.reservation_version,
        state=state,
        created_at_ms=900,
        finalized_at_ms=1100 if state in {_85c_FINALIZED, _85c_SETTLED} else None,
        released_at_ms=1100 if state == _85c_RELEASED else None,
        settled_at_ms=None,
    )
    values.update(overrides)
    receipt = _85cReservationReceipt(**values)
    raw = tuple(sorted({
        "operation_id": receipt.operation_id,
        "run_id": receipt.run_id,
        "tenant_id": receipt.tenant_id,
        "state": receipt.state,
        "proof_sha256": receipt.proof_sha256,
    }.items()))
    return ResourceReceiptEvidence("hash", raw, receipt)


def test_85_0c_contract_version_is_frozen():
    assert HISTORICAL_RECONCILIATION_CONTRACT_VERSION == 1


def test_85_0c_reachability_matrix_is_exact():
    expected = {
        _85cOperationType.RUN_CREATE: OperationReachability.DERIVABLE_FROM_CURRENT_TRUTH,
        _85cOperationType.TASK_ADMIT: OperationReachability.DERIVABLE_FROM_CURRENT_TRUTH,
        _85cOperationType.TASK_DISPATCH: OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        _85cOperationType.TASK_CLAIM: OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        _85cOperationType.TASK_COMPLETE: OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        _85cOperationType.TASK_FAIL: OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        _85cOperationType.TASK_REQUEUE: OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
        _85cOperationType.RUN_TERMINATE: OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID,
    }
    for operation, facade in expected.items():
        spec = operation_reachability(operation)
        assert spec.durable_data is OperationReachability.DISCOVERABLE_BY_EXISTING_INDEX
        assert spec.current_facade is facade


def test_85_0c_run_terminate_reachability_requires_proof_sha():
    without = operation_reachability(_85cOperationType.RUN_TERMINATE)
    with_proof = operation_reachability(
        _85cOperationType.RUN_TERMINATE, terminal_proof_available=True
    )
    assert without.operation_id_dependency == "run_id+terminal_proof_sha256"
    assert without.current_facade is OperationReachability.REACHABLE_ONLY_IF_CALLER_SUPPLIES_ID
    assert with_proof.current_facade is OperationReachability.DERIVABLE_FROM_CURRENT_TRUTH
    proof = _85c_proof()
    assert _85c_run_terminate_operation_id(proof.run_id, proof.proof_sha256) == _85c_terminate_record(proof).operation_id


def test_85_0c_unknown_operation_reachability_is_rejected():
    with pytest.raises(ValueError):
        operation_reachability(_85cOperationType.TASK_HEARTBEAT)


@pytest.mark.asyncio
async def test_85_0c_exact_run_terminate_record_proof_binding_is_consistent():
    proof = _85c_proof(states=("done", "blocked_by_failure", "skipped"))
    record = _85c_terminate_record(proof)
    history = _85cHistory(record)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        terminal_proof_reader=_85cProofReader([proof, proof]),
        clock_ms=lambda: 10,
    ).reconcile_run_terminate_proof(run_id=proof.run_id)
    assert len(findings) == 1
    assert findings[0].contract_id == RUN_TERMINATE_PROOF_CONTRACT_ID
    assert findings[0].status is ReconciliationStatus.CONSISTENT


@pytest.mark.asyncio
async def test_85_0c_runtime_terminal_vocabulary_does_not_require_canonical_task_mapping():
    states = (
        "done", "failed", "blocked_by_failure", "dead_lettered",
        "rejected", "cancelled", "skipped",
    )
    proof = _85c_proof(states=states)
    record = _85c_terminate_record(proof)
    history = _85cHistory(record)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        terminal_proof_reader=_85cProofReader([proof, proof]),
        clock_ms=lambda: 10,
    ).reconcile_run_terminate_proof(run_id=proof.run_id)
    assert {f.status for f in findings} == {ReconciliationStatus.CONSISTENT}
    source = inspect.getsource(HistoricalCrossAggregateReconciler)
    assert "run_tasks" not in source
    assert "read_current_head" not in source


@pytest.mark.asyncio
async def test_85_0c_terminal_proof_positive_contradiction_is_drift():
    proof = _85c_proof()
    record = _85c_terminate_record(proof)
    bad_terminal = dict(record.authoritative_metadata_changes["terminal_evidence"])
    bad_terminal["task_count"] = proof.task_count + 1
    bad_metadata = dict(record.authoritative_metadata_changes)
    bad_metadata["terminal_evidence"] = bad_terminal
    bad_record = _SimpleNamespace(**record.__dict__)
    bad_record.authoritative_metadata_changes = bad_metadata
    history = _85cHistory(bad_record)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        terminal_proof_reader=_85cProofReader([proof, proof]),
        clock_ms=lambda: 10,
    ).reconcile_run_terminate_proof(run_id=proof.run_id)
    assert findings[0].status is ReconciliationStatus.DRIFT
    assert findings[0].reason_code is ReconciliationReason.CANONICAL_PROOF_MISMATCH


@pytest.mark.asyncio
async def test_85_0c_terminal_proof_race_blocks_not_drifts():
    first = _85c_proof()
    second = _85cTerminalAggregateProof(**{**first.__dict__, "proof_sha256": "b" * 64})
    record = _85c_terminate_record(first)
    history = _85cHistory(record)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        terminal_proof_reader=_85cProofReader([first, second]),
        clock_ms=lambda: 10,
    ).reconcile_run_terminate_proof(run_id=first.run_id)
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "status", "reason"),
    [
        (_85c_RESERVED, ReconciliationStatus.BLOCKED_EVIDENCE, ReconciliationReason.RESOURCE_FINALIZATION_PENDING),
        (_85c_FINALIZED, ReconciliationStatus.CONSISTENT, ReconciliationReason.CONSISTENT),
        (_85c_SETTLED, ReconciliationStatus.CONSISTENT, ReconciliationReason.CONSISTENT),
        (_85c_RELEASED, ReconciliationStatus.DRIFT, ReconciliationReason.RESOURCE_PROOF_MISMATCH),
    ],
)
async def test_85_0c_run_create_resource_state_contract(state, status, reason):
    create = _85c_create_record()
    history = _85cHistory(create)
    resource = _85c_resource_evidence(create, state)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        resource_reader=_85cResourceReader([resource, resource]),
        clock_ms=lambda: 10,
    ).reconcile_run_create_resource(run_id=create.aggregate_identity.run_id)
    assert findings[0].contract_id == RUN_CREATE_RESOURCE_CONTRACT_ID
    assert findings[0].status is status
    assert findings[0].reason_code is reason


@pytest.mark.asyncio
async def test_85_0c_resource_immutable_mismatch_is_drift():
    create = _85c_create_record()
    history = _85cHistory(create)
    good = _85c_resource_evidence(create, _85c_FINALIZED)
    bad = ResourceReceiptEvidence(
        redis_type="hash",
        raw_fingerprint=good.raw_fingerprint,
        receipt=None,
        immutable_mismatch=True,
    )
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        resource_reader=_85cResourceReader([bad, bad]),
        clock_ms=lambda: 10,
    ).reconcile_run_create_resource(run_id=create.aggregate_identity.run_id)
    assert findings[0].status is ReconciliationStatus.DRIFT
    assert findings[0].reason_code is ReconciliationReason.RESOURCE_PROOF_MISMATCH


@pytest.mark.asyncio
async def test_85_0c_resource_race_blocks_not_drifts():
    create = _85c_create_record()
    history = _85cHistory(create)
    first = _85c_resource_evidence(create, _85c_RESERVED)
    second = _85c_resource_evidence(create, _85c_FINALIZED)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        resource_reader=_85cResourceReader([first, second]),
        clock_ms=lambda: 10,
    ).reconcile_run_create_resource(run_id=create.aggregate_identity.run_id)
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION


@pytest.mark.asyncio
async def test_85_0c_missing_settlement_with_unresolved_applicability_blocks():
    create = _85c_create_record()
    proof = _85c_proof(run_id=create.aggregate_identity.run_id, states=("done",))
    terminate = _85c_terminate_record(proof)
    history = _85cHistory(create, terminate)
    finalized = _85c_resource_evidence(create, _85c_FINALIZED)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        terminal_proof_reader=_85cProofReader([proof, proof]),
        resource_reader=_85cResourceReader([finalized, finalized]),
        clock_ms=lambda: 10,
    ).reconcile_run_terminate_settlement(run_id=create.aggregate_identity.run_id)
    assert findings[0].contract_id == RUN_TERMINATE_RESOURCE_CONTRACT_ID
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE


def test_85_0c_reconcilers_expose_no_mutation_surface():
    forbidden = {
        "set", "hset", "hdel", "delete", "expire", "xadd", "eval", "evalsha",
        "commit", "record_authority_conflict", "reserve_once", "finalize_once",
        "release_once", "settle_once", "capture", "project", "repair", "replay",
    }
    public = {
        name for name, _ in inspect.getmembers(HistoricalCrossAggregateReconciler)
        if not name.startswith("_")
    }
    assert public.isdisjoint(forbidden)

# Sprint 85.0C R1.1 — exact terminal-proof semantics and evidence taxonomy.
from dataclasses import replace as _85c_replace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("finalized_at_ms", 2001),
        ("worker_instance_id", "wrong-worker"),
        ("trigger_task_id", "wrong-task"),
        ("trigger_terminal_state", "failed"),
    ],
)
async def test_85_0c_r1_1_terminal_proof_metadata_binding_contradiction_drifts(field, value):
    proof = _85c_proof()
    record = _85c_terminate_record(proof)
    bad_metadata = dict(record.authoritative_metadata_changes)
    bad_metadata[field] = value
    bad_record = _SimpleNamespace(**record.__dict__)
    bad_record.authoritative_metadata_changes = bad_metadata
    history = _85cHistory(bad_record)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        terminal_proof_reader=_85cProofReader([proof, proof]),
        clock_ms=lambda: 10,
    ).reconcile_run_terminate_proof(run_id=proof.run_id)
    assert findings[0].status is ReconciliationStatus.DRIFT
    assert findings[0].reason_code is ReconciliationReason.CANONICAL_PROOF_MISMATCH


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("from_revision", 0),
        ("to_revision", 3),
        ("previous_state", "running"),
    ],
)
async def test_85_0c_r1_1_terminal_proof_revision_state_continuity_contradiction_drifts(field, value):
    proof = _85c_proof()
    record = _85c_terminate_record(proof)
    bad_record = _SimpleNamespace(**record.__dict__)
    setattr(bad_record, field, value)
    history = _85cHistory(bad_record)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        terminal_proof_reader=_85cProofReader([proof, proof]),
        clock_ms=lambda: 10,
    ).reconcile_run_terminate_proof(run_id=proof.run_id)
    assert findings[0].status is ReconciliationStatus.DRIFT
    assert findings[0].reason_code is ReconciliationReason.CANONICAL_PROOF_MISMATCH


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("worker_instance_id", "worker-raced"),
        ("finalized_at_ms", 2001),
        ("trigger_task_id", "task-raced"),
    ],
)
async def test_85_0c_r1_1_complete_terminal_proof_fingerprint_race_blocks(field, value):
    first = _85c_proof()
    second = _85c_replace(first, **{field: value})
    record = _85c_terminate_record(first)
    history = _85cHistory(record)
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        terminal_proof_reader=_85cProofReader([first, second]),
        clock_ms=lambda: 10,
    ).reconcile_run_terminate_proof(run_id=first.run_id)
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.DURABLE_EVIDENCE_CHANGED_DURING_OBSERVATION


@pytest.mark.asyncio
async def test_85_0c_r1_1_missing_resource_immutable_field_blocks_not_drifts():
    create = _85c_create_record()
    history = _85cHistory(create)
    evidence = ResourceReceiptEvidence(
        redis_type="hash",
        raw_fingerprint=(("operation_id", create.operation_id),),
        receipt=None,
        missing_immutable_fields=("tenant_id",),
    )
    findings = await HistoricalCrossAggregateReconciler(
        canonical_reader=_85cHistoryReader([history, history]),
        resource_reader=_85cResourceReader([evidence, evidence]),
        clock_ms=lambda: 10,
    ).reconcile_run_create_resource(run_id=create.aggregate_identity.run_id)
    assert findings[0].status is ReconciliationStatus.BLOCKED_EVIDENCE
    assert findings[0].reason_code is ReconciliationReason.RESOURCE_EFFECT_EVIDENCE_UNAVAILABLE

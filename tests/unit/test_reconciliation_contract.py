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

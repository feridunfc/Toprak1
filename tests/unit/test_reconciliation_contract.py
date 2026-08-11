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

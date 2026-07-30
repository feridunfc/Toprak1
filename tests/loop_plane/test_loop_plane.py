from dataclasses import replace
import pytest

from hfa.loop_plane import (
    CanonicalCommandProposal, CommandReceipt, CriterionResult,
    InMemoryPrototypeLoopStore, LoopContract, LoopEvent, LoopMode,
    LoopPlaneService, Outcome, Recommendation, RuntimeEventTranslator,
    TranslationStatus, canonical_hash,
)
from hfa.loop_plane.errors import (
    DomainTransitionError, IdempotencyConflict, InvalidEventStream,
    ModeViolation, ReceiptConflict,
)

H = "a" * 64
E = "b" * 64


def contract():
    return LoopContract("c1", "1", "p1", ("quality",), 2, 1)


def admitted(event_id="a1"):
    return dict(event_id=event_id, event_type="TaskAdmitted", run_id="r1", task_id="t1", canonical_operation_id="op-admit", source_transition_id="tr-admit", source_task_revision=0, occurred_at_ms=1, correlation_id="corr", causation_id="cause")


def claimed(event_id="c1", generation=1):
    return dict(event_id=event_id, event_type="TaskClaimed", run_id="r1", task_id="t1", canonical_operation_id="op-claim", source_transition_id="tr-claim", source_task_revision=1, occurred_at_ms=2, correlation_id="corr", causation_id="cause", execution_generation=generation, claim_fence="f1", input_hash=H)


def completed(event_id="d1"):
    return dict(event_id=event_id, event_type="TaskCompleted", run_id="r1", task_id="t1", canonical_operation_id="op-complete", source_transition_id="tr-complete", source_task_revision=2, occurred_at_ms=3, correlation_id="corr", causation_id="cause", artifact_or_output_ref="artifact:1")


def criterion(outcome=Outcome.PASS, attempt_id="attempt:t1:1:f1", produced=10, refs=("e1",)):
    return CriterionResult("quality", outcome, "loop:r1", attempt_id, "t1", "r1", H, E, "validator", "v1", "principal", refs, E, produced, 100, "tr-complete", 2, "op-claim", "f1", 1)


def started_service(mode=LoopMode.PROPOSE):
    store = InMemoryPrototypeLoopStore()
    service = LoopPlaneService(store, mode)
    service.observe(admitted(), 0, contract=contract())
    service.observe(claimed(), 1)
    return store, service


def test_off_mode_zero_writes():
    store = InMemoryPrototypeLoopStore(); service = LoopPlaneService(store, LoopMode.OFF)
    before = store.snapshot_counts()
    with pytest.raises(ModeViolation): service.observe(admitted(), 0, contract=contract())
    assert store.snapshot_counts() == before


def test_observe_cannot_evaluate():
    store, service = started_service(LoopMode.OBSERVE)
    before = store.snapshot_counts()
    with pytest.raises(ModeViolation): service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion()], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="e1", causation_id="cause", correlation_id="corr")
    assert store.snapshot_counts() == before


def test_shadow_decide_cannot_propose():
    _, service = started_service(LoopMode.SHADOW_DECIDE)
    with pytest.raises(ModeViolation): service.proposal(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", decision_event_id="x", recommendation=Recommendation.ACCEPT, requested_canonical_operation="TASK_ACCEPT", expected_runtime_revision=1, reason_code="r", evidence_set_hash=H, source_transition_ids=("tr",), now_ms=30, expected_revision=2, idempotency_key="p", causation_id="cause", correlation_id="corr")


def test_admitted_starts_loop_and_claim_sequence():
    store, _ = started_service()
    state = store.load_state("loop:r1")
    assert state.started and state.current_attempt_id == "attempt:t1:1:f1" and state.revision == 2


def test_admitted_without_contract_is_classified():
    result = RuntimeEventTranslator().translate(admitted(), 1)
    assert result.status is TranslationStatus.PROVENANCE_INSUFFICIENT and result.event is None


def test_runtime_completion_is_observation_not_evaluation():
    result = RuntimeEventTranslator().translate(completed(), 3)
    assert result.status is TranslationStatus.TRANSLATED
    assert result.event.event_type == "RuntimeObservationRecorded"


def test_evaluate_commits_two_events_and_accept_closes_loop():
    store, service = started_service()
    result = service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion()], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="eval-1", causation_id="cause", correlation_id="corr")
    assert result.recommendation is Recommendation.ACCEPT
    assert [event.event_type for event in store.load_events("loop:r1")[-2:]] == ["CriteriaEvaluated", "ShadowDecisionRecorded"]
    assert store.load_state("loop:r1").closed


def test_decision_after_closure_rejected():
    store, service = started_service()
    service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion()], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="eval-1", causation_id="cause", correlation_id="corr")
    with pytest.raises(DomainTransitionError): service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion()], generator_principal="generator", now_ms=21, expected_revision=4, idempotency_key="eval-2", causation_id="cause", correlation_id="corr")


def test_missing_required_criterion_cannot_accept():
    _, service = started_service()
    result = service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="eval-missing", causation_id="cause", correlation_id="corr")
    assert result.recommendation is Recommendation.BLOCK


def test_invalid_timestamp_and_empty_refs_become_unknown():
    _, service = started_service()
    result = service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion(produced=None, refs=())], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="eval-bad", causation_id="cause", correlation_id="corr")
    assert result.recommendation is Recommendation.BLOCK


def test_proposal_persists_event_and_is_non_executable():
    store, service = started_service()
    decision = service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion(Outcome.FAIL)], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="eval-1", causation_id="cause", correlation_id="corr", defect_type="transient")
    result = service.proposal(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", decision_event_id=decision.decision_event_id, recommendation=decision.recommendation, requested_canonical_operation="TASK_REQUEUE", expected_runtime_revision=2, reason_code=decision.reason_code, evidence_set_hash=decision.evidence_set_hash, source_transition_ids=("tr-complete",), now_ms=30, expected_revision=4, idempotency_key="proposal-1", causation_id="cause", correlation_id="corr")
    assert store.load_events("loop:r1")[-1].event_type == "ProposalRecorded"
    assert result.proposal.executable is False and result.proposal.auto_submit is False


def test_proposal_without_decision_fails_closed():
    _, service = started_service()
    with pytest.raises(DomainTransitionError): service.proposal(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", decision_event_id="missing", recommendation=Recommendation.BLOCK, requested_canonical_operation="TASK_WAIT", expected_runtime_revision=2, reason_code="x", evidence_set_hash=H, source_transition_ids=("tr",), now_ms=30, expected_revision=2, idempotency_key="p", causation_id="cause", correlation_id="corr")


def test_exact_duplicate_evaluation_is_idempotent():
    store, service = started_service()
    kwargs = dict(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion(Outcome.FAIL)], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="same", causation_id="cause", correlation_id="corr", defect_type="transient")
    first = service.evaluate(**kwargs); second = service.evaluate(**kwargs)
    assert first.receipt == second.receipt and len(store.load_events("loop:r1")) == 4


def test_changed_evaluation_same_identity_conflicts():
    _, service = started_service()
    service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion(Outcome.FAIL)], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="same", causation_id="cause", correlation_id="corr", defect_type="transient")
    with pytest.raises(IdempotencyConflict): service.evaluate(loop_id="loop:r1", attempt_id="attempt:t1:1:f1", criterion_results=[criterion(Outcome.FAIL)], generator_principal="generator", now_ms=20, expected_revision=2, idempotency_key="same", causation_id="cause", correlation_id="corr", defect_type="wrong_plan")


def test_receipt_result_contract_corruption_conflicts():
    store = InMemoryPrototypeLoopStore(); event = RuntimeEventTranslator().translate(admitted(), 1, contract=contract()).event
    receipt = CommandReceipt("RUNTIME_EVENT_OBSERVE", "loop:r1", "k", canonical_hash({"x": 1}), (event.event_id,), 1, "cause", "corr")
    store.commit("loop:r1", [event], receipt, 0)
    with pytest.raises(ReceiptConflict): store.commit("loop:r1", [event], replace(receipt, result_event_ids=("other",)), 0)


def test_empty_event_batch_fails_closed():
    store = InMemoryPrototypeLoopStore()
    receipt = CommandReceipt("RUNTIME_EVENT_OBSERVE", "loop:r1", "k", H, ("x",), 1, "cause", "corr")
    with pytest.raises(InvalidEventStream): store.commit("loop:r1", [], receipt, 0)


def test_bool_revision_and_non_string_identity_rejected():
    data = admitted(); data["source_task_revision"] = True
    assert RuntimeEventTranslator().translate(data, 1, contract=contract()).status is TranslationStatus.INVALID_REVISION
    data = admitted(); data["run_id"] = 5
    assert RuntimeEventTranslator().translate(data, 1, contract=contract()).status is TranslationStatus.INVALID_IDENTITY


def test_identity_conflicts_rejected():
    data = claimed(); data["loop_id"] = "wrong"
    assert RuntimeEventTranslator().translate(data, 2).status is TranslationStatus.IDENTITY_CONFLICT
    data = claimed(); data["attempt_id"] = "wrong"
    assert RuntimeEventTranslator().translate(data, 2).status is TranslationStatus.IDENTITY_CONFLICT


def test_payload_is_deeply_immutable_and_defensive():
    payload = {"nested": {"x": 1}}
    event = LoopEvent("e", "l", 1, "RuntimeObservationRecorded", 1, "c", "r", payload)
    payload["nested"]["x"] = 2
    assert event.payload["nested"]["x"] == 1
    with pytest.raises(TypeError): event.payload["nested"]["x"] = 3


def test_unknown_event_type_fails_closed():
    store = InMemoryPrototypeLoopStore(); event = LoopEvent("e", "l", 1, "Unknown", 1, "c", "r", {})
    receipt = CommandReceipt("RUNTIME_EVENT_OBSERVE", "l", "k", H, ("e",), 1, "c", "r")
    with pytest.raises(InvalidEventStream): store.commit("l", [event], receipt, 0)


def test_max_attempts_exact_boundary():
    _, service = started_service()
    service.observe(claimed("c2", 2) | {"claim_fence": "f2"}, 2)
    with pytest.raises(DomainTransitionError): service.observe(claimed("c3", 3) | {"claim_fence": "f3"}, 3)


def test_proposal_safety_cannot_be_overridden():
    with pytest.raises(ValueError): CanonicalCommandProposal("p", "l", "a", "d", Recommendation.BLOCK, "TASK_WAIT", 0, "r", H, ("t",), "p1", 1, executable=True)

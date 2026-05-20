from types import SimpleNamespace

from hfa_worker.feedback_writer import (
    CANONICAL_AUTHORITY_WRITES_ALLOWED,
    ADVISORY_ONLY_SURFACE,
    validate_feedback_governance,
)


def _result(**overrides):
    base = {
        "status": "success",
        "confidence": 0.91,
        "requires_hitl": False,
        "output_data": {"answer": "ok"},
        "reasoning_trace": ["a", "b"],
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_feedbackwriter_surface_is_advisory_and_non_authoritative():
    assert ADVISORY_ONLY_SURFACE is True
    assert CANONICAL_AUTHORITY_WRITES_ALLOWED is False


def test_feedback_governance_accepts_valid_feedback():
    decision = validate_feedback_governance(
        result=_result(),
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        trace_id="run-1",
    )

    assert decision.accepted is True
    assert decision.reason == "all_checks_passed"


def test_feedback_governance_rejects_low_confidence():
    decision = validate_feedback_governance(
        result=_result(confidence=0.10),
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        trace_id="run-1",
    )

    assert decision.rejected is True
    assert decision.reason.startswith("confidence_below_threshold")


def test_feedback_governance_rejects_non_success_status():
    decision = validate_feedback_governance(
        result=_result(status="failed"),
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        trace_id="run-1",
    )

    assert decision.rejected is True
    assert decision.reason == "non_accepted_status:failed"


def test_feedback_governance_rejects_missing_required_ids():
    for field in ("task_id", "run_id", "tenant_id", "trace_id"):
        kwargs = {
            "result": _result(),
            "task_id": "task-1",
            "run_id": "run-1",
            "tenant_id": "tenant-1",
            "trace_id": "run-1",
        }
        kwargs[field] = ""

        decision = validate_feedback_governance(**kwargs)

        assert decision.rejected is True
        assert decision.reason == f"missing_{field}"


def test_feedback_governance_rejects_malformed_payload():
    decision = validate_feedback_governance(
        result=_result(output_data=[]),
        task_id="task-1",
        run_id="run-1",
        tenant_id="tenant-1",
        trace_id="run-1",
    )

    assert decision.rejected is True
    assert decision.reason == "output_data_not_mapping"

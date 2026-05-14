
from hfa_control.decision_observability import (
    DecisionBreakdown,
    RejectedWorkerReason,
    SchedulerDecisionTrace,
)


def test_trace_to_dict_serializes_scores_and_reasons():
    trace = SchedulerDecisionTrace(
        decision_id="d1",
        stage="reserve_dispatch",
        selected_worker_id="worker-1",
        selected_task_id="task-1",
        selected_tenant_id="tenant-a",
        selected_score=(1.0, 2, 3, -4, "worker-1", "task-1"),
        selected_reason="best_compatible_worker_by_score",
        candidate_count=1,
        compatible_worker_ids=["worker-1"],
        rejected_workers=[
            RejectedWorkerReason("worker-2", "capability_mismatch", {"missing_capabilities": ["python"]})
        ],
        scored_candidates=[
            DecisionBreakdown("tenant-a", "task-1", "worker-1", 1.0, 2, 3, 4, (1.0, 2, 3, -4, "worker-1", "task-1"))
        ],
    )
    data = trace.to_dict()
    assert data["selected_score"] == [1.0, 2, 3, -4, "worker-1", "task-1"]
    assert data["rejected_workers"][0]["reason"] == "capability_mismatch"
    assert data["scored_candidates"][0]["worker_id"] == "worker-1"

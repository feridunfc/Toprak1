
from __future__ import annotations

from pathlib import Path


def test_crash_boundary_evidence_module_is_product_placed_not_script_owned() -> None:
    assert Path("hfa-control/src/hfa_control/api/crash_boundary_evidence.py").exists()
    assert not Path("scripts/crash_after_completion_before_ack_drill.py").exists()


def test_crash_boundary_evidence_static_contract_is_read_only() -> None:
    source = Path("hfa-control/src/hfa_control/api/crash_boundary_evidence.py").read_text(
        encoding="utf-8"
    )

    assert "read_crash_boundary_evidence" in source
    assert "read_task_evidence" in source
    assert "xpending_range" in source
    assert "xrange" in source
    assert "terminal_task_with_pending_message" in source
    assert "operator_action_required" in source
    assert "production_ready_claim" in source

    forbidden = [
        ".set(",
        ".hset(",
        ".delete(",
        ".xadd(",
        ".xack(",
        ".xclaim(",
        "xautoclaim",
        "requeue_stale_task",
        "proof_gated_requeue",
        "runtime_repair_attempted = True",
        "production_ready_claim = True",
    ]

    lowered = source.lower()
    for fragment in forbidden:
        assert fragment.lower() not in lowered


def test_crash_boundary_endpoint_is_operator_only_and_read_only() -> None:
    source = Path("hfa-control/src/hfa_control/api/router.py").read_text(encoding="utf-8")

    assert '@router.get("/tasks/{task_id}/crash-boundary-evidence")' in source
    assert "_require_operator(x_cp_auth)" in source
    assert "read_crash_boundary_evidence" in source
    assert "must not acknowledge, reclaim, requeue, retry, repair" in source

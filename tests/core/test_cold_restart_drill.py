import json
import os
import subprocess
import sys

from scripts.cold_restart_drill import ColdRestartDrillArtifact


def test_cold_restart_drill_fake_redis_is_skipped(tmp_path):
    output = tmp_path / "latest_cold_restart_drill.json"
    env = os.environ.copy()
    env["USE_FAKE_REDIS"] = "1"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/cold_restart_drill.py",
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "SKIPPED"
    assert payload["recovery_requeue_status"] == "REAL_REDIS_REQUIRED"
    assert payload["mutation_attempted"] is False
    assert payload["zombie_completion_rejection_checked"] is False
    assert payload["zombie_completion_rejection_status"] == "NOT_CHECKED"
    assert output.exists()

    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["status"] == "SKIPPED"
    assert written["recovery_requeue_status"] == "REAL_REDIS_REQUIRED"


def test_cold_restart_drill_artifact_shape_for_pass():
    artifact = ColdRestartDrillArtifact(
        source="cold_restart_drill",
        status="PASS",
        mode="staging-cold-restart",
        run_id="task-1",
        tenant_id="tenant-1",
        recovery_requeue_status="TASK_REQUEUED",
        recovery_requeue_ok=True,
        proof_allowed=True,
        mutation_attempted=True,
        pre_restart_state={
            "redis": {
                "state": "running",
                "running_score": 1.0,
                "ready_score": None,
                "meta": {"claim_epoch": "1", "worker_instance_id": "stale-worker"},
            },
            "audit_candidate_count": 1,
            "audit_status": "PASS",
        },
        post_requeue_state={
            "redis": {
                "state": "ready",
                "running_score": None,
                "ready_score": 2.0,
                "meta": {
                    "claim_epoch": "1",
                    "worker_instance_id": "",
                    "scheduler_epoch": "",
                    "requeue_count": "1",
                },
            }
        },
        zombie_completion_rejection_status="PENDING_COMPLETION_HARNESS",
        zombie_completion_rejection_checked=False,
        recovery_requeue_drill_artifact={
            "status": "PASS",
            "requeue_status": "TASK_REQUEUED",
            "requeue_ok": True,
            "proof_allowed": True,
        },
        notes=["No automatic recovery daemon is enabled."],
    )

    assert artifact.status == "PASS"
    assert artifact.recovery_requeue_status == "TASK_REQUEUED"
    assert artifact.recovery_requeue_ok is True
    assert artifact.proof_allowed is True
    assert artifact.mutation_attempted is True
    assert artifact.pre_restart_state["redis"]["state"] == "running"
    assert artifact.post_requeue_state["redis"]["state"] == "ready"
    assert artifact.post_requeue_state["redis"]["running_score"] is None
    assert artifact.post_requeue_state["redis"]["ready_score"] == 2.0
    assert artifact.post_requeue_state["redis"]["meta"]["claim_epoch"] == "1"
    assert artifact.zombie_completion_rejection_checked is False
    assert artifact.zombie_completion_rejection_status == "PENDING_COMPLETION_HARNESS"

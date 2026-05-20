import json
import os
import subprocess
import sys

from scripts.recovery_requeue_drill import DrillArtifact


def test_recovery_requeue_drill_fake_redis_is_skipped(tmp_path):
    output = tmp_path / "latest_recovery_requeue_drill.json"
    env = os.environ.copy()
    env["USE_FAKE_REDIS"] = "1"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/recovery_requeue_drill.py",
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
    assert payload["requeue_status"] == "REAL_REDIS_REQUIRED"
    assert payload["mutation_attempted"] is False
    assert output.exists()

    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["status"] == "SKIPPED"
    assert written["requeue_status"] == "REAL_REDIS_REQUIRED"


def test_recovery_requeue_drill_artifact_shape_for_pass():
    artifact = DrillArtifact(
        source="recovery_requeue_drill",
        mode="redis-backed-single-task",
        status="PASS",
        run_id="run-1",
        tenant_id="tenant-1",
        proof_allowed=True,
        candidate_found=True,
        mutation_attempted=True,
        requeue_ok=True,
        requeue_status="TASK_REQUEUED",
        pre_state={
            "redis": {
                "state": "running",
                "running_score": 1.0,
                "ready_score": None,
                "meta": {"claim_epoch": "1"},
            },
            "audit_candidate_count": 1,
            "audit_status": "PASS",
        },
        post_state={
            "redis": {
                "state": "ready",
                "running_score": None,
                "ready_score": 2.0,
                "meta": {"claim_epoch": "1", "requeue_count": "1"},
            }
        },
        recovery_requeue_artifact={
            "proof_mode": "artifacts",
            "replay_artifact_status": "PASS",
            "authority_artifact_status": "PASS",
            "recovery_audit_artifact_status": "PASS",
            "artifact_candidate_found": True,
            "requeue_status": "TASK_REQUEUED",
        },
        notes=["No automatic recovery loop is enabled."],
    )

    assert artifact.status == "PASS"
    assert artifact.proof_allowed is True
    assert artifact.mutation_attempted is True
    assert artifact.requeue_status == "TASK_REQUEUED"
    assert artifact.pre_state["redis"]["state"] == "running"
    assert artifact.post_state["redis"]["state"] == "ready"
    assert artifact.post_state["redis"]["running_score"] is None
    assert artifact.post_state["redis"]["ready_score"] == 2.0
    assert artifact.post_state["redis"]["meta"]["claim_epoch"] == "1"
    assert artifact.recovery_requeue_artifact["proof_mode"] == "artifacts"
    assert artifact.recovery_requeue_artifact["artifact_candidate_found"] is True

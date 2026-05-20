import json
import os
import subprocess
import sys

from scripts.zombie_completion_drill import ZombieCompletionDrillArtifact


def test_zombie_completion_drill_fake_redis_is_skipped(tmp_path):
    output = tmp_path / "latest_zombie_completion_drill.json"
    env = os.environ.copy()
    env["USE_FAKE_REDIS"] = "1"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/zombie_completion_drill.py",
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
    assert payload["zombie_completion_status"] == "REAL_REDIS_REQUIRED"
    assert payload["zombie_completion_attempted"] is False
    assert payload["zombie_completion_accepted"] is False
    assert output.exists()

    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["status"] == "SKIPPED"
    assert written["rejection_reason"] == "REAL_REDIS_REQUIRED"


def test_zombie_completion_drill_artifact_shape_for_pass():
    artifact = ZombieCompletionDrillArtifact(
        source="zombie_completion_drill",
        status="PASS",
        mode="zombie-completion-rejection",
        task_id="task-1",
        tenant_id="tenant-1",
        pre_requeue_claim_epoch="1",
        post_requeue_claim_epoch="1",
        stale_worker_identity="stale-worker",
        stale_scheduler_epoch="1",
        zombie_completion_attempted=True,
        zombie_completion_accepted=False,
        zombie_completion_status="illegal_transition",
        rejection_reason="illegal_transition",
        recovery_requeue_status="TASK_REQUEUED",
        recovery_requeue_ok=True,
        notes=["No automatic recovery daemon is enabled."],
    )

    assert artifact.status == "PASS"
    assert artifact.recovery_requeue_status == "TASK_REQUEUED"
    assert artifact.recovery_requeue_ok is True
    assert artifact.pre_requeue_claim_epoch == "1"
    assert artifact.post_requeue_claim_epoch == "1"
    assert artifact.zombie_completion_attempted is True
    assert artifact.zombie_completion_accepted is False
    assert artifact.zombie_completion_status == "illegal_transition"
    assert artifact.rejection_reason == "illegal_transition"

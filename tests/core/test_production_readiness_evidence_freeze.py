import json
import subprocess
import sys
from pathlib import Path

from scripts.production_readiness_evidence_freeze import REQUIRED_ARTIFACTS, build_artifact


def _payload_for(name: str, *, decision: str = "READY", rollup_status: str = "PASS"):
    if name == "latest_production_readiness_decision.json":
        return {
            "source": "production_readiness_decision",
            "status": "PASS",
            "decision": decision,
            "rollup_status": rollup_status,
            "reasons": [],
        }

    if name == "latest_production_readiness_rollup.json":
        return {
            "source": "production_readiness_rollup",
            "status": rollup_status,
            "components": [],
        }

    if name == "latest_authority.json":
        return {"source": "authority", "banned_count": 0}

    if name == "latest_replay.json":
        return {"source": "replay_compare", "replay_status": "PASS"}

    return {"source": name.removesuffix(".json"), "status": "PASS"}


def _write_required_artifacts(root: Path, *, decision: str = "READY", rollup_status: str = "PASS"):
    root.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED_ARTIFACTS:
        (root / name).write_text(
            json.dumps(
                _payload_for(name, decision=decision, rollup_status=rollup_status),
                sort_keys=True,
            ),
            encoding="utf-8",
        )


def test_evidence_freeze_passes_ready_complete_artifacts(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    _write_required_artifacts(artifact_dir)

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "PASS"
    assert artifact.decision == "READY"
    assert artifact.rollup_status == "PASS"
    assert artifact.artifact_count == len(REQUIRED_ARTIFACTS)
    assert artifact.required_artifacts_complete is True
    assert artifact.mutation_attempted is False
    assert artifact.evidence_manifest_hash


def test_evidence_freeze_fails_missing_decision_artifact(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    _write_required_artifacts(artifact_dir)
    (artifact_dir / "latest_production_readiness_decision.json").unlink()

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "FAIL"
    assert "missing_artifact:latest_production_readiness_decision.json" in artifact.reasons
    assert artifact.required_artifacts_complete is False


def test_evidence_freeze_fails_decision_not_ready(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    _write_required_artifacts(artifact_dir, decision="NOT_READY")

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "FAIL"
    assert artifact.decision == "NOT_READY"
    assert "decision_not_ready:NOT_READY" in artifact.reasons


def test_evidence_freeze_fails_rollup_non_pass(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    _write_required_artifacts(artifact_dir, rollup_status="FAIL")

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "FAIL"
    assert artifact.rollup_status == "FAIL"
    assert "rollup_not_pass:FAIL" in artifact.reasons


def test_evidence_freeze_fails_malformed_json(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    _write_required_artifacts(artifact_dir)
    (artifact_dir / "latest_replay.json").write_text("{not-json", encoding="utf-8")

    artifact = build_artifact(artifact_dir)

    assert artifact.status == "FAIL"
    assert "malformed_artifact:latest_replay.json" in artifact.reasons


def test_evidence_freeze_manifest_hash_is_deterministic(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    _write_required_artifacts(artifact_dir)

    first = build_artifact(artifact_dir)
    second = build_artifact(artifact_dir)

    assert first.evidence_manifest_hash == second.evidence_manifest_hash


def test_evidence_freeze_manifest_hash_changes_on_artifact_change(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    _write_required_artifacts(artifact_dir)

    first = build_artifact(artifact_dir)

    payload = _payload_for("latest_recovery_audit.json")
    payload["extra"] = "changed"
    (artifact_dir / "latest_recovery_audit.json").write_text(
        json.dumps(payload, sort_keys=True),
        encoding="utf-8",
    )

    second = build_artifact(artifact_dir)

    assert first.evidence_manifest_hash != second.evidence_manifest_hash


def test_evidence_freeze_cli_writes_artifact(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    output = tmp_path / "latest_production_readiness_evidence_freeze.json"
    _write_required_artifacts(artifact_dir)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/production_readiness_evidence_freeze.py",
            "--artifact-dir",
            str(artifact_dir),
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "production_readiness_evidence_freeze"
    assert payload["status"] == "PASS"
    assert payload["decision"] == "READY"
    assert payload["mutation_attempted"] is False
    assert output.exists()

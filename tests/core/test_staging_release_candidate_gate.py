import json
import subprocess
import sys
from pathlib import Path

from scripts.staging_release_candidate_gate import build_artifact


def _write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _write_ready_inputs(root: Path):
    decision = root / "latest_production_readiness_decision.json"
    freeze = root / "latest_production_readiness_evidence_freeze.json"

    _write_json(
        decision,
        {
            "source": "production_readiness_decision",
            "status": "PASS",
            "decision": "READY",
            "reasons": [],
        },
    )
    _write_json(
        freeze,
        {
            "source": "production_readiness_evidence_freeze",
            "status": "PASS",
            "decision": "READY",
            "rollup_status": "PASS",
            "required_artifacts_complete": True,
            "evidence_manifest_hash": "abc123",
            "mutation_attempted": False,
        },
    )
    return decision, freeze


def test_staging_rc_gate_allows_ready_frozen_evidence(tmp_path):
    decision, freeze = _write_ready_inputs(tmp_path)

    artifact = build_artifact(decision, freeze)

    assert artifact.status == "PASS"
    assert artifact.rc_decision == "RC_ALLOWED"
    assert artifact.reasons == []
    assert artifact.mutation_attempted is False
    assert artifact.deployment_attempted is False
    assert artifact.release_tag_attempted is False


def test_staging_rc_gate_blocks_missing_decision(tmp_path):
    decision, freeze = _write_ready_inputs(tmp_path)
    decision.unlink()

    artifact = build_artifact(decision, freeze)

    assert artifact.rc_decision == "RC_BLOCKED"
    assert "production_decision_missing" in artifact.reasons


def test_staging_rc_gate_blocks_malformed_freeze(tmp_path):
    decision, freeze = _write_ready_inputs(tmp_path)
    freeze.write_text("{not-json", encoding="utf-8")

    artifact = build_artifact(decision, freeze)

    assert artifact.rc_decision == "RC_BLOCKED"
    assert "evidence_freeze_malformed" in artifact.reasons


def test_staging_rc_gate_blocks_not_ready_decision(tmp_path):
    decision, freeze = _write_ready_inputs(tmp_path)
    _write_json(
        decision,
        {
            "source": "production_readiness_decision",
            "status": "PASS",
            "decision": "NOT_READY",
            "reasons": ["x"],
        },
    )

    artifact = build_artifact(decision, freeze)

    assert artifact.rc_decision == "RC_BLOCKED"
    assert "production_decision_not_ready:NOT_READY" in artifact.reasons


def test_staging_rc_gate_blocks_freeze_non_pass(tmp_path):
    decision, freeze = _write_ready_inputs(tmp_path)
    _write_json(
        freeze,
        {
            "source": "production_readiness_evidence_freeze",
            "status": "FAIL",
            "decision": "READY",
            "required_artifacts_complete": True,
            "evidence_manifest_hash": "abc123",
            "mutation_attempted": False,
        },
    )

    artifact = build_artifact(decision, freeze)

    assert artifact.rc_decision == "RC_BLOCKED"
    assert "evidence_freeze_status_not_pass:FAIL" in artifact.reasons


def test_staging_rc_gate_blocks_missing_manifest_hash(tmp_path):
    decision, freeze = _write_ready_inputs(tmp_path)
    _write_json(
        freeze,
        {
            "source": "production_readiness_evidence_freeze",
            "status": "PASS",
            "decision": "READY",
            "required_artifacts_complete": True,
            "evidence_manifest_hash": "",
            "mutation_attempted": False,
        },
    )

    artifact = build_artifact(decision, freeze)

    assert artifact.rc_decision == "RC_BLOCKED"
    assert "evidence_manifest_hash_missing" in artifact.reasons


def test_staging_rc_gate_cli_writes_artifact(tmp_path):
    decision, freeze = _write_ready_inputs(tmp_path)
    output = tmp_path / "latest_staging_release_candidate_gate.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/staging_release_candidate_gate.py",
            "--decision-input",
            str(decision),
            "--evidence-freeze-input",
            str(freeze),
            "--output",
            str(output),
            "--json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["source"] == "staging_release_candidate_gate"
    assert payload["status"] == "PASS"
    assert payload["rc_decision"] == "RC_ALLOWED"
    assert payload["deployment_attempted"] is False
    assert payload["release_tag_attempted"] is False
    assert output.exists()

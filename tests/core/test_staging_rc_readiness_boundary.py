from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.staging_rc_readiness_boundary as boundary


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _patch_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    artifact_dir = tmp_path / "docs" / "dashboard" / "artifacts"

    monkeypatch.setattr(boundary, "ROOT", tmp_path)
    monkeypatch.setattr(boundary, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(
        boundary,
        "DECISION_PATH",
        artifact_dir / "latest_production_readiness_decision.json",
    )
    monkeypatch.setattr(
        boundary,
        "EVIDENCE_FREEZE_PATH",
        artifact_dir / "latest_production_readiness_evidence_freeze.json",
    )
    monkeypatch.setattr(
        boundary,
        "STAGING_RC_GATE_PATH",
        artifact_dir / "latest_staging_release_candidate_gate.json",
    )
    monkeypatch.setattr(
        boundary,
        "OUTPUT_PATH",
        artifact_dir / "latest_staging_rc_readiness_boundary.json",
    )

    return artifact_dir


def _write_pass_inputs(artifact_dir: Path) -> None:
    _write_json(
        artifact_dir / "latest_production_readiness_decision.json",
        {
            "status": "PASS",
            "decision": "READY",
        },
    )
    _write_json(
        artifact_dir / "latest_production_readiness_evidence_freeze.json",
        {
            "status": "PASS",
            "decision": "READY",
            "required_artifacts_complete": True,
            "evidence_manifest_hash": "abc123",
            "mutation_attempted": False,
        },
    )
    _write_json(
        artifact_dir / "latest_staging_release_candidate_gate.json",
        {
            "status": "PASS",
            "rc_decision": "RC_ALLOWED",
            "deployment_attempted": False,
            "release_tag_attempted": False,
            "mutation_attempted": False,
        },
    )


def test_boundary_passes_for_ready_frozen_rc_allowed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    artifact = boundary.build_boundary()

    assert artifact["status"] == "PASS"
    assert artifact["decision_scope"] == boundary.DECISION_SCOPE
    assert artifact["findings"] == []
    assert artifact["production_ready_claim"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["redis_mutation_attempted"] is False
    assert artifact["runtime_mutation_attempted"] is False
    assert artifact["canonical_state_mutation_attempted"] is False
    assert artifact["observed"]["staging_release_candidate_gate_decision"] == "RC_ALLOWED"


def test_boundary_fails_when_evidence_freeze_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    _write_json(
        artifact_dir / "latest_production_readiness_evidence_freeze.json",
        {
            "status": "FAIL",
            "decision": "READY",
            "required_artifacts_complete": True,
            "evidence_manifest_hash": "abc123",
            "mutation_attempted": False,
        },
    )

    artifact = boundary.build_boundary()

    assert artifact["status"] == "FAIL"
    assert "production readiness evidence freeze status is not PASS" in artifact["findings"]


def test_boundary_fails_when_rc_gate_blocks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    _write_json(
        artifact_dir / "latest_staging_release_candidate_gate.json",
        {
            "status": "PASS",
            "rc_decision": "RC_BLOCKED",
            "deployment_attempted": False,
            "release_tag_attempted": False,
            "mutation_attempted": False,
        },
    )

    artifact = boundary.build_boundary()

    assert artifact["status"] == "FAIL"
    assert "staging release candidate gate decision is not RC_ALLOWED" in artifact["findings"]


def test_boundary_fails_when_production_decision_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    _write_json(
        artifact_dir / "latest_production_readiness_decision.json",
        {
            "status": "PASS",
            "decision": "NOT_READY",
        },
    )

    artifact = boundary.build_boundary()

    assert artifact["status"] == "FAIL"
    assert "production readiness decision is not READY" in artifact["findings"]


def test_boundary_fails_closed_when_required_artifact_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    (artifact_dir / "latest_staging_release_candidate_gate.json").unlink()

    artifact = boundary.build_boundary()

    assert artifact["status"] == "FAIL"
    assert any(
        finding.startswith("missing artifact:")
        for finding in artifact["findings"]
    )


def test_boundary_fails_closed_when_required_artifact_malformed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    (
        artifact_dir / "latest_staging_release_candidate_gate.json"
    ).write_text("{not-json", encoding="utf-8")

    artifact = boundary.build_boundary()

    assert artifact["status"] == "FAIL"
    assert any(
        finding.startswith("malformed artifact:")
        for finding in artifact["findings"]
    )


def test_boundary_main_writes_output_and_returns_zero_on_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    output_path = artifact_dir / "boundary.json"

    assert boundary.main_args(["--output", str(output_path)]) == 0

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["status"] == "PASS"
    assert written["decision_scope"] == boundary.DECISION_SCOPE

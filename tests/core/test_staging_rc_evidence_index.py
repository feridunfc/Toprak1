from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.staging_rc_evidence_index as index


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _patch_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    artifact_dir = tmp_path / "docs" / "dashboard" / "artifacts"

    monkeypatch.setattr(index, "ROOT", tmp_path)
    monkeypatch.setattr(index, "ARTIFACT_DIR", artifact_dir)

    monkeypatch.setattr(index, "ROLLUP_PATH", artifact_dir / "latest_production_readiness_rollup.json")
    monkeypatch.setattr(index, "DECISION_PATH", artifact_dir / "latest_production_readiness_decision.json")
    monkeypatch.setattr(index, "EVIDENCE_FREEZE_PATH", artifact_dir / "latest_production_readiness_evidence_freeze.json")
    monkeypatch.setattr(index, "STAGING_RC_GATE_PATH", artifact_dir / "latest_staging_release_candidate_gate.json")
    monkeypatch.setattr(index, "STAGING_RC_BOUNDARY_PATH", artifact_dir / "latest_staging_rc_readiness_boundary.json")
    monkeypatch.setattr(index, "OUTPUT_PATH", artifact_dir / "latest_staging_rc_evidence_index.json")

    monkeypatch.setattr(
        index,
        "REQUIRED_ARTIFACTS",
        (
            ("production_readiness_rollup", artifact_dir / "latest_production_readiness_rollup.json"),
            ("production_readiness_decision", artifact_dir / "latest_production_readiness_decision.json"),
            ("production_readiness_evidence_freeze", artifact_dir / "latest_production_readiness_evidence_freeze.json"),
            ("staging_release_candidate_gate", artifact_dir / "latest_staging_release_candidate_gate.json"),
            ("staging_rc_readiness_boundary", artifact_dir / "latest_staging_rc_readiness_boundary.json"),
        ),
    )

    return artifact_dir


def _write_pass_inputs(artifact_dir: Path) -> None:
    _write_json(
        artifact_dir / "latest_production_readiness_rollup.json",
        {
            "status": "PASS",
        },
    )
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
    _write_json(
        artifact_dir / "latest_staging_rc_readiness_boundary.json",
        {
            "status": "PASS",
            "decision_scope": index.DECISION_SCOPE,
            "production_ready_claim": False,
            "deployment_attempted": False,
            "release_tag_created": False,
            "redis_mutation_attempted": False,
            "runtime_mutation_attempted": False,
            "canonical_state_mutation_attempted": False,
        },
    )


def test_index_passes_for_complete_staging_rc_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    artifact = index.build_index()

    assert artifact["status"] == "PASS"
    assert artifact["source"] == index.SOURCE
    assert artifact["decision_scope"] == index.DECISION_SCOPE
    assert artifact["staging_rc_indexed"] is True
    assert artifact["production_ready_claim"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["redis_mutation_attempted"] is False
    assert artifact["runtime_mutation_attempted"] is False
    assert artifact["canonical_state_mutation_attempted"] is False
    assert artifact["production_readiness_rollup_status"] == "PASS"
    assert artifact["production_readiness_decision"] == "READY"
    assert artifact["evidence_freeze_status"] == "PASS"
    assert artifact["staging_rc_decision"] == "RC_ALLOWED"
    assert artifact["staging_rc_boundary_status"] == "PASS"
    assert artifact["indexed_artifacts_count"] == 5
    assert artifact["findings"] == []


def test_index_fails_when_decision_artifact_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_production_readiness_decision.json").unlink()

    artifact = index.build_index()

    assert artifact["status"] == "FAIL"
    assert artifact["staging_rc_indexed"] is False
    assert any(f.startswith("missing artifact:") for f in artifact["findings"])


def test_index_fails_when_artifact_malformed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_staging_release_candidate_gate.json").write_text("{not-json", encoding="utf-8")

    artifact = index.build_index()

    assert artifact["status"] == "FAIL"
    assert any(f.startswith("malformed artifact:") for f in artifact["findings"])


def test_index_fails_when_production_decision_not_ready(
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

    artifact = index.build_index()

    assert artifact["status"] == "FAIL"
    assert "production readiness decision is not READY" in artifact["findings"]


def test_index_fails_when_evidence_freeze_fails(
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

    artifact = index.build_index()

    assert artifact["status"] == "FAIL"
    assert "evidence freeze status is not PASS" in artifact["findings"]


def test_index_fails_when_rc_gate_blocks(
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

    artifact = index.build_index()

    assert artifact["status"] == "FAIL"
    assert "staging RC decision is not RC_ALLOWED" in artifact["findings"]


def test_index_fails_when_boundary_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(
        artifact_dir / "latest_staging_rc_readiness_boundary.json",
        {
            "status": "FAIL",
            "decision_scope": index.DECISION_SCOPE,
        },
    )

    artifact = index.build_index()

    assert artifact["status"] == "FAIL"
    assert "staging RC boundary status is not PASS" in artifact["findings"]


def test_index_uses_stable_artifact_ordering(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    artifact = index.build_index()

    assert [entry["name"] for entry in artifact["indexed_artifacts"]] == [
        "production_readiness_rollup",
        "production_readiness_decision",
        "production_readiness_evidence_freeze",
        "staging_release_candidate_gate",
        "staging_rc_readiness_boundary",
    ]


def test_index_main_writes_output_and_returns_zero_on_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    output_path = artifact_dir / "index.json"

    assert index.main_args(["--output", str(output_path)]) == 0

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["status"] == "PASS"
    assert written["staging_rc_indexed"] is True

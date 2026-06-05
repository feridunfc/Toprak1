from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.operator_rc_evidence_panel as panel


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _patch_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    artifact_dir = tmp_path / "docs" / "dashboard" / "artifacts"

    monkeypatch.setattr(panel, "ROOT", tmp_path)
    monkeypatch.setattr(panel, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(
        panel,
        "INDEX_PATH",
        artifact_dir / "latest_staging_rc_evidence_index.json",
    )
    monkeypatch.setattr(
        panel,
        "OUTPUT_PATH",
        artifact_dir / "latest_operator_rc_evidence_panel.json",
    )

    return artifact_dir


def _pass_index() -> dict:
    return {
        "source": "staging_rc_evidence_index",
        "status": "PASS",
        "decision_scope": panel.DECISION_SCOPE,
        "staging_rc_indexed": True,
        "production_ready_claim": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "redis_mutation_attempted": False,
        "runtime_mutation_attempted": False,
        "canonical_state_mutation_attempted": False,
        "evidence_manifest_hash": "abc123",
        "indexed_artifacts_count": 5,
        "findings": [],
    }


def _write_pass_index(artifact_dir: Path) -> None:
    _write_json(
        artifact_dir / "latest_staging_rc_evidence_index.json",
        _pass_index(),
    )


def test_panel_passes_for_allowed_index(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_index(artifact_dir)

    artifact = panel.build_panel()

    assert artifact["status"] == "PASS"
    assert artifact["panel_status"] == "RC_ALLOWED_INDEXED"
    assert artifact["decision_scope"] == panel.DECISION_SCOPE
    assert artifact["operator_message"] == (
        "Staging RC evidence is indexed and allowed. This is not a production deployment."
    )
    assert artifact["actionable"] is False
    assert artifact["input_artifact"] == panel.INPUT_ARTIFACT
    assert artifact["badges"] == panel.PASS_BADGES
    assert artifact["production_ready_claim"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["redis_mutation_attempted"] is False
    assert artifact["runtime_mutation_attempted"] is False
    assert artifact["canonical_state_mutation_attempted"] is False
    assert artifact["evidence_manifest_hash"] == "abc123"
    assert artifact["indexed_artifacts_count"] == 5
    assert artifact["failing_reasons"] == []


def test_panel_missing_index_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_paths(monkeypatch, tmp_path)

    artifact = panel.build_panel()

    assert artifact["status"] == "FAIL"
    assert artifact["panel_status"] == "PANEL_UNAVAILABLE"
    assert artifact["actionable"] is False
    assert artifact["badges"] == ["NOT_PRODUCTION_DEPLOYMENT"]
    assert any(
        reason.startswith("missing index artifact:")
        for reason in artifact["failing_reasons"]
    )


def test_panel_malformed_index_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    path = artifact_dir / "latest_staging_rc_evidence_index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not-json", encoding="utf-8")

    artifact = panel.build_panel()

    assert artifact["status"] == "FAIL"
    assert artifact["panel_status"] == "PANEL_INVALID"
    assert artifact["actionable"] is False
    assert any(
        reason.startswith("malformed index artifact:")
        for reason in artifact["failing_reasons"]
    )


def test_panel_index_fail_maps_to_blocked_or_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_index()
    payload["status"] = "FAIL"
    payload["findings"] = ["evidence freeze status is not PASS"]
    _write_json(artifact_dir / "latest_staging_rc_evidence_index.json", payload)

    artifact = panel.build_panel()

    assert artifact["status"] == "FAIL"
    assert artifact["panel_status"] == "RC_BLOCKED_OR_NOT_READY"
    assert artifact["actionable"] is False
    assert "evidence freeze status is not PASS" in artifact["failing_reasons"]


def test_panel_not_indexed_maps_to_rc_not_indexed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_index()
    payload["staging_rc_indexed"] = False
    _write_json(artifact_dir / "latest_staging_rc_evidence_index.json", payload)

    artifact = panel.build_panel()

    assert artifact["status"] == "FAIL"
    assert artifact["panel_status"] == "RC_NOT_INDEXED"
    assert artifact["actionable"] is False
    assert "staging_rc_indexed is not true" in artifact["failing_reasons"]


def test_panel_invalid_decision_scope_is_safety_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_index()
    payload["decision_scope"] = "PRODUCTION_DEPLOYMENT"
    _write_json(artifact_dir / "latest_staging_rc_evidence_index.json", payload)

    artifact = panel.build_panel()

    assert artifact["status"] == "FAIL"
    assert artifact["panel_status"] == "SAFETY_VIOLATION"
    assert artifact["actionable"] is False
    assert "decision scope is invalid" in artifact["failing_reasons"]


def test_panel_missing_manifest_hash_blocks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_index()
    payload["evidence_manifest_hash"] = ""
    _write_json(artifact_dir / "latest_staging_rc_evidence_index.json", payload)

    artifact = panel.build_panel()

    assert artifact["status"] == "FAIL"
    assert artifact["panel_status"] == "RC_BLOCKED_OR_NOT_READY"
    assert artifact["actionable"] is False
    assert "evidence manifest hash is missing" in artifact["failing_reasons"]


@pytest.mark.parametrize(
    "flag",
    [
        "production_ready_claim",
        "deployment_attempted",
        "release_tag_created",
        "redis_mutation_attempted",
        "runtime_mutation_attempted",
        "canonical_state_mutation_attempted",
    ],
)
def test_panel_safety_flags_are_violations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    flag: str,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_index()
    payload[flag] = True
    _write_json(artifact_dir / "latest_staging_rc_evidence_index.json", payload)

    artifact = panel.build_panel()

    assert artifact["status"] == "FAIL"
    assert artifact["panel_status"] == "SAFETY_VIOLATION"
    assert artifact["actionable"] is False
    assert f"{flag} is not false" in artifact["failing_reasons"]


def test_panel_main_writes_output_and_returns_zero_on_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_index(artifact_dir)
    output_path = artifact_dir / "panel.json"

    assert panel.main_args(["--output", str(output_path)]) == 0

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["status"] == "PASS"
    assert written["panel_status"] == "RC_ALLOWED_INDEXED"
    assert written["actionable"] is False

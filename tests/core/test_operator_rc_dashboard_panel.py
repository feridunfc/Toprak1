from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.operator_rc_dashboard_panel as dashboard_panel


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _patch_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    artifact_dir = tmp_path / "docs" / "dashboard" / "artifacts"

    monkeypatch.setattr(dashboard_panel, "ROOT", tmp_path)
    monkeypatch.setattr(dashboard_panel, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(
        dashboard_panel,
        "INPUT_PATH",
        artifact_dir / "latest_operator_rc_evidence_panel.json",
    )
    monkeypatch.setattr(
        dashboard_panel,
        "OUTPUT_PATH",
        artifact_dir / "latest_operator_rc_dashboard_panel.json",
    )

    return artifact_dir


def _pass_operator_panel() -> dict:
    return {
        "source": "operator_rc_evidence_panel",
        "status": "PASS",
        "panel_status": "RC_ALLOWED_INDEXED",
        "decision_scope": dashboard_panel.DECISION_SCOPE,
        "badges": [
            "READY",
            "RC_ALLOWED",
            "BOUNDARY_PASS",
            "INDEXED",
            "NOT_PRODUCTION_DEPLOYMENT",
        ],
        "operator_message": (
            "Staging RC evidence is indexed and allowed. "
            "This is not a production deployment."
        ),
        "actionable": False,
        "input_artifact": "docs/dashboard/artifacts/latest_staging_rc_evidence_index.json",
        "production_ready_claim": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "redis_mutation_attempted": False,
        "runtime_mutation_attempted": False,
        "canonical_state_mutation_attempted": False,
        "evidence_manifest_hash": "abc123",
        "indexed_artifacts_count": 5,
        "failing_reasons": [],
    }


def _write_pass_operator_panel(artifact_dir: Path) -> None:
    _write_json(
        artifact_dir / "latest_operator_rc_evidence_panel.json",
        _pass_operator_panel(),
    )


def test_dashboard_panel_passes_for_allowed_operator_panel(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_operator_panel(artifact_dir)

    rendered = dashboard_panel.build_dashboard_panel()

    assert rendered["status"] == "PASS"
    assert rendered["title"] == "Staging RC Evidence"
    assert rendered["panel_status"] == "RC_ALLOWED_INDEXED"
    assert rendered["actionable"] is False
    assert rendered["actions"] == []
    assert rendered["source"] == dashboard_panel.SOURCE
    assert rendered["input_artifact"] == dashboard_panel.INPUT_ARTIFACT
    assert rendered["evidence_manifest_hash"] == "abc123"
    assert rendered["indexed_artifacts_count"] == 5
    assert rendered["failing_reasons"] == []
    assert "NOT_PRODUCTION_DEPLOYMENT" in rendered["badges"]


def test_dashboard_panel_missing_input_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_paths(monkeypatch, tmp_path)

    rendered = dashboard_panel.build_dashboard_panel()

    assert rendered["status"] == "FAIL"
    assert rendered["panel_status"] == "PANEL_UNAVAILABLE"
    assert rendered["actionable"] is False
    assert rendered["actions"] == []
    assert "NOT_PRODUCTION_DEPLOYMENT" in rendered["badges"]
    assert any(
        reason.startswith("missing operator panel artifact:")
        for reason in rendered["failing_reasons"]
    )


def test_dashboard_panel_malformed_input_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    path = artifact_dir / "latest_operator_rc_evidence_panel.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not-json", encoding="utf-8")

    rendered = dashboard_panel.build_dashboard_panel()

    assert rendered["status"] == "FAIL"
    assert rendered["panel_status"] == "PANEL_INVALID"
    assert rendered["actionable"] is False
    assert rendered["actions"] == []
    assert "NOT_PRODUCTION_DEPLOYMENT" in rendered["badges"]
    assert any(
        reason.startswith("malformed operator panel artifact:")
        for reason in rendered["failing_reasons"]
    )


def test_dashboard_panel_operator_fail_is_degraded_read_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_operator_panel()
    payload["status"] = "FAIL"
    payload["panel_status"] = "RC_BLOCKED_OR_NOT_READY"
    payload["failing_reasons"] = ["evidence freeze status is not PASS"]
    _write_json(artifact_dir / "latest_operator_rc_evidence_panel.json", payload)

    rendered = dashboard_panel.build_dashboard_panel()

    assert rendered["status"] == "FAIL"
    assert rendered["panel_status"] == "RC_BLOCKED_OR_NOT_READY"
    assert rendered["actionable"] is False
    assert rendered["actions"] == []
    assert "evidence freeze status is not PASS" in rendered["failing_reasons"]


def test_dashboard_panel_invalid_decision_scope_is_safety_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_operator_panel()
    payload["decision_scope"] = "PRODUCTION_DEPLOYMENT"
    _write_json(artifact_dir / "latest_operator_rc_evidence_panel.json", payload)

    rendered = dashboard_panel.build_dashboard_panel()

    assert rendered["status"] == "FAIL"
    assert rendered["panel_status"] == "SAFETY_VIOLATION"
    assert rendered["actionable"] is False
    assert "decision scope is invalid" in rendered["failing_reasons"]


def test_dashboard_panel_actionable_true_is_safety_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_operator_panel()
    payload["actionable"] = True
    _write_json(artifact_dir / "latest_operator_rc_evidence_panel.json", payload)

    rendered = dashboard_panel.build_dashboard_panel()

    assert rendered["status"] == "FAIL"
    assert rendered["panel_status"] == "SAFETY_VIOLATION"
    assert rendered["actionable"] is False
    assert rendered["actions"] == []
    assert "operator panel actionable is not false" in rendered["failing_reasons"]


def test_dashboard_panel_missing_manifest_hash_blocks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_operator_panel()
    payload["evidence_manifest_hash"] = ""
    _write_json(artifact_dir / "latest_operator_rc_evidence_panel.json", payload)

    rendered = dashboard_panel.build_dashboard_panel()

    assert rendered["status"] == "FAIL"
    assert rendered["panel_status"] == "RC_BLOCKED_OR_NOT_READY"
    assert rendered["actionable"] is False
    assert "evidence manifest hash is missing" in rendered["failing_reasons"]


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
def test_dashboard_panel_safety_flags_are_violations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    flag: str,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_operator_panel()
    payload[flag] = True
    _write_json(artifact_dir / "latest_operator_rc_evidence_panel.json", payload)

    rendered = dashboard_panel.build_dashboard_panel()

    assert rendered["status"] == "FAIL"
    assert rendered["panel_status"] == "SAFETY_VIOLATION"
    assert rendered["actionable"] is False
    assert rendered["actions"] == []
    assert f"{flag} is not false" in rendered["failing_reasons"]


def test_dashboard_panel_forbidden_action_labels_are_not_rendered_as_actions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_operator_panel(artifact_dir)

    rendered = dashboard_panel.build_dashboard_panel()

    rendered_actions = " ".join(rendered["actions"])
    for label in dashboard_panel.FORBIDDEN_ACTION_LABELS:
        assert label not in rendered_actions


def test_dashboard_panel_forces_not_production_badge_when_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    payload = _pass_operator_panel()
    payload["badges"] = ["READY", "RC_ALLOWED"]
    _write_json(artifact_dir / "latest_operator_rc_evidence_panel.json", payload)

    rendered = dashboard_panel.build_dashboard_panel()

    assert "NOT_PRODUCTION_DEPLOYMENT" in rendered["badges"]


def test_dashboard_panel_main_writes_output_and_returns_zero_on_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_operator_panel(artifact_dir)
    output_path = artifact_dir / "dashboard-panel.json"

    assert dashboard_panel.main_args(["--output", str(output_path)]) == 0

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["status"] == "PASS"
    assert written["title"] == "Staging RC Evidence"
    assert written["panel_status"] == "RC_ALLOWED_INDEXED"
    assert written["actionable"] is False
    assert written["actions"] == []

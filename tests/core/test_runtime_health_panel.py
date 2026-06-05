from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.runtime_health_panel as health


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _patch_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    artifact_dir = tmp_path / "docs" / "dashboard" / "artifacts"

    monkeypatch.setattr(health, "ROOT", tmp_path)
    monkeypatch.setattr(health, "ARTIFACT_DIR", artifact_dir)

    monkeypatch.setattr(health, "AUTHORITY_PATH", artifact_dir / "latest_authority.json")
    monkeypatch.setattr(health, "REPLAY_PATH", artifact_dir / "latest_replay.json")
    monkeypatch.setattr(
        health,
        "REDIS_FAILOVER_PATH",
        artifact_dir / "latest_redis_failover_smoke.json",
    )
    monkeypatch.setattr(
        health,
        "RECOVERY_AUDIT_PATH",
        artifact_dir / "latest_recovery_audit.json",
    )
    monkeypatch.setattr(
        health,
        "RECOVERY_REQUEUE_PATH",
        artifact_dir / "latest_recovery_requeue.json",
    )
    monkeypatch.setattr(
        health,
        "RECOVERY_REQUEUE_DRILL_PATH",
        artifact_dir / "latest_recovery_requeue_drill.json",
    )
    monkeypatch.setattr(
        health,
        "COLD_RESTART_DRILL_PATH",
        artifact_dir / "latest_cold_restart_drill.json",
    )
    monkeypatch.setattr(
        health,
        "ZOMBIE_COMPLETION_DRILL_PATH",
        artifact_dir / "latest_zombie_completion_drill.json",
    )
    monkeypatch.setattr(
        health,
        "RECOVERY_AUTO_RESUME_GUARDRAIL_PATH",
        artifact_dir / "latest_recovery_auto_resume_guardrail.json",
    )
    monkeypatch.setattr(
        health,
        "OUTPUT_PATH",
        artifact_dir / "latest_runtime_health_panel.json",
    )

    monkeypatch.setattr(
        health,
        "REQUIRED_ARTIFACTS",
        (
            ("authority", artifact_dir / "latest_authority.json"),
            ("replay", artifact_dir / "latest_replay.json"),
            ("redis_failover_smoke", artifact_dir / "latest_redis_failover_smoke.json"),
            ("recovery_audit", artifact_dir / "latest_recovery_audit.json"),
            ("recovery_requeue", artifact_dir / "latest_recovery_requeue.json"),
            (
                "recovery_requeue_drill",
                artifact_dir / "latest_recovery_requeue_drill.json",
            ),
            ("cold_restart_drill", artifact_dir / "latest_cold_restart_drill.json"),
            (
                "zombie_completion_drill",
                artifact_dir / "latest_zombie_completion_drill.json",
            ),
            (
                "recovery_auto_resume_guardrail",
                artifact_dir / "latest_recovery_auto_resume_guardrail.json",
            ),
        ),
    )

    return artifact_dir


def _write_pass_inputs(artifact_dir: Path) -> None:
    _write_json(artifact_dir / "latest_authority.json", {"status": "PASS"})
    _write_json(artifact_dir / "latest_replay.json", {"status": "PASS"})
    _write_json(artifact_dir / "latest_redis_failover_smoke.json", {"status": "PASS"})
    _write_json(artifact_dir / "latest_recovery_audit.json", {"status": "PASS"})
    _write_json(artifact_dir / "latest_recovery_requeue.json", {"status": "PASS"})
    _write_json(
        artifact_dir / "latest_recovery_requeue_drill.json",
        {
            "status": "PASS",
            "mutation_attempted": True,
        },
    )
    _write_json(artifact_dir / "latest_cold_restart_drill.json", {"status": "SKIPPED"})
    _write_json(
        artifact_dir / "latest_zombie_completion_drill.json",
        {"status": "SKIPPED"},
    )
    _write_json(
        artifact_dir / "latest_recovery_auto_resume_guardrail.json",
        {"status": "PASS"},
    )


def test_runtime_health_panel_passes_for_visible_runtime_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    panel = health.build_runtime_health_panel()

    assert panel["status"] == "PASS"
    assert panel["panel_status"] == "RUNTIME_HEALTH_VISIBLE"
    assert panel["title"] == "Runtime Health"
    assert panel["redis_reachable"] is True
    assert panel["worker_heartbeat_visible"] is True
    assert panel["scheduler_control_visible"] is True
    assert panel["authority_status"] == "PASS"
    assert panel["replay_status"] == "PASS"
    assert panel["recovery_audit_status"] == "PASS"
    assert panel["actionable"] is False
    assert panel["actions"] == []
    assert panel["badges"] == health.READ_ONLY_BADGES
    assert panel["production_ready_claim"] is False
    assert panel["deployment_attempted"] is False
    assert panel["release_tag_created"] is False
    assert panel["redis_mutation_attempted"] is False
    assert panel["runtime_mutation_attempted"] is False
    assert panel["canonical_state_mutation_attempted"] is False
    assert panel["failing_reasons"] == []


def test_runtime_health_panel_missing_artifact_degrades(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_replay.json").unlink()

    panel = health.build_runtime_health_panel()

    assert panel["status"] == "FAIL"
    assert panel["panel_status"] == "RUNTIME_HEALTH_DEGRADED"
    assert panel["actionable"] is False
    assert any(
        reason.startswith("missing artifact:")
        for reason in panel["failing_reasons"]
    )


def test_runtime_health_panel_malformed_artifact_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_replay.json").write_text("{not-json", encoding="utf-8")

    panel = health.build_runtime_health_panel()

    assert panel["status"] == "FAIL"
    assert panel["panel_status"] == "PANEL_INVALID"
    assert panel["actionable"] is False
    assert any(
        reason.startswith("malformed artifact:")
        for reason in panel["failing_reasons"]
    )


def test_runtime_health_panel_replay_fail_degrades(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(artifact_dir / "latest_replay.json", {"status": "FAIL"})

    panel = health.build_runtime_health_panel()

    assert panel["status"] == "FAIL"
    assert panel["panel_status"] == "RUNTIME_HEALTH_DEGRADED"
    assert "replay status is not PASS or acceptable SKIPPED" in panel["failing_reasons"]


def test_runtime_health_panel_redis_fail_degrades(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(artifact_dir / "latest_redis_failover_smoke.json", {"status": "FAIL"})

    panel = health.build_runtime_health_panel()

    assert panel["status"] == "FAIL"
    assert panel["panel_status"] == "RUNTIME_HEALTH_DEGRADED"
    assert panel["redis_reachable"] is False
    assert "redis reachable evidence is not PASS" in panel["failing_reasons"]


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
def test_runtime_health_panel_safety_flags_are_violations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    flag: str,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    _write_json(
        artifact_dir / "latest_recovery_audit.json",
        {
            "status": "PASS",
            flag: True,
        },
    )

    panel = health.build_runtime_health_panel()

    assert panel["status"] == "FAIL"
    assert panel["panel_status"] == "SAFETY_VIOLATION"
    assert panel["actionable"] is False
    assert f"recovery_audit {flag} is true" in panel["failing_reasons"]


def test_runtime_health_panel_actions_are_safety_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    _write_json(
        artifact_dir / "latest_recovery_audit.json",
        {
            "status": "PASS",
            "actions": ["Requeue"],
        },
    )

    panel = health.build_runtime_health_panel()

    assert panel["status"] == "FAIL"
    assert panel["panel_status"] == "SAFETY_VIOLATION"
    assert "recovery_audit exposes actions" in panel["failing_reasons"]


def test_runtime_health_panel_actionable_true_is_safety_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    _write_json(
        artifact_dir / "latest_recovery_audit.json",
        {
            "status": "PASS",
            "actionable": True,
        },
    )

    panel = health.build_runtime_health_panel()

    assert panel["status"] == "FAIL"
    assert panel["panel_status"] == "SAFETY_VIOLATION"
    assert "recovery_audit actionable is true" in panel["failing_reasons"]


def test_runtime_health_panel_required_badges_are_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    panel = health.build_runtime_health_panel()

    assert "READ_ONLY" in panel["badges"]
    assert "NO_RUNTIME_MUTATION" in panel["badges"]
    assert "NO_OPERATOR_ACTIONS" in panel["badges"]


def test_runtime_health_panel_forbidden_action_labels_are_not_rendered_as_actions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    panel = health.build_runtime_health_panel()

    rendered_actions = " ".join(panel["actions"])
    for label in health.FORBIDDEN_ACTION_LABELS:
        assert label not in rendered_actions


def test_runtime_health_panel_main_writes_output_and_returns_zero_on_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    output_path = artifact_dir / "runtime-health-panel.json"

    assert health.main_args(["--output", str(output_path)]) == 0

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["status"] == "PASS"
    assert written["panel_status"] == "RUNTIME_HEALTH_VISIBLE"
    assert written["actionable"] is False
    assert written["actions"] == []

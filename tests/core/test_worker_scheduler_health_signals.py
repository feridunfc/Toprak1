from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.worker_scheduler_health_signals as signals


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _patch_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    artifact_dir = tmp_path / "docs" / "dashboard" / "artifacts"

    monkeypatch.setattr(signals, "ROOT", tmp_path)
    monkeypatch.setattr(signals, "ARTIFACT_DIR", artifact_dir)

    monkeypatch.setattr(signals, "WORKER_OUTPUT_PATH", artifact_dir / "latest_worker_health_signal.json")
    monkeypatch.setattr(signals, "SCHEDULER_OUTPUT_PATH", artifact_dir / "latest_scheduler_control_signal.json")

    monkeypatch.setattr(signals, "RECOVERY_AUDIT_PATH", artifact_dir / "latest_recovery_audit.json")
    monkeypatch.setattr(signals, "RECOVERY_REQUEUE_PATH", artifact_dir / "latest_recovery_requeue.json")
    monkeypatch.setattr(signals, "RECOVERY_REQUEUE_DRILL_PATH", artifact_dir / "latest_recovery_requeue_drill.json")
    monkeypatch.setattr(signals, "ZOMBIE_COMPLETION_DRILL_PATH", artifact_dir / "latest_zombie_completion_drill.json")

    monkeypatch.setattr(
        signals,
        "RECOVERY_AUTO_RESUME_GUARDRAIL_PATH",
        artifact_dir / "latest_recovery_auto_resume_guardrail.json",
    )
    monkeypatch.setattr(signals, "COLD_RESTART_DRILL_PATH", artifact_dir / "latest_cold_restart_drill.json")

    monkeypatch.setattr(
        signals,
        "WORKER_INPUTS",
        (
            ("recovery_audit", artifact_dir / "latest_recovery_audit.json"),
            ("recovery_requeue", artifact_dir / "latest_recovery_requeue.json"),
            ("recovery_requeue_drill", artifact_dir / "latest_recovery_requeue_drill.json"),
            ("zombie_completion_drill", artifact_dir / "latest_zombie_completion_drill.json"),
        ),
    )
    monkeypatch.setattr(
        signals,
        "SCHEDULER_INPUTS",
        (
            ("recovery_auto_resume_guardrail", artifact_dir / "latest_recovery_auto_resume_guardrail.json"),
            ("cold_restart_drill", artifact_dir / "latest_cold_restart_drill.json"),
            ("zombie_completion_drill", artifact_dir / "latest_zombie_completion_drill.json"),
        ),
    )

    return artifact_dir


def _write_pass_inputs(artifact_dir: Path) -> None:
    _write_json(artifact_dir / "latest_recovery_audit.json", {"status": "PASS"})
    _write_json(artifact_dir / "latest_recovery_requeue.json", {"status": "PASS"})
    _write_json(artifact_dir / "latest_recovery_requeue_drill.json", {"status": "PASS", "mutation_attempted": True})
    _write_json(artifact_dir / "latest_zombie_completion_drill.json", {"status": "SKIPPED"})
    _write_json(artifact_dir / "latest_recovery_auto_resume_guardrail.json", {"status": "PASS"})
    _write_json(artifact_dir / "latest_cold_restart_drill.json", {"status": "SKIPPED"})


def test_worker_signal_passes_for_visible_worker_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    worker = signals.build_worker_health_signal()

    assert worker["status"] == "PASS"
    assert worker["signal_status"] == "WORKER_HEARTBEAT_VISIBLE"
    assert worker["worker_heartbeat_visible"] is True
    assert worker["evidence_source"] == signals.EVIDENCE_SOURCE
    assert worker["actionable"] is False
    assert worker["actions"] == []
    assert worker["redis_mutation_attempted"] is False
    assert worker["runtime_mutation_attempted"] is False
    assert worker["requeue_attempted"] is False
    assert worker["auto_resume_attempted"] is False
    assert worker["production_ready_claim"] is False
    assert worker["failing_reasons"] == []


def test_scheduler_signal_passes_for_visible_scheduler_control(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    scheduler = signals.build_scheduler_control_signal()

    assert scheduler["status"] == "PASS"
    assert scheduler["signal_status"] == "SCHEDULER_CONTROL_VISIBLE"
    assert scheduler["scheduler_control_visible"] is True
    assert scheduler["evidence_source"] == signals.EVIDENCE_SOURCE
    assert scheduler["actionable"] is False
    assert scheduler["actions"] == []
    assert scheduler["redis_mutation_attempted"] is False
    assert scheduler["runtime_mutation_attempted"] is False
    assert scheduler["requeue_attempted"] is False
    assert scheduler["auto_resume_attempted"] is False
    assert scheduler["production_ready_claim"] is False
    assert scheduler["failing_reasons"] == []


def test_worker_signal_missing_evidence_degrades(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_recovery_audit.json").unlink()

    worker = signals.build_worker_health_signal()

    assert worker["status"] == "FAIL"
    assert worker["signal_status"] == "WORKER_HEARTBEAT_DEGRADED"
    assert worker["worker_heartbeat_visible"] is False
    assert any(reason.startswith("missing artifact:") for reason in worker["failing_reasons"])


def test_scheduler_signal_missing_evidence_degrades(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_recovery_auto_resume_guardrail.json").unlink()

    scheduler = signals.build_scheduler_control_signal()

    assert scheduler["status"] == "FAIL"
    assert scheduler["signal_status"] == "SCHEDULER_CONTROL_DEGRADED"
    assert scheduler["scheduler_control_visible"] is False
    assert any(reason.startswith("missing artifact:") for reason in scheduler["failing_reasons"])


def test_worker_signal_malformed_evidence_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_recovery_audit.json").write_text("{not-json", encoding="utf-8")

    worker = signals.build_worker_health_signal()

    assert worker["status"] == "FAIL"
    assert worker["signal_status"] == "WORKER_HEALTH_INVALID"
    assert any(reason.startswith("malformed artifact:") for reason in worker["failing_reasons"])


def test_scheduler_signal_malformed_evidence_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_recovery_auto_resume_guardrail.json").write_text("{not-json", encoding="utf-8")

    scheduler = signals.build_scheduler_control_signal()

    assert scheduler["status"] == "FAIL"
    assert scheduler["signal_status"] == "SCHEDULER_CONTROL_INVALID"
    assert any(reason.startswith("malformed artifact:") for reason in scheduler["failing_reasons"])


def test_worker_signal_source_fail_degrades(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(artifact_dir / "latest_recovery_audit.json", {"status": "FAIL"})

    worker = signals.build_worker_health_signal()

    assert worker["status"] == "FAIL"
    assert worker["signal_status"] == "WORKER_HEARTBEAT_DEGRADED"
    assert "recovery_audit status is not PASS or acceptable SKIPPED" in worker["failing_reasons"]


def test_scheduler_signal_source_fail_degrades(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(artifact_dir / "latest_recovery_auto_resume_guardrail.json", {"status": "FAIL"})

    scheduler = signals.build_scheduler_control_signal()

    assert scheduler["status"] == "FAIL"
    assert scheduler["signal_status"] == "SCHEDULER_CONTROL_DEGRADED"
    assert "recovery_auto_resume_guardrail status is not PASS or acceptable SKIPPED" in scheduler["failing_reasons"]


@pytest.mark.parametrize(
    "flag",
    [
        "redis_mutation_attempted",
        "runtime_mutation_attempted",
        "canonical_state_mutation_attempted",
        "requeue_attempted",
        "auto_resume_attempted",
        "deployment_attempted",
        "release_tag_created",
        "production_ready_claim",
    ],
)
def test_worker_signal_safety_flags_are_violations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    flag: str,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(artifact_dir / "latest_recovery_audit.json", {"status": "PASS", flag: True})

    worker = signals.build_worker_health_signal()

    assert worker["status"] == "FAIL"
    assert worker["signal_status"] == "SAFETY_VIOLATION"
    assert f"recovery_audit {flag} is true" in worker["failing_reasons"]


@pytest.mark.parametrize(
    "flag",
    [
        "redis_mutation_attempted",
        "runtime_mutation_attempted",
        "canonical_state_mutation_attempted",
        "requeue_attempted",
        "auto_resume_attempted",
        "deployment_attempted",
        "release_tag_created",
        "production_ready_claim",
    ],
)
def test_scheduler_signal_safety_flags_are_violations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    flag: str,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(
        artifact_dir / "latest_recovery_auto_resume_guardrail.json",
        {"status": "PASS", flag: True},
    )

    scheduler = signals.build_scheduler_control_signal()

    assert scheduler["status"] == "FAIL"
    assert scheduler["signal_status"] == "SAFETY_VIOLATION"
    assert f"recovery_auto_resume_guardrail {flag} is true" in scheduler["failing_reasons"]


def test_worker_signal_actions_are_safety_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(artifact_dir / "latest_recovery_audit.json", {"status": "PASS", "actions": ["Requeue"]})

    worker = signals.build_worker_health_signal()

    assert worker["status"] == "FAIL"
    assert worker["signal_status"] == "SAFETY_VIOLATION"
    assert "recovery_audit exposes actions" in worker["failing_reasons"]


def test_scheduler_signal_actionable_true_is_safety_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(
        artifact_dir / "latest_recovery_auto_resume_guardrail.json",
        {"status": "PASS", "actionable": True},
    )

    scheduler = signals.build_scheduler_control_signal()

    assert scheduler["status"] == "FAIL"
    assert scheduler["signal_status"] == "SAFETY_VIOLATION"
    assert "recovery_auto_resume_guardrail actionable is true" in scheduler["failing_reasons"]


def test_main_writes_both_signal_artifacts_and_returns_zero_on_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)

    assert signals.main_args([]) == 0

    worker = json.loads((artifact_dir / "latest_worker_health_signal.json").read_text(encoding="utf-8"))
    scheduler = json.loads((artifact_dir / "latest_scheduler_control_signal.json").read_text(encoding="utf-8"))

    assert worker["signal_status"] == "WORKER_HEARTBEAT_VISIBLE"
    assert scheduler["signal_status"] == "SCHEDULER_CONTROL_VISIBLE"


def test_main_returns_zero_on_degraded_signals(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_recovery_audit.json").unlink()

    assert signals.main_args([]) == 0

    worker = json.loads((artifact_dir / "latest_worker_health_signal.json").read_text(encoding="utf-8"))
    assert worker["signal_status"] == "WORKER_HEARTBEAT_DEGRADED"


def test_main_returns_nonzero_on_invalid_signal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    (artifact_dir / "latest_recovery_audit.json").write_text("{not-json", encoding="utf-8")

    assert signals.main_args([]) == 1


def test_main_returns_nonzero_on_safety_violation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)
    _write_pass_inputs(artifact_dir)
    _write_json(artifact_dir / "latest_recovery_audit.json", {"status": "PASS", "actionable": True})

    assert signals.main_args([]) == 1

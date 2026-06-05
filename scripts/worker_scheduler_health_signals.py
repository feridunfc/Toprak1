from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"

WORKER_OUTPUT_PATH = ARTIFACT_DIR / "latest_worker_health_signal.json"
SCHEDULER_OUTPUT_PATH = ARTIFACT_DIR / "latest_scheduler_control_signal.json"

RECOVERY_AUDIT_PATH = ARTIFACT_DIR / "latest_recovery_audit.json"
RECOVERY_REQUEUE_PATH = ARTIFACT_DIR / "latest_recovery_requeue.json"
RECOVERY_REQUEUE_DRILL_PATH = ARTIFACT_DIR / "latest_recovery_requeue_drill.json"
ZOMBIE_COMPLETION_DRILL_PATH = ARTIFACT_DIR / "latest_zombie_completion_drill.json"

RECOVERY_AUTO_RESUME_GUARDRAIL_PATH = (
    ARTIFACT_DIR / "latest_recovery_auto_resume_guardrail.json"
)
COLD_RESTART_DRILL_PATH = ARTIFACT_DIR / "latest_cold_restart_drill.json"

WORKER_SOURCE = "worker_health_signal"
SCHEDULER_SOURCE = "scheduler_control_signal"
EVIDENCE_SOURCE = "artifacts"

PASS_OR_ACCEPTABLE_SKIPPED = {"PASS", "SKIPPED"}

WORKER_INPUTS: tuple[tuple[str, Path], ...] = (
    ("recovery_audit", RECOVERY_AUDIT_PATH),
    ("recovery_requeue", RECOVERY_REQUEUE_PATH),
    ("recovery_requeue_drill", RECOVERY_REQUEUE_DRILL_PATH),
    ("zombie_completion_drill", ZOMBIE_COMPLETION_DRILL_PATH),
)

SCHEDULER_INPUTS: tuple[tuple[str, Path], ...] = (
    ("recovery_auto_resume_guardrail", RECOVERY_AUTO_RESUME_GUARDRAIL_PATH),
    ("cold_restart_drill", COLD_RESTART_DRILL_PATH),
    ("zombie_completion_drill", ZOMBIE_COMPLETION_DRILL_PATH),
)


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, f"missing artifact:{_rel(path)}"
    except OSError as exc:
        return None, f"unreadable artifact:{_rel(path)}:{exc}"

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"malformed artifact:{_rel(path)}:{exc}"

    if not isinstance(data, dict):
        return None, f"malformed artifact:{_rel(path)}:expected object"

    return data, None


def _status(data: dict[str, Any]) -> str | None:
    value = data.get("status")
    return value if isinstance(value, str) else None


def _artifact_entry(path: Path, present: bool, valid_json: bool, status: str | None) -> dict[str, Any]:
    return {
        "path": _rel(path),
        "present": present,
        "valid_json": valid_json,
        "status": status,
    }


def _safety_findings(data: dict[str, Any], name: str) -> list[str]:
    findings: list[str] = []

    forbidden_flags = [
        "redis_mutation_attempted",
        "runtime_mutation_attempted",
        "canonical_state_mutation_attempted",
        "requeue_attempted",
        "auto_resume_attempted",
        "deployment_attempted",
        "release_tag_created",
        "production_ready_claim",
    ]

    for flag in forbidden_flags:
        if data.get(flag) is True:
            findings.append(f"{name} {flag} is true")

    actions = data.get("actions")
    if isinstance(actions, list) and actions:
        findings.append(f"{name} exposes actions")

    if data.get("actionable") is True:
        findings.append(f"{name} actionable is true")

    return findings


def _collect_inputs(
    inputs: tuple[tuple[str, Path], ...],
) -> tuple[dict[str, dict[str, Any]], list[str], list[str], bool]:
    observed: dict[str, dict[str, Any]] = {}
    degraded_reasons: list[str] = []
    safety_reasons: list[str] = []
    invalid = False

    for name, path in inputs:
        data, error = _read_json(path)

        if error:
            if error.startswith("malformed") or error.startswith("unreadable"):
                observed[name] = _artifact_entry(path, present=True, valid_json=False, status=None)
                degraded_reasons.append(error)
                invalid = True
                continue

            observed[name] = _artifact_entry(path, present=False, valid_json=False, status=None)
            degraded_reasons.append(error)
            continue

        assert data is not None
        artifact_status = _status(data)
        observed[name] = _artifact_entry(
            path,
            present=True,
            valid_json=True,
            status=artifact_status,
        )

        safety_reasons.extend(_safety_findings(data, name))

        if artifact_status not in PASS_OR_ACCEPTABLE_SKIPPED:
            degraded_reasons.append(f"{name} status is not PASS or acceptable SKIPPED")

    return observed, degraded_reasons, safety_reasons, invalid


def build_worker_health_signal() -> dict[str, Any]:
    observed, degraded_reasons, safety_reasons, invalid = _collect_inputs(WORKER_INPUTS)

    if safety_reasons:
        status = "FAIL"
        signal_status = "SAFETY_VIOLATION"
        visible = False
        failing_reasons = degraded_reasons + safety_reasons
    elif invalid:
        status = "FAIL"
        signal_status = "WORKER_HEALTH_INVALID"
        visible = False
        failing_reasons = degraded_reasons
    elif degraded_reasons:
        status = "FAIL"
        signal_status = "WORKER_HEARTBEAT_DEGRADED"
        visible = False
        failing_reasons = degraded_reasons
    else:
        status = "PASS"
        signal_status = "WORKER_HEARTBEAT_VISIBLE"
        visible = True
        failing_reasons = []

    return {
        "source": WORKER_SOURCE,
        "status": status,
        "signal_status": signal_status,
        "worker_heartbeat_visible": visible,
        "evidence_source": EVIDENCE_SOURCE,
        "actionable": False,
        "actions": [],
        "redis_mutation_attempted": False,
        "runtime_mutation_attempted": False,
        "canonical_state_mutation_attempted": False,
        "requeue_attempted": False,
        "auto_resume_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "production_ready_claim": False,
        "observed_artifacts": observed,
        "failing_reasons": failing_reasons,
    }


def build_scheduler_control_signal() -> dict[str, Any]:
    observed, degraded_reasons, safety_reasons, invalid = _collect_inputs(SCHEDULER_INPUTS)

    if safety_reasons:
        status = "FAIL"
        signal_status = "SAFETY_VIOLATION"
        visible = False
        failing_reasons = degraded_reasons + safety_reasons
    elif invalid:
        status = "FAIL"
        signal_status = "SCHEDULER_CONTROL_INVALID"
        visible = False
        failing_reasons = degraded_reasons
    elif degraded_reasons:
        status = "FAIL"
        signal_status = "SCHEDULER_CONTROL_DEGRADED"
        visible = False
        failing_reasons = degraded_reasons
    else:
        status = "PASS"
        signal_status = "SCHEDULER_CONTROL_VISIBLE"
        visible = True
        failing_reasons = []

    return {
        "source": SCHEDULER_SOURCE,
        "status": status,
        "signal_status": signal_status,
        "scheduler_control_visible": visible,
        "evidence_source": EVIDENCE_SOURCE,
        "actionable": False,
        "actions": [],
        "redis_mutation_attempted": False,
        "runtime_mutation_attempted": False,
        "canonical_state_mutation_attempted": False,
        "requeue_attempted": False,
        "auto_resume_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "production_ready_claim": False,
        "observed_artifacts": observed,
        "failing_reasons": failing_reasons,
    }


def write_outputs(worker: dict[str, Any], scheduler: dict[str, Any]) -> None:
    WORKER_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    WORKER_OUTPUT_PATH.write_text(
        json.dumps(worker, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    SCHEDULER_OUTPUT_PATH.write_text(
        json.dumps(scheduler, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate read-only worker and scheduler health signal artifacts."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print generated signal artifacts to stdout.",
    )
    args = parser.parse_args(argv)

    worker = build_worker_health_signal()
    scheduler = build_scheduler_control_signal()
    write_outputs(worker, scheduler)

    payload = {
        "source": "worker_scheduler_health_signals",
        "status": "PASS"
        if worker["signal_status"] != "SAFETY_VIOLATION"
        and scheduler["signal_status"] != "SAFETY_VIOLATION"
        else "FAIL",
        "worker": worker,
        "scheduler": scheduler,
    }

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))

    hard_fail = (
        worker["signal_status"] == "SAFETY_VIOLATION"
        or scheduler["signal_status"] == "SAFETY_VIOLATION"
        or worker["signal_status"].endswith("_INVALID")
        or scheduler["signal_status"].endswith("_INVALID")
    )
    return 1 if hard_fail else 0


if __name__ == "__main__":
    raise SystemExit(main_args())

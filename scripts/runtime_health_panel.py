from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"

AUTHORITY_PATH = ARTIFACT_DIR / "latest_authority.json"
REPLAY_PATH = ARTIFACT_DIR / "latest_replay.json"
REDIS_FAILOVER_PATH = ARTIFACT_DIR / "latest_redis_failover_smoke.json"
RECOVERY_AUDIT_PATH = ARTIFACT_DIR / "latest_recovery_audit.json"
RECOVERY_REQUEUE_PATH = ARTIFACT_DIR / "latest_recovery_requeue.json"
RECOVERY_REQUEUE_DRILL_PATH = ARTIFACT_DIR / "latest_recovery_requeue_drill.json"
COLD_RESTART_DRILL_PATH = ARTIFACT_DIR / "latest_cold_restart_drill.json"
ZOMBIE_COMPLETION_DRILL_PATH = ARTIFACT_DIR / "latest_zombie_completion_drill.json"
RECOVERY_AUTO_RESUME_GUARDRAIL_PATH = (
    ARTIFACT_DIR / "latest_recovery_auto_resume_guardrail.json"
)

OUTPUT_PATH = ARTIFACT_DIR / "latest_runtime_health_panel.json"

SOURCE = "dashboard_runtime_health_panel"
TITLE = "Runtime Health"

READ_ONLY_BADGES = [
    "READ_ONLY",
    "NO_RUNTIME_MUTATION",
    "NO_OPERATOR_ACTIONS",
]

FORBIDDEN_ACTION_LABELS = [
    "Deploy",
    "Promote",
    "Promote to production",
    "Create tag",
    "Create release tag",
    "Release",
    "Auto resume",
    "Requeue",
    "Retry",
    "Retry mutation",
    "Recover",
    "Run recovery",
    "Approve deployment",
    "Enable production",
    "Run release",
    "Mutate Redis",
    "Restart worker",
    "Resume scheduler",
]

REQUIRED_ARTIFACTS: tuple[tuple[str, Path], ...] = (
    ("authority", AUTHORITY_PATH),
    ("replay", REPLAY_PATH),
    ("redis_failover_smoke", REDIS_FAILOVER_PATH),
    ("recovery_audit", RECOVERY_AUDIT_PATH),
    ("recovery_requeue", RECOVERY_REQUEUE_PATH),
    ("recovery_requeue_drill", RECOVERY_REQUEUE_DRILL_PATH),
    ("cold_restart_drill", COLD_RESTART_DRILL_PATH),
    ("zombie_completion_drill", ZOMBIE_COMPLETION_DRILL_PATH),
    ("recovery_auto_resume_guardrail", RECOVERY_AUTO_RESUME_GUARDRAIL_PATH),
)

PASS_OR_ACCEPTABLE_SKIPPED = {"PASS", "SKIPPED"}


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


def _artifact_status(data: dict[str, Any]) -> str | None:
    value = data.get("status")
    return value if isinstance(value, str) else None


def _base_panel(
    *,
    status: str,
    panel_status: str,
    failing_reasons: list[str],
    authority_status: str | None = None,
    replay_status: str | None = None,
    redis_reachable: bool = False,
    worker_heartbeat_visible: bool = False,
    scheduler_control_visible: bool = False,
    recovery_audit_status: str | None = None,
    observed_artifacts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "source": SOURCE,
        "status": status,
        "title": TITLE,
        "panel_status": panel_status,
        "redis_reachable": redis_reachable,
        "worker_heartbeat_visible": worker_heartbeat_visible,
        "scheduler_control_visible": scheduler_control_visible,
        "recovery_audit_status": recovery_audit_status,
        "replay_status": replay_status,
        "authority_status": authority_status,
        "actionable": False,
        "actions": [],
        "badges": READ_ONLY_BADGES,
        "forbidden_actions": FORBIDDEN_ACTION_LABELS,
        "production_ready_claim": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "redis_mutation_attempted": False,
        "runtime_mutation_attempted": False,
        "canonical_state_mutation_attempted": False,
        "operator_message": _operator_message(panel_status),
        "observed_artifacts": observed_artifacts or {},
        "failing_reasons": failing_reasons,
    }


def _operator_message(panel_status: str) -> str:
    if panel_status == "RUNTIME_HEALTH_VISIBLE":
        return "Runtime health evidence is visible. This panel is read-only and does not mutate runtime state."
    if panel_status == "PANEL_INVALID":
        return "Runtime health evidence is malformed. This panel is read-only and non-actionable."
    if panel_status == "SAFETY_VIOLATION":
        return "Runtime health panel detected a safety violation. This panel is read-only and non-actionable."
    if panel_status == "PANEL_UNAVAILABLE":
        return "Runtime health evidence is unavailable. This panel is read-only and non-actionable."
    return "Runtime health evidence is degraded. This panel is read-only and non-actionable."


def _has_forbidden_flags(data: dict[str, Any], artifact_name: str) -> list[str]:
    findings: list[str] = []

    forbidden_flags = [
        "production_ready_claim",
        "deployment_attempted",
        "release_tag_created",
        "redis_mutation_attempted",
        "runtime_mutation_attempted",
        "canonical_state_mutation_attempted",
    ]

    for flag in forbidden_flags:
        if data.get(flag) is True:
            findings.append(f"{artifact_name} {flag} is true")

    actions = data.get("actions")
    if isinstance(actions, list) and actions:
        findings.append(f"{artifact_name} exposes actions")

    actionable = data.get("actionable")
    if actionable is True:
        findings.append(f"{artifact_name} actionable is true")

    return findings


def _status_is_acceptable(name: str, status: str | None) -> bool:
    if status in PASS_OR_ACCEPTABLE_SKIPPED:
        return True

    # Some older authority artifacts use banned count rather than status.
    if name == "authority" and status is None:
        return True

    return False


def build_runtime_health_panel() -> dict[str, Any]:
    failing_reasons: list[str] = []
    safety_violations: list[str] = []
    loaded: dict[str, dict[str, Any]] = {}
    observed_artifacts: dict[str, Any] = {}

    for name, path in REQUIRED_ARTIFACTS:
        data, error = _read_json(path)

        if error:
            if error.startswith("malformed") or error.startswith("unreadable"):
                return _base_panel(
                    status="FAIL",
                    panel_status="PANEL_INVALID",
                    failing_reasons=[error],
                    observed_artifacts=observed_artifacts,
                )
            failing_reasons.append(error)
            observed_artifacts[name] = {
                "path": _rel(path),
                "present": False,
                "valid_json": False,
                "status": None,
            }
            continue

        assert data is not None
        loaded[name] = data
        artifact_status = _artifact_status(data)
        observed_artifacts[name] = {
            "path": _rel(path),
            "present": True,
            "valid_json": True,
            "status": artifact_status,
        }

        safety_violations.extend(_has_forbidden_flags(data, name))

        if not _status_is_acceptable(name, artifact_status):
            failing_reasons.append(f"{name} status is not PASS or acceptable SKIPPED")

    if safety_violations:
        return _base_panel(
            status="FAIL",
            panel_status="SAFETY_VIOLATION",
            failing_reasons=failing_reasons + safety_violations,
            observed_artifacts=observed_artifacts,
        )

    authority = loaded.get("authority", {})
    replay = loaded.get("replay", {})
    redis = loaded.get("redis_failover_smoke", {})
    recovery_audit = loaded.get("recovery_audit", {})
    recovery_requeue = loaded.get("recovery_requeue", {})
    recovery_requeue_drill = loaded.get("recovery_requeue_drill", {})
    auto_resume_guardrail = loaded.get("recovery_auto_resume_guardrail", {})

    authority_status = _artifact_status(authority)
    replay_status = _artifact_status(replay)
    redis_status = _artifact_status(redis)
    recovery_audit_status = _artifact_status(recovery_audit)

    redis_reachable = redis_status == "PASS"
    worker_heartbeat_visible = bool(
        recovery_audit
        or recovery_requeue
        or recovery_requeue_drill
    )
    scheduler_control_visible = bool(auto_resume_guardrail)

    if not redis_reachable:
        failing_reasons.append("redis reachable evidence is not PASS")
    if not worker_heartbeat_visible:
        failing_reasons.append("worker heartbeat visibility evidence is missing")
    if not scheduler_control_visible:
        failing_reasons.append("scheduler/control visibility evidence is missing")

    panel_status = (
        "RUNTIME_HEALTH_VISIBLE"
        if not failing_reasons
        else "RUNTIME_HEALTH_DEGRADED"
    )

    return _base_panel(
        status="PASS" if panel_status == "RUNTIME_HEALTH_VISIBLE" else "FAIL",
        panel_status=panel_status,
        failing_reasons=failing_reasons,
        authority_status=authority_status,
        replay_status=replay_status,
        redis_reachable=redis_reachable,
        worker_heartbeat_visible=worker_heartbeat_visible,
        scheduler_control_visible=scheduler_control_visible,
        recovery_audit_status=recovery_audit_status,
        observed_artifacts=observed_artifacts,
    )


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate read-only dashboard runtime health panel model."
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_PATH),
        help="Output JSON artifact path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print generated runtime health panel JSON to stdout.",
    )
    args = parser.parse_args(argv)

    panel = build_runtime_health_panel()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(panel, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if args.json:
        print(json.dumps(panel, indent=2, sort_keys=True))

    hard_fail_statuses = {"PANEL_INVALID", "SAFETY_VIOLATION"}
    return 1 if panel["panel_status"] in hard_fail_statuses else 0


if __name__ == "__main__":
    raise SystemExit(main_args())



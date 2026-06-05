from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"

INPUT_PATH = ARTIFACT_DIR / "latest_operator_rc_evidence_panel.json"
OUTPUT_PATH = ARTIFACT_DIR / "latest_operator_rc_dashboard_panel.json"

SOURCE = "operator_rc_dashboard_panel"
TITLE = "Staging RC Evidence"
INPUT_ARTIFACT = "docs/dashboard/artifacts/latest_operator_rc_evidence_panel.json"
DECISION_SCOPE = "EVIDENCE_BACKED_STAGING_RELEASE_CANDIDATE_ONLY"
NOT_PRODUCTION_BADGE = "NOT_PRODUCTION_DEPLOYMENT"

FORBIDDEN_ACTION_LABELS = [
    "Deploy",
    "Promote",
    "Promote to production",
    "Create tag",
    "Create release tag",
    "Release",
    "Auto resume",
    "Requeue",
    "Retry mutation",
    "Approve deployment",
    "Enable production",
    "Run release",
]


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _base_panel(
    *,
    status: str,
    panel_status: str,
    operator_message: str,
    failing_reasons: list[str],
    badges: list[str] | None = None,
    evidence_manifest_hash: str = "",
    indexed_artifacts_count: int = 0,
) -> dict[str, Any]:
    safe_badges = list(badges or [])
    if NOT_PRODUCTION_BADGE not in safe_badges:
        safe_badges.append(NOT_PRODUCTION_BADGE)

    return {
        "source": SOURCE,
        "status": status,
        "title": TITLE,
        "panel_status": panel_status,
        "badges": safe_badges,
        "evidence_manifest_hash": evidence_manifest_hash,
        "indexed_artifacts_count": indexed_artifacts_count,
        "operator_message": operator_message,
        "actionable": False,
        "actions": [],
        "forbidden_actions": FORBIDDEN_ACTION_LABELS,
        "input_artifact": INPUT_ARTIFACT,
        "decision_scope": DECISION_SCOPE,
        "production_ready_claim": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "redis_mutation_attempted": False,
        "runtime_mutation_attempted": False,
        "canonical_state_mutation_attempted": False,
        "failing_reasons": failing_reasons,
    }


def _read_operator_panel() -> tuple[dict[str, Any] | None, str | None]:
    try:
        raw = INPUT_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, f"missing operator panel artifact:{_rel(INPUT_PATH)}"
    except OSError as exc:
        return None, f"unreadable operator panel artifact:{_rel(INPUT_PATH)}:{exc}"

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"malformed operator panel artifact:{_rel(INPUT_PATH)}:{exc}"

    if not isinstance(data, dict):
        return None, f"malformed operator panel artifact:{_rel(INPUT_PATH)}:expected object"

    return data, None


def build_dashboard_panel() -> dict[str, Any]:
    operator_panel, error = _read_operator_panel()

    if error:
        panel_status = "PANEL_UNAVAILABLE" if error.startswith("missing") else "PANEL_INVALID"
        return _base_panel(
            status="FAIL",
            panel_status=panel_status,
            operator_message="Staging RC evidence dashboard panel is unavailable. This is not a production deployment.",
            failing_reasons=[error],
        )

    assert operator_panel is not None

    failing_reasons = list(operator_panel.get("failing_reasons") or [])
    badges = list(operator_panel.get("badges") or [])
    evidence_manifest_hash = operator_panel.get("evidence_manifest_hash")
    indexed_artifacts_count = operator_panel.get("indexed_artifacts_count")

    safety_violations: list[str] = []

    if operator_panel.get("decision_scope") != DECISION_SCOPE:
        safety_violations.append("decision scope is invalid")
    if operator_panel.get("actionable") is not False:
        safety_violations.append("operator panel actionable is not false")
    if operator_panel.get("production_ready_claim") is not False:
        safety_violations.append("production_ready_claim is not false")
    if operator_panel.get("deployment_attempted") is not False:
        safety_violations.append("deployment_attempted is not false")
    if operator_panel.get("release_tag_created") is not False:
        safety_violations.append("release_tag_created is not false")
    if operator_panel.get("redis_mutation_attempted") is not False:
        safety_violations.append("redis_mutation_attempted is not false")
    if operator_panel.get("runtime_mutation_attempted") is not False:
        safety_violations.append("runtime_mutation_attempted is not false")
    if operator_panel.get("canonical_state_mutation_attempted") is not False:
        safety_violations.append("canonical_state_mutation_attempted is not false")

    if safety_violations:
        return _base_panel(
            status="FAIL",
            panel_status="SAFETY_VIOLATION",
            operator_message="Staging RC evidence dashboard panel has a safety violation. This is not a production deployment.",
            failing_reasons=failing_reasons + safety_violations,
            badges=badges,
            evidence_manifest_hash=evidence_manifest_hash or "",
            indexed_artifacts_count=indexed_artifacts_count or 0,
        )

    if operator_panel.get("status") != "PASS":
        return _base_panel(
            status="FAIL",
            panel_status=operator_panel.get("panel_status") or "RC_BLOCKED_OR_NOT_READY",
            operator_message=operator_panel.get("operator_message")
            or "Staging RC evidence is blocked or unavailable. This is not a production deployment.",
            failing_reasons=failing_reasons or ["operator panel status is not PASS"],
            badges=badges,
            evidence_manifest_hash=evidence_manifest_hash or "",
            indexed_artifacts_count=indexed_artifacts_count or 0,
        )

    if operator_panel.get("panel_status") != "RC_ALLOWED_INDEXED":
        return _base_panel(
            status="FAIL",
            panel_status=operator_panel.get("panel_status") or "RC_BLOCKED_OR_NOT_READY",
            operator_message=operator_panel.get("operator_message")
            or "Staging RC evidence is not in an allowed indexed state. This is not a production deployment.",
            failing_reasons=failing_reasons or ["operator panel status is not RC_ALLOWED_INDEXED"],
            badges=badges,
            evidence_manifest_hash=evidence_manifest_hash or "",
            indexed_artifacts_count=indexed_artifacts_count or 0,
        )

    if not isinstance(evidence_manifest_hash, str) or not evidence_manifest_hash:
        return _base_panel(
            status="FAIL",
            panel_status="RC_BLOCKED_OR_NOT_READY",
            operator_message="Staging RC evidence manifest hash is missing. This is not a production deployment.",
            failing_reasons=failing_reasons or ["evidence manifest hash is missing"],
            badges=badges,
            evidence_manifest_hash="",
            indexed_artifacts_count=indexed_artifacts_count or 0,
        )

    return _base_panel(
        status="PASS",
        panel_status="RC_ALLOWED_INDEXED",
        operator_message=operator_panel.get("operator_message")
        or "Staging RC evidence is indexed and allowed. This is not a production deployment.",
        failing_reasons=[],
        badges=badges,
        evidence_manifest_hash=evidence_manifest_hash,
        indexed_artifacts_count=indexed_artifacts_count or 0,
    )


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate read-only operator RC dashboard panel model."
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_PATH),
        help="Output JSON artifact path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print generated dashboard panel JSON to stdout.",
    )
    args = parser.parse_args(argv)

    panel = build_dashboard_panel()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(panel, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if args.json:
        print(json.dumps(panel, indent=2, sort_keys=True))

    return 0 if panel["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main_args())

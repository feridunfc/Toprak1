from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"

INDEX_PATH = ARTIFACT_DIR / "latest_staging_rc_evidence_index.json"
OUTPUT_PATH = ARTIFACT_DIR / "latest_operator_rc_evidence_panel.json"

SOURCE = "operator_rc_evidence_panel"
DECISION_SCOPE = "EVIDENCE_BACKED_STAGING_RELEASE_CANDIDATE_ONLY"
INPUT_ARTIFACT = "docs/dashboard/artifacts/latest_staging_rc_evidence_index.json"

PASS_BADGES = [
    "READY",
    "RC_ALLOWED",
    "BOUNDARY_PASS",
    "INDEXED",
    "NOT_PRODUCTION_DEPLOYMENT",
]


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _base_panel(
    *,
    status: str,
    panel_status: str,
    operator_message: str,
    failing_reasons: list[str],
    evidence_manifest_hash: str | None = None,
    indexed_artifacts_count: int | None = None,
    badges: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "source": SOURCE,
        "status": status,
        "panel_status": panel_status,
        "decision_scope": DECISION_SCOPE,
        "badges": badges or ["NOT_PRODUCTION_DEPLOYMENT"],
        "operator_message": operator_message,
        "actionable": False,
        "input_artifact": INPUT_ARTIFACT,
        "production_ready_claim": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "redis_mutation_attempted": False,
        "runtime_mutation_attempted": False,
        "canonical_state_mutation_attempted": False,
        "evidence_manifest_hash": evidence_manifest_hash or "",
        "indexed_artifacts_count": indexed_artifacts_count or 0,
        "failing_reasons": failing_reasons,
    }


def _read_index() -> tuple[dict[str, Any] | None, str | None]:
    try:
        raw = INDEX_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, f"missing index artifact:{_rel(INDEX_PATH)}"
    except OSError as exc:
        return None, f"unreadable index artifact:{_rel(INDEX_PATH)}:{exc}"

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"malformed index artifact:{_rel(INDEX_PATH)}:{exc}"

    if not isinstance(data, dict):
        return None, f"malformed index artifact:{_rel(INDEX_PATH)}:expected object"

    return data, None


def build_panel() -> dict[str, Any]:
    index, error = _read_index()

    if error:
        panel_status = "PANEL_UNAVAILABLE" if error.startswith("missing") else "PANEL_INVALID"
        return _base_panel(
            status="FAIL",
            panel_status=panel_status,
            operator_message="Staging RC evidence panel is unavailable. This is not a production deployment.",
            failing_reasons=[error],
        )

    assert index is not None

    failing_reasons = list(index.get("findings") or [])
    evidence_manifest_hash = index.get("evidence_manifest_hash")
    indexed_artifacts_count = index.get("indexed_artifacts_count")

    safety_violations: list[str] = []

    if index.get("decision_scope") != DECISION_SCOPE:
        safety_violations.append("decision scope is invalid")
    if index.get("production_ready_claim") is not False:
        safety_violations.append("production_ready_claim is not false")
    if index.get("deployment_attempted") is not False:
        safety_violations.append("deployment_attempted is not false")
    if index.get("release_tag_created") is not False:
        safety_violations.append("release_tag_created is not false")
    if index.get("redis_mutation_attempted") is not False:
        safety_violations.append("redis_mutation_attempted is not false")
    if index.get("runtime_mutation_attempted") is not False:
        safety_violations.append("runtime_mutation_attempted is not false")
    if index.get("canonical_state_mutation_attempted") is not False:
        safety_violations.append("canonical_state_mutation_attempted is not false")

    if safety_violations:
        return _base_panel(
            status="FAIL",
            panel_status="SAFETY_VIOLATION",
            operator_message="Staging RC evidence has a safety violation. This is not a production deployment.",
            failing_reasons=failing_reasons + safety_violations,
            evidence_manifest_hash=evidence_manifest_hash,
            indexed_artifacts_count=indexed_artifacts_count,
        )

    if index.get("status") != "PASS":
        return _base_panel(
            status="FAIL",
            panel_status="RC_BLOCKED_OR_NOT_READY",
            operator_message="Staging RC evidence is blocked or not ready. This is not a production deployment.",
            failing_reasons=failing_reasons or ["staging RC evidence index status is not PASS"],
            evidence_manifest_hash=evidence_manifest_hash,
            indexed_artifacts_count=indexed_artifacts_count,
        )

    if index.get("staging_rc_indexed") is not True:
        return _base_panel(
            status="FAIL",
            panel_status="RC_NOT_INDEXED",
            operator_message="Staging RC evidence is not indexed. This is not a production deployment.",
            failing_reasons=failing_reasons or ["staging_rc_indexed is not true"],
            evidence_manifest_hash=evidence_manifest_hash,
            indexed_artifacts_count=indexed_artifacts_count,
        )

    if not isinstance(evidence_manifest_hash, str) or not evidence_manifest_hash:
        return _base_panel(
            status="FAIL",
            panel_status="RC_BLOCKED_OR_NOT_READY",
            operator_message="Staging RC evidence manifest hash is missing. This is not a production deployment.",
            failing_reasons=failing_reasons or ["evidence manifest hash is missing"],
            evidence_manifest_hash="",
            indexed_artifacts_count=indexed_artifacts_count,
        )

    return _base_panel(
        status="PASS",
        panel_status="RC_ALLOWED_INDEXED",
        operator_message="Staging RC evidence is indexed and allowed. This is not a production deployment.",
        failing_reasons=[],
        evidence_manifest_hash=evidence_manifest_hash,
        indexed_artifacts_count=indexed_artifacts_count,
        badges=PASS_BADGES,
    )


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate read-only operator RC evidence panel read model."
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_PATH),
        help="Output JSON artifact path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print generated panel JSON to stdout.",
    )
    args = parser.parse_args(argv)

    panel = build_panel()

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

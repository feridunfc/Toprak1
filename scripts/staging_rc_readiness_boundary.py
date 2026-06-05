from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"

DECISION_PATH = ARTIFACT_DIR / "latest_production_readiness_decision.json"
EVIDENCE_FREEZE_PATH = ARTIFACT_DIR / "latest_production_readiness_evidence_freeze.json"
STAGING_RC_GATE_PATH = ARTIFACT_DIR / "latest_staging_release_candidate_gate.json"
OUTPUT_PATH = ARTIFACT_DIR / "latest_staging_rc_readiness_boundary.json"

DECISION_SCOPE = "EVIDENCE_BACKED_STAGING_RELEASE_CANDIDATE_ONLY"


def _read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, f"missing artifact: {path.as_posix()}"
    except OSError as exc:
        return None, f"unreadable artifact: {path.as_posix()}: {exc}"

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"malformed artifact: {path.as_posix()}: {exc}"

    if not isinstance(data, dict):
        return None, f"malformed artifact: {path.as_posix()}: expected object"

    return data, None


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def build_boundary() -> dict[str, Any]:
    findings: list[str] = []

    decision, error = _read_json(DECISION_PATH)
    if error:
        findings.append(error)
        decision = {}

    evidence_freeze, error = _read_json(EVIDENCE_FREEZE_PATH)
    if error:
        findings.append(error)
        evidence_freeze = {}

    staging_gate, error = _read_json(STAGING_RC_GATE_PATH)
    if error:
        findings.append(error)
        staging_gate = {}

    decision_status = decision.get("status")
    decision_value = decision.get("decision")

    evidence_status = evidence_freeze.get("status")
    evidence_decision = evidence_freeze.get("decision")
    required_artifacts_complete = evidence_freeze.get("required_artifacts_complete")
    evidence_manifest_hash = evidence_freeze.get("evidence_manifest_hash")
    evidence_mutation_attempted = evidence_freeze.get("mutation_attempted")

    staging_gate_decision = staging_gate.get("rc_decision")

    if decision_status != "PASS":
        findings.append("production readiness decision status is not PASS")
    if decision_value != "READY":
        findings.append("production readiness decision is not READY")
    if evidence_status != "PASS":
        findings.append("production readiness evidence freeze status is not PASS")
    if evidence_decision != "READY":
        findings.append("production readiness evidence freeze decision is not READY")
    if required_artifacts_complete is not True:
        findings.append("production readiness evidence freeze required artifacts are incomplete")
    if not isinstance(evidence_manifest_hash, str) or not evidence_manifest_hash:
        findings.append("production readiness evidence freeze manifest hash is missing")
    if evidence_mutation_attempted is not False:
        findings.append("production readiness evidence freeze mutation_attempted is not false")
    if staging_gate_decision != "RC_ALLOWED":
        findings.append("staging release candidate gate decision is not RC_ALLOWED")

    production_ready_claim = False
    deployment_attempted = False
    release_tag_created = False
    redis_mutation_attempted = False
    runtime_mutation_attempted = False
    canonical_state_mutation_attempted = False

    forbidden_flags = {
        "production_ready_claim": production_ready_claim,
        "deployment_attempted": deployment_attempted,
        "release_tag_created": release_tag_created,
        "redis_mutation_attempted": redis_mutation_attempted,
        "runtime_mutation_attempted": runtime_mutation_attempted,
        "canonical_state_mutation_attempted": canonical_state_mutation_attempted,
    }

    for name, value in forbidden_flags.items():
        if value is not False:
            findings.append(f"{name} is not false")

    status = "PASS" if not findings else "FAIL"

    return {
        "status": status,
        "decision_scope": DECISION_SCOPE,
        "production_ready_claim": production_ready_claim,
        "deployment_attempted": deployment_attempted,
        "release_tag_created": release_tag_created,
        "redis_mutation_attempted": redis_mutation_attempted,
        "runtime_mutation_attempted": runtime_mutation_attempted,
        "canonical_state_mutation_attempted": canonical_state_mutation_attempted,
        "inputs": {
            "production_readiness_decision": _rel(DECISION_PATH),
            "production_readiness_evidence_freeze": _rel(EVIDENCE_FREEZE_PATH),
            "staging_release_candidate_gate": _rel(STAGING_RC_GATE_PATH),
        },
        "observed": {
            "production_readiness_decision_status": decision_status,
            "production_readiness_decision": decision_value,
            "production_readiness_evidence_freeze_status": evidence_status,
            "production_readiness_evidence_freeze_decision": evidence_decision,
            "required_artifacts_complete": required_artifacts_complete,
            "evidence_manifest_hash_present": isinstance(evidence_manifest_hash, str)
            and bool(evidence_manifest_hash),
            "evidence_freeze_mutation_attempted": evidence_mutation_attempted,
            "staging_release_candidate_gate_decision": staging_gate_decision,
        },
        "findings": findings,
    }


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate read-only staging RC readiness boundary artifact."
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_PATH),
        help="Output JSON artifact path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print generated artifact JSON to stdout.",
    )
    args = parser.parse_args(argv)

    artifact = build_boundary()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if args.json:
        print(json.dumps(artifact, indent=2, sort_keys=True))

    return 0 if artifact["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main_args())




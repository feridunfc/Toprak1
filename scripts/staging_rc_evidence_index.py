from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"

ROLLUP_PATH = ARTIFACT_DIR / "latest_production_readiness_rollup.json"
DECISION_PATH = ARTIFACT_DIR / "latest_production_readiness_decision.json"
EVIDENCE_FREEZE_PATH = ARTIFACT_DIR / "latest_production_readiness_evidence_freeze.json"
STAGING_RC_GATE_PATH = ARTIFACT_DIR / "latest_staging_release_candidate_gate.json"
STAGING_RC_BOUNDARY_PATH = ARTIFACT_DIR / "latest_staging_rc_readiness_boundary.json"
OUTPUT_PATH = ARTIFACT_DIR / "latest_staging_rc_evidence_index.json"

SOURCE = "staging_rc_evidence_index"
DECISION_SCOPE = "EVIDENCE_BACKED_STAGING_RELEASE_CANDIDATE_ONLY"

REQUIRED_ARTIFACTS: tuple[tuple[str, Path], ...] = (
    ("production_readiness_rollup", ROLLUP_PATH),
    ("production_readiness_decision", DECISION_PATH),
    ("production_readiness_evidence_freeze", EVIDENCE_FREEZE_PATH),
    ("staging_release_candidate_gate", STAGING_RC_GATE_PATH),
    ("staging_rc_readiness_boundary", STAGING_RC_BOUNDARY_PATH),
)


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read_indexed_artifact(name: str, path: Path) -> tuple[dict[str, Any], dict[str, Any] | None]:
    entry: dict[str, Any] = {
        "name": name,
        "path": _rel(path),
        "present": False,
        "valid_json": False,
        "sha256": "",
        "status": None,
        "decision": None,
    }

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return entry, None

    entry["present"] = True
    entry["sha256"] = _sha256(raw)

    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return entry, None

    if not isinstance(data, dict):
        return entry, None

    entry["valid_json"] = True
    entry["status"] = data.get("status")
    entry["decision"] = data.get("decision") or data.get("rc_decision")
    return entry, data


def build_index() -> dict[str, Any]:
    findings: list[str] = []
    indexed_artifacts: list[dict[str, Any]] = []
    loaded: dict[str, dict[str, Any]] = {}

    for name, path in REQUIRED_ARTIFACTS:
        entry, data = _read_indexed_artifact(name, path)
        indexed_artifacts.append(entry)

        if not entry["present"]:
            findings.append(f"missing artifact:{entry['path']}")
            continue
        if not entry["valid_json"]:
            findings.append(f"malformed artifact:{entry['path']}")
            continue
        if data is not None:
            loaded[name] = data

    rollup = loaded.get("production_readiness_rollup", {})
    decision = loaded.get("production_readiness_decision", {})
    evidence = loaded.get("production_readiness_evidence_freeze", {})
    rc_gate = loaded.get("staging_release_candidate_gate", {})
    boundary = loaded.get("staging_rc_readiness_boundary", {})

    production_readiness_rollup_status = rollup.get("status")
    production_readiness_decision_status = decision.get("status")
    production_readiness_decision = decision.get("decision")
    evidence_freeze_status = evidence.get("status")
    evidence_freeze_decision = evidence.get("decision")
    required_artifacts_complete = evidence.get("required_artifacts_complete")
    evidence_manifest_hash = evidence.get("evidence_manifest_hash")
    evidence_mutation_attempted = evidence.get("mutation_attempted")
    staging_rc_gate_status = rc_gate.get("status")
    staging_rc_decision = rc_gate.get("rc_decision")
    staging_rc_boundary_status = boundary.get("status")
    boundary_decision_scope = boundary.get("decision_scope")

    if production_readiness_rollup_status != "PASS":
        findings.append("production readiness rollup status is not PASS")
    if production_readiness_decision_status != "PASS":
        findings.append("production readiness decision status is not PASS")
    if production_readiness_decision != "READY":
        findings.append("production readiness decision is not READY")
    if evidence_freeze_status != "PASS":
        findings.append("evidence freeze status is not PASS")
    if evidence_freeze_decision != "READY":
        findings.append("evidence freeze decision is not READY")
    if required_artifacts_complete is not True:
        findings.append("evidence freeze required artifacts are incomplete")
    if not isinstance(evidence_manifest_hash, str) or not evidence_manifest_hash:
        findings.append("evidence manifest hash is missing")
    if evidence_mutation_attempted is not False:
        findings.append("evidence freeze mutation_attempted is not false")
    if staging_rc_gate_status != "PASS":
        findings.append("staging RC gate status is not PASS")
    if staging_rc_decision != "RC_ALLOWED":
        findings.append("staging RC decision is not RC_ALLOWED")
    if staging_rc_boundary_status != "PASS":
        findings.append("staging RC boundary status is not PASS")
    if boundary_decision_scope != DECISION_SCOPE:
        findings.append("staging RC boundary decision scope is invalid")

    production_ready_claim = False
    deployment_attempted = False
    release_tag_created = False
    redis_mutation_attempted = False
    runtime_mutation_attempted = False
    canonical_state_mutation_attempted = False
    staging_rc_indexed = not findings

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
        "source": SOURCE,
        "status": status,
        "decision_scope": DECISION_SCOPE,
        "staging_rc_indexed": status == "PASS" and staging_rc_indexed,
        "production_ready_claim": production_ready_claim,
        "deployment_attempted": deployment_attempted,
        "release_tag_created": release_tag_created,
        "redis_mutation_attempted": redis_mutation_attempted,
        "runtime_mutation_attempted": runtime_mutation_attempted,
        "canonical_state_mutation_attempted": canonical_state_mutation_attempted,
        "production_readiness_rollup_status": production_readiness_rollup_status,
        "production_readiness_decision_status": production_readiness_decision_status,
        "production_readiness_decision": production_readiness_decision,
        "evidence_freeze_status": evidence_freeze_status,
        "evidence_freeze_decision": evidence_freeze_decision,
        "evidence_manifest_hash": evidence_manifest_hash,
        "staging_rc_gate_status": staging_rc_gate_status,
        "staging_rc_decision": staging_rc_decision,
        "staging_rc_boundary_status": staging_rc_boundary_status,
        "indexed_artifacts_count": len(indexed_artifacts),
        "indexed_artifacts": indexed_artifacts,
        "findings": findings,
    }


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate read-only staging RC evidence index artifact."
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

    artifact = build_index()

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

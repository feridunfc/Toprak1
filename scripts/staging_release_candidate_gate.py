#!/usr/bin/env python3
"""Generate staging release-candidate gate artifact."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_DECISION_INPUT = Path("docs/dashboard/artifacts/latest_production_readiness_decision.json")
DEFAULT_EVIDENCE_FREEZE_INPUT = Path("docs/dashboard/artifacts/latest_production_readiness_evidence_freeze.json")
DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_staging_release_candidate_gate.json")


@dataclass(frozen=True)
class StagingReleaseCandidateGateArtifact:
    source: str
    status: str
    rc_decision: str
    mode: str
    production_decision: str
    production_decision_status: str
    evidence_freeze_status: str
    evidence_freeze_decision: str
    evidence_manifest_hash: str
    mutation_attempted: bool
    deployment_attempted: bool
    release_tag_attempted: bool
    reasons: list[str]
    notes: list[str]


def _load_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, "missing"

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None, "malformed"

    if not isinstance(payload, dict):
        return None, "malformed"

    return payload, None


def build_artifact(
    decision_input: Path = DEFAULT_DECISION_INPUT,
    evidence_freeze_input: Path = DEFAULT_EVIDENCE_FREEZE_INPUT,
) -> StagingReleaseCandidateGateArtifact:
    reasons: list[str] = []

    decision_payload, decision_error = _load_json(decision_input)
    freeze_payload, freeze_error = _load_json(evidence_freeze_input)

    production_decision = "UNKNOWN"
    production_decision_status = "UNKNOWN"
    evidence_freeze_status = "UNKNOWN"
    evidence_freeze_decision = "UNKNOWN"
    evidence_manifest_hash = ""
    freeze_required_complete = False
    freeze_mutation_attempted = True

    if decision_error:
        reasons.append(f"production_decision_{decision_error}")
    else:
        assert decision_payload is not None
        production_decision = str(decision_payload.get("decision", "UNKNOWN"))
        production_decision_status = str(decision_payload.get("status", "UNKNOWN"))

    if freeze_error:
        reasons.append(f"evidence_freeze_{freeze_error}")
    else:
        assert freeze_payload is not None
        evidence_freeze_status = str(freeze_payload.get("status", "UNKNOWN"))
        evidence_freeze_decision = str(freeze_payload.get("decision", "UNKNOWN"))
        evidence_manifest_hash = str(freeze_payload.get("evidence_manifest_hash", "") or "")
        freeze_required_complete = bool(freeze_payload.get("required_artifacts_complete", False))
        freeze_mutation_attempted = bool(freeze_payload.get("mutation_attempted", True))

    if production_decision_status != "PASS":
        reasons.append(f"production_decision_status_not_pass:{production_decision_status}")
    if production_decision != "READY":
        reasons.append(f"production_decision_not_ready:{production_decision}")
    if evidence_freeze_status != "PASS":
        reasons.append(f"evidence_freeze_status_not_pass:{evidence_freeze_status}")
    if evidence_freeze_decision != "READY":
        reasons.append(f"evidence_freeze_decision_not_ready:{evidence_freeze_decision}")
    if not freeze_required_complete:
        reasons.append("evidence_freeze_required_artifacts_incomplete")
    if not evidence_manifest_hash:
        reasons.append("evidence_manifest_hash_missing")
    if freeze_mutation_attempted:
        reasons.append("evidence_freeze_mutation_attempted")

    rc_decision = "RC_ALLOWED" if not reasons else "RC_BLOCKED"

    return StagingReleaseCandidateGateArtifact(
        source="staging_release_candidate_gate",
        status="PASS",
        rc_decision=rc_decision,
        mode="read-only-rc-gate",
        production_decision=production_decision,
        production_decision_status=production_decision_status,
        evidence_freeze_status=evidence_freeze_status,
        evidence_freeze_decision=evidence_freeze_decision,
        evidence_manifest_hash=evidence_manifest_hash,
        mutation_attempted=False,
        deployment_attempted=False,
        release_tag_attempted=False,
        reasons=reasons,
        notes=[
            "RC gate performs no Redis/runtime mutation.",
            "RC gate does not deploy or tag a release.",
            "RC_ALLOWED requires READY decision and PASS frozen evidence.",
            "Missing, malformed, or non-ready inputs fail closed to RC_BLOCKED.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate staging release-candidate gate artifact")
    parser.add_argument("--decision-input", default=str(DEFAULT_DECISION_INPUT))
    parser.add_argument("--evidence-freeze-input", default=str(DEFAULT_EVIDENCE_FREEZE_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    artifact = build_artifact(
        decision_input=Path(args.decision_input),
        evidence_freeze_input=Path(args.evidence_freeze_input),
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(f"STAGING_RELEASE_CANDIDATE_GATE_{artifact.rc_decision}: reasons={len(artifact.reasons)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

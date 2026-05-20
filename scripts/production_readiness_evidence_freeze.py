#!/usr/bin/env python3
"""Generate production readiness evidence freeze artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_ARTIFACT_DIR = Path("docs/dashboard/artifacts")
DEFAULT_OUTPUT = DEFAULT_ARTIFACT_DIR / "latest_production_readiness_evidence_freeze.json"

REQUIRED_ARTIFACTS = [
    "latest_authority.json",
    "latest_replay.json",
    "latest_deployment_smoke.json",
    "latest_redis_failover_smoke.json",
    "latest_recovery_audit.json",
    "latest_recovery_requeue.json",
    "latest_recovery_requeue_drill.json",
    "latest_cold_restart_drill.json",
    "latest_zombie_completion_drill.json",
    "latest_recovery_auto_resume_guardrail.json",
    "latest_advisory_governance_rollup.json",
    "latest_production_readiness_rollup.json",
    "latest_production_readiness_decision.json",
]


@dataclass(frozen=True)
class EvidenceArtifactEntry:
    name: str
    path: str
    present: bool
    valid_json: bool
    sha256: str
    status: str
    source: str


@dataclass(frozen=True)
class ProductionReadinessEvidenceFreezeArtifact:
    source: str
    status: str
    mode: str
    decision: str
    rollup_status: str
    artifact_count: int
    required_artifacts_complete: bool
    evidence_manifest_hash: str
    mutation_attempted: bool
    reasons: list[str]
    artifacts: list[EvidenceArtifactEntry]
    notes: list[str]


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_status(payload: dict[str, Any], artifact_name: str) -> str:
    if artifact_name == "latest_production_readiness_decision.json":
        return str(payload.get("decision", payload.get("status", "UNKNOWN")))

    if artifact_name == "latest_production_readiness_rollup.json":
        return str(payload.get("status", "UNKNOWN"))

    if "status" in payload:
        return str(payload.get("status", "UNKNOWN"))

    if artifact_name == "latest_authority.json":
        banned = int(payload.get("banned_count", payload.get("banned", 0)) or 0)
        return "PASS" if banned == 0 else "FAIL"

    if artifact_name == "latest_replay.json":
        return str(payload.get("replay_status", "UNKNOWN"))

    return "UNKNOWN"


def _json_source(payload: dict[str, Any], artifact_name: str) -> str:
    return str(payload.get("source", artifact_name.removesuffix(".json")))


def _read_artifact_entry(artifact_dir: Path, artifact_name: str) -> tuple[EvidenceArtifactEntry, dict[str, Any] | None, str | None]:
    path = artifact_dir / artifact_name
    if not path.exists():
        return (
            EvidenceArtifactEntry(
                name=artifact_name,
                path=str(path),
                present=False,
                valid_json=False,
                sha256="",
                status="MISSING",
                source="missing",
            ),
            None,
            f"missing_artifact:{artifact_name}",
        )

    data = path.read_bytes()
    digest = _sha256_bytes(data)

    try:
        payload = json.loads(data.decode("utf-8"))
    except Exception:
        return (
            EvidenceArtifactEntry(
                name=artifact_name,
                path=str(path),
                present=True,
                valid_json=False,
                sha256=digest,
                status="MALFORMED",
                source="malformed",
            ),
            None,
            f"malformed_artifact:{artifact_name}",
        )

    if not isinstance(payload, dict):
        return (
            EvidenceArtifactEntry(
                name=artifact_name,
                path=str(path),
                present=True,
                valid_json=False,
                sha256=digest,
                status="MALFORMED",
                source="malformed",
            ),
            None,
            f"malformed_artifact:{artifact_name}",
        )

    entry = EvidenceArtifactEntry(
        name=artifact_name,
        path=str(path),
        present=True,
        valid_json=True,
        sha256=digest,
        status=_json_status(payload, artifact_name),
        source=_json_source(payload, artifact_name),
    )
    return entry, payload, None


def _manifest_hash(entries: list[EvidenceArtifactEntry]) -> str:
    material = [
        {
            "name": entry.name,
            "sha256": entry.sha256,
            "status": entry.status,
            "source": entry.source,
        }
        for entry in sorted(entries, key=lambda item: item.name)
    ]
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _sha256_bytes(encoded)


def build_artifact(
    artifact_dir: Path = DEFAULT_ARTIFACT_DIR,
) -> ProductionReadinessEvidenceFreezeArtifact:
    reasons: list[str] = []
    entries: list[EvidenceArtifactEntry] = []
    payloads: dict[str, dict[str, Any]] = {}

    for artifact_name in REQUIRED_ARTIFACTS:
        entry, payload, reason = _read_artifact_entry(artifact_dir, artifact_name)
        entries.append(entry)
        if payload is not None:
            payloads[artifact_name] = payload
        if reason:
            reasons.append(reason)

    decision_payload = payloads.get("latest_production_readiness_decision.json")
    rollup_payload = payloads.get("latest_production_readiness_rollup.json")

    decision = "UNKNOWN"
    if decision_payload is not None:
        decision = str(decision_payload.get("decision", "UNKNOWN"))
        if decision != "READY":
            reasons.append(f"decision_not_ready:{decision}")

    rollup_status = "UNKNOWN"
    if rollup_payload is not None:
        rollup_status = str(rollup_payload.get("status", "UNKNOWN"))
        if rollup_status != "PASS":
            reasons.append(f"rollup_not_pass:{rollup_status}")

    required_complete = all(entry.present and entry.valid_json for entry in entries)
    if not required_complete:
        reasons.append("required_artifacts_incomplete")

    manifest_hash = _manifest_hash(entries)
    status = "PASS" if required_complete and decision == "READY" and rollup_status == "PASS" and not reasons else "FAIL"

    return ProductionReadinessEvidenceFreezeArtifact(
        source="production_readiness_evidence_freeze",
        status=status,
        mode="read-only-evidence-freeze",
        decision=decision,
        rollup_status=rollup_status,
        artifact_count=len(entries),
        required_artifacts_complete=required_complete,
        evidence_manifest_hash=manifest_hash,
        mutation_attempted=False,
        reasons=reasons,
        artifacts=entries,
        notes=[
            "Evidence freeze performs no Redis/runtime mutation.",
            "READY evidence is bound to deterministic artifact SHA-256 hashes.",
            "Missing, malformed, non-ready, or non-PASS evidence fails closed.",
            "Evidence freeze does not enable production auto-deployment.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate production readiness evidence freeze artifact")
    parser.add_argument("--artifact-dir", default=str(DEFAULT_ARTIFACT_DIR))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    artifact = build_artifact(Path(args.artifact_dir))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(
            f"PRODUCTION_READINESS_EVIDENCE_FREEZE_{artifact.status}: "
            f"decision={artifact.decision} artifacts={artifact.artifact_count}"
        )

    return 0 if artifact.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

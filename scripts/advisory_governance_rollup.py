#!/usr/bin/env python3
"""Generate advisory governance rollup artifact."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in (
    REPO_ROOT,
    REPO_ROOT / "hfa-worker" / "src",
    REPO_ROOT / "hfa-agents" / "src",
):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from scripts.cognitive_governance_audit import audit_repo
from scripts.feedbackwriter_governance import build_artifact as build_feedbackwriter_artifact
from scripts.semantic_advisory_contract import build_artifact as build_semantic_advisory_artifact
from scripts.semanticbridge_gate import build_artifact as build_semanticbridge_gate_artifact


DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_advisory_governance_rollup.json")


@dataclass(frozen=True)
class AdvisoryGovernanceRollupComponent:
    name: str
    status: str
    source: str
    canonical_authority_writes_allowed: bool | None = None


@dataclass(frozen=True)
class AdvisoryGovernanceRollupArtifact:
    source: str
    status: str
    mode: str
    components: list[AdvisoryGovernanceRollupComponent]
    components_checked: int
    notes: list[str]


def _status_of(payload: Any) -> str:
    if isinstance(payload, dict):
        return str(payload.get("status", "UNKNOWN"))
    return str(getattr(payload, "status", "UNKNOWN"))


def _source_of(payload: Any) -> str:
    if isinstance(payload, dict):
        return str(payload.get("source", "unknown"))
    return str(getattr(payload, "source", "unknown"))


def _canonical_allowed(payload: Any) -> bool | None:
    if isinstance(payload, dict):
        value = payload.get("canonical_authority_writes_allowed")
    else:
        value = getattr(payload, "canonical_authority_writes_allowed", None)

    if value is None:
        return None
    return bool(value)


def build_artifact(repo_root: Path = REPO_ROOT) -> AdvisoryGovernanceRollupArtifact:
    cognitive = audit_repo(repo_root)
    semantic_advisory = build_semantic_advisory_artifact(repo_root)
    feedbackwriter = build_feedbackwriter_artifact()
    semanticbridge_gate = build_semanticbridge_gate_artifact()

    payloads = [
        ("cognitive_governance_audit", cognitive),
        ("semantic_advisory_contract", semantic_advisory),
        ("feedbackwriter_governance", feedbackwriter),
        ("semanticbridge_gate", semanticbridge_gate),
    ]

    components = [
        AdvisoryGovernanceRollupComponent(
            name=name,
            status=_status_of(payload),
            source=_source_of(payload),
            canonical_authority_writes_allowed=_canonical_allowed(payload),
        )
        for name, payload in payloads
    ]

    all_pass = all(component.status == "PASS" for component in components)
    no_advisory_authority = all(
        component.canonical_authority_writes_allowed is not True
        for component in components
    )

    status = "PASS" if all_pass and no_advisory_authority else "FAIL"

    return AdvisoryGovernanceRollupArtifact(
        source="advisory_governance_rollup",
        status=status,
        mode="static-rollup",
        components=components,
        components_checked=len(components),
        notes=[
            "Advisory governance rollup performs no Redis/runtime mutation.",
            "Cognitive, semantic, and feedback surfaces remain advisory/non-authoritative.",
            "Canonical runtime truth authority remains in runtime/Lua/control-plane approved paths.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate advisory governance rollup artifact")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    artifact = build_artifact()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(f"ADVISORY_GOVERNANCE_ROLLUP_{artifact.status}: components={artifact.components_checked}")

    return 0 if artifact.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

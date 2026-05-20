#!/usr/bin/env python3
"""Generate semantic advisory contract artifact."""

from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.cognitive_governance_audit import audit_repo


DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_semantic_advisory_contract.json")

SEMANTIC_BRIDGE = Path("hfa-agents/src/hfa_agents/integration/semantic_bridge.py")
FEEDBACK_WRITER = Path("hfa-worker/src/hfa_worker/feedback_writer.py")
CONTRACT_DOC = Path("docs/architecture/semantic_advisory_contract.md")


@dataclass(frozen=True)
class SurfaceMarker:
    path: str
    advisory_only_surface: bool
    canonical_authority_writes_allowed: bool


@dataclass(frozen=True)
class SemanticAdvisoryContractArtifact:
    source: str
    status: str
    mode: str
    contract_doc_exists: bool
    surfaces_checked: int
    markers: list[SurfaceMarker]
    governance_audit_status: str
    governance_audit_findings_count: int
    notes: list[str]


def _module_assignments(path: Path) -> dict[str, object]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    values: dict[str, object] = {}

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        try:
            values[node.targets[0].id] = ast.literal_eval(node.value)
        except Exception:
            continue

    return values


def build_artifact(repo_root: Path) -> SemanticAdvisoryContractArtifact:
    surfaces = [SEMANTIC_BRIDGE, FEEDBACK_WRITER]
    markers: list[SurfaceMarker] = []

    for rel in surfaces:
        path = repo_root / rel
        values = _module_assignments(path)
        markers.append(
            SurfaceMarker(
                path=str(rel),
                advisory_only_surface=values.get("ADVISORY_ONLY_SURFACE") is True,
                canonical_authority_writes_allowed=values.get("CANONICAL_AUTHORITY_WRITES_ALLOWED") is True,
            )
        )

    governance = audit_repo(repo_root)

    all_marked = all(
        marker.advisory_only_surface and not marker.canonical_authority_writes_allowed
        for marker in markers
    )
    doc_exists = (repo_root / CONTRACT_DOC).exists()
    status = "PASS" if doc_exists and all_marked and governance.status == "PASS" else "FAIL"

    return SemanticAdvisoryContractArtifact(
        source="semantic_advisory_contract",
        status=status,
        mode="static-contract",
        contract_doc_exists=doc_exists,
        surfaces_checked=len(markers),
        markers=markers,
        governance_audit_status=governance.status,
        governance_audit_findings_count=governance.findings_count,
        notes=[
            "SemanticBridge and FeedbackWriter are advisory/non-authoritative surfaces.",
            "Canonical runtime truth authority remains in runtime/Lua/control-plane paths.",
            "Future promotion to authoritative behavior requires an authority-reviewed sprint.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate semantic advisory contract artifact")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    artifact = build_artifact(Path(args.repo_root).resolve())
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(f"SEMANTIC_ADVISORY_CONTRACT_{artifact.status}: surfaces={artifact.surfaces_checked}")

    return 0 if artifact.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

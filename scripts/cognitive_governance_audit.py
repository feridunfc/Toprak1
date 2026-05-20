#!/usr/bin/env python3
"""Audit cognitive/semantic/feedback surfaces for hidden authority writes.

This audit is intentionally conservative. It allows advisory, feedback, memory,
policy, validation, and governance-local surfaces, while flagging direct writes
to canonical runtime authority keys or authority transition APIs.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path


DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_cognitive_governance_audit.json")

SURFACE_PATHS = (
    "hfa-worker/src/hfa_worker/feedback_writer.py",
    "hfa-worker/src/hfa_worker/integration/feedback_client.py",
    "hfa-worker/src/hfa_worker/cognitive_executor.py",
    "hfa-worker/src/hfa_worker/scheduler_semantic_hook.py",
    "hfa-agents/src/hfa_agents/integration/semantic_bridge.py",
    "hfa-control/src/hfa_control/feedback/handle_result.py",
    "hfa-semantic/src/hfa_semantic/memory/outcome_writer.py",
    "hfa-semantic/src/hfa_semantic/memory/feedback_loop.py",
)

SURFACE_DIRS = (
    "hfa-semantic/src/hfa_semantic/validation",
    "hfa-semantic/src/hfa_semantic/policy",
)

FORBIDDEN_PATTERNS = (
    r"hfa:dag:task:.*:state",
    r"hfa:dag:tenant:.*:running",
    r"hfa:dag:tenant:.*:ready",
    r"hfa:run:state:",
    r"task_state\(",
    r"task_meta\(",
    r"task_running_zset\(",
    r"tenant_ready_queue\(",
    r"requeue_stale_task\(",
    r"task_complete\(",
    r"task_claim_start\(",
    r"claim_task\(",
    r"mark_completed\(",
    r"mark_running\(",
    r"release_claim\(",
    r"renew_claim\(",
)

ALLOWLIST_MARKERS = (
    "AUTHORITY_REVIEWED_GOVERNANCE_STATE",
    "AUTHORITY_REVIEWED_NON_TRUTH_STATE",
    "COGNITIVE_GOVERNANCE_ADVISORY_ONLY",
    "SEMANTIC_BRIDGE_ADVISORY_ONLY",
    "FEEDBACK_WRITE_NON_AUTHORITATIVE",
)


@dataclass(frozen=True)
class CognitiveGovernanceFinding:
    path: str
    line: int
    pattern: str
    text: str


@dataclass(frozen=True)
class CognitiveGovernanceAuditArtifact:
    source: str
    status: str
    mode: str
    checked_files: int
    findings_count: int
    findings: list[CognitiveGovernanceFinding]
    notes: list[str]


def _surface_files(repo_root: Path) -> list[Path]:
    files: list[Path] = []

    for rel in SURFACE_PATHS:
        path = repo_root / rel
        if path.exists():
            files.append(path)

    for rel in SURFACE_DIRS:
        root = repo_root / rel
        if root.exists():
            files.extend(sorted(root.rglob("*.py")))

    return sorted(set(files))


def _has_allowlist_marker(lines: list[str], index: int) -> bool:
    start = max(0, index - 3)
    end = min(len(lines), index + 4)
    window = "\n".join(lines[start:end])
    return any(marker in window for marker in ALLOWLIST_MARKERS)


def audit_repo(repo_root: Path) -> CognitiveGovernanceAuditArtifact:
    findings: list[CognitiveGovernanceFinding] = []
    files = _surface_files(repo_root)

    compiled = [(pattern, re.compile(pattern)) for pattern in FORBIDDEN_PATTERNS]

    for path in files:
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue

        for index, line in enumerate(lines):
            if _has_allowlist_marker(lines, index):
                continue

            for pattern, regex in compiled:
                if regex.search(line):
                    findings.append(
                        CognitiveGovernanceFinding(
                            path=str(path.relative_to(repo_root)),
                            line=index + 1,
                            pattern=pattern,
                            text=line.strip(),
                        )
                    )

    return CognitiveGovernanceAuditArtifact(
        source="cognitive_governance_audit",
        status="PASS" if not findings else "FAIL",
        mode="static-audit",
        checked_files=len(files),
        findings_count=len(findings),
        findings=findings,
        notes=[
            "Cognitive/semantic/feedback surfaces must remain advisory or feedback-only.",
            "Canonical runtime authority writes must stay in runtime/Lua/control-plane paths.",
            "Allowlisted governance-local writes require explicit review markers.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit cognitive governance authority boundaries")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    artifact = audit_repo(Path(args.repo_root).resolve())
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(f"COGNITIVE_GOVERNANCE_AUDIT_{artifact.status}: findings={artifact.findings_count}")

    return 0 if artifact.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

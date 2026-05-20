#!/usr/bin/env python3
"""Audit that recovery auto-resume remains disabled by default.

This is a static guardrail audit. It does not mutate Redis and does not execute
recovery.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path


DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_recovery_auto_resume_guardrail.json")

BANNED_PATTERNS = (
    "AUTO_RECOVERY_ENABLED=true",
    "RECOVERY_AUTO_RESUME_ENABLED=true",
    "ENABLE_RECOVERY_DAEMON=true",
    "startup_auto_resume=True",
    "auto_recovery_daemon=True",
)

SEARCH_GLOBS = (
    "*.py",
    "*.yml",
    "*.yaml",
    "*.env",
    "*.md",
)


@dataclass(frozen=True)
class GuardrailFinding:
    path: str
    pattern: str


@dataclass(frozen=True)
class GuardrailArtifact:
    source: str
    status: str
    mode: str
    checked_files: int
    banned_findings_count: int
    banned_findings: list[GuardrailFinding]
    notes: list[str]


def audit_repo(repo_root: Path) -> GuardrailArtifact:
    findings: list[GuardrailFinding] = []
    checked = 0

    for glob in SEARCH_GLOBS:
        for path in repo_root.rglob(glob):
            if ".git" in path.parts or ".venv" in path.parts or "__pycache__" in path.parts:
                continue
            if path.name == "recovery_auto_resume_guardrail.py":
                continue
            if path.is_dir():
                continue
            checked += 1
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for pattern in BANNED_PATTERNS:
                if pattern in text:
                    findings.append(GuardrailFinding(path=str(path.relative_to(repo_root)), pattern=pattern))

    return GuardrailArtifact(
        source="recovery_auto_resume_guardrail",
        status="PASS" if not findings else "FAIL",
        mode="static-audit",
        checked_files=checked,
        banned_findings_count=len(findings),
        banned_findings=findings,
        notes=[
            "Static guardrail only; no Redis mutation is performed.",
            "Automatic recovery daemon must remain disabled by default.",
            "Single-task recovery remains explicit and proof-gated.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit recovery auto-resume guardrail")
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
        print(f"RECOVERY_AUTO_RESUME_GUARDRAIL_{artifact.status}: findings={artifact.banned_findings_count}")

    return 0 if artifact.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Generate FeedbackWriter governance artifact."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in (
    REPO_ROOT,
    REPO_ROOT / "hfa-worker" / "src",
    REPO_ROOT / "hfa-agents" / "src",
):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from hfa_worker.feedback_writer import (
    ADVISORY_ONLY_SURFACE,
    CANONICAL_AUTHORITY_WRITES_ALLOWED,
    validate_feedback_governance,
)


DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_feedbackwriter_governance.json")


@dataclass(frozen=True)
class FeedbackWriterGovernanceCase:
    name: str
    accepted: bool
    reason: str


@dataclass(frozen=True)
class FeedbackWriterGovernanceArtifact:
    source: str
    status: str
    mode: str
    advisory_only_surface: bool
    canonical_authority_writes_allowed: bool
    cases: list[FeedbackWriterGovernanceCase]
    notes: list[str]


def _result(**overrides):
    base = {
        "status": "success",
        "confidence": 0.91,
        "requires_hitl": False,
        "output_data": {"answer": "ok"},
        "reasoning_trace": ["a", "b"],
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def build_artifact() -> FeedbackWriterGovernanceArtifact:
    checks = [
        (
            "valid_feedback",
            _result(),
            {"task_id": "task-1", "run_id": "run-1", "tenant_id": "tenant-1", "trace_id": "run-1"},
        ),
        (
            "low_confidence",
            _result(confidence=0.10),
            {"task_id": "task-1", "run_id": "run-1", "tenant_id": "tenant-1", "trace_id": "run-1"},
        ),
        (
            "non_success_status",
            _result(status="failed"),
            {"task_id": "task-1", "run_id": "run-1", "tenant_id": "tenant-1", "trace_id": "run-1"},
        ),
        (
            "missing_trace_id",
            _result(),
            {"task_id": "task-1", "run_id": "run-1", "tenant_id": "tenant-1", "trace_id": ""},
        ),
        (
            "malformed_output_data",
            _result(output_data=[]),
            {"task_id": "task-1", "run_id": "run-1", "tenant_id": "tenant-1", "trace_id": "run-1"},
        ),
    ]

    cases: list[FeedbackWriterGovernanceCase] = []
    for name, result, kwargs in checks:
        decision = validate_feedback_governance(result=result, **kwargs)
        cases.append(
            FeedbackWriterGovernanceCase(
                name=name,
                accepted=decision.accepted,
                reason=decision.reason,
            )
        )

    expected = {
        "valid_feedback": True,
        "low_confidence": False,
        "non_success_status": False,
        "missing_trace_id": False,
        "malformed_output_data": False,
    }
    cases_ok = all(case.accepted is expected[case.name] for case in cases)

    status = "PASS" if (
        ADVISORY_ONLY_SURFACE
        and not CANONICAL_AUTHORITY_WRITES_ALLOWED
        and cases_ok
    ) else "FAIL"

    return FeedbackWriterGovernanceArtifact(
        source="feedbackwriter_governance",
        status=status,
        mode="static-contract",
        advisory_only_surface=ADVISORY_ONLY_SURFACE,
        canonical_authority_writes_allowed=CANONICAL_AUTHORITY_WRITES_ALLOWED,
        cases=cases,
        notes=[
            "FeedbackWriter remains advisory/non-authoritative.",
            "Feedback writes are locally gated before persistence.",
            "Feedback governance does not mutate canonical runtime truth.",
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate FeedbackWriter governance artifact")
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
        print(f"FEEDBACKWRITER_GOVERNANCE_{artifact.status}: cases={len(artifact.cases)}")

    return 0 if artifact.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

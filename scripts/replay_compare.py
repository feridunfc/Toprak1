"""Sprint 6 replay/runtime proof CLI.

This is intentionally small and dependency-light.  It gives CI and operators a
non-zero exit path when replay integrity is not trustworthy.  The heavy replay
engine remains elsewhere; this script is a deterministic enforcement wrapper for
already-computed proof signals.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass

_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}


def is_proof_enforcement_enabled() -> bool:
    return os.getenv("IRON_V3_PROOF_ENFORCEMENT", "0") not in _FALSE_VALUES


@dataclass(frozen=True)
class ReplayCompareResult:
    replay_clean: bool
    deterministic_replay_ok: bool
    mismatch: bool = False
    gaps_detected: bool = False
    duplicates_detected: bool = False
    integrity_issue: bool = False
    ambiguous: bool = False
    exit_code: int = 0
    reason: str = ""


def evaluate_replay_compare(
    *,
    replay_clean: bool = True,
    deterministic_replay_ok: bool = True,
    mismatch: bool = False,
    gaps_detected: bool = False,
    duplicates_detected: bool = False,
    integrity_issue: bool = False,
) -> ReplayCompareResult:
    reasons: list[str] = []
    if not replay_clean:
        reasons.append("replay_not_clean")
    if not deterministic_replay_ok:
        reasons.append("deterministic_replay_failed")
    if mismatch:
        reasons.append("runtime_replay_mismatch")
    if gaps_detected:
        reasons.append("gaps_detected")
    if duplicates_detected:
        reasons.append("duplicates_detected")
    if integrity_issue:
        reasons.append("integrity_issue")

    ambiguous = bool(reasons)
    return ReplayCompareResult(
        replay_clean=replay_clean,
        deterministic_replay_ok=deterministic_replay_ok,
        mismatch=mismatch,
        gaps_detected=gaps_detected,
        duplicates_detected=duplicates_detected,
        integrity_issue=integrity_issue,
        ambiguous=ambiguous,
        exit_code=1 if ambiguous else 0,
        reason=";".join(reasons),
    )


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="IRONCLAD replay proof enforcement")
    p.add_argument("--mismatch", action="store_true")
    p.add_argument("--gap", dest="gaps_detected", action="store_true")
    p.add_argument("--duplicate", dest="duplicates_detected", action="store_true")
    p.add_argument("--integrity-issue", action="store_true")
    p.add_argument("--replay-clean", choices=("true", "false"), default="true")
    p.add_argument("--deterministic-replay-ok", choices=("true", "false"), default="true")
    p.add_argument("--json", action="store_true", help="print machine-readable result")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = evaluate_replay_compare(
        replay_clean=args.replay_clean == "true",
        deterministic_replay_ok=args.deterministic_replay_ok == "true",
        mismatch=args.mismatch,
        gaps_detected=args.gaps_detected,
        duplicates_detected=args.duplicates_detected,
        integrity_issue=args.integrity_issue,
    )
    if args.json:
        print(json.dumps(asdict(result), sort_keys=True))
    elif result.exit_code:
        print(f"REPLAY_COMPARE_FAIL: {result.reason}", file=sys.stderr)
    else:
        print("REPLAY_COMPARE_OK")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())

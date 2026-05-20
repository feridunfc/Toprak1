#!/usr/bin/env python3
"""Proof-gated single recovery requeue command.

This command intentionally does not run a background recovery loop.

Flow:
1. Run/read recovery audit.
2. Find the requested run/task candidate.
3. Evaluate replay/runtime proof.
4. If proof allows auto-resume, call TaskRecoveryManager.requeue_stale_task(...).
5. Write dashboard artifact.

Mutation is fail-closed and only happens through the existing recovery manager.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in (
    REPO_ROOT,
    REPO_ROOT / "hfa-core" / "src",
    REPO_ROOT / "hfa-control" / "src",
    REPO_ROOT / "hfa-worker" / "src",
    REPO_ROOT / "hfa-semantic" / "src",
):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from scripts.recovery_audit import audit


@dataclass(frozen=True)
class ProofDecision:
    allowed: bool
    reason: str


@dataclass(frozen=True)
class RecoveryRequeueArtifact:
    source: str
    mode: str
    status: str
    run_id: str
    tenant_id: str
    proof_allowed: bool
    proof_reason: str
    candidate_found: bool
    mutation_attempted: bool
    requeue_ok: bool
    requeue_status: str
    requeue_count: int
    notes: list[str]


def evaluate_proof(
    *,
    replay_clean: bool,
    deterministic_replay_ok: bool,
    authority_artifact_ok: bool,
) -> ProofDecision:
    reasons: list[str] = []
    if not replay_clean:
        reasons.append("replay_dirty")
    if not deterministic_replay_ok:
        reasons.append("deterministic_replay_failed")
    if not authority_artifact_ok:
        reasons.append("authority_artifact_not_ok")

    if reasons:
        return ProofDecision(False, ";".join(reasons))
    return ProofDecision(True, "proof_allows_auto_resume")


def _find_candidate(audit_result: dict[str, Any], run_id: str) -> dict[str, Any] | None:
    for item in audit_result.get("runs", []):
        if item.get("run_id") == run_id and (
            item.get("stale") or item.get("expired_claim") or item.get("missing_claim")
        ):
            return item
    return None


async def proof_gated_requeue(
    redis: Any,
    *,
    run_id: str,
    tenant_id: str,
    manager: Any,
    replay_clean: bool = True,
    deterministic_replay_ok: bool = True,
    authority_artifact_ok: bool = True,
    stale_after_seconds: int = 300,
    dry_run: bool = False,
) -> RecoveryRequeueArtifact:
    audit_result = await audit(redis, stale_after_seconds=stale_after_seconds)
    candidate = _find_candidate(audit_result, run_id)
    proof = evaluate_proof(
        replay_clean=replay_clean,
        deterministic_replay_ok=deterministic_replay_ok,
        authority_artifact_ok=authority_artifact_ok,
    )

    notes = [
        "Single-task proof-gated requeue only; no automatic recovery loop is enabled.",
        "Mutation path must go through TaskRecoveryManager.requeue_stale_task.",
    ]

    if candidate is None:
        return RecoveryRequeueArtifact(
            source="recovery_requeue",
            mode="dry-run" if dry_run else "proof-gated",
            status="BLOCKED",
            run_id=run_id,
            tenant_id=tenant_id,
            proof_allowed=proof.allowed,
            proof_reason=proof.reason,
            candidate_found=False,
            mutation_attempted=False,
            requeue_ok=False,
            requeue_status="NO_RECOVERY_CANDIDATE",
            requeue_count=0,
            notes=notes,
        )

    if not proof.allowed:
        return RecoveryRequeueArtifact(
            source="recovery_requeue",
            mode="dry-run" if dry_run else "proof-gated",
            status="BLOCKED",
            run_id=run_id,
            tenant_id=tenant_id,
            proof_allowed=False,
            proof_reason=proof.reason,
            candidate_found=True,
            mutation_attempted=False,
            requeue_ok=False,
            requeue_status="PROOF_DENIED",
            requeue_count=0,
            notes=notes,
        )

    if dry_run:
        return RecoveryRequeueArtifact(
            source="recovery_requeue",
            mode="dry-run",
            status="PASS",
            run_id=run_id,
            tenant_id=tenant_id,
            proof_allowed=True,
            proof_reason=proof.reason,
            candidate_found=True,
            mutation_attempted=False,
            requeue_ok=False,
            requeue_status="DRY_RUN",
            requeue_count=0,
            notes=notes,
        )

    result = await manager.requeue_stale_task(task_id=run_id, tenant_id=tenant_id)

    return RecoveryRequeueArtifact(
        source="recovery_requeue",
        mode="proof-gated",
        status="PASS" if bool(result.ok) else "FAIL",
        run_id=run_id,
        tenant_id=tenant_id,
        proof_allowed=True,
        proof_reason=proof.reason,
        candidate_found=True,
        mutation_attempted=True,
        requeue_ok=bool(result.ok),
        requeue_status=str(result.status),
        requeue_count=int(getattr(result, "requeue_count", 0)),
        notes=notes,
    )


async def _connect_redis() -> Any:
    use_fake = os.getenv("USE_FAKE_REDIS", "0") == "1"
    if use_fake:
        import fakeredis.aioredis as fakeredis

        return fakeredis.FakeRedis()

    import redis.asyncio as redis

    url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    return redis.from_url(url)


async def amain(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run proof-gated recovery requeue for one task/run")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--output", default="docs/dashboard/artifacts/latest_recovery_requeue.json")
    parser.add_argument("--stale-after-seconds", type=int, default=300)
    parser.add_argument("--replay-clean", choices=("true", "false"), default="true")
    parser.add_argument("--deterministic-replay-ok", choices=("true", "false"), default="true")
    parser.add_argument("--authority-artifact-ok", choices=("true", "false"), default="true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    redis = await _connect_redis()

    try:
        from hfa_control.task_recovery import TaskRecoveryManager

        manager = TaskRecoveryManager(redis)
        result = await proof_gated_requeue(
            redis,
            run_id=args.run_id,
            tenant_id=args.tenant_id,
            manager=manager,
            replay_clean=args.replay_clean == "true",
            deterministic_replay_ok=args.deterministic_replay_ok == "true",
            authority_artifact_ok=args.authority_artifact_ok == "true",
            stale_after_seconds=args.stale_after_seconds,
            dry_run=args.dry_run,
        )
    finally:
        close = getattr(redis, "aclose", None) or getattr(redis, "close", None)
        if close:
            maybe = close()
            if hasattr(maybe, "__await__"):
                await maybe

    payload = asdict(result)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(f"RECOVERY_REQUEUE_{result.status}: run_id={result.run_id} status={result.requeue_status}")

    return 0 if result.status in {"PASS", "BLOCKED"} else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())

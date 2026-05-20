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
    proof_mode: str = "manual"
    replay_artifact_status: str = "not_used"
    authority_artifact_status: str = "not_used"
    recovery_audit_artifact_status: str = "not_used"
    artifact_candidate_found: bool = False


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


def _read_json_artifact(path: Path) -> tuple[dict[str, Any] | None, str]:
    if not path.exists():
        return None, "missing"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None, "malformed"
    if not isinstance(data, dict):
        return None, "malformed"
    return data, "loaded"


def _artifact_status(data: dict[str, Any] | None) -> str:
    if not data:
        return "missing"
    status = data.get("status")
    if status is None:
        status = data.get("result")
    return str(status or "unknown")


def _truthy_pass(value: Any) -> bool:
    return str(value).strip().lower() in {"pass", "passed", "ok", "clean", "success", "true"}


def _authority_has_banned_findings(data: dict[str, Any]) -> bool:
    for key in ("banned_count", "banned_findings", "banned", "violations", "failed"):
        value = data.get(key)
        if isinstance(value, int) and value > 0:
            return True
        if isinstance(value, list) and len(value) > 0:
            return True
        if isinstance(value, bool) and value:
            return True
    return False


@dataclass(frozen=True)
class ArtifactProofResult:
    proof: ProofDecision
    audit_result: dict[str, Any] | None
    replay_status: str
    authority_status: str
    recovery_audit_status: str


def evaluate_artifact_proof(
    *,
    run_id: str,
    replay_artifact: Path,
    authority_artifact: Path,
    recovery_audit_artifact: Path,
) -> ArtifactProofResult:
    replay, replay_load_status = _read_json_artifact(replay_artifact)
    authority, authority_load_status = _read_json_artifact(authority_artifact)
    recovery_audit, audit_load_status = _read_json_artifact(recovery_audit_artifact)

    reasons: list[str] = []

    if replay_load_status != "loaded":
        reasons.append(f"replay_artifact_{replay_load_status}")
    elif not _truthy_pass(_artifact_status(replay)):
        reasons.append(f"replay_artifact_status_{_artifact_status(replay)}")

    if authority_load_status != "loaded":
        reasons.append(f"authority_artifact_{authority_load_status}")
    elif not _truthy_pass(_artifact_status(authority)):
        reasons.append(f"authority_artifact_status_{_artifact_status(authority)}")
    elif _authority_has_banned_findings(authority):
        reasons.append("authority_artifact_has_banned_findings")

    candidate_found = False
    if audit_load_status != "loaded":
        reasons.append(f"recovery_audit_artifact_{audit_load_status}")
    else:
        candidate_found = _find_candidate(recovery_audit, run_id) is not None
        if not candidate_found:
            reasons.append("recovery_audit_candidate_missing")

    if reasons:
        return ArtifactProofResult(
            proof=ProofDecision(False, ";".join(reasons)),
            audit_result=recovery_audit,
            replay_status=replay_load_status if replay_load_status != "loaded" else _artifact_status(replay),
            authority_status=authority_load_status if authority_load_status != "loaded" else _artifact_status(authority),
            recovery_audit_status=audit_load_status if audit_load_status != "loaded" else _artifact_status(recovery_audit),
        )

    return ArtifactProofResult(
        proof=ProofDecision(True, "artifact_proof_allows_auto_resume"),
        audit_result=recovery_audit,
        replay_status=_artifact_status(replay),
        authority_status=_artifact_status(authority),
        recovery_audit_status=_artifact_status(recovery_audit),
    )


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
    proof_mode: str = "manual",
    replay_artifact: str | Path = "docs/dashboard/artifacts/latest_replay.json",
    authority_artifact: str | Path = "docs/dashboard/artifacts/latest_authority.json",
    recovery_audit_artifact: str | Path = "docs/dashboard/artifacts/latest_recovery_audit.json",
) -> RecoveryRequeueArtifact:
    artifact_proof: ArtifactProofResult | None = None

    if proof_mode == "artifacts":
        artifact_proof = evaluate_artifact_proof(
            run_id=run_id,
            replay_artifact=Path(replay_artifact),
            authority_artifact=Path(authority_artifact),
            recovery_audit_artifact=Path(recovery_audit_artifact),
        )
        audit_result = artifact_proof.audit_result or {"runs": []}
        proof = artifact_proof.proof
    else:
        audit_result = await audit(redis, stale_after_seconds=stale_after_seconds)
        proof = evaluate_proof(
            replay_clean=replay_clean,
            deterministic_replay_ok=deterministic_replay_ok,
            authority_artifact_ok=authority_artifact_ok,
        )

    candidate = _find_candidate(audit_result, run_id)

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
            proof_mode=proof_mode,
            replay_artifact_status=artifact_proof.replay_status if artifact_proof else "not_used",
            authority_artifact_status=artifact_proof.authority_status if artifact_proof else "not_used",
            recovery_audit_artifact_status=artifact_proof.recovery_audit_status if artifact_proof else "not_used",
            artifact_candidate_found=candidate is not None if artifact_proof else False,
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
            proof_mode=proof_mode,
            replay_artifact_status=artifact_proof.replay_status if artifact_proof else "not_used",
            authority_artifact_status=artifact_proof.authority_status if artifact_proof else "not_used",
            recovery_audit_artifact_status=artifact_proof.recovery_audit_status if artifact_proof else "not_used",
            artifact_candidate_found=candidate is not None if artifact_proof else False,
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
            proof_mode=proof_mode,
            replay_artifact_status=artifact_proof.replay_status if artifact_proof else "not_used",
            authority_artifact_status=artifact_proof.authority_status if artifact_proof else "not_used",
            recovery_audit_artifact_status=artifact_proof.recovery_audit_status if artifact_proof else "not_used",
            artifact_candidate_found=candidate is not None if artifact_proof else False,
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
    parser.add_argument("--proof-mode", choices=("manual", "artifacts"), default="manual")
    parser.add_argument("--replay-artifact", default="docs/dashboard/artifacts/latest_replay.json")
    parser.add_argument("--authority-artifact", default="docs/dashboard/artifacts/latest_authority.json")
    parser.add_argument("--recovery-audit-artifact", default="docs/dashboard/artifacts/latest_recovery_audit.json")
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
            proof_mode=args.proof_mode,
            replay_artifact=args.replay_artifact,
            authority_artifact=args.authority_artifact,
            recovery_audit_artifact=args.recovery_audit_artifact,
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

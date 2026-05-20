#!/usr/bin/env python3
"""Controlled staging cold restart recovery drill.

This drill composes the established recovery chain:
1. recovery audit
2. artifact-backed proof
3. proof-gated requeue
4. Redis-backed canonical mutation

It does not enable an automatic recovery daemon.
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

from scripts.recovery_requeue_drill import DEFAULT_OUTPUT as REQUEUE_DRILL_OUTPUT
from scripts.recovery_requeue_drill import run_drill as run_requeue_drill


DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_cold_restart_drill.json")


@dataclass(frozen=True)
class ColdRestartDrillArtifact:
    source: str
    status: str
    mode: str
    run_id: str
    tenant_id: str
    recovery_requeue_status: str
    recovery_requeue_ok: bool
    proof_allowed: bool
    mutation_attempted: bool
    pre_restart_state: dict[str, Any]
    post_requeue_state: dict[str, Any]
    zombie_completion_rejection_status: str
    zombie_completion_rejection_checked: bool
    recovery_requeue_drill_artifact: dict[str, Any]
    notes: list[str]


async def _connect_redis() -> Any:
    use_fake = os.getenv("USE_FAKE_REDIS", "0") == "1"
    if use_fake:
        import fakeredis.aioredis as fakeredis

        return fakeredis.FakeRedis()

    import redis.asyncio as redis

    url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    return redis.from_url(url)


def _skipped_artifact(*, run_id: str, tenant_id: str) -> ColdRestartDrillArtifact:
    return ColdRestartDrillArtifact(
        source="cold_restart_drill",
        status="SKIPPED",
        mode="staging-cold-restart",
        run_id=run_id,
        tenant_id=tenant_id,
        recovery_requeue_status="REAL_REDIS_REQUIRED",
        recovery_requeue_ok=False,
        proof_allowed=False,
        mutation_attempted=False,
        pre_restart_state={},
        post_requeue_state={},
        zombie_completion_rejection_status="NOT_CHECKED",
        zombie_completion_rejection_checked=False,
        recovery_requeue_drill_artifact={},
        notes=[
            "Cold restart drill requires real Redis Lua EVAL support.",
            "fakeredis is intentionally skipped for this drill.",
            "No automatic recovery daemon is enabled.",
        ],
    )


async def run_cold_restart_drill(
    redis: Any,
    *,
    run_id: str,
    tenant_id: str,
    output: Path = DEFAULT_OUTPUT,
) -> ColdRestartDrillArtifact:
    requeue_artifact = await run_requeue_drill(
        redis,
        run_id=run_id,
        tenant_id=tenant_id,
        output=REQUEUE_DRILL_OUTPUT,
    )
    requeue_payload = asdict(requeue_artifact)

    pre_state = requeue_payload.get("pre_state", {})
    post_state = requeue_payload.get("post_state", {})

    status = "PASS" if (
        requeue_artifact.status == "PASS"
        and requeue_artifact.requeue_ok
        and requeue_artifact.requeue_status == "TASK_REQUEUED"
    ) else "FAIL"

    payload = ColdRestartDrillArtifact(
        source="cold_restart_drill",
        status=status,
        mode="staging-cold-restart",
        run_id=run_id,
        tenant_id=tenant_id,
        recovery_requeue_status=requeue_artifact.requeue_status,
        recovery_requeue_ok=requeue_artifact.requeue_ok,
        proof_allowed=requeue_artifact.proof_allowed,
        mutation_attempted=requeue_artifact.mutation_attempted,
        pre_restart_state=pre_state,
        post_requeue_state=post_state,
        zombie_completion_rejection_status="PENDING_COMPLETION_HARNESS",
        zombie_completion_rejection_checked=False,
        recovery_requeue_drill_artifact=requeue_payload,
        notes=[
            "Controlled cold restart drill composed from Redis-backed recovery requeue drill.",
            "No automatic recovery daemon is enabled.",
            "Zombie completion rejection check is pending explicit completion harness integration.",
        ],
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(payload), indent=2, sort_keys=True), encoding="utf-8")
    return payload


async def amain(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run controlled staging cold restart recovery drill")
    parser.add_argument("--run-id", default="cold-restart-drill-task")
    parser.add_argument("--tenant-id", default="cold-restart-tenant")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    output = Path(args.output)

    if os.getenv("USE_FAKE_REDIS", "0") == "1":
        artifact = _skipped_artifact(run_id=args.run_id, tenant_id=args.tenant_id)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")
        if args.json:
            print(json.dumps(asdict(artifact), sort_keys=True))
        else:
            print("COLD_RESTART_DRILL_SKIPPED: REAL_REDIS_REQUIRED")
        return 0

    redis = await _connect_redis()
    try:
        artifact = await run_cold_restart_drill(
            redis,
            run_id=args.run_id,
            tenant_id=args.tenant_id,
            output=output,
        )
    finally:
        close = getattr(redis, "aclose", None) or getattr(redis, "close", None)
        if close:
            maybe = close()
            if hasattr(maybe, "__await__"):
                await maybe

    if args.json:
        print(json.dumps(asdict(artifact), sort_keys=True))
    else:
        print(f"COLD_RESTART_DRILL_{artifact.status}: run_id={artifact.run_id} requeue={artifact.recovery_requeue_status}")

    return 0 if artifact.status == "PASS" else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())

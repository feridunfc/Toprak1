#!/usr/bin/env python3
"""Zombie/stale-owner completion rejection drill.

This drill verifies that after recovery requeue, a stale worker completion cannot
commit. The expected rejection after requeue is usually `illegal_transition`
because task_requeue.lua moves the task from running to ready before the old
worker tries to complete.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
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

from hfa_control.dag_lua import DagLua
from scripts.recovery_requeue_drill import run_drill as run_requeue_drill


DEFAULT_OUTPUT = Path("docs/dashboard/artifacts/latest_zombie_completion_drill.json")


@dataclass(frozen=True)
class ZombieCompletionDrillArtifact:
    source: str
    status: str
    mode: str
    task_id: str
    tenant_id: str
    pre_requeue_claim_epoch: str
    post_requeue_claim_epoch: str
    stale_worker_identity: str
    stale_scheduler_epoch: str
    zombie_completion_attempted: bool
    zombie_completion_accepted: bool
    zombie_completion_status: str
    rejection_reason: str
    recovery_requeue_status: str
    recovery_requeue_ok: bool
    notes: list[str]


async def _connect_redis() -> Any:
    use_fake = os.getenv("USE_FAKE_REDIS", "0") == "1"
    if use_fake:
        import fakeredis.aioredis as fakeredis

        return fakeredis.FakeRedis()

    import redis.asyncio as redis

    url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    return redis.from_url(url)


def _skipped_artifact(*, task_id: str, tenant_id: str) -> ZombieCompletionDrillArtifact:
    return ZombieCompletionDrillArtifact(
        source="zombie_completion_drill",
        status="SKIPPED",
        mode="zombie-completion-rejection",
        task_id=task_id,
        tenant_id=tenant_id,
        pre_requeue_claim_epoch="",
        post_requeue_claim_epoch="",
        stale_worker_identity="stale-worker",
        stale_scheduler_epoch="1",
        zombie_completion_attempted=False,
        zombie_completion_accepted=False,
        zombie_completion_status="REAL_REDIS_REQUIRED",
        rejection_reason="REAL_REDIS_REQUIRED",
        recovery_requeue_status="REAL_REDIS_REQUIRED",
        recovery_requeue_ok=False,
        notes=[
            "Zombie completion drill requires real Redis Lua EVAL support.",
            "fakeredis is intentionally skipped for this drill.",
            "No automatic recovery daemon is enabled.",
        ],
    )


def _meta_value(payload: dict[str, Any], key: str) -> str:
    try:
        value = payload["redis"]["meta"].get(key, "")
    except Exception:
        value = ""
    return "" if value is None else str(value)


async def run_zombie_completion_drill(
    redis: Any,
    *,
    task_id: str,
    tenant_id: str,
    output: Path = DEFAULT_OUTPUT,
) -> ZombieCompletionDrillArtifact:
    requeue_artifact = await run_requeue_drill(
        redis,
        run_id=task_id,
        tenant_id=tenant_id,
    )
    requeue_payload = asdict(requeue_artifact)

    pre_requeue_claim_epoch = _meta_value(requeue_payload.get("pre_state", {}), "claim_epoch")
    post_requeue_claim_epoch = _meta_value(requeue_payload.get("post_state", {}), "claim_epoch")

    stale_worker = "stale-worker"
    stale_scheduler_epoch = "1"
    stale_claim_epoch = pre_requeue_claim_epoch or "1"

    dag = DagLua(redis)
    completion = await dag.task_complete(
        task_id=task_id,
        run_id=task_id,
        tenant_id=tenant_id,
        terminal_state="done",
        finished_at_ms=int(time.time() * 1000),
        reason_code="zombie_completion_probe",
        worker_instance_id=stale_worker,
        expected_scheduler_epoch=stale_scheduler_epoch,
        expected_claim_epoch=stale_claim_epoch,
    )

    accepted = bool(completion.completed)
    rejection_statuses = {
        "illegal_transition",
        "owner_mismatch",
        "scheduler_epoch_mismatch",
        "claim_epoch_mismatch",
    }

    status = "PASS" if (
        requeue_artifact.requeue_ok
        and not accepted
        and completion.status in rejection_statuses
        and pre_requeue_claim_epoch == post_requeue_claim_epoch
    ) else "FAIL"

    artifact = ZombieCompletionDrillArtifact(
        source="zombie_completion_drill",
        status=status,
        mode="zombie-completion-rejection",
        task_id=task_id,
        tenant_id=tenant_id,
        pre_requeue_claim_epoch=pre_requeue_claim_epoch,
        post_requeue_claim_epoch=post_requeue_claim_epoch,
        stale_worker_identity=stale_worker,
        stale_scheduler_epoch=stale_scheduler_epoch,
        zombie_completion_attempted=True,
        zombie_completion_accepted=accepted,
        zombie_completion_status=completion.status,
        rejection_reason=completion.status if not accepted else "ACCEPTED",
        recovery_requeue_status=requeue_artifact.requeue_status,
        recovery_requeue_ok=requeue_artifact.requeue_ok,
        notes=[
            "Controlled zombie completion rejection drill.",
            "Requeue must not reset claim_epoch.",
            "After requeue, stale completion is expected to be rejected.",
            "No automatic recovery daemon is enabled.",
        ],
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")
    return artifact


async def amain(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run zombie completion rejection drill")
    parser.add_argument("--task-id", default="zombie-completion-drill-task")
    parser.add_argument("--tenant-id", default="zombie-completion-tenant")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    output = Path(args.output)

    if os.getenv("USE_FAKE_REDIS", "0") == "1":
        artifact = _skipped_artifact(task_id=args.task_id, tenant_id=args.tenant_id)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(asdict(artifact), indent=2, sort_keys=True), encoding="utf-8")
        if args.json:
            print(json.dumps(asdict(artifact), sort_keys=True))
        else:
            print("ZOMBIE_COMPLETION_DRILL_SKIPPED: REAL_REDIS_REQUIRED")
        return 0

    redis = await _connect_redis()
    try:
        artifact = await run_zombie_completion_drill(
            redis,
            task_id=args.task_id,
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
        print(f"ZOMBIE_COMPLETION_DRILL_{artifact.status}: task_id={artifact.task_id} status={artifact.zombie_completion_status}")

    return 0 if artifact.status == "PASS" else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())

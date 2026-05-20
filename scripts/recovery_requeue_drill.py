#!/usr/bin/env python3
"""Redis-backed proof-gated recovery requeue drill.

This is a controlled single-task drill. It is not a recovery daemon.

The drill seeds a stale running candidate, writes clean proof artifacts, invokes
the artifact-backed recovery requeue command without dry-run, and writes a drill
artifact summarizing pre/post Redis state.
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

from hfa.dag.schema import DagRedisKey

from scripts.recovery_audit import RUNNING_ZSET, audit
from scripts.recovery_requeue import proof_gated_requeue


ARTIFACT_DIR = Path("docs/dashboard/artifacts")
DEFAULT_OUTPUT = ARTIFACT_DIR / "latest_recovery_requeue_drill.json"


@dataclass(frozen=True)
class DrillArtifact:
    source: str
    mode: str
    status: str
    run_id: str
    tenant_id: str
    proof_allowed: bool
    candidate_found: bool
    mutation_attempted: bool
    requeue_ok: bool
    requeue_status: str
    pre_state: dict[str, Any]
    post_state: dict[str, Any]
    recovery_requeue_artifact: dict[str, Any]
    notes: list[str]


async def _connect_redis() -> Any:
    use_fake = os.getenv("USE_FAKE_REDIS", "0") == "1"
    if use_fake:
        import fakeredis.aioredis as fakeredis

        return fakeredis.FakeRedis()

    import redis.asyncio as redis

    url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    return redis.from_url(url)


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, dict):
        return {_decode(k): _decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode(v) for v in value]
    return value


async def _state_snapshot(redis: Any, run_id: str, tenant_id: str) -> dict[str, Any]:
    state_key = DagRedisKey.task_state(run_id)
    meta_key = DagRedisKey.task_meta(run_id)
    running_key = DagRedisKey.task_running_zset(tenant_id)
    ready_key = DagRedisKey.tenant_ready_queue(tenant_id)
    completion_key = DagRedisKey.completion_stream(tenant_id)

    return {
        "state": _decode(await redis.get(state_key)),
        "meta": _decode(await redis.hgetall(meta_key)),
        "running_score": await redis.zscore(running_key, run_id),
        "ready_score": await redis.zscore(ready_key, run_id),
        "completion_stream_len": await redis.xlen(completion_key),
    }


async def _seed_stale_candidate(redis: Any, *, run_id: str, tenant_id: str) -> None:
    now_ms = int(time.time() * 1000)
    old_score = now_ms - 3600_000

    state_key = DagRedisKey.task_state(run_id)
    meta_key = DagRedisKey.task_meta(run_id)
    running_key = DagRedisKey.task_running_zset(tenant_id)
    ready_key = DagRedisKey.tenant_ready_queue(tenant_id)
    completion_key = DagRedisKey.completion_stream(tenant_id)

    await redis.delete(state_key, meta_key, completion_key)
    await redis.zrem(running_key, run_id)
    await redis.zrem(ready_key, run_id)

    await redis.set(state_key, "running")
    await redis.hset(
        meta_key,
        mapping={
            "task_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "drill",
            "state": "running",
            "admitted_at_ms": str(old_score),
            "started_at_ms": str(old_score),
            "worker_instance_id": "stale-worker",
            "scheduler_epoch": "1",
            "claim_epoch": "1",
            "requeue_count": "0",
            "last_heartbeat_at_ms": str(old_score),
        },
    )
    await redis.zadd(running_key, {run_id: old_score})


def _write_clean_proof_artifacts(*, run_id: str) -> dict[str, str]:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    replay = ARTIFACT_DIR / "latest_replay.json"
    authority = ARTIFACT_DIR / "latest_authority.json"
    recovery_audit = ARTIFACT_DIR / "latest_recovery_audit.json"

    replay.write_text(json.dumps({"status": "PASS"}, indent=2), encoding="utf-8")
    authority.write_text(json.dumps({"status": "PASS", "banned_count": 0}, indent=2), encoding="utf-8")
    recovery_audit.write_text(
        json.dumps(
            {
                "status": "PASS",
                "runs": [
                    {
                        "run_id": run_id,
                        "stale": True,
                        "missing_claim": True,
                        "expired_claim": False,
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "replay": str(replay),
        "authority": str(authority),
        "recovery_audit": str(recovery_audit),
    }


async def run_drill(
    redis: Any,
    *,
    run_id: str,
    tenant_id: str,
    output: Path = DEFAULT_OUTPUT,
) -> DrillArtifact:
    await _seed_stale_candidate(redis, run_id=run_id, tenant_id=tenant_id)
    proof_artifacts = _write_clean_proof_artifacts(run_id=run_id)

    pre_state = await _state_snapshot(redis, run_id, tenant_id)
    pre_audit = await audit(redis, stale_after_seconds=300)

    from hfa_control.task_recovery import TaskRecoveryManager

    manager = TaskRecoveryManager(redis)
    requeue = await proof_gated_requeue(
        redis,
        run_id=run_id,
        tenant_id=tenant_id,
        manager=manager,
        proof_mode="artifacts",
        dry_run=False,
        replay_artifact=proof_artifacts["replay"],
        authority_artifact=proof_artifacts["authority"],
        recovery_audit_artifact=proof_artifacts["recovery_audit"],
    )
    post_state = await _state_snapshot(redis, run_id, tenant_id)

    payload = DrillArtifact(
        source="recovery_requeue_drill",
        mode="redis-backed-single-task",
        status="PASS" if requeue.mutation_attempted and requeue.requeue_ok else "FAIL",
        run_id=run_id,
        tenant_id=tenant_id,
        proof_allowed=requeue.proof_allowed,
        candidate_found=requeue.candidate_found,
        mutation_attempted=requeue.mutation_attempted,
        requeue_ok=requeue.requeue_ok,
        requeue_status=requeue.requeue_status,
        pre_state={
            "redis": pre_state,
            "audit_candidate_count": pre_audit.get("candidate_count"),
            "audit_status": pre_audit.get("status"),
        },
        post_state={"redis": post_state},
        recovery_requeue_artifact=asdict(requeue),
        notes=[
            "Controlled single-task Redis-backed drill.",
            "No automatic recovery loop is enabled.",
            "Mutation is routed through TaskRecoveryManager.requeue_stale_task.",
        ],
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(asdict(payload), indent=2, sort_keys=True), encoding="utf-8")
    return payload


async def amain(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Redis-backed recovery requeue drill")
    parser.add_argument("--run-id", default="drill-stale-run")
    parser.add_argument("--tenant-id", default="drill-tenant")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    output = Path(args.output)

    if os.getenv("USE_FAKE_REDIS", "0") == "1":
        payload = DrillArtifact(
            source="recovery_requeue_drill",
            mode="redis-backed-single-task",
            status="SKIPPED",
            run_id=args.run_id,
            tenant_id=args.tenant_id,
            proof_allowed=False,
            candidate_found=False,
            mutation_attempted=False,
            requeue_ok=False,
            requeue_status="REAL_REDIS_REQUIRED",
            pre_state={},
            post_state={},
            recovery_requeue_artifact={},
            notes=[
                "Redis-backed mutation drill requires real Redis Lua EVAL support.",
                "fakeredis is intentionally skipped for this drill.",
                "Use Docker Redis or staging Redis without USE_FAKE_REDIS=1.",
            ],
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(asdict(payload), indent=2, sort_keys=True), encoding="utf-8")
        if args.json:
            print(json.dumps(asdict(payload), sort_keys=True))
        else:
            print(f"RECOVERY_REQUEUE_DRILL_SKIPPED: run_id={args.run_id} status=REAL_REDIS_REQUIRED")
        return 0

    redis = await _connect_redis()
    try:
        result = await run_drill(
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

    payload = asdict(result)
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(f"RECOVERY_REQUEUE_DRILL_{result.status}: run_id={result.run_id} status={result.requeue_status}")

    return 0 if result.status == "PASS" else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())

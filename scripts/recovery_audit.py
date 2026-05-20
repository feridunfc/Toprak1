#!/usr/bin/env python3
"""Read-only cold restart / in-flight recovery audit.

This script inspects the legacy compatibility runtime projections:
- hfa:cp:running
- hfa:run:meta:{run_id}
- hfa:run:state:{run_id}
- hfa:run:claim:{run_id}

It does not mutate Redis. It produces a dashboard artifact that helps decide
whether a cold restart recovery pass is safe to attempt.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


RUNNING_ZSET = "hfa:cp:running"


@dataclass(frozen=True)
class RecoveryAuditRun:
    run_id: str
    state: str | None
    claim_owner: str | None
    claim_ttl: int
    running_score: float
    admitted_at: float | None
    started_at: float | None
    stale: bool
    expired_claim: bool
    missing_claim: bool
    reason: str


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()
    return value


def _decode_mapping(data: dict) -> dict[str, Any]:
    return {_decode(k): _decode(v) for k, v in (data or {}).items()}


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


async def audit(redis: Any, *, now: float | None = None, stale_after_seconds: int = 300, limit: int = 1000) -> dict[str, Any]:
    now = now or time.time()

    raw = await redis.zrange(RUNNING_ZSET, 0, max(limit - 1, 0), withscores=True)
    runs: list[RecoveryAuditRun] = []

    for item in raw:
        run_id_raw, score = item if isinstance(item, tuple) else (item, 0)
        run_id = str(_decode(run_id_raw))
        score_float = float(score or 0)

        meta = _decode_mapping(await redis.hgetall(f"hfa:run:meta:{run_id}"))
        state = _decode(await redis.get(f"hfa:run:state:{run_id}"))
        claim_owner = _decode(await redis.get(f"hfa:run:claim:{run_id}"))
        claim_ttl = int(await redis.ttl(f"hfa:run:claim:{run_id}"))

        started_at = _safe_float(meta.get("started_at"))
        admitted_at = _safe_float(meta.get("admitted_at"))
        age_anchor = started_at or admitted_at or score_float
        stale = bool(age_anchor and (now - age_anchor) > stale_after_seconds)
        expired_claim = claim_ttl < 0
        missing_claim = claim_owner is None

        reasons: list[str] = []
        if stale:
            reasons.append("stale_running")
        if expired_claim:
            reasons.append("expired_claim")
        if missing_claim:
            reasons.append("missing_claim")
        if state not in {"running", "scheduled"}:
            reasons.append(f"unexpected_state:{state}")

        runs.append(
            RecoveryAuditRun(
                run_id=run_id,
                state=state,
                claim_owner=claim_owner,
                claim_ttl=claim_ttl,
                running_score=score_float,
                admitted_at=admitted_at,
                started_at=started_at,
                stale=stale,
                expired_claim=expired_claim,
                missing_claim=missing_claim,
                reason=";".join(reasons),
            )
        )

    candidates = [run for run in runs if run.stale or run.expired_claim or run.missing_claim]
    return {
        "source": "recovery_audit",
        "mode": "read-only",
        "status": "PASS",
        "running_count": len(runs),
        "candidate_count": len(candidates),
        "stale_count": sum(1 for run in runs if run.stale),
        "expired_claim_count": sum(1 for run in runs if run.expired_claim),
        "missing_claim_count": sum(1 for run in runs if run.missing_claim),
        "running_zset": RUNNING_ZSET,
        "stale_after_seconds": stale_after_seconds,
        "runs": [asdict(run) for run in runs],
        "notes": [
            "Read-only audit only; no requeue or mutation is performed.",
            "Candidates require replay/proof gate before automatic recovery.",
        ],
    }


async def _connect_redis() -> Any:
    use_fake = os.getenv("USE_FAKE_REDIS", "0") == "1"
    if use_fake:
        import fakeredis.aioredis as fakeredis

        return fakeredis.FakeRedis()

    import redis.asyncio as redis

    url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    return redis.from_url(url)


async def amain(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run read-only cold restart recovery audit")
    parser.add_argument("--output", default="docs/dashboard/artifacts/latest_recovery_audit.json")
    parser.add_argument("--stale-after-seconds", type=int, default=300)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    redis = await _connect_redis()
    try:
        result = await audit(redis, stale_after_seconds=args.stale_after_seconds, limit=args.limit)
    finally:
        close = getattr(redis, "aclose", None) or getattr(redis, "close", None)
        if close:
            maybe = close()
            if hasattr(maybe, "__await__"):
                await maybe

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(f"RECOVERY_AUDIT_{result['status']}: running={result['running_count']} candidates={result['candidate_count']}")

    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())

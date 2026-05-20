import time

import fakeredis.aioredis as faredis
import pytest

from scripts.recovery_audit import RUNNING_ZSET, audit


@pytest.mark.asyncio
async def test_recovery_audit_empty_running_zset_has_no_candidates():
    redis = faredis.FakeRedis()

    result = await audit(redis, now=1000.0, stale_after_seconds=300)

    assert result["status"] == "PASS"
    assert result["running_count"] == 0
    assert result["candidate_count"] == 0
    assert result["stale_count"] == 0
    assert result["expired_claim_count"] == 0
    assert result["missing_claim_count"] == 0


@pytest.mark.asyncio
async def test_recovery_audit_stale_running_missing_claim_is_candidate():
    redis = faredis.FakeRedis()
    run_id = "stale-run-1"

    await redis.zadd(RUNNING_ZSET, {run_id: 100.0})
    await redis.hset(
        f"hfa:run:meta:{run_id}",
        mapping={
            "run_id": run_id,
            "tenant_id": "acme",
            "admitted_at": "100.0",
            "state": "running",
        },
    )
    await redis.set(f"hfa:run:state:{run_id}", "running")

    result = await audit(redis, now=1000.0, stale_after_seconds=300)

    assert result["running_count"] == 1
    assert result["candidate_count"] == 1
    assert result["stale_count"] == 1
    assert result["missing_claim_count"] == 1
    assert result["expired_claim_count"] == 1

    run = result["runs"][0]
    assert run["run_id"] == run_id
    assert run["stale"] is True
    assert run["missing_claim"] is True
    assert "stale_running" in run["reason"]
    assert "missing_claim" in run["reason"]


@pytest.mark.asyncio
async def test_recovery_audit_fresh_running_valid_claim_is_not_candidate():
    redis = faredis.FakeRedis()
    run_id = "fresh-run-1"

    now = time.time()
    await redis.zadd(RUNNING_ZSET, {run_id: now})
    await redis.hset(
        f"hfa:run:meta:{run_id}",
        mapping={
            "run_id": run_id,
            "tenant_id": "acme",
            "admitted_at": str(now),
            "started_at": str(now),
            "state": "running",
        },
    )
    await redis.set(f"hfa:run:state:{run_id}", "running")
    await redis.set(f"hfa:run:claim:{run_id}", "worker-1", ex=60)

    result = await audit(redis, now=now + 5, stale_after_seconds=300)

    assert result["running_count"] == 1
    assert result["candidate_count"] == 0
    assert result["stale_count"] == 0
    assert result["missing_claim_count"] == 0
    assert result["expired_claim_count"] == 0

    run = result["runs"][0]
    assert run["claim_owner"] == "worker-1"
    assert run["claim_ttl"] > 0
    assert run["reason"] == ""

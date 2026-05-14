"""
tests/core/test_sprint13_run_api.py
IRONCLAD Sprint 13 — Run introspection API tests

Covers:
  - RunStateResponse / RunClaimResponse / RunResultResponse model construction
  - running runs ZSET-backed listing
  - run state returns correct state for all lifecycle stages
  - claim endpoint owner + TTL semantics
  - result endpoint returns stored result
  - missing run → safe empty / unknown state
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from fakeredis.aioredis import FakeRedis

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-core" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-control" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-worker" / "src"))

from hfa.runtime.state_store import StateStore
from hfa_control.api.models import (
    RunStateResponse,
    RunClaimResponse,
    RunResultResponse,
    RunningRunSummary,
    RunningRunsResponse,
)
from hfa_control.models import ControlPlaneConfig


def _cfg():
    return ControlPlaneConfig(instance_id="cp1", stale_run_timeout=600.0)


async def _seed_run(
    redis, run_id, state="running", tenant_id="t1", worker_group="grp", shard=0, age=10.0, rc=0
):
    store = StateStore(redis)
    admitted = time.time() - age
    await store.create_run_meta(
        run_id,
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "base",
            "worker_group": worker_group,
            "shard": str(shard),
            "reschedule_count": str(rc),
            "admitted_at": str(admitted),
            "state": state,
        },
    )
    await redis.set(f"hfa:run:state:{run_id}", state, ex=86400)
    if state in ("running", "scheduled"):
        await redis.zadd("hfa:cp:running", {run_id: admitted})


# ---------------------------------------------------------------------------
# RunStateResponse model
# ---------------------------------------------------------------------------


def test_run_state_response_fields():
    r = RunStateResponse(
        run_id="r1",
        tenant_id="t1",
        state="running",
        worker_group="grp",
        shard=0,
        reschedule_count=0,
        admitted_at=1700000000.0,
    )
    assert r.run_id == "r1"
    assert r.state == "running"


# ---------------------------------------------------------------------------
# Running runs listing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_running_runs_returns_entries():
    redis = FakeRedis()
    await _seed_run(redis, "r1")
    await _seed_run(redis, "r2")
    store = StateStore(redis)
    base = await store.get_running_runs(limit=10)
    ids = {r["run_id"] for r in base}
    assert "r1" in ids
    assert "r2" in ids


@pytest.mark.asyncio
async def test_running_runs_empty_zset():
    redis = FakeRedis()
    store = StateStore(redis)
    runs = await store.get_running_runs()
    assert runs == []


@pytest.mark.asyncio
async def test_running_runs_response_construction():
    redis = FakeRedis()
    await _seed_run(redis, "r1", worker_group="grp-a", shard=1)
    store = StateStore(redis)
    base = await store.get_running_runs()
    summaries = [
        RunningRunSummary(
            run_id=r["run_id"],
            tenant_id="t1",
            state=r["state"],
            worker_group="grp-a",
            shard=1,
            started_at=r["started_at"],
            claim_owner=None,
        )
        for r in base
    ]
    resp = RunningRunsResponse(count=len(summaries), runs=summaries)
    assert resp.count == 1
    assert resp.runs[0].shard == 1


# ---------------------------------------------------------------------------
# Run state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_run_state_running():
    redis = FakeRedis()
    await _seed_run(redis, "r1", state="running", tenant_id="t1")
    store = StateStore(redis)
    state = await store.get_run_state("r1")
    assert state == "running"


@pytest.mark.asyncio
async def test_get_run_state_done():
    redis = FakeRedis()
    await _seed_run(redis, "r2", state="done", tenant_id="t1")
    store = StateStore(redis)
    state = await store.get_run_state("r2")
    assert state == "done"


@pytest.mark.asyncio
async def test_get_run_state_unknown_for_missing():
    redis = FakeRedis()
    store = StateStore(redis)
    state = await store.get_run_state("nonexistent")
    assert state is None


@pytest.mark.asyncio
async def test_get_run_meta_returns_all_fields():
    redis = FakeRedis()
    await _seed_run(redis, "r1", tenant_id="t1", worker_group="g1", shard=3, rc=2)
    store = StateStore(redis)
    meta = await store.get_run_meta("r1")
    assert meta["tenant_id"] == "t1"
    assert meta["worker_group"] == "g1"
    assert meta["shard"] == "3"
    assert meta["reschedule_count"] == "2"


# ---------------------------------------------------------------------------
# Claim
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_claim_owner_present():
    redis = FakeRedis()
    store = StateStore(redis)
    await store.claim_execution("r1", "w-owner")
    owner = await store.get_claim_owner("r1")
    ttl = await store.get_claim_ttl("r1")
    assert owner == "w-owner"
    assert ttl > 0

    resp = RunClaimResponse(run_id="r1", claimed=True, owner=owner, ttl_seconds=ttl)
    assert resp.claimed is True
    assert resp.owner == "w-owner"


@pytest.mark.asyncio
async def test_run_claim_not_claimed():
    redis = FakeRedis()
    store = StateStore(redis)
    owner = await store.get_claim_owner("r-no-claim")
    resp = RunClaimResponse(run_id="r-no-claim", claimed=False, owner=None, ttl_seconds=0)
    assert resp.claimed is False
    assert resp.owner is None


@pytest.mark.asyncio
async def test_run_claim_released():
    redis = FakeRedis()
    store = StateStore(redis)
    await store.claim_execution("r1", "w1")
    await store.release_claim("r1")
    owner = await store.get_claim_owner("r1")
    assert owner is None


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_result_stored_and_retrieved():
    redis = FakeRedis()
    store = StateStore(redis)
    await store.store_result(
        "r1",
        "t1",
        "done",
        {"out": "ok"},
        cost_cents=42,
        tokens_used=100,
    )
    result = await store.get_result("r1")
    assert result is not None
    assert result["status"] == "done"
    assert result["cost_cents"] == 42
    assert result["tokens_used"] == 100
    assert result["payload"] == {"out": "ok"}

    resp = RunResultResponse(
        run_id="r1",
        tenant_id="t1",
        status="done",
        cost_cents=42,
        tokens_used=100,
        error=None,
        payload={"out": "ok"},
        completed_at=result["completed_at"],
    )
    assert resp.status == "done"
    assert resp.cost_cents == 42


@pytest.mark.asyncio
async def test_run_result_none_for_missing():
    redis = FakeRedis()
    store = StateStore(redis)
    result = await store.get_result("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_run_result_with_error():
    redis = FakeRedis()
    store = StateStore(redis)
    await store.store_result(
        "r-err",
        "t1",
        "failed",
        {},
        cost_cents=0,
        tokens_used=0,
        error="timeout",
    )
    result = await store.get_result("r-err")
    assert result["status"] == "failed"
    assert result["error"] == "timeout"

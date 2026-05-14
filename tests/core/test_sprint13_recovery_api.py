"""
tests/core/test_sprint13_recovery_api.py
IRONCLAD Sprint 13 — Recovery visibility API tests

Covers:
  - stale runs endpoint detects correct runs
  - stale runs endpoint returns empty safely
  - recovery summary aggregates counts
  - DLQ endpoint lists dead-lettered runs
  - DLQ all-tenant listing via sentinel
  - Sprint 10 reschedule / DLQ semantics unchanged
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

from hfa_control.models import ControlPlaneConfig
from hfa_control.recovery import RecoveryService
from hfa_control.api.models import (
    StaleRunsResponse,
    StaleRunSummary,
    RecoverySummaryResponse,
    DLQListResponse,
    DLQEntryResponse,
)


def _cfg(**kw) -> ControlPlaneConfig:
    defaults = dict(
        instance_id="cp1",
        stale_run_timeout=5.0,
        max_reschedule_attempts=3,
        recovery_sweep_interval=999.0,
    )
    defaults.update(kw)
    return ControlPlaneConfig(**defaults)


async def _seed_stale(redis, run_id, rc=0, state="running", age=120.0):
    score = time.time() - age
    await redis.zadd("hfa:cp:running", {run_id: score})
    await redis.set(f"hfa:run:state:{run_id}", state, ex=86400)
    await redis.hset(
        f"hfa:run:meta:{run_id}",
        mapping={
            "run_id": run_id,
            "tenant_id": "t1",
            "agent_type": "base",
            "worker_group": "grp",
            "reschedule_count": str(rc),
            "admitted_at": str(score),
            "state": state,
        },
    )


# ---------------------------------------------------------------------------
# Stale detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_runs_detected():
    redis = FakeRedis()
    cfg = _cfg(stale_run_timeout=5.0)
    svc = RecoveryService(redis, cfg)
    await _seed_stale(redis, "r-stale", age=120.0)
    stale = await svc._find_stale_runs()
    assert "r-stale" in stale


@pytest.mark.asyncio
async def test_stale_runs_empty_for_fresh():
    redis = FakeRedis()
    cfg = _cfg(stale_run_timeout=600.0)
    svc = RecoveryService(redis, cfg)
    await redis.zadd("hfa:cp:running", {"r-fresh": time.time()})
    await redis.set("hfa:run:state:r-fresh", "running", ex=86400)
    stale = await svc._find_stale_runs()
    assert "r-fresh" not in stale


@pytest.mark.asyncio
async def test_stale_runs_ignores_terminal():
    redis = FakeRedis()
    cfg = _cfg(stale_run_timeout=5.0)
    svc = RecoveryService(redis, cfg)
    await _seed_stale(redis, "r-done", state="done", age=120.0)
    stale = await svc._find_stale_runs()
    assert "r-done" not in stale


@pytest.mark.asyncio
async def test_stale_run_summary_model():
    summary = StaleRunSummary(
        run_id="r1",
        tenant_id="t1",
        state="running",
        worker_group="grp",
        reschedule_count=1,
        running_since=time.time() - 200,
        stale_for_seconds=200.0,
    )
    assert summary.run_id == "r1"
    assert summary.stale_for_seconds == 200.0


@pytest.mark.asyncio
async def test_stale_runs_response_construction():
    redis = FakeRedis()
    cfg = _cfg(stale_run_timeout=5.0)
    svc = RecoveryService(redis, cfg)
    await _seed_stale(redis, "r-s1")
    await _seed_stale(redis, "r-s2")
    stale = await svc._find_stale_runs()
    summaries = [
        StaleRunSummary(
            run_id=r,
            tenant_id="t1",
            state="running",
            worker_group="grp",
            reschedule_count=0,
            running_since=time.time() - 120,
            stale_for_seconds=120.0,
        )
        for r in stale
    ]
    resp = StaleRunsResponse(count=len(summaries), runs=summaries)
    assert resp.count >= 2


# ---------------------------------------------------------------------------
# Recovery summary
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_summary_model():
    resp = RecoverySummaryResponse(
        stale_count=3,
        dlq_count=1,
        schedulable_workers=5,
        draining_workers=1,
    )
    assert resp.stale_count == 3
    assert resp.dlq_count == 1
    assert resp.schedulable_workers == 5


@pytest.mark.asyncio
async def test_recovery_summary_no_stale():
    redis = FakeRedis()
    cfg = _cfg()
    svc = RecoveryService(redis, cfg)
    depth = await svc.dlq_depth()
    stale = await svc._find_stale_runs()
    assert len(stale) == 0
    assert depth == 0


# ---------------------------------------------------------------------------
# DLQ listing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dlq_list_returns_entries():
    redis = FakeRedis()
    cfg = _cfg()
    svc = RecoveryService(redis, cfg)
    await redis.hset(
        "hfa:cp:dlq:meta:r-dlq-1",
        mapping={
            "run_id": "r-dlq-1",
            "tenant_id": "t1",
            "reason": "max_reschedule_exceeded",
            "reschedule_count": "3",
            "dead_lettered_at": str(time.time()),
            "original_error": "",
            "cost_cents": "0",
        },
    )
    entries = await svc.list_dlq("t1")
    ids = {e["run_id"] for e in entries}
    assert "r-dlq-1" in ids


@pytest.mark.asyncio
async def test_dlq_list_all_tenants_sentinel():
    redis = FakeRedis()
    cfg = _cfg()
    svc = RecoveryService(redis, cfg)
    for i in range(3):
        await redis.hset(
            f"hfa:cp:dlq:meta:r-dlq-{i}",
            mapping={
                "run_id": f"r-dlq-{i}",
                "tenant_id": f"t{i}",
                "reason": "max_reschedule_exceeded",
                "reschedule_count": "3",
                "dead_lettered_at": str(time.time()),
                "original_error": "",
                "cost_cents": "0",
            },
        )
    entries = await svc.list_dlq("__all__")
    assert len(entries) == 3


@pytest.mark.asyncio
async def test_dlq_list_empty():
    redis = FakeRedis()
    svc = RecoveryService(redis, _cfg())
    assert await svc.list_dlq("t1") == []


@pytest.mark.asyncio
async def test_dlq_entry_response_model():
    resp = DLQEntryResponse(
        run_id="r1",
        tenant_id="t1",
        reason="max_reschedule_exceeded",
        delivery_count=3,
        dead_lettered_at=1700000000.0,
        original_error="",
        cost_cents=0,
    )
    assert resp.run_id == "r1"
    assert resp.delivery_count == 3


@pytest.mark.asyncio
async def test_dlq_list_response_model():
    entries = [
        DLQEntryResponse(
            run_id=f"r{i}",
            tenant_id="t1",
            reason="max_reschedule_exceeded",
            delivery_count=3,
            dead_lettered_at=1700000000.0,
            original_error="",
            cost_cents=0,
        )
        for i in range(3)
    ]
    resp = DLQListResponse(count=len(entries), entries=entries)
    assert resp.count == 3


# ---------------------------------------------------------------------------
# Sprint 10 semantics preserved
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reschedule_semantics_preserved():
    redis = FakeRedis()
    cfg = _cfg()
    svc = RecoveryService(redis, cfg)
    await _seed_stale(redis, "r-sem", rc=0)
    result = await svc._handle_stale("r-sem")
    assert result == "rescheduled"
    state = await redis.get("hfa:run:state:r-sem")
    assert state.decode() == "rescheduled"
    events = await redis.xrange(cfg.control_stream)
    types = [d.get(b"event_type", b"").decode() for _, d in events]
    assert "RunAdmitted" in types


@pytest.mark.asyncio
async def test_dlq_semantics_preserved():
    redis = FakeRedis()
    cfg = _cfg(max_reschedule_attempts=3)
    svc = RecoveryService(redis, cfg)
    await _seed_stale(redis, "r-dlq-sem", rc=3)
    result = await svc._handle_stale("r-dlq-sem")
    assert result == "dlq"
    state = await redis.get("hfa:run:state:r-dlq-sem")
    assert state.decode() == "dead_lettered"

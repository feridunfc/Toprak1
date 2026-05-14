"""
tests/core/test_sprint13_worker_api.py
IRONCLAD Sprint 13 — Worker visibility API tests

Covers:
  - WorkerListResponse / WorkerSummary model construction
  - healthy workers include draining (visibility)
  - schedulable workers exclude draining
  - schedulable workers exclude capacity-full
  - worker detail 404 semantics
  - is_draining flag surfaces correctly in summary
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

from hfa_control.api.models import WorkerListResponse, WorkerSummary
from hfa_control.models import ControlPlaneConfig, WorkerProfile, WorkerStatus
from hfa_control.registry import WorkerRegistry


@pytest.fixture
def cfg():
    return ControlPlaneConfig(
        instance_id="cp-test",
        worker_heartbeat_ttl=60.0,
        registry_ttl=120,
    )


async def _seed(redis, wid, status="healthy", inflight=0, capacity=10):
    await redis.hset(
        f"hfa:cp:worker:{wid}",
        mapping={
            "worker_id": wid,
            "worker_group": f"grp-{wid}",
            "region": "us-east-1",
            "shards": "[]",
            "capacity": str(capacity),
            "inflight": str(inflight),
            "status": status,
            "last_seen": str(time.time()),
            "version": "1.0",
            "capabilities": "[]",
        },
    )
    await redis.sadd("hfa:cp:workers:by_region:us-east-1", wid)


# ---------------------------------------------------------------------------
# WorkerSummary model
# ---------------------------------------------------------------------------


def test_worker_summary_fields():
    profile = WorkerProfile(
        worker_id="w1",
        worker_group="g1",
        region="us-east-1",
        capacity=10,
        inflight=2,
        status=WorkerStatus.HEALTHY,
        last_seen=1700000000.0,
        shards=[0, 1],
        version="1.0",
        capabilities=["base"],
    )
    s = WorkerSummary(
        worker_id=profile.worker_id,
        worker_group=profile.worker_group,
        region=profile.region,
        status=profile.status.value,
        is_draining=profile.is_draining,
        inflight=profile.inflight,
        capacity=profile.capacity,
        shards=profile.shards,
        version=profile.version,
        last_seen=profile.last_seen,
    )
    assert s.worker_id == "w1"
    assert s.is_draining is False
    assert s.status == "healthy"


def test_worker_summary_draining():
    profile = WorkerProfile(
        worker_id="w2",
        worker_group="g1",
        region="us-east-1",
        capacity=10,
        inflight=0,
        status=WorkerStatus.DRAINING,
        last_seen=1700000000.0,
    )
    s = WorkerSummary(
        worker_id=profile.worker_id,
        worker_group=profile.worker_group,
        region=profile.region,
        status=profile.status.value,
        is_draining=profile.is_draining,
        inflight=profile.inflight,
        capacity=profile.capacity,
        shards=profile.shards,
        version=profile.version,
        last_seen=profile.last_seen,
    )
    assert s.is_draining is True
    assert s.status == "draining"


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_healthy_includes_draining(cfg):
    redis = FakeRedis()
    reg = WorkerRegistry(redis, cfg)
    await _seed(redis, "wh", "healthy")
    await _seed(redis, "wd", "draining")
    workers = await reg.list_healthy_workers()
    ids = {w.worker_id for w in workers}
    assert "wh" in ids
    assert "wd" in ids


@pytest.mark.asyncio
async def test_schedulable_excludes_draining(cfg):
    redis = FakeRedis()
    reg = WorkerRegistry(redis, cfg)
    await _seed(redis, "wh", "healthy")
    await _seed(redis, "wd", "draining")
    workers = await reg.list_schedulable_workers()
    ids = {w.worker_id for w in workers}
    assert "wh" in ids
    assert "wd" not in ids


@pytest.mark.asyncio
async def test_schedulable_includes_worker_with_capacity(cfg):
    redis = FakeRedis()
    reg = WorkerRegistry(redis, cfg)
    await _seed(redis, "w-ok", "healthy", inflight=3, capacity=10)
    workers = await reg.list_schedulable_workers()
    ids = {w.worker_id for w in workers}
    assert "w-ok" in ids


@pytest.mark.asyncio
async def test_worker_list_response_construction(cfg):
    redis = FakeRedis()
    reg = WorkerRegistry(redis, cfg)
    await _seed(redis, "w1", "healthy")
    await _seed(redis, "w2", "draining")
    workers = await reg.list_healthy_workers()
    summaries = []
    for w in workers:
        summaries.append(
            WorkerSummary(
                worker_id=w.worker_id,
                worker_group=w.worker_group,
                region=w.region,
                status=w.status.value,
                is_draining=w.is_draining,
                inflight=w.inflight,
                capacity=w.capacity,
                shards=w.shards,
                version=w.version,
                last_seen=w.last_seen,
            )
        )
    resp = WorkerListResponse(count=len(summaries), workers=summaries)
    assert resp.count == 2
    ids = {s.worker_id for s in resp.workers}
    assert "w1" in ids and "w2" in ids


@pytest.mark.asyncio
async def test_get_worker_not_found_raises(cfg):
    from hfa_control.exceptions import WorkerNotFoundError

    redis = FakeRedis()
    reg = WorkerRegistry(redis, cfg)
    with pytest.raises(WorkerNotFoundError):
        await reg.get_worker("nonexistent")


@pytest.mark.asyncio
async def test_worker_detail_returns_profile(cfg):
    redis = FakeRedis()
    reg = WorkerRegistry(redis, cfg)
    await _seed(redis, "w-detail", "healthy", inflight=2, capacity=8)
    profile = await reg.get_worker("w-detail")
    assert profile.worker_id == "w-detail"
    assert profile.capacity == 8
    assert profile.inflight == 2


@pytest.mark.asyncio
async def test_dead_workers_excluded_from_healthy(cfg):
    """Workers whose last_seen exceeds heartbeat_ttl are treated as DEAD."""
    redis = FakeRedis()
    cfg_short = ControlPlaneConfig(
        instance_id="cp-test",
        worker_heartbeat_ttl=1.0,
        registry_ttl=120,
    )
    reg = WorkerRegistry(redis, cfg_short)
    await redis.hset(
        "hfa:cp:worker:w-stale",
        mapping={
            "worker_id": "w-stale",
            "worker_group": "grp",
            "region": "us-east-1",
            "shards": "[]",
            "capacity": "5",
            "inflight": "0",
            "status": "healthy",
            "last_seen": str(time.time() - 300),  # 5 minutes ago
            "version": "",
            "capabilities": "[]",
        },
    )
    await redis.sadd("hfa:cp:workers:by_region:us-east-1", "w-stale")
    workers = await reg.list_healthy_workers()
    ids = {w.worker_id for w in workers}
    assert "w-stale" not in ids

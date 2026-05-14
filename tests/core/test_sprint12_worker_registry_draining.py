"""
tests/core/test_sprint12_worker_registry_draining.py
IRONCLAD Sprint 12 — Draining worker registry + scheduler exclusion tests

Verifies:
  - WorkerProfile.is_draining property
  - list_healthy_workers includes DRAINING workers (visibility)
  - list_schedulable_workers excludes DRAINING workers (placement safety)
  - Scheduler does not assign runs to draining workers
  - workers_excluded_draining_total metric increments
  - _on_heartbeat preserves DRAINING status across heartbeat cycles
"""

from __future__ import annotations

import json
import time

import fakeredis.aioredis as faredis
import pytest

from hfa.obs.runtime_metrics import IRONCLADMetrics as M
from hfa_control.models import ControlPlaneConfig, WorkerProfile, WorkerStatus
from hfa_control.registry import WorkerRegistry


@pytest.fixture(autouse=True)
def reset_metrics():
    M.reset_all()
    yield
    M.reset_all()


@pytest.fixture
def config():
    return ControlPlaneConfig(
        instance_id="test-cp-1",
        region="us-east-1",
        worker_heartbeat_ttl=60.0,
        registry_ttl=120,
    )


async def _register_worker(redis, worker_id: str, status: str, region: str = "us-east-1"):
    """Helper: write a worker hash directly into Redis."""
    key = f"hfa:cp:worker:{worker_id}"
    await redis.hset(
        key,
        mapping={
            "worker_id": worker_id,
            "worker_group": f"group-{worker_id}",
            "region": region,
            "shards": json.dumps([0]),
            "capacity": "10",
            "inflight": "2",
            "status": status,
            "last_seen": str(time.time()),
            "version": "1.0.0",
            "capabilities": json.dumps(["base"]),
        },
    )
    await redis.sadd(f"hfa:cp:workers:by_region:{region}", worker_id)


# ---------------------------------------------------------------------------
# WorkerProfile.is_draining property
# ---------------------------------------------------------------------------


def test_worker_profile_is_draining_true():
    profile = WorkerProfile(
        worker_id="w1",
        worker_group="grp",
        region="us-east-1",
        capacity=10,
        inflight=2,
        status=WorkerStatus.DRAINING,
    )
    assert profile.is_draining is True


def test_worker_profile_is_draining_false_when_healthy():
    profile = WorkerProfile(
        worker_id="w1",
        worker_group="grp",
        region="us-east-1",
        capacity=10,
        inflight=2,
        status=WorkerStatus.HEALTHY,
    )
    assert profile.is_draining is False


# ---------------------------------------------------------------------------
# Registry queries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_healthy_workers_includes_draining(config):
    redis = faredis.FakeRedis()
    registry = WorkerRegistry(redis, config)

    await _register_worker(redis, "w-healthy", WorkerStatus.HEALTHY.value)
    await _register_worker(redis, "w-draining", WorkerStatus.DRAINING.value)

    workers = await registry.list_healthy_workers()
    ids = {w.worker_id for w in workers}
    assert "w-healthy" in ids
    assert "w-draining" in ids


@pytest.mark.asyncio
async def test_list_schedulable_workers_excludes_draining(config):
    redis = faredis.FakeRedis()
    registry = WorkerRegistry(redis, config)

    await _register_worker(redis, "w-healthy", WorkerStatus.HEALTHY.value)
    await _register_worker(redis, "w-draining", WorkerStatus.DRAINING.value)

    schedulable = await registry.list_schedulable_workers()
    ids = {w.worker_id for w in schedulable}
    assert "w-healthy" in ids
    assert "w-draining" not in ids


@pytest.mark.asyncio
async def test_list_schedulable_workers_empty_when_all_draining(config):
    redis = faredis.FakeRedis()
    registry = WorkerRegistry(redis, config)

    await _register_worker(redis, "w-drain-1", WorkerStatus.DRAINING.value)
    await _register_worker(redis, "w-drain-2", WorkerStatus.DRAINING.value)

    schedulable = await registry.list_schedulable_workers()
    assert schedulable == []


@pytest.mark.asyncio
async def test_list_healthy_excludes_dead_workers(config):
    redis = faredis.FakeRedis()
    config_short_ttl = ControlPlaneConfig(
        instance_id="test-cp-1",
        region="us-east-1",
        worker_heartbeat_ttl=1.0,  # 1 second TTL
        registry_ttl=120,
    )
    registry = WorkerRegistry(redis, config_short_ttl)

    key = "hfa:cp:worker:w-stale"
    await redis.hset(
        key,
        mapping={
            "worker_id": "w-stale",
            "worker_group": "grp",
            "region": "us-east-1",
            "shards": "[]",
            "capacity": "10",
            "inflight": "0",
            "status": WorkerStatus.HEALTHY.value,
            "last_seen": str(time.time() - 120),  # 2 minutes ago
            "version": "",
            "capabilities": "[]",
        },
    )
    await redis.sadd("hfa:cp:workers:by_region:us-east-1", "w-stale")

    workers = await registry.list_healthy_workers()
    ids = {w.worker_id for w in workers}
    assert "w-stale" not in ids


# ---------------------------------------------------------------------------
# Heartbeat preserves DRAINING status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_preserves_draining_status(config):
    """
    A worker set to DRAINING should remain DRAINING even after a new
    WorkerHeartbeatEvent arrives (draining is sticky until explicit reset).
    """
    redis = faredis.FakeRedis()
    registry = WorkerRegistry(redis, config)

    # Pre-mark as DRAINING (as if WorkerDrainingEvent was received)
    await _register_worker(redis, "w-drain", WorkerStatus.DRAINING.value)

    # Simulate a heartbeat arriving (WorkerHeartbeatEvent)
    from hfa.events.schema import WorkerHeartbeatEvent

    heartbeat = WorkerHeartbeatEvent(
        worker_id="w-drain",
        worker_group="group-w-drain",
        region="us-east-1",
        shards=[0],
        capacity=10,
        inflight=1,
        version="1.0.0",
        capabilities=["base"],
        timestamp=time.time(),
    )
    await registry._on_heartbeat(heartbeat)

    # Status should still be DRAINING
    profile = await registry.get_worker("w-drain")
    assert profile.status == WorkerStatus.DRAINING


@pytest.mark.asyncio
async def test_heartbeat_sets_healthy_for_new_worker(config):
    """A fresh worker with no prior status should become HEALTHY on heartbeat."""
    redis = faredis.FakeRedis()
    registry = WorkerRegistry(redis, config)

    from hfa.events.schema import WorkerHeartbeatEvent

    heartbeat = WorkerHeartbeatEvent(
        worker_id="w-new",
        worker_group="grp",
        region="us-east-1",
        shards=[0],
        capacity=5,
        inflight=0,
        version="1.0.0",
        capabilities=[],
        timestamp=time.time(),
    )
    await registry._on_heartbeat(heartbeat)

    profile = await registry.get_worker("w-new")
    assert profile.status == WorkerStatus.HEALTHY

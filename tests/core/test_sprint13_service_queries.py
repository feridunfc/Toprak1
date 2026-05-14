"""
tests/core/test_sprint13_service_queries.py
IRONCLAD Sprint 13 — ControlPlaneService query surface tests

Verifies service-layer methods that underpin all Sprint 13 API endpoints:
  - get_liveness / get_readiness
  - list_all_workers / healthy / schedulable
  - list_running_runs / get_run_state / get_run_claim / get_run_result
  - list_stale_runs / get_recovery_summary / list_dlq
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
from hfa_control.registry import WorkerRegistry
from hfa_control.recovery import RecoveryService
from hfa_control.service import ControlPlaneService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**kw) -> ControlPlaneConfig:
    defaults = dict(
        instance_id="test-cp-1",
        region="us-east-1",
        worker_heartbeat_ttl=60.0,
        registry_ttl=120,
        stale_run_timeout=5.0,
        max_reschedule_attempts=3,
        recovery_sweep_interval=999.0,
    )
    defaults.update(kw)
    return ControlPlaneConfig(**defaults)


async def _seed_worker(
    redis, wid: str, status: str = "healthy", inflight: int = 0, capacity: int = 10
) -> None:
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


async def _seed_run(
    redis, run_id: str, state: str = "running", tenant_id: str = "t1", age: float = 10.0
) -> None:
    from hfa.runtime.state_store import StateStore

    store = StateStore(redis)
    admitted = time.time() - age
    await store.create_run_meta(
        run_id,
        {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": "base",
            "worker_group": "grp-w1",
            "shard": "0",
            "reschedule_count": "0",
            "admitted_at": str(admitted),
            "state": state,
        },
    )
    await redis.set(f"hfa:run:state:{run_id}", state, ex=86400)
    if state == "running":
        await redis.zadd("hfa:cp:running", {run_id: admitted})


# ---------------------------------------------------------------------------
# Liveness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_liveness_returns_alive():
    redis = FakeRedis()
    cfg = _make_config()
    # Build a minimal service without starting background tasks
    svc = _make_service(redis, cfg)
    data = await svc.get_liveness()
    assert data["status"] == "alive"
    assert data["service"] == "hfa-control"
    assert data["instance_id"] == "test-cp-1"


# ---------------------------------------------------------------------------
# Readiness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_readiness_ready_when_redis_ok():
    redis = FakeRedis()
    cfg = _make_config()
    svc = _make_service(redis, cfg)
    data = await svc.get_readiness()
    assert data["status"] == "ready"
    assert data["checks"]["redis"]["ok"] is True


@pytest.mark.asyncio
async def test_get_readiness_reports_leader_flag():
    redis = FakeRedis()
    cfg = _make_config()
    svc = _make_service(redis, cfg)
    data = await svc.get_readiness()
    assert "is_leader" in data
    assert isinstance(data["is_leader"], bool)


# ---------------------------------------------------------------------------
# Worker queries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_all_workers_includes_draining():
    redis = FakeRedis()
    cfg = _make_config()
    await _seed_worker(redis, "w-healthy", "healthy")
    await _seed_worker(redis, "w-draining", "draining")
    svc = _make_service(redis, cfg)
    workers = await svc.list_all_workers()
    ids = {w.worker_id for w in workers}
    assert "w-healthy" in ids
    assert "w-draining" in ids


@pytest.mark.asyncio
async def test_list_schedulable_excludes_draining():
    redis = FakeRedis()
    cfg = _make_config()
    await _seed_worker(redis, "w-healthy", "healthy")
    await _seed_worker(redis, "w-draining", "draining")
    svc = _make_service(redis, cfg)
    workers = await svc.list_schedulable_workers()
    ids = {w.worker_id for w in workers}
    assert "w-healthy" in ids
    assert "w-draining" not in ids


@pytest.mark.asyncio
async def test_list_schedulable_empty_when_all_draining():
    redis = FakeRedis()
    cfg = _make_config()
    await _seed_worker(redis, "d1", "draining")
    await _seed_worker(redis, "d2", "draining")
    svc = _make_service(redis, cfg)
    assert await svc.list_schedulable_workers() == []


@pytest.mark.asyncio
async def test_get_worker_returns_profile():
    redis = FakeRedis()
    cfg = _make_config()
    await _seed_worker(redis, "w1", "healthy")
    svc = _make_service(redis, cfg)
    profile = await svc.get_worker("w1")
    assert profile.worker_id == "w1"


@pytest.mark.asyncio
async def test_get_worker_raises_on_missing():
    from hfa_control.exceptions import WorkerNotFoundError

    redis = FakeRedis()
    svc = _make_service(redis, _make_config())
    with pytest.raises(WorkerNotFoundError):
        await svc.get_worker("nonexistent")


# ---------------------------------------------------------------------------
# Run queries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_running_runs_returns_entries():
    redis = FakeRedis()
    await _seed_run(redis, "r1", state="running")
    await _seed_run(redis, "r2", state="running")
    svc = _make_service(redis, _make_config())
    runs = await svc.list_running_runs(limit=10)
    ids = {r["run_id"] for r in runs}
    assert "r1" in ids and "r2" in ids


@pytest.mark.asyncio
async def test_list_running_runs_empty():
    redis = FakeRedis()
    svc = _make_service(redis, _make_config())
    assert await svc.list_running_runs() == []


@pytest.mark.asyncio
async def test_get_run_state_returns_state():
    redis = FakeRedis()
    await _seed_run(redis, "r1", state="running", tenant_id="t1")
    svc = _make_service(redis, _make_config())
    data = await svc.get_run_state("r1")
    assert data["run_id"] == "r1"
    assert data["state"] == "running"
    assert data["tenant_id"] == "t1"


@pytest.mark.asyncio
async def test_get_run_state_unknown_for_missing():
    redis = FakeRedis()
    svc = _make_service(redis, _make_config())
    data = await svc.get_run_state("nonexistent")
    assert data["state"] == "unknown"


@pytest.mark.asyncio
async def test_get_run_claim_returns_owner():
    from hfa.runtime.state_store import StateStore

    redis = FakeRedis()
    store = StateStore(redis)
    await store.claim_execution("r1", "w1")
    svc = _make_service(redis, _make_config())
    data = await svc.get_run_claim("r1")
    assert data["claimed"] is True
    assert data["owner"] == "w1"
    assert data["ttl_seconds"] > 0


@pytest.mark.asyncio
async def test_get_run_claim_unclaimed():
    redis = FakeRedis()
    svc = _make_service(redis, _make_config())
    data = await svc.get_run_claim("r-no-claim")
    assert data["claimed"] is False
    assert data["owner"] is None


@pytest.mark.asyncio
async def test_get_run_result_returns_result():
    from hfa.runtime.state_store import StateStore

    redis = FakeRedis()
    store = StateStore(redis)
    await store.store_result("r1", "t1", "done", {"out": 1}, cost_cents=5, tokens_used=10)
    svc = _make_service(redis, _make_config())
    result = await svc.get_run_result("r1")
    assert result is not None
    assert result["status"] == "done"
    assert result["cost_cents"] == 5


@pytest.mark.asyncio
async def test_get_run_result_none_for_missing():
    redis = FakeRedis()
    svc = _make_service(redis, _make_config())
    assert await svc.get_run_result("nonexistent") is None


# ---------------------------------------------------------------------------
# Recovery queries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_stale_runs_detects_stale():
    redis = FakeRedis()
    cfg = _make_config(stale_run_timeout=5.0)
    # Seed a run that is 120s old (older than 5s threshold)
    await _seed_run(redis, "r-stale", state="running", age=120.0)
    svc = _make_service(redis, cfg)
    stale = await svc.list_stale_runs()
    ids = {r["run_id"] for r in stale}
    assert "r-stale" in ids


@pytest.mark.asyncio
async def test_list_stale_runs_empty_for_fresh():
    redis = FakeRedis()
    cfg = _make_config(stale_run_timeout=600.0)
    await _seed_run(redis, "r-fresh", state="running", age=5.0)
    svc = _make_service(redis, cfg)
    stale = await svc.list_stale_runs()
    ids = {r["run_id"] for r in stale}
    assert "r-fresh" not in ids


@pytest.mark.asyncio
async def test_get_recovery_summary_fields():
    redis = FakeRedis()
    cfg = _make_config()
    svc = _make_service(redis, cfg)
    summary = await svc.get_recovery_summary()
    assert "stale_count" in summary
    assert "dlq_count" in summary
    assert "schedulable_workers" in summary
    assert "draining_workers" in summary


@pytest.mark.asyncio
async def test_get_recovery_summary_counts_draining():
    redis = FakeRedis()
    cfg = _make_config()
    await _seed_worker(redis, "w-sched", "healthy")
    await _seed_worker(redis, "w-drain", "draining")
    svc = _make_service(redis, cfg)
    summary = await svc.get_recovery_summary()
    assert summary["schedulable_workers"] >= 1
    assert summary["draining_workers"] >= 1


@pytest.mark.asyncio
async def test_list_dlq_returns_entries():
    redis = FakeRedis()
    cfg = _make_config()
    # Seed a DLQ entry directly
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
    svc = _make_service(redis, cfg)
    entries = await svc.list_dlq(tenant_id="", limit=50)
    ids = {e["run_id"] for e in entries}
    assert "r-dlq-1" in ids


@pytest.mark.asyncio
async def test_list_dlq_empty_when_none():
    redis = FakeRedis()
    svc = _make_service(redis, _make_config())
    assert await svc.list_dlq() == []


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_preserves_worker_profile_backward_compat():
    """WorkerProfile from_redis_hash with missing fields uses safe defaults."""
    redis = FakeRedis()
    cfg = _make_config()
    # Seed a minimal worker hash (missing capabilities, version)
    await redis.hset(
        "hfa:cp:worker:w-minimal",
        mapping={
            "worker_id": "w-minimal",
            "worker_group": "grp",
            "region": "us-east-1",
            "capacity": "5",
            "inflight": "0",
            "status": "healthy",
            "last_seen": str(time.time()),
            "shards": "[]",
        },
    )
    await redis.sadd("hfa:cp:workers:by_region:us-east-1", "w-minimal")
    svc = _make_service(redis, cfg)
    workers = await svc.list_all_workers()
    ids = {w.worker_id for w in workers}
    assert "w-minimal" in ids
    profile = next(w for w in workers if w.worker_id == "w-minimal")
    assert profile.version == ""
    assert profile.capabilities == []


# ---------------------------------------------------------------------------
# Fixture helper
# ---------------------------------------------------------------------------


def _make_service(redis, cfg: ControlPlaneConfig) -> ControlPlaneService:
    """Build a ControlPlaneService with injected redis without starting background tasks."""
    svc = object.__new__(ControlPlaneService)
    svc._redis = redis
    svc._config = cfg
    from hfa_control.leader import LeaderElection
    from hfa_control.shard import ShardOwnershipManager
    from hfa_control.admission import AdmissionController
    from hfa_control.scheduler import Scheduler

    svc._leader = LeaderElection(redis, cfg.instance_id, cfg)
    svc._registry = WorkerRegistry(redis, cfg)
    svc._shards = ShardOwnershipManager(redis, cfg)
    svc._admitter = AdmissionController(redis, cfg)
    svc._scheduler = Scheduler(redis, svc._registry, svc._shards, cfg)
    svc._recovery = RecoveryService(redis, cfg)
    svc._leader_task = None
    svc._sched_started = False
    svc._recovery_started = False
    return svc

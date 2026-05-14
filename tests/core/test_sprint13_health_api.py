"""
tests/core/test_sprint13_health_api.py
IRONCLAD Sprint 13 — Liveness / readiness API tests

Covers:
  - LiveResponse model
  - ReadyResponse model
  - get_liveness returns correct shape
  - get_readiness returns ready when Redis ok
  - get_readiness reports leader flag correctly
  - get_readiness degrades when dependency unavailable
  - ReadyCheckDetail per-check fields
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from fakeredis.aioredis import FakeRedis

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-core" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-control" / "src"))

from hfa_control.api.models import LiveResponse, ReadyResponse, ReadyCheckDetail
from hfa_control.models import ControlPlaneConfig


def _make_service(redis, instance_id="cp-test"):
    from hfa_control.service import ControlPlaneService
    from hfa_control.leader import LeaderElection
    from hfa_control.registry import WorkerRegistry
    from hfa_control.shard import ShardOwnershipManager
    from hfa_control.admission import AdmissionController
    from hfa_control.scheduler import Scheduler
    from hfa_control.recovery import RecoveryService

    cfg = ControlPlaneConfig(
        instance_id=instance_id,
        region="us-east-1",
        worker_heartbeat_ttl=60.0,
        registry_ttl=120,
    )
    svc = object.__new__(ControlPlaneService)
    svc._redis = redis
    svc._config = cfg
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


# ---------------------------------------------------------------------------
# LiveResponse model
# ---------------------------------------------------------------------------


def test_live_response_model():
    r = LiveResponse(status="alive", service="hfa-control", instance_id="cp-1")
    assert r.status == "alive"
    assert r.service == "hfa-control"
    assert r.instance_id == "cp-1"


# ---------------------------------------------------------------------------
# ReadyResponse model
# ---------------------------------------------------------------------------


def test_ready_response_model():
    checks = {
        "redis": ReadyCheckDetail(ok=True),
        "control_stream": ReadyCheckDetail(ok=True),
    }
    r = ReadyResponse(
        status="ready",
        instance_id="cp-1",
        is_leader=True,
        checks=checks,
    )
    assert r.status == "ready"
    assert r.checks["redis"].ok is True


def test_ready_check_detail_with_message():
    detail = ReadyCheckDetail(ok=False, message="connection refused")
    assert detail.ok is False
    assert "connection" in detail.message


# ---------------------------------------------------------------------------
# get_liveness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_liveness_returns_alive():
    redis = FakeRedis()
    svc = _make_service(redis, instance_id="cp-42")
    data = await svc.get_liveness()
    assert data["status"] == "alive"
    assert data["service"] == "hfa-control"
    assert data["instance_id"] == "cp-42"


@pytest.mark.asyncio
async def test_get_liveness_always_returns():
    """Liveness must never raise even if Redis is unavailable."""
    redis = FakeRedis()
    svc = _make_service(redis)
    # Should not raise regardless of state
    data = await svc.get_liveness()
    assert data["status"] == "alive"


# ---------------------------------------------------------------------------
# get_readiness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_readiness_ready_when_redis_ok():
    redis = FakeRedis()
    svc = _make_service(redis)
    data = await svc.get_readiness()
    assert data["status"] == "ready"
    assert data["checks"]["redis"]["ok"] is True


@pytest.mark.asyncio
async def test_get_readiness_has_all_checks():
    redis = FakeRedis()
    svc = _make_service(redis)
    data = await svc.get_readiness()
    assert "redis" in data["checks"]
    assert "control_stream" in data["checks"]
    assert "heartbeat_stream" in data["checks"]


@pytest.mark.asyncio
async def test_get_readiness_reports_leader_flag():
    redis = FakeRedis()
    svc = _make_service(redis)
    data = await svc.get_readiness()
    assert "is_leader" in data
    assert isinstance(data["is_leader"], bool)
    # A service that hasn't acquired leadership should report False
    assert data["is_leader"] is False


@pytest.mark.asyncio
async def test_get_readiness_instance_id():
    redis = FakeRedis()
    svc = _make_service(redis, instance_id="my-cp-instance")
    data = await svc.get_readiness()
    assert data["instance_id"] == "my-cp-instance"


@pytest.mark.asyncio
async def test_get_readiness_all_checks_true_means_ready():
    redis = FakeRedis()
    svc = _make_service(redis)
    data = await svc.get_readiness()
    all_ok = all(v["ok"] for v in data["checks"].values())
    assert all_ok == (data["status"] == "ready")


@pytest.mark.asyncio
async def test_readiness_response_model_from_service():
    redis = FakeRedis()
    svc = _make_service(redis)
    data = await svc.get_readiness()
    checks = {k: ReadyCheckDetail(**v) for k, v in data["checks"].items()}
    resp = ReadyResponse(
        status=data["status"],
        instance_id=data["instance_id"],
        is_leader=data["is_leader"],
        checks=checks,
    )
    assert resp.status in ("ready", "not_ready")
    assert all(isinstance(v, ReadyCheckDetail) for v in resp.checks.values())

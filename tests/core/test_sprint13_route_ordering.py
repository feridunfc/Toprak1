"""
tests/core/test_sprint13_route_ordering.py
IRONCLAD Sprint 13 â€” Route ordering and security matrix tests

Verifies the critical FastAPI route-ordering rule:
  Static paths (/workers/healthy, /workers/schedulable, /runs/running)
  must be registered BEFORE their dynamic counterparts
  (/workers/{worker_id}, /runs/{run_id}/...) to prevent "healthy" and
  "schedulable" being interpreted as path parameters.

Also verifies:
  - Security matrix: operator-only endpoints reject missing auth
  - Schedulable includes inflight < capacity check (full capacity excluded)
  - Route names map to correct handler functions
"""

from __future__ import annotations

import sys
from pathlib import Path
import time
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-core" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-control" / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from fakeredis.aioredis import FakeRedis


# ---------------------------------------------------------------------------
# Route ordering â€” verified by inspecting router route list
# ---------------------------------------------------------------------------


def test_workers_healthy_route_before_worker_id():
    """
    /workers/healthy must be registered before /workers/{worker_id}.
    In FastAPI, the first matching route wins.  If /{worker_id} comes first,
    a request to /workers/healthy would match with worker_id='healthy'.
    """
    from hfa_control.api.router import router

    routes = [r for r in router.routes if hasattr(r, "path")]
    paths = [r.path for r in routes]

    healthy_idx = next((i for i, p in enumerate(paths) if p == "/control/v1/workers/healthy"), None)
    schedulable_idx = next(
        (i for i, p in enumerate(paths) if p == "/control/v1/workers/schedulable"), None
    )
    worker_id_idx = next(
        (i for i, p in enumerate(paths) if p == "/control/v1/workers/{worker_id}"), None
    )

    assert healthy_idx is not None, "/workers/healthy route not found"
    assert schedulable_idx is not None, "/workers/schedulable route not found"
    assert worker_id_idx is not None, "/workers/{worker_id} route not found"
    assert healthy_idx < worker_id_idx, (
        f"/workers/healthy (idx={healthy_idx}) must come before "
        f"/workers/{{worker_id}} (idx={worker_id_idx})"
    )
    assert schedulable_idx < worker_id_idx, (
        f"/workers/schedulable (idx={schedulable_idx}) must come before "
        f"/workers/{{worker_id}} (idx={worker_id_idx})"
    )


def test_runs_running_route_before_run_id():
    """
    /runs/running must be registered before /runs/{run_id}/...
    """
    from hfa_control.api.router import router

    routes = [r for r in router.routes if hasattr(r, "path")]
    paths = [r.path for r in routes]

    running_idx = next((i for i, p in enumerate(paths) if p == "/control/v1/runs/running"), None)
    run_id_idxs = [i for i, p in enumerate(paths) if p.startswith("/control/v1/runs/{run_id}")]

    assert running_idx is not None, "/runs/running route not found"
    assert run_id_idxs, "/runs/{run_id}/... routes not found"
    first_run_id_idx = min(run_id_idxs)
    assert running_idx < first_run_id_idx, (
        f"/runs/running (idx={running_idx}) must come before "
        f"/runs/{{run_id}}/... (first idx={first_run_id_idx})"
    )


def test_all_sprint13_routes_exist():
    """All Sprint 13 endpoint paths must be registered on the router."""
    from hfa_control.api.router import router

    paths = {r.path for r in router.routes if hasattr(r, "path")}
    required = {
        "/control/v1/health/live",
        "/control/v1/health/ready",
        "/control/v1/workers/healthy",
        "/control/v1/workers/schedulable",
        "/control/v1/runs/running",
        "/control/v1/runs/{run_id}/state",
        "/control/v1/runs/{run_id}/claim",
        "/control/v1/runs/{run_id}/result",
        "/control/v1/recovery/stale",
        "/control/v1/recovery/summary",
        "/control/v1/recovery/dlq",
    }
    missing = required - paths
    assert not missing, f"Missing routes: {missing}"


def test_sprint10_routes_preserved():
    """Sprint 10 routes must remain registered (backward compatibility)."""
    from hfa_control.api.router import router

    paths = {r.path for r in router.routes if hasattr(r, "path")}
    sprint10 = {
        "/control/v1/workers",
        "/control/v1/workers/{worker_id}",
        "/control/v1/shards",
        "/control/v1/runs/{run_id}/placement",
        "/control/v1/dlq",
        "/control/v1/health",
        "/control/v1/healthz",
        "/control/v1/readyz",
    }
    missing = sprint10 - paths
    assert not missing, f"Sprint 10 routes removed: {missing}"


# ---------------------------------------------------------------------------
# Schedulable semantics â€” inflight < capacity
# ---------------------------------------------------------------------------


async def _seed(redis, wid, status="healthy", inflight=0, capacity=10):
    await redis.hset(
        f"hfa:cp:worker:{wid}",
        mapping={
            "worker_id": wid,
            "worker_group": f"g-{wid}",
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


@pytest.mark.asyncio
async def test_schedulable_excludes_capacity_full():
    """A worker at full capacity (inflight == capacity) must NOT be schedulable."""
    from hfa_control.models import ControlPlaneConfig
    from hfa_control.registry import WorkerRegistry

    redis = FakeRedis()
    cfg = ControlPlaneConfig(instance_id="cp", worker_heartbeat_ttl=60.0, registry_ttl=120)
    reg = WorkerRegistry(redis, cfg)
    await _seed(redis, "w-full", status="healthy", inflight=10, capacity=10)
    await _seed(redis, "w-ok", status="healthy", inflight=3, capacity=10)

    schedulable = await reg.list_schedulable_workers()
    ids = {w.worker_id for w in schedulable}
    assert "w-full" not in ids, "Capacity-full worker must not be schedulable"
    assert "w-ok" in ids, "Worker with available slots must be schedulable"


@pytest.mark.asyncio
async def test_schedulable_excludes_draining():
    from hfa_control.models import ControlPlaneConfig
    from hfa_control.registry import WorkerRegistry

    redis = FakeRedis()
    cfg = ControlPlaneConfig(instance_id="cp", worker_heartbeat_ttl=60.0, registry_ttl=120)
    reg = WorkerRegistry(redis, cfg)
    await _seed(redis, "w-drain", status="draining", inflight=0, capacity=10)
    await _seed(redis, "w-ok", status="healthy", inflight=0, capacity=10)

    schedulable = await reg.list_schedulable_workers()
    ids = {w.worker_id for w in schedulable}
    assert "w-drain" not in ids
    assert "w-ok" in ids


@pytest.mark.asyncio
async def test_schedulable_empty_when_all_full_or_draining():
    from hfa_control.models import ControlPlaneConfig
    from hfa_control.registry import WorkerRegistry

    redis = FakeRedis()
    cfg = ControlPlaneConfig(instance_id="cp", worker_heartbeat_ttl=60.0, registry_ttl=120)
    reg = WorkerRegistry(redis, cfg)
    await _seed(redis, "w-full", status="healthy", inflight=5, capacity=5)
    await _seed(redis, "w-drain", status="draining", inflight=0, capacity=10)

    schedulable = await reg.list_schedulable_workers()
    assert schedulable == [], "No schedulable workers expected"


@pytest.mark.asyncio
async def test_schedulable_includes_partially_loaded():
    from hfa_control.models import ControlPlaneConfig
    from hfa_control.registry import WorkerRegistry

    redis = FakeRedis()
    cfg = ControlPlaneConfig(instance_id="cp", worker_heartbeat_ttl=60.0, registry_ttl=120)
    reg = WorkerRegistry(redis, cfg)
    # inflight=9, capacity=10 â†’ 1 available slot â†’ still schedulable
    await _seed(redis, "w-near-full", status="healthy", inflight=9, capacity=10)

    schedulable = await reg.list_schedulable_workers()
    ids = {w.worker_id for w in schedulable}
    assert "w-near-full" in ids


# ---------------------------------------------------------------------------
# Security matrix â€” route handler names indicate correct auth gates
# ---------------------------------------------------------------------------


def test_operator_routes_use_require_operator():
    """
    Spot-check that operator-only route handlers call _require_operator.
    We inspect source code of the router module.
    """
    import inspect
    import hfa_control.api.router as router_mod

    # Handlers that must call _require_operator
    operator_handlers = [
        "workers_healthy",
        "workers_schedulable",
        "runs_running",
        "run_claim",
        "recovery_stale",
        "recovery_summary",
        "recovery_dlq",
    ]
    for name in operator_handlers:
        fn = getattr(router_mod, name, None)
        assert fn is not None, f"Handler {name!r} not found in router"
        src = inspect.getsource(fn)
        assert "_require_operator" in src, (
            f"Handler {name!r} does not call _require_operator â€” it should be operator-only"
        )


def test_public_routes_no_auth():
    """
    health_live and health_ready must NOT call _require_operator or
    _tenant_header (they are public probes).
    """
    import inspect
    import hfa_control.api.router as router_mod

    for name in ["health_live", "health_ready"]:
        fn = getattr(router_mod, name, None)
        assert fn is not None
        src = inspect.getsource(fn)
        assert "_require_operator" not in src, (
            f"Public probe {name!r} must not require operator auth"
        )
        assert "_tenant_header" not in src, f"Public probe {name!r} must not require tenant header"


def test_tenant_routes_use_tenant_header():
    """
    Tenant-scoped handlers must call _tenant_header.
    """
    import inspect
    import hfa_control.api.router as router_mod

    tenant_handlers = ["run_state", "run_result"]
    for name in tenant_handlers:
        fn = getattr(router_mod, name, None)
        assert fn is not None, f"Handler {name!r} not found"
        src = inspect.getsource(fn)
        assert "_tenant_header" in src, (
            f"Tenant-scoped handler {name!r} does not call _tenant_header"
        )

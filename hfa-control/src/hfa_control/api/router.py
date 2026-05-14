"""
hfa-control/src/hfa_control/api/router.py
IRONCLAD Sprint 10 / 12 / 13 — Control Plane REST API

Route ordering rule (FastAPI):
  Static paths MUST be defined before path-parameter routes at the
  same prefix level.  /workers/healthy must come before /workers/{worker_id}.

Security matrix (Sprint 13):
  Public (no auth):
    GET /health/live
    GET /health/ready

  Operator-only (X-CP-Auth header):
    GET /workers/healthy
    GET /workers/schedulable
    GET /runs/running
    GET /runs/{run_id}/claim
    GET /recovery/stale
    GET /recovery/summary
    GET /recovery/dlq
    POST /workers/{worker_id}/drain
    POST /runs/{run_id}/reschedule
    POST/DELETE /dlq/*

  Tenant-scoped (X-Tenant-ID header, ownership check):
    GET /workers                       (tenant sees fleet; no sensitive detail)
    GET /workers/{worker_id}
    GET /runs/{run_id}/state
    GET /runs/{run_id}/result
    GET /runs/{run_id}/placement       (Sprint 10)
    GET /dlq                           (Sprint 10 — tenant-filtered)
    GET /shards                        (Sprint 10)
    GET /health                        (Sprint 10)

IRONCLAD rules
--------------
* No print() — logging only.
* Tenant cross-check on tenant-scoped endpoints.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from hfa.config.keys import RedisKey
from hfa.events.schema import WorkerDrainingEvent
from hfa.events.codec import serialize_event
from hfa_control.exceptions import (
    WorkerNotFoundError,
    DLQEntryNotFoundError,
    TenantMismatchError,
    LeadershipError,
)
from hfa_control.api.models import (
    WorkerResponse,
    ShardResponse,
    PlacementResponse,
    DLQEntryResponse,
    HealthResponse,
    LiveResponse,
    ReadyResponse,
    ReadyCheckDetail,
    WorkerSummary,
    WorkerListResponse,
    RunStateResponse,
    RunClaimResponse,
    RunResultResponse,
    RunningRunSummary,
    RunningRunsResponse,
    StaleRunSummary,
    StaleRunsResponse,
    RecoverySummaryResponse,
    DLQListResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/control/v1", tags=["control-plane"])

from hfa_control.auth import require_operator, require_tenant  # noqa: E402

# ---------------------------------------------------------------------------
# Backward-compat aliases (old names still used throughout this file)
# ---------------------------------------------------------------------------


def _require_operator(x_cp_auth: str = "") -> None:
    """Delegate to auth module — constant-time HMAC validation."""
    require_operator(x_cp_auth)


def _tenant_header(x_tenant_id: str) -> str:
    """Delegate to auth module."""
    return require_tenant(x_tenant_id)


# ===========================================================================
# Sprint 13 — Liveness / Readiness (PUBLIC — no auth)
# ===========================================================================


@router.get("/health/live", response_model=LiveResponse)
async def health_live(request: Request) -> LiveResponse:
    """Liveness probe — always 200 if the process is alive."""
    data = await request.app.state.cp.get_liveness()
    return LiveResponse(**data)


@router.get("/health/ready", response_model=ReadyResponse)
async def health_ready(request: Request) -> ReadyResponse:
    """Readiness probe — 200 when all critical dependencies are reachable."""
    data = await request.app.state.cp.get_readiness()
    checks = {k: ReadyCheckDetail(**v) for k, v in data["checks"].items()}
    resp = ReadyResponse(
        status=data["status"],
        instance_id=data["instance_id"],
        is_leader=data["is_leader"],
        checks=checks,
    )
    if data["status"] != "ready":
        return JSONResponse(status_code=503, content=resp.model_dump())
    return resp


# ===========================================================================
# Sprint 13 — Worker visibility
# IMPORTANT: /workers/healthy and /workers/schedulable MUST be declared
# before /workers/{worker_id} so FastAPI does not treat "healthy" / "schedulable"
# as a worker_id path parameter.
# ===========================================================================


@router.get("/workers/healthy", response_model=WorkerListResponse)
async def workers_healthy(
    request: Request,
    x_cp_auth: str = Header(default=""),
) -> WorkerListResponse:
    """
    Operator-only.
    Returns all alive workers (HEALTHY + DRAINING, not DEAD).
    Useful for fleet visibility including workers in wind-down state.
    """
    _require_operator(x_cp_auth)
    workers = await request.app.state.cp.list_healthy_workers()
    return WorkerListResponse(
        count=len(workers),
        workers=[_worker_summary(w) for w in workers],
    )


@router.get("/workers/schedulable", response_model=WorkerListResponse)
async def workers_schedulable(
    request: Request,
    x_cp_auth: str = Header(default=""),
) -> WorkerListResponse:
    """
    Operator-only.
    Returns workers eligible to receive new runs:
      - status HEALTHY
      - not draining
      - inflight < capacity (available_slots > 0)
    """
    _require_operator(x_cp_auth)
    workers = await request.app.state.cp.list_schedulable_workers()
    return WorkerListResponse(
        count=len(workers),
        workers=[_worker_summary(w) for w in workers],
    )


# ===========================================================================
# Sprint 10 — Workers (preserved; dynamic route AFTER static routes above)
# ===========================================================================


@router.get("/workers", response_model=List[WorkerResponse])
async def list_workers(
    request: Request,
    x_tenant_id: str = Header(...),
) -> List[WorkerResponse]:
    _tenant_header(x_tenant_id)
    workers = await request.app.state.cp.registry.list_healthy_workers()
    return [_worker_resp(w) for w in workers]


@router.get("/workers/{worker_id}", response_model=WorkerResponse)
async def get_worker(
    worker_id: str,
    request: Request,
    x_tenant_id: str = Header(...),
) -> WorkerResponse:
    _tenant_header(x_tenant_id)
    try:
        profile = await request.app.state.cp.registry.get_worker(worker_id)
        return _worker_resp(profile)
    except WorkerNotFoundError:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id!r} not found")


@router.post("/workers/{worker_id}/drain")
async def drain_worker(
    worker_id: str,
    request: Request,
    x_tenant_id: str = Header(...),
    x_cp_auth: str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    try:
        profile = await request.app.state.cp.registry.get_worker(worker_id)
    except WorkerNotFoundError:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id!r} not found")
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat()
    evt = WorkerDrainingEvent(
        worker_id=worker_id,
        worker_group=profile.worker_group,
        region=profile.region,
        drain_deadline_utc=deadline,
        reason="manual",
    )
    cfg = request.app.state.cp._config
    await request.app.state.redis.xadd(
        cfg.heartbeat_stream, serialize_event(evt), maxlen=10_000, approximate=True
    )
    logger.info("Drain initiated: worker=%s deadline=%s", worker_id, deadline)
    audit = getattr(request.app.state.cp, "_audit", None)
    if audit:
        await audit.drain_started(worker_id=worker_id, operator="operator")
    return {
        "worker_id": worker_id,
        "drain_deadline_utc": deadline,
        "status": "draining",
    }


# ===========================================================================
# Sprint 10 — Shards (preserved)
# ===========================================================================


@router.get("/shards", response_model=List[ShardResponse])
async def list_shards(
    request: Request,
    x_tenant_id: str = Header(...),
) -> List[ShardResponse]:
    _tenant_header(x_tenant_id)
    redis = request.app.state.redis
    shard_mgr = request.app.state.cp.shards
    owners = await shard_mgr.all_owners()
    result = []
    for shard, group in sorted(owners.items()):
        try:
            stream_len = await redis.xlen(RedisKey.stream_shard(shard))
        except Exception:
            stream_len = -1
        alive = bool(await redis.exists(RedisKey.cp_shard_owner(shard)))
        result.append(
            ShardResponse(
                shard=shard,
                worker_group=group,
                stream_len=stream_len,
                owner_alive=alive,
            )
        )
    return result


# ===========================================================================
# Sprint 13 — Run visibility
# Static paths (/runs/running) BEFORE dynamic (/runs/{run_id}/...)
# ===========================================================================


@router.get("/runs/running", response_model=RunningRunsResponse)
async def runs_running(
    request: Request,
    x_cp_auth: str = Header(default=""),
    limit: int = 100,
) -> RunningRunsResponse:
    """
    Operator-only.
    Lists runs currently tracked in the running ZSET with claim ownership.
    """
    _require_operator(x_cp_auth)
    runs = await request.app.state.cp.list_running_runs(limit=limit)
    summaries = [
        RunningRunSummary(
            run_id=r["run_id"],
            tenant_id=r["tenant_id"],
            state=r["state"],
            worker_group=r["worker_group"],
            shard=r["shard"],
            started_at=r["started_at"],
            claim_owner=r.get("claim_owner"),
        )
        for r in runs
    ]
    return RunningRunsResponse(count=len(summaries), runs=summaries)


@router.get("/runs/{run_id}/state", response_model=RunStateResponse)
async def run_state(
    run_id: str,
    request: Request,
    x_tenant_id: str = Header(...),
) -> RunStateResponse:
    """Tenant-scoped. Returns state and metadata for a run."""
    _tenant_header(x_tenant_id)
    data = await request.app.state.cp.get_run_state(run_id)
    if data["state"] == "unknown" and not data["tenant_id"]:
        raise HTTPException(status_code=404, detail=f"Run {run_id!r} not found")
    if data["tenant_id"] and data["tenant_id"] != x_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch")
    return RunStateResponse(**data)


@router.get("/runs/{run_id}/claim", response_model=RunClaimResponse)
async def run_claim(
    run_id: str,
    request: Request,
    x_cp_auth: str = Header(default=""),
) -> RunClaimResponse:
    """
    Operator-only.
    Returns claim ownership and TTL. Exposes internal execution locking detail.
    """
    _require_operator(x_cp_auth)
    data = await request.app.state.cp.get_run_claim(run_id)
    return RunClaimResponse(**data)


@router.get("/runs/{run_id}/result", response_model=RunResultResponse)
async def run_result(
    run_id: str,
    request: Request,
    x_tenant_id: str = Header(...),
) -> RunResultResponse:
    """Tenant-scoped. Returns stored result for a completed run."""
    _tenant_header(x_tenant_id)
    result = await request.app.state.cp.get_run_result(run_id)
    if result is None:
        raise HTTPException(
            status_code=404, detail=f"No result found for run {run_id!r}"
        )
    if result.get("tenant_id") and result["tenant_id"] != x_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch")
    return RunResultResponse(
        run_id=result.get("run_id", run_id),
        tenant_id=result.get("tenant_id", ""),
        status=result.get("status", ""),
        cost_cents=int(result.get("cost_cents", 0)),
        tokens_used=int(result.get("tokens_used", 0)),
        error=result.get("error"),
        payload=result.get("payload", {}),
        completed_at=float(result.get("completed_at", 0)),
    )


# ===========================================================================
# Sprint 10 — Run placement (preserved; dynamic route after /runs/running)
# ===========================================================================


@router.get("/runs/{run_id}/placement", response_model=PlacementResponse)
async def get_placement(
    run_id: str,
    request: Request,
    x_tenant_id: str = Header(...),
) -> PlacementResponse:
    _tenant_header(x_tenant_id)
    redis = request.app.state.redis
    meta = await redis.hgetall(RedisKey.run_meta(run_id))
    if not meta:
        raise HTTPException(status_code=404, detail=f"Run {run_id!r} not found")

    def _s(k: str) -> str:
        v = meta.get(k.encode()) or meta.get(k)
        return (v.decode() if isinstance(v, bytes) else v) or ""

    if _s("tenant_id") != x_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch")
    state_raw = await redis.get(RedisKey.run_state(run_id))
    state = (
        state_raw.decode() if isinstance(state_raw, bytes) else state_raw
    ) or "unknown"
    return PlacementResponse(
        run_id=run_id,
        tenant_id=_s("tenant_id"),
        state=state,
        worker_group=_s("worker_group"),
        shard=int(_s("shard") or "0"),
        reschedule_count=int(_s("reschedule_count") or "0"),
        admitted_at=float(_s("admitted_at") or "0"),
    )


@router.post("/runs/{run_id}/reschedule")
async def force_reschedule(
    run_id: str,
    request: Request,
    x_tenant_id: str = Header(...),
    x_cp_auth: str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    try:
        await request.app.state.cp._leader.assert_leader()
    except LeadershipError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    redis = request.app.state.redis
    meta = await redis.hgetall(RedisKey.run_meta(run_id))
    if not meta:
        raise HTTPException(status_code=404, detail=f"Run {run_id!r} not found")

    def _s(k: str) -> str:
        v = meta.get(k.encode()) or meta.get(k)
        return (v.decode() if isinstance(v, bytes) else v) or ""

    if _s("tenant_id") != x_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch")
    result = await request.app.state.cp.recovery._handle_stale(run_id)
    return {"run_id": run_id, "result": result}


# ===========================================================================
# Sprint 13 — Recovery visibility (OPERATOR-ONLY)
# ===========================================================================


@router.get("/recovery/stale", response_model=StaleRunsResponse)
async def recovery_stale(
    request: Request,
    x_cp_auth: str = Header(default=""),
) -> StaleRunsResponse:
    """Operator-only. Runs the recovery stale detection and returns candidates."""
    _require_operator(x_cp_auth)
    runs = await request.app.state.cp.list_stale_runs()
    summaries = [
        StaleRunSummary(
            run_id=r["run_id"],
            tenant_id=r["tenant_id"],
            state=r["state"],
            worker_group=r["worker_group"],
            reschedule_count=r["reschedule_count"],
            running_since=r["running_since"],
            stale_for_seconds=r["stale_for_seconds"],
        )
        for r in runs
    ]
    return StaleRunsResponse(count=len(summaries), runs=summaries)


@router.get("/recovery/summary", response_model=RecoverySummaryResponse)
async def recovery_summary(
    request: Request,
    x_cp_auth: str = Header(default=""),
) -> RecoverySummaryResponse:
    """Operator-only. Aggregated recovery metrics."""
    _require_operator(x_cp_auth)
    data = await request.app.state.cp.get_recovery_summary()
    return RecoverySummaryResponse(**data)


@router.get("/recovery/dlq", response_model=DLQListResponse)
async def recovery_dlq(
    request: Request,
    x_cp_auth: str = Header(default=""),
) -> DLQListResponse:
    """Operator-only. DLQ entries across all tenants."""
    _require_operator(x_cp_auth)
    entries = await request.app.state.cp.list_dlq(tenant_id="", limit=100)
    responses = [
        DLQEntryResponse(
            run_id=e["run_id"],
            tenant_id=e["tenant_id"],
            reason=e["reason"],
            delivery_count=e["reschedule_count"],
            dead_lettered_at=e["dead_lettered_at"],
            original_error=e.get("original_error", ""),
            cost_cents=e.get("cost_cents", 0),
        )
        for e in entries
    ]
    return DLQListResponse(count=len(responses), entries=responses)


# ===========================================================================
# Sprint 10 — DLQ (preserved, tenant-scoped)
# ===========================================================================


@router.get("/dlq", response_model=List[DLQEntryResponse])
async def list_dlq(
    request: Request,
    x_tenant_id: str = Header(...),
    x_cp_auth: str = Header(default=""),
) -> List[DLQEntryResponse]:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    entries = await request.app.state.cp.recovery.list_dlq(x_tenant_id)
    return [
        DLQEntryResponse(
            run_id=e["run_id"],
            tenant_id=e["tenant_id"],
            reason=e["reason"],
            delivery_count=e["reschedule_count"],
            dead_lettered_at=e["dead_lettered_at"],
            original_error=e.get("original_error", ""),
            cost_cents=e.get("cost_cents", 0),
        )
        for e in entries
    ]


@router.post("/dlq/{run_id}/replay")
async def replay_dlq(
    run_id: str,
    request: Request,
    x_tenant_id: str = Header(...),
    x_cp_auth: str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    try:
        await request.app.state.cp.recovery.replay_dlq_run(run_id, x_tenant_id)
        audit = getattr(request.app.state.cp, "_audit", None)
        if audit:
            await audit.dlq_replay(
                run_id=run_id, tenant_id=x_tenant_id, operator="operator"
            )
        return {"run_id": run_id, "status": "replayed"}
    except DLQEntryNotFoundError:
        raise HTTPException(status_code=404, detail=f"DLQ entry {run_id!r} not found")
    except TenantMismatchError as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@router.delete("/dlq/{run_id}")
async def delete_dlq(
    run_id: str,
    request: Request,
    x_tenant_id: str = Header(...),
    x_cp_auth: str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    redis = request.app.state.redis
    meta = await redis.hgetall(RedisKey.cp_dlq_meta(run_id))
    if not meta:
        raise HTTPException(status_code=404, detail=f"DLQ {run_id!r} not found")

    def _s(k: str) -> str:
        v = meta.get(k.encode()) or meta.get(k)
        return (v.decode() if isinstance(v, bytes) else v) or ""

    if _s("tenant_id") != x_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch")
    await redis.delete(RedisKey.cp_dlq_meta(run_id))
    logger.info("DLQ entry deleted: run=%s by tenant=%s", run_id, x_tenant_id)
    return {"run_id": run_id, "status": "deleted"}


# ===========================================================================
# Sprint 10 — Health (preserved)
# ===========================================================================


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    cp = request.app.state.cp
    redis = request.app.state.redis
    config = cp._config
    reg_size = await cp.registry.registry_size()
    healthy = len(await cp.registry.list_healthy_workers())
    try:
        dlq_depth = await cp.recovery.dlq_depth()
    except Exception:
        dlq_depth = -1
    try:
        sched_lag = await redis.xlen(config.control_stream)
    except Exception:
        sched_lag = -1
    return HealthResponse(
        is_leader=cp.is_leader,
        instance_id=config.instance_id,
        region=config.region,
        registry_size=reg_size,
        healthy_workers=healthy,
        scheduler_lag=sched_lag,
        dlq_depth=dlq_depth,
    )


# ===========================================================================
# Sprint 12 — Operational probes (preserved)
# ===========================================================================


@router.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request) -> dict:
    redis = request.app.state.redis
    cp = request.app.state.cp
    checks: dict = {}
    try:
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"
    try:
        reg_size = await cp.registry.registry_size()
        checks["registry"] = f"size={reg_size}"
    except Exception as exc:
        checks["registry"] = f"error: {exc}"
    ready = checks["redis"] == "ok"
    if not ready:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "checks": checks},
        )
    return {"status": "ready", "checks": checks}


@router.get("/diagnostics/running")
async def diagnostics_running(
    request: Request,
    x_tenant_id: str = Header(...),
    x_cp_auth: str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    from hfa.runtime.state_store import StateStore

    runs = await StateStore(request.app.state.redis).get_running_runs(limit=200)
    return {"running_count": len(runs), "runs": runs}


@router.get("/diagnostics/recovery")
async def diagnostics_recovery(
    request: Request,
    x_tenant_id: str = Header(...),
    x_cp_auth: str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    cp = request.app.state.cp
    try:
        dlq_depth = await cp.recovery.dlq_depth()
    except Exception:
        dlq_depth = -1
    try:
        schedulable = await cp.registry.list_schedulable_workers()
        draining = [
            w for w in await cp.registry.list_healthy_workers() if w.is_draining
        ]
    except Exception:
        schedulable = []
        draining = []
    return {
        "dlq_depth": dlq_depth,
        "schedulable_workers": len(schedulable),
        "draining_workers": len(draining),
    }


# ===========================================================================
# Helpers
# ===========================================================================


def _worker_resp(profile) -> WorkerResponse:
    return WorkerResponse(
        worker_id=profile.worker_id,
        worker_group=profile.worker_group,
        region=profile.region,
        shards=profile.shards,
        capacity=profile.capacity,
        inflight=profile.inflight,
        load_factor=profile.load_factor,
        status=profile.status.value,
        last_seen=profile.last_seen,
        version=profile.version,
        capabilities=profile.capabilities,
    )


def _worker_summary(profile) -> WorkerSummary:
    return WorkerSummary(
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

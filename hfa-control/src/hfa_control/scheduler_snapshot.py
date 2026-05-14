"""
hfa-control/src/hfa_control/scheduler_snapshot.py

IRONCLAD Sprint 22 (PR-1) — Canonical & Deterministic Scheduler Input
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from hfa.config.keys import RedisKey
from hfa_control.models import WorkerStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TenantSchedulingSnapshot:
    tenant_id: str
    queued_depth: int
    inflight: int
    vruntime: float
    head_run_id: Optional[str]
    head_age_seconds: float
    dispatchable: bool
    blocked_reason: Optional[str] = None


@dataclass(frozen=True)
class WorkerSchedulingSnapshot:
    worker_id: str
    worker_group: str
    region: str
    capacity: int
    inflight: int
    available_slots: int
    load_factor: float
    status: str
    status_enum: WorkerStatus
    last_seen: float
    capabilities: Tuple[str, ...] = ()
    latency_ewma_ms: float = 0.0
    failure_penalty: float = 0.0
    dispatch_reject_penalty: float = 0.0
    drained: bool = False
    schedulable: bool = True
    blocked_reason: Optional[str] = None


@dataclass(frozen=True)
class CapacitySnapshot:
    workers: Tuple[WorkerSchedulingSnapshot, ...]
    total_available_slots: int
    dispatch_allowed: bool
    blocked_reason: Optional[str]
    redis_degraded: bool
    max_dispatches_this_cycle: int


def _clean_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_worker_status(value: object) -> WorkerStatus:
    if isinstance(value, WorkerStatus):
        return value

    if value is None:
        return WorkerStatus.HEALTHY

    raw = str(value).strip().lower()
    if not raw:
        return WorkerStatus.HEALTHY

    try:
        return WorkerStatus(raw)
    except ValueError:
        logger.warning(
            "Unknown worker status=%r; treating as DEAD/draining (fail-closed)",
            value,
        )
        try:
            return WorkerStatus("dead")
        except ValueError:
            return WorkerStatus("draining")


def _normalize_capabilities(*raw_groups: object) -> Tuple[str, ...]:
    merged: set[str] = set()

    for group in raw_groups:
        if isinstance(group, (list, tuple, set)):
            for item in group:
                cleaned = _clean_str(item)
                if cleaned:
                    merged.add(cleaned)

    return tuple(sorted(merged))


class SchedulerSnapshotBuilder:
    def __init__(self, redis, registry, tenant_queue, tenant_fairness, config) -> None:
        self._redis = redis
        self._registry = registry
        self._tenant_queue = tenant_queue
        self._tenant_fairness = tenant_fairness
        self._config = config

    async def list_candidate_tenants(self) -> list[TenantSchedulingSnapshot]:
        result: list[TenantSchedulingSnapshot] = []
        raw_tenant_ids = await self._tenant_queue.active_tenants()
        tenant_ids = sorted(_clean_str(t) for t in raw_tenant_ids if _clean_str(t))
        now = time.time()

        for tenant_id in tenant_ids:
            depth = max(0, _safe_int(await self._tenant_queue.depth(tenant_id), 0))

            if depth <= 0:
                try:
                    await self._redis.srem(RedisKey.tenant_active_set(), tenant_id)
                except Exception:
                    logger.debug(
                        "Failed to prune empty tenant from active set: tenant=%s",
                        tenant_id,
                        exc_info=True,
                    )
                continue

            head_run_id_raw = await self._tenant_queue.peek(tenant_id)
            head_run_id = _clean_str(head_run_id_raw) or None

            inflight_val = await self._redis.get(RedisKey.tenant_inflight(tenant_id))
            inflight = max(0, _safe_int(inflight_val, 0))

            head_age = 0.0
            if head_run_id:
                admitted = await self._redis.hget(
                    RedisKey.run_meta(head_run_id),
                    "admitted_at",
                )
                if isinstance(admitted, bytes):
                    admitted = admitted.decode("utf-8", errors="ignore")

                admitted_ts = _safe_float(admitted, 0.0)
                if admitted_ts > 0:
                    head_age = max(0.0, now - admitted_ts)

            vruntime = _safe_float(self._tenant_fairness.get(tenant_id), 0.0)

            result.append(
                TenantSchedulingSnapshot(
                    tenant_id=tenant_id,
                    queued_depth=depth,
                    inflight=inflight,
                    vruntime=vruntime,
                    head_run_id=head_run_id,
                    head_age_seconds=head_age,
                    dispatchable=head_run_id is not None,
                    blocked_reason=None if head_run_id else "empty_head",
                )
            )

        return result

    async def build_capacity_snapshot(self) -> CapacitySnapshot:
        # Important:
        # scheduler must see the broad worker pool and derive blocked_reason itself.
        raw_workers = await self._registry.list_all_workers(region=None)

        worker_candidates: dict[str, list[WorkerSchedulingSnapshot]] = {}

        for worker in raw_workers:
            worker_id = _clean_str(getattr(worker, "worker_id", ""))
            if not worker_id:
                logger.warning("Skipping worker with empty worker_id: %r", worker)
                continue

            worker_group = _clean_str(getattr(worker, "worker_group", ""))
            region = _clean_str(getattr(worker, "region", ""))

            capacity = max(0, _safe_int(getattr(worker, "capacity", 0), 0))
            inflight = max(0, _safe_int(getattr(worker, "inflight", 0), 0))
            available = max(0, capacity - inflight)
            load_factor = (inflight / capacity) if capacity > 0 else 1.0

            capabilities = _normalize_capabilities(
                getattr(worker, "capabilities", ()),
                getattr(worker, "agent_types", ()),
            )

            status_enum = _normalize_worker_status(
                getattr(worker, "status", WorkerStatus.HEALTHY)
            )
            status_str = status_enum.value

            last_seen = _safe_float(
                getattr(worker, "last_heartbeat_at", getattr(worker, "last_seen", 0.0)),
                0.0,
            )

            latency_ewma_ms = max(
                0.0,
                _safe_float(getattr(worker, "latency_ewma_ms", 0.0), 0.0),
            )
            failure_penalty = max(
                0.0,
                _safe_float(getattr(worker, "failure_penalty", 0.0), 0.0),
            )
            dispatch_reject_penalty = max(
                0.0,
                _safe_float(getattr(worker, "dispatch_reject_penalty", 0.0), 0.0),
            )

            is_draining = status_enum == WorkerStatus.DRAINING
            is_healthy = status_enum == WorkerStatus.HEALTHY

            if is_draining:
                blocked_reason = "draining"
            elif not is_healthy:
                blocked_reason = "unhealthy"
            elif capacity == 0:
                blocked_reason = "capacity_zero"
            elif available <= 0:
                blocked_reason = "at_capacity"
            else:
                blocked_reason = None

            schedulable = blocked_reason is None

            snapshot = WorkerSchedulingSnapshot(
                worker_id=worker_id,
                worker_group=worker_group,
                region=region,
                capacity=capacity,
                inflight=inflight,
                available_slots=available,
                load_factor=load_factor,
                status=status_str,
                status_enum=status_enum,
                last_seen=last_seen,
                capabilities=capabilities,
                latency_ewma_ms=latency_ewma_ms,
                failure_penalty=failure_penalty,
                dispatch_reject_penalty=dispatch_reject_penalty,
                drained=is_draining,
                schedulable=schedulable,
                blocked_reason=blocked_reason,
            )

            worker_candidates.setdefault(worker_id, []).append(snapshot)

        resolved_workers: list[WorkerSchedulingSnapshot] = []

        for worker_id, candidates in worker_candidates.items():
            if len(candidates) > 1:
                logger.warning(
                    "Duplicate worker_id detected in scheduler snapshot: worker_id=%s count=%d",
                    worker_id,
                    len(candidates),
                )

            # Deterministic duplicate resolution
            chosen = sorted(
                candidates,
                key=lambda x: (
                    x.worker_group,
                    x.region,
                    x.worker_id,
                    -x.last_seen,   # prefer freshest heartbeat
                    -x.capacity,    # then stronger worker
                    x.inflight,     # then lower inflight
                ),
            )[0]
            resolved_workers.append(chosen)

        # Canonical deterministic topology ordering
        resolved_workers.sort(
            key=lambda x: (
                x.worker_group,
                x.region,
                x.worker_id,
            )
        )

        total_slots = sum(w.available_slots for w in resolved_workers if w.schedulable)

        raw_limit = getattr(self._config, "scheduler_loop_max_dispatches", 32)
        cfg_limit = max(0, _safe_int(raw_limit, 32))
        max_dispatches = min(total_slots, cfg_limit)

        return CapacitySnapshot(
            workers=tuple(resolved_workers),
            total_available_slots=total_slots,
            dispatch_allowed=total_slots > 0,
            blocked_reason=None if total_slots > 0 else "worker_pool_empty",
            redis_degraded=False,
            max_dispatches_this_cycle=max_dispatches,
        )
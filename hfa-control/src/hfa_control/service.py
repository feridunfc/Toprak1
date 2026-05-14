"""
hfa-control/src/hfa_control/service.py
IRONCLAD Sprint 10 — Control Plane Service

Wires all components and manages lifecycle.
Leader-gated: Scheduler and RecoveryService only run on the leader instance.
Standby instances run WorkerRegistry (heartbeat consumption) continuously.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop().
* close() always safe (idempotent, each component guards its own close).
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from typing import Optional

from hfa_control.models import ControlPlaneConfig
from hfa_control.leader import LeaderElection
from hfa_control.registry import WorkerRegistry
from hfa_control.shard import ShardOwnershipManager
from hfa_control.admission import AdmissionController
from hfa_control.scheduler import Scheduler
from hfa_control.recovery import RecoveryService
from hfa_control.audit import build_audit_logger
from hfa_control.redis_resilience import RedisHealthMonitor
from hfa.config.keys import RedisKey

logger = logging.getLogger(__name__)

_LEADER_CHECK_INTERVAL = 5.0  # seconds


class ControlPlaneService:
    def __init__(self, redis, config: Optional[ControlPlaneConfig] = None) -> None:
        self._redis = redis
        self._config = config or _config_from_env()
        # Assign a unique instance_id if not set
        if not self._config.instance_id:
            self._config.instance_id = os.environ.get(
                "CP_INSTANCE_ID", uuid.uuid4().hex[:8]
            )

        self._leader = LeaderElection(redis, self._config.instance_id, self._config)
        self._registry = WorkerRegistry(redis, self._config)
        self._shards = ShardOwnershipManager(redis, self._config)
        self._audit = build_audit_logger(redis)
        self._admitter = AdmissionController(redis, self._config, audit=self._audit)
        self._scheduler = Scheduler(redis, self._registry, self._shards, self._config)
        self._recovery = RecoveryService(redis, self._config)
        self._redis_monitor = RedisHealthMonitor(redis)

        self._leader_task: Optional[asyncio.Task] = None
        self._sched_started = False
        self._recovery_started = False

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def admission(self) -> AdmissionController:
        return self._admitter

    @property
    def registry(self) -> WorkerRegistry:
        return self._registry

    @property
    def recovery(self) -> RecoveryService:
        return self._recovery

    @property
    def shards(self) -> ShardOwnershipManager:
        return self._shards

    @property
    def is_leader(self) -> bool:
        return self._leader.is_leader

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        # All instances run registry and shard monitor
        await self._registry.start()
        await self._shards.start()
        await self._leader.start()

        # Sprint 17: audit logger (no-op when keys not configured)
        await self._audit.initialise()

        # Sprint 17: Redis health monitor for readiness probe
        await self._redis_monitor.start()

        # Leader-gated components started via watchdog loop
        loop = asyncio.get_running_loop()
        self._leader_task = loop.create_task(
            self._leader_watchdog(), name="cp.leader_watchdog"
        )
        logger.info(
            "ControlPlaneService started: instance=%s region=%s",
            self._config.instance_id,
            self._config.region,
        )

    async def close(self) -> None:
        if self._leader_task:
            self._leader_task.cancel()
            try:
                await self._leader_task
            except asyncio.CancelledError:
                pass

        # Close leader-gated components
        if self._sched_started:
            await self._scheduler.close()
        if self._recovery_started:
            await self._recovery.close()

        # Close always-running components
        await self._shards.close()
        await self._registry.close()
        await self._leader.close()

        logger.info("ControlPlaneService closed: instance=%s", self._config.instance_id)

    # ------------------------------------------------------------------
    # Leader watchdog — starts Scheduler+Recovery when leadership acquired
    # ------------------------------------------------------------------

    async def _leader_watchdog(self) -> None:
        """
        Monitor leadership state.
        Start leader-only components when leadership is acquired.
        Stop them gracefully when leadership is lost.
        (Leadership loss is rare but possible during rolling redeploys.)
        """
        while True:
            try:
                if self._leader.is_leader:
                    if not self._sched_started:
                        await self._scheduler.start()
                        self._sched_started = True
                        logger.info(
                            "Leader components started: instance=%s",
                            self._config.instance_id,
                        )
                    if not self._recovery_started:
                        await self._recovery.start()
                        self._recovery_started = True
                else:
                    if self._sched_started:
                        await self._scheduler.close()
                        self._sched_started = False
                        logger.info(
                            "Scheduler stopped (leadership lost): instance=%s",
                            self._config.instance_id,
                        )
                    if self._recovery_started:
                        await self._recovery.close()
                        self._recovery_started = False

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("_leader_watchdog error: %s", exc)

            try:
                await asyncio.sleep(_LEADER_CHECK_INTERVAL)
            except asyncio.CancelledError:
                break

    # ------------------------------------------------------------------
    # Sprint 13 — operational query surface
    # All methods are read-only and safe to call from any instance.
    # ------------------------------------------------------------------

    async def get_liveness(self) -> dict:
        """Return liveness information. Never raises."""
        return {
            "status": "alive",
            "service": "hfa-control",
            "instance_id": self._config.instance_id,
        }

    async def get_readiness(self) -> dict:
        """
        Return readiness state with per-check details.
        status = "ready" only when all critical checks pass.
        """
        checks: dict = {}

        # Check 1: Redis ping
        try:
            await self._redis.ping()
            checks["redis"] = {"ok": True, "message": ""}
        except Exception as exc:
            checks["redis"] = {"ok": False, "message": str(exc)[:120]}

        # Check 2: control stream exists / reachable
        try:
            await self._redis.xlen(self._config.control_stream)
            checks["control_stream"] = {"ok": True, "message": ""}
        except Exception as exc:
            checks["control_stream"] = {"ok": False, "message": str(exc)[:120]}

        # Check 3: heartbeat stream exists / reachable
        try:
            await self._redis.xlen(self._config.heartbeat_stream)
            checks["heartbeat_stream"] = {"ok": True, "message": ""}
        except Exception as exc:
            checks["heartbeat_stream"] = {"ok": False, "message": str(exc)[:120]}

        all_ok = all(v["ok"] for v in checks.values())
        return {
            "status": "ready" if all_ok else "not_ready",
            "instance_id": self._config.instance_id,
            "is_leader": self._leader.is_leader,
            "checks": checks,
        }

    async def list_all_workers(self) -> list:
        """Return all known workers (healthy + draining), not DEAD."""
        return await self._registry.list_healthy_workers()

    async def list_healthy_workers(self) -> list:
        """Return healthy workers (alive, not DEAD)."""
        return await self._registry.list_healthy_workers()

    async def list_schedulable_workers(self) -> list:
        """Return workers eligible to receive new runs."""
        return await self._registry.list_schedulable_workers()

    async def get_worker(self, worker_id: str):
        """Return a single worker profile. Raises WorkerNotFoundError if absent."""
        return await self._registry.get_worker(worker_id)

    async def list_running_runs(self, limit: int = 100) -> list:
        """
        Return up to `limit` runs from the running ZSET with metadata.
        Each entry: run_id, state, started_at, tenant_id, worker_group,
        shard, claim_owner.
        """
        from hfa.runtime.state_store import StateStore

        store = StateStore(self._redis)
        base = await store.get_running_runs(limit=limit)
        enriched = []
        for entry in base:
            run_id = entry["run_id"]
            meta = await store.get_run_meta(run_id)
            claim_owner = await store.get_claim_owner(run_id)
            enriched.append(
                {
                    "run_id": run_id,
                    "tenant_id": meta.get("tenant_id", ""),
                    "state": entry.get("state", "unknown"),
                    "worker_group": meta.get("worker_group", ""),
                    "shard": int(meta.get("shard", "0") or "0"),
                    "started_at": entry.get("started_at", 0.0),
                    "claim_owner": claim_owner,
                }
            )
        return enriched

    async def get_run_state(self, run_id: str) -> dict:
        """Return run state + metadata dict."""
        from hfa.runtime.state_store import StateStore

        store = StateStore(self._redis)
        state = await store.get_run_state(run_id)
        meta = await store.get_run_meta(run_id)
        return {
            "run_id": run_id,
            "tenant_id": meta.get("tenant_id", ""),
            "state": state or "unknown",
            "worker_group": meta.get("worker_group", ""),
            "shard": int(meta.get("shard", "0") or "0"),
            "reschedule_count": int(meta.get("reschedule_count", "0") or "0"),
            "admitted_at": float(meta.get("admitted_at", "0") or "0"),
        }

    async def get_run_claim(self, run_id: str) -> dict:
        """Return claim ownership and TTL for a run."""
        from hfa.runtime.state_store import StateStore

        store = StateStore(self._redis)
        owner = await store.get_claim_owner(run_id)
        ttl = await store.get_claim_ttl(run_id) if owner else -2
        return {
            "run_id": run_id,
            "claimed": owner is not None,
            "owner": owner,
            "ttl_seconds": max(ttl, 0) if ttl > 0 else 0,
        }

    async def get_run_result(self, run_id: str):
        """Return stored run result dict or None."""
        from hfa.runtime.state_store import StateStore

        return await StateStore(self._redis).get_result(run_id)

    async def list_stale_runs(self) -> list:
        """
        Return run_ids that would be considered stale by recovery,
        enriched with metadata for operator visibility.
        """
        import time

        stale_ids = await self._recovery._find_stale_runs()
        result = []
        for run_id in stale_ids:
            meta = await self._redis.hgetall(RedisKey.run_meta(run_id))

            def _s(k: str) -> str:
                v = meta.get(k.encode()) or meta.get(k)
                return (v.decode() if isinstance(v, bytes) else v) or ""

            score_raw = await self._redis.zscore(self._config.running_zset, run_id)
            running_since = float(score_raw) if score_raw else 0.0
            stale_for = time.time() - running_since

            result.append(
                {
                    "run_id": run_id,
                    "tenant_id": _s("tenant_id"),
                    "state": _s("state") or "unknown",
                    "worker_group": _s("worker_group"),
                    "reschedule_count": int(_s("reschedule_count") or "0"),
                    "running_since": running_since,
                    "stale_for_seconds": round(stale_for, 1),
                }
            )
        return result

    async def get_recovery_summary(self) -> dict:
        """Return aggregated recovery visibility metrics."""
        stale_ids = await self._recovery._find_stale_runs()
        dlq_entries = await self._recovery.list_dlq("__all__", limit=1000)
        schedulable = await self._registry.list_schedulable_workers()
        all_alive = await self._registry.list_healthy_workers()
        draining = [w for w in all_alive if w.is_draining]
        return {
            "stale_count": len(stale_ids),
            "dlq_count": len(dlq_entries),
            "schedulable_workers": len(schedulable),
            "draining_workers": len(draining),
        }

    async def list_dlq(self, tenant_id: str = "", limit: int = 50) -> list:
        """Return DLQ entries. If tenant_id is empty, returns all entries."""
        return await self._recovery.list_dlq(tenant_id or "__all__", limit)


def _config_from_env() -> ControlPlaneConfig:
    return ControlPlaneConfig(
        region=os.environ.get("CP_REGION", "us-east-1"),
        instance_id=os.environ.get("CP_INSTANCE_ID", ""),
        worker_heartbeat_ttl=float(os.environ.get("WORKER_HEARTBEAT_TTL", "30")),
        stale_run_timeout=float(os.environ.get("STALE_RUN_TIMEOUT", "600")),
        recovery_sweep_interval=float(os.environ.get("RECOVERY_SWEEP_INTERVAL", "30")),
        max_reschedule_attempts=int(os.environ.get("MAX_RESCHEDULE_ATTEMPTS", "3")),
    )

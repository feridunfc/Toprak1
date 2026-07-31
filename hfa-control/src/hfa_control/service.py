"""Control-plane composition root and operational read service."""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey
from hfa_control.admission import AdmissionController
from hfa_control.audit import build_audit_logger
from hfa_control.leader import LeaderElection
from hfa_control.models import ControlPlaneConfig
from hfa_control.recovery import RecoveryService
from hfa_control.redis_resilience import RedisHealthMonitor
from hfa_control.registry import WorkerRegistry
from hfa_control.scheduler import Scheduler, build_production_scheduler
from hfa_control.shard import ShardOwnershipManager

logger = logging.getLogger(__name__)
_LEADER_CHECK_INTERVAL = 5.0


def _decode(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value) if value is not None else ""


def _decode_mapping(raw: dict) -> dict[str, str]:
    return {_decode(key): _decode(value) for key, value in (raw or {}).items()}


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(_decode(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(_decode(value))
    except (TypeError, ValueError):
        return default


def _terminal_run_state(state: str) -> bool:
    return state in {"done", "failed", "rejected", "dead_lettered"}


def _terminal_task_state(state: str) -> bool:
    return state in {
        "done",
        "failed",
        "blocked_by_failure",
        "dead_lettered",
        "skipped",
    }


class ControlPlaneService:
    def __init__(self, redis, config: Optional[ControlPlaneConfig] = None) -> None:
        self._redis = redis
        self._config = config or _config_from_env()
        self._leader = LeaderElection(redis, self._config.instance_id, self._config)
        self._registry = WorkerRegistry(redis, self._config)
        self._shards = ShardOwnershipManager(redis, self._config)
        self._audit = build_audit_logger(redis)
        self._admitter = AdmissionController(redis, self._config, audit=self._audit)
        self._scheduler = build_production_scheduler(
            redis=redis,
            config=self._config,
            registry=self._registry,
            shards=self._shards,
            event_store=None,
        )
        self._recovery = RecoveryService(redis, self._config)
        self._redis_monitor = RedisHealthMonitor(redis)
        self._leader_task: Optional[asyncio.Task] = None
        self._sched_started = False
        self._recovery_started = False

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

    async def start(self) -> None:
        await self._registry.start()
        await self._shards.start()
        await self._leader.start()
        await self._audit.initialise()
        await self._redis_monitor.start()
        loop = asyncio.get_running_loop()
        self._leader_task = loop.create_task(
            self._leader_watchdog(),
            name="cp.leader_watchdog",
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
            self._leader_task = None
        await self._scheduler.close()
        self._sched_started = False
        if self._recovery_started:
            await self._recovery.close()
            self._recovery_started = False
        await self._shards.close()
        await self._registry.close()
        await self._leader.close()
        logger.info("ControlPlaneService closed: instance=%s", self._config.instance_id)

    async def _reconcile_leadership_once(self) -> None:
        if self._leader.is_leader:
            token = int(getattr(self._leader, "fencing_token", 0) or 0)
            if token <= 0:
                raise ValueError("leader fencing_token must be greater than zero")
            scheduler_started_now = False
            if not self._sched_started:
                await self._scheduler.start(scheduler_epoch=str(token))
                self._sched_started = True
                scheduler_started_now = True
            if not self._recovery_started:
                try:
                    await self._recovery.start()
                    self._recovery_started = True
                except Exception:
                    if scheduler_started_now:
                        await self._scheduler.stop()
                        self._sched_started = False
                    raise
            return

        errors: list[BaseException] = []
        if self._sched_started:
            try:
                await self._scheduler.stop()
                self._sched_started = False
            except BaseException as exc:
                errors.append(exc)
        if self._recovery_started:
            try:
                await self._recovery.close()
                self._recovery_started = False
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError("leadership loss reconciliation failed") from errors[0]

    async def _leader_watchdog(self) -> None:
        while True:
            try:
                await self._reconcile_leadership_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("_leader_watchdog error: %s", exc)
            try:
                await asyncio.sleep(_LEADER_CHECK_INTERVAL)
            except asyncio.CancelledError:
                break

    # ------------------------------------------------------------------
    # Operational query surface — read-only on every instance
    # ------------------------------------------------------------------

    async def get_liveness(self) -> dict:
        return {
            "status": "alive",
            "service": "hfa-control",
            "instance_id": self._config.instance_id,
        }

    async def get_readiness(self) -> dict:
        checks: dict = {}
        try:
            await self._redis.ping()
            checks["redis"] = {"ok": True, "message": ""}
        except Exception as exc:
            checks["redis"] = {"ok": False, "message": str(exc)[:120]}
        try:
            await self._redis.xlen(self._config.control_stream)
            checks["control_stream"] = {"ok": True, "message": ""}
        except Exception as exc:
            checks["control_stream"] = {"ok": False, "message": str(exc)[:120]}
        try:
            await self._redis.xlen(self._config.heartbeat_stream)
            checks["heartbeat_stream"] = {"ok": True, "message": ""}
        except Exception as exc:
            checks["heartbeat_stream"] = {"ok": False, "message": str(exc)[:120]}
        return {
            "status": "ready" if all(value["ok"] for value in checks.values()) else "not_ready",
            "instance_id": self._config.instance_id,
            "is_leader": self._leader.is_leader,
            "checks": checks,
        }

    async def list_all_workers(self) -> list:
        return await self._registry.list_healthy_workers()

    async def list_healthy_workers(self) -> list:
        return await self._registry.list_healthy_workers()

    async def list_schedulable_workers(self) -> list:
        return await self._registry.list_schedulable_workers()

    async def get_worker(self, worker_id: str):
        return await self._registry.get_worker(worker_id)

    async def list_running_runs(self, limit: int = 100) -> list:
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
                    "shard": _safe_int(meta.get("shard"), 0),
                    "started_at": entry.get("started_at", 0.0),
                    "claim_owner": claim_owner,
                }
            )
        return enriched

    async def get_run_state(self, run_id: str) -> dict:
        """Return RUN-scoped truth plus explicit TASK conflict metadata.

        This method is strictly read-only. TASK truth never replaces RUN truth;
        it is returned as enrichment and contradiction evidence for operators and
        tenant clients.
        """

        conflicts: list[dict] = []
        task_truth: list[dict] = []
        tenant_candidates: set[str] = set()

        run_state_key = RedisKey.run_state(run_id)
        run_state_kind = _decode(await self._redis.type(run_state_key))
        if run_state_kind == "string":
            run_state = _decode(await self._redis.get(run_state_key))
            if not run_state:
                run_state = "unknown"
                conflicts.append(
                    {
                        "scope": "RUN",
                        "conflict_type": "run_truth_corruption_conflict",
                        "detail_code": "run_state_empty_or_unreadable",
                        "run_id": run_id,
                        "task_id": "",
                        "observed_run_state": "",
                        "observed_task_state": None,
                    }
                )
        elif run_state_kind == "none":
            run_state = "unknown"
            conflicts.append(
                {
                    "scope": "RUN",
                    "conflict_type": "run_truth_missing",
                    "detail_code": "run_state_missing",
                    "run_id": run_id,
                    "task_id": "",
                    "observed_run_state": None,
                    "observed_task_state": None,
                }
            )
        else:
            run_state = "unknown"
            conflicts.append(
                {
                    "scope": "RUN",
                    "conflict_type": "run_truth_corruption_conflict",
                    "detail_code": "run_state_key_type_mismatch",
                    "run_id": run_id,
                    "task_id": "",
                    "observed_run_state": run_state_kind,
                    "observed_task_state": None,
                }
            )

        run_meta_key = RedisKey.run_meta(run_id)
        run_meta_kind = _decode(await self._redis.type(run_meta_key))
        if run_meta_kind == "hash":
            run_meta = _decode_mapping(await self._redis.hgetall(run_meta_key))
        elif run_meta_kind == "none":
            run_meta = {}
            conflicts.append(
                {
                    "scope": "RUN",
                    "conflict_type": "run_truth_corruption_conflict",
                    "detail_code": "run_meta_missing",
                    "run_id": run_id,
                    "task_id": "",
                    "observed_run_state": run_state if run_state != "unknown" else None,
                    "observed_task_state": None,
                }
            )
        else:
            run_meta = {}
            conflicts.append(
                {
                    "scope": "RUN",
                    "conflict_type": "run_truth_corruption_conflict",
                    "detail_code": "run_meta_type_mismatch",
                    "run_id": run_id,
                    "task_id": "",
                    "observed_run_state": run_state if run_state != "unknown" else None,
                    "observed_task_state": None,
                }
            )

        run_tenant = run_meta.get("tenant_id", "")
        if run_tenant:
            tenant_candidates.add(run_tenant)

        run_tasks_key = DagRedisKey.run_tasks(run_id)
        task_index_kind = _decode(await self._redis.type(run_tasks_key))
        if task_index_kind == "set":
            raw_task_ids = await self._redis.smembers(run_tasks_key)
            task_ids = sorted({_decode(value) for value in raw_task_ids if _decode(value)})
            if not task_ids:
                conflicts.append(
                    {
                        "scope": "TASK",
                        "conflict_type": "task_truth_missing",
                        "detail_code": "run_has_no_task_authority",
                        "run_id": run_id,
                        "task_id": "",
                        "observed_run_state": run_state if run_state != "unknown" else None,
                        "observed_task_state": None,
                    }
                )
        elif task_index_kind == "none":
            task_ids = []
            conflicts.append(
                {
                    "scope": "TASK",
                    "conflict_type": "task_truth_missing",
                    "detail_code": "run_task_index_missing",
                    "run_id": run_id,
                    "task_id": "",
                    "observed_run_state": run_state if run_state != "unknown" else None,
                    "observed_task_state": None,
                }
            )
        else:
            task_ids = []
            conflicts.append(
                {
                    "scope": "TASK",
                    "conflict_type": "task_truth_corruption_conflict",
                    "detail_code": "run_task_index_type_mismatch",
                    "run_id": run_id,
                    "task_id": "",
                    "observed_run_state": run_state if run_state != "unknown" else None,
                    "observed_task_state": task_index_kind,
                }
            )

        run_is_terminal = _terminal_run_state(run_state)
        for task_id in task_ids:
            task_meta_key = DagRedisKey.task_meta(task_id)
            task_meta_kind = _decode(await self._redis.type(task_meta_key))
            if task_meta_kind == "hash":
                task_meta = _decode_mapping(await self._redis.hgetall(task_meta_key))
            else:
                task_meta = {}
                conflicts.append(
                    {
                        "scope": "TASK",
                        "conflict_type": (
                            "task_truth_missing"
                            if task_meta_kind == "none"
                            else "task_truth_corruption_conflict"
                        ),
                        "detail_code": (
                            "task_meta_missing"
                            if task_meta_kind == "none"
                            else "task_meta_type_mismatch"
                        ),
                        "run_id": run_id,
                        "task_id": task_id,
                        "observed_run_state": run_state if run_state != "unknown" else None,
                        "observed_task_state": None,
                    }
                )

            task_tenant = task_meta.get("tenant_id", "")
            if task_tenant:
                tenant_candidates.add(task_tenant)
            identity_status = "exact"
            if task_meta:
                if task_meta.get("task_id", "") != task_id:
                    identity_status = "task_id_mismatch"
                elif task_meta.get("run_id", "") != run_id:
                    identity_status = "run_id_mismatch"
                if identity_status != "exact":
                    conflicts.append(
                        {
                            "scope": "TASK",
                            "conflict_type": "task_truth_corruption_conflict",
                            "detail_code": identity_status,
                            "run_id": run_id,
                            "task_id": task_id,
                            "observed_run_state": run_state if run_state != "unknown" else None,
                            "observed_task_state": None,
                        }
                    )

            task_state_key = DagRedisKey.task_state(task_id)
            task_state_kind = _decode(await self._redis.type(task_state_key))
            if task_state_kind == "string":
                task_state = _decode(await self._redis.get(task_state_key))
            else:
                task_state = "unknown"
                conflicts.append(
                    {
                        "scope": "TASK",
                        "conflict_type": (
                            "task_truth_missing"
                            if task_state_kind == "none"
                            else "task_truth_corruption_conflict"
                        ),
                        "detail_code": (
                            "task_state_missing"
                            if task_state_kind == "none"
                            else "task_state_key_type_mismatch"
                        ),
                        "run_id": run_id,
                        "task_id": task_id,
                        "observed_run_state": run_state if run_state != "unknown" else None,
                        "observed_task_state": (
                            None if task_state_kind == "none" else task_state_kind
                        ),
                    }
                )

            if task_state and task_state != "unknown" and run_state != "unknown":
                task_is_terminal = _terminal_task_state(task_state)
                if task_is_terminal != run_is_terminal:
                    conflicts.append(
                        {
                            "scope": "CROSS_PLANE",
                            "conflict_type": (
                                "task_truth_terminal_conflict"
                                if task_is_terminal
                                else "run_truth_terminal_conflict"
                            ),
                            "detail_code": (
                                "task_terminal_run_nonterminal"
                                if task_is_terminal
                                else "run_terminal_task_nonterminal"
                            ),
                            "run_id": run_id,
                            "task_id": task_id,
                            "observed_run_state": run_state,
                            "observed_task_state": task_state,
                        }
                    )

            task_truth.append(
                {
                    "task_id": task_id,
                    "run_id": task_meta.get("run_id", ""),
                    "tenant_id": task_tenant,
                    "state": task_state or "unknown",
                    "identity_status": identity_status,
                }
            )

        tenant_id = run_tenant
        if not tenant_id and len(tenant_candidates) == 1:
            tenant_id = next(iter(tenant_candidates))
        if len(tenant_candidates) > 1:
            conflicts.append(
                {
                    "scope": "CROSS_PLANE",
                    "conflict_type": "task_truth_corruption_conflict",
                    "detail_code": "tenant_identity_disagreement",
                    "run_id": run_id,
                    "task_id": "",
                    "observed_run_state": run_state if run_state != "unknown" else None,
                    "observed_task_state": None,
                }
            )

        return {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "state": run_state,
            "worker_group": run_meta.get("worker_group", ""),
            "shard": _safe_int(run_meta.get("shard"), 0),
            "reschedule_count": _safe_int(run_meta.get("reschedule_count"), 0),
            "admitted_at": _safe_float(run_meta.get("admitted_at"), 0.0),
            "task_count": len(task_truth),
            "task_truth": task_truth,
            "truth_status": "conflict" if conflicts else "consistent",
            "truth_conflict": bool(conflicts),
            "truth_conflicts": conflicts,
        }

    async def get_run_claim(self, run_id: str) -> dict:
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
        from hfa.runtime.state_store import StateStore

        return await StateStore(self._redis).get_result(run_id)

    async def get_run_status_result(self, run_id: str) -> dict:
        """Return a typed combined status/result view without mutating runtime state."""
        from hfa_control.run_status_read_model import DurableRunStatusResultReader

        return (await DurableRunStatusResultReader(self._redis).read(run_id)).to_dict()

    async def list_stale_runs(self) -> list:
        import time

        stale_ids = await self._recovery._find_stale_runs()
        result = []
        for run_id in stale_ids:
            meta = _decode_mapping(
                await self._redis.hgetall(RedisKey.run_meta(run_id))
            )
            score_raw = await self._redis.zscore(self._config.running_zset, run_id)
            running_since = float(score_raw) if score_raw else 0.0
            result.append(
                {
                    "run_id": run_id,
                    "tenant_id": meta.get("tenant_id", ""),
                    "state": meta.get("state", "") or "unknown",
                    "worker_group": meta.get("worker_group", ""),
                    "reschedule_count": _safe_int(meta.get("reschedule_count"), 0),
                    "running_since": running_since,
                    "stale_for_seconds": round(time.time() - running_since, 1),
                }
            )
        return result

    async def get_recovery_summary(self) -> dict:
        stale_ids = await self._recovery._find_stale_runs()
        dlq_entries = await self._recovery.list_dlq("__all__", limit=1000)
        schedulable = await self._registry.list_schedulable_workers()
        all_alive = await self._registry.list_healthy_workers()
        draining = [worker for worker in all_alive if worker.is_draining]
        return {
            "stale_count": len(stale_ids),
            "dlq_count": len(dlq_entries),
            "schedulable_workers": len(schedulable),
            "draining_workers": len(draining),
        }

    async def list_dlq(self, tenant_id: str = "", limit: int = 50) -> list:
        return await self._recovery.list_dlq(tenant_id or "__all__", limit)


def _config_from_env() -> ControlPlaneConfig:
    return ControlPlaneConfig(
        region=os.environ.get("CP_REGION", "us-east-1"),
        instance_id=os.environ.get("CP_INSTANCE_ID", ""),
        worker_heartbeat_ttl=float(os.environ.get("WORKER_HEARTBEAT_TTL", "30")),
        stale_run_timeout=float(os.environ.get("STALE_RUN_TIMEOUT", "600")),
        recovery_sweep_interval=float(os.environ.get("RECOVERY_SWEEP_INTERVAL", "30")),
        max_reschedule_attempts=int(os.environ.get("MAX_RESCHEDULE_ATTEMPTS", "3")),
        scheduler_reservation_ttl_seconds=int(
            os.environ.get("SCHEDULER_RESERVATION_TTL_SECONDS", "30")
        ),
    )
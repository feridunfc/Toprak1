"""
hfa_control/dag_scheduler_bridge.py
-------------------------------------
Non-destructive ready-queue bridge to the atomic TASK_DISPATCH writer.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import Optional

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey, DagTaskDispatchInput
from hfa_control.dag_lua import TaskDispatchCommitResult


@dataclass(frozen=True)
class DagReadyTask:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str
    priority: int
    admitted_at: float
    payload_json: str
    preferred_region: str
    preferred_placement: str
    trace_parent: str
    trace_state: str


class DagReadyQueue:
    """Non-destructive access to the canonical DAG ready queue."""

    def __init__(self, redis) -> None:
        self._redis = redis

    async def list_active_tenants(self) -> list[str]:
        rows = await self._redis.smembers(
            DagRedisKey.tenant_active_set()
        )
        tenants: set[str] = set()
        for row in rows or ():
            if isinstance(row, bytes):
                try:
                    value = row.decode("utf-8").strip()
                except UnicodeDecodeError:
                    continue
            else:
                value = str(row or "").strip()
            if value:
                tenants.add(value)
        return sorted(tenants)

    async def peek(
        self,
        tenant_id: str,
    ) -> Optional[str]:
        rows = await self._redis.zrange(
            DagRedisKey.task_ready_queue(tenant_id),
            0,
            0,
        )
        if not rows:
            return None
        task_id = rows[0]
        return (
            task_id.decode()
            if isinstance(task_id, bytes)
            else task_id
        )

    async def rebuild_dispatch_input(
        self,
        task_id: str,
        *,
        tenant_id: str = "",
        worker_group: str,
        shard: int,
        running_zset: str = "",
        control_stream: str = "",
        shard_stream: str = "",
        region: str = "",
    ) -> Optional[DagTaskDispatchInput]:
        raw = await self._redis.hgetall(
            DagRedisKey.task_meta(task_id)
        )
        if not raw:
            return None

        def _s(key: str) -> str:
            value = raw.get(key.encode()) or raw.get(key)
            if isinstance(value, bytes):
                return value.decode(
                    "utf-8",
                    errors="replace",
                )
            return (
                str(value)
                if value is not None
                else ""
            )

        def _i(key: str, default: int = 0) -> int:
            try:
                return int(_s(key))
            except (ValueError, TypeError):
                return default

        def _f(
            key: str,
            default: float = 0.0,
        ) -> float:
            try:
                return float(_s(key))
            except (ValueError, TypeError):
                return default

        resolved_tenant = (
            _s("tenant_id") or tenant_id
        )
        now = int(time.time() * 1000)
        requeue_count = _i("requeue_count", 0)
        attempt = (
            requeue_count + 1
            if requeue_count >= 0
            else 1
        )

        return DagTaskDispatchInput(
            task_id=task_id,
            run_id=_s("run_id"),
            tenant_id=resolved_tenant,
            agent_type=_s("agent_type"),
            worker_group=worker_group,
            shard=shard,
            priority=_i("priority", 5),
            admitted_at=_f(
                "admitted_at",
                float(now),
            ),
            scheduled_at=float(now),
            scheduled_zset=(
                DagRedisKey.task_scheduled_zset(
                    resolved_tenant
                )
            ),
            running_zset=(
                running_zset
                or DagRedisKey.task_running_zset(
                    resolved_tenant
                )
            ),
            control_stream=(
                control_stream
                or RedisKey.stream_control()
            ),
            shard_stream=(
                shard_stream
                or RedisKey.stream_shard(shard)
            ),
            region=region,
            policy=(
                _s("policy")
                or _s("preferred_placement")
                or "LEAST_LOADED"
            ),
            trace_parent=_s("trace_parent"),
            trace_state=_s("trace_state"),
            payload_json=(
                _s("payload_json") or "{}"
            ),
            attempt=attempt,
        )


class DagSchedulerDispatchWriter:
    """Validate dispatch identity and invoke DagLua."""

    def __init__(
        self,
        *,
        ready_queue: DagReadyQueue,
        dag_lua,
    ) -> None:
        self._ready_queue = ready_queue
        self._dag_lua = dag_lua

    @staticmethod
    def _failure(
        *,
        task_id: str,
        status: str,
        reason: str,
    ) -> TaskDispatchCommitResult:
        return TaskDispatchCommitResult(
            committed=False,
            status=status,
            task_id=task_id,
            reason=reason,
        )

    async def __call__(
        self,
        *,
        task_id: str,
        worker_id: str,
        scheduler_epoch: str,
        dispatch_payload: dict,
    ) -> TaskDispatchCommitResult:
        payload = (
            dict(dispatch_payload)
            if isinstance(dispatch_payload, dict)
            else {}
        )

        task_id = str(task_id or "").strip()
        worker_id = str(worker_id or "").strip()
        scheduler_epoch = str(
            scheduler_epoch or ""
        ).strip()

        payload_task_id_present = (
            "task_id" in payload
        )
        payload_task_id = str(
            payload.get("task_id") or ""
        ).strip()
        run_id = str(
            payload.get("run_id") or ""
        ).strip()
        payload_epoch_present = (
            "scheduler_epoch" in payload
        )
        payload_scheduler_epoch = str(
            payload.get("scheduler_epoch") or ""
        ).strip()

        if not task_id:
            return self._failure(
                task_id="",
                status="dispatch_task_id_missing",
                reason="explicit_task_id_required",
            )

        if (
            payload_task_id_present
            and payload_task_id != task_id
        ):
            return self._failure(
                task_id=task_id,
                status="dispatch_task_id_mismatch",
                reason=(
                    "payload_task_id_differs_from_"
                    "dispatch_task_id"
                ),
            )

        if not run_id:
            return self._failure(
                task_id=task_id,
                status="dispatch_run_id_missing",
                reason="explicit_run_id_required",
            )

        if not worker_id:
            return self._failure(
                task_id=task_id,
                status="dispatch_worker_id_missing",
                reason="explicit_worker_id_required",
            )

        if (
            not scheduler_epoch
            or scheduler_epoch == "0"
        ):
            return self._failure(
                task_id=task_id,
                status=(
                    "dispatch_scheduler_epoch_missing"
                ),
                reason=(
                    "explicit_scheduler_epoch_required"
                ),
            )

        if (
            payload_epoch_present
            and payload_scheduler_epoch
            != scheduler_epoch
        ):
            return self._failure(
                task_id=task_id,
                status=(
                    "dispatch_scheduler_epoch_mismatch"
                ),
                reason=(
                    "payload_scheduler_epoch_differs_"
                    "from_dispatch_epoch"
                ),
            )

        tenant_id = str(
            payload.get("tenant_id") or ""
        ).strip()
        worker_group = str(
            payload.get("worker_group") or ""
        ).strip()
        region = str(
            payload.get("region") or ""
        ).strip()

        try:
            shard = int(payload.get("shard", 0) or 0)
        except (TypeError, ValueError):
            return self._failure(
                task_id=task_id,
                status="dispatch_shard_invalid",
                reason="shard_must_be_an_integer",
            )

        rebuilt = (
            await self._ready_queue
            .rebuild_dispatch_input(
                task_id,
                tenant_id=tenant_id,
                worker_group=worker_group,
                shard=shard,
                region=region,
            )
        )
        if rebuilt is None:
            return self._failure(
                task_id=task_id,
                status="missing_task_meta",
                reason="task_meta_not_found",
            )

        try:
            attempt = int(
                payload.get(
                    "attempt",
                    rebuilt.attempt,
                )
            )
        except (TypeError, ValueError):
            return self._failure(
                task_id=task_id,
                status="dispatch_attempt_invalid",
                reason="attempt_must_be_a_positive_integer",
            )
        if (
            type(payload.get("attempt", attempt))
            is bool
            or attempt <= 0
        ):
            return self._failure(
                task_id=task_id,
                status="dispatch_attempt_invalid",
                reason="attempt_must_be_a_positive_integer",
            )

        try:
            scheduled_at = int(
                payload.get(
                    "scheduled_at",
                    rebuilt.scheduled_at,
                )
            )
        except (TypeError, ValueError):
            return self._failure(
                task_id=task_id,
                status="dispatch_timestamp_invalid",
                reason=(
                    "scheduled_at_must_be_a_"
                    "non_negative_integer"
                ),
            )
        if (
            type(
                payload.get(
                    "scheduled_at",
                    scheduled_at,
                )
            )
            is bool
            or scheduled_at < 0
        ):
            return self._failure(
                task_id=task_id,
                status="dispatch_timestamp_invalid",
                reason=(
                    "scheduled_at_must_be_a_"
                    "non_negative_integer"
                ),
            )

        dispatch_input = replace(
            rebuilt,
            task_id=task_id,
            run_id=run_id,
            worker_id=worker_id,
            scheduler_epoch=scheduler_epoch,
            tenant_id=(
                tenant_id or rebuilt.tenant_id
            ),
            worker_group=(
                worker_group
                or rebuilt.worker_group
            ),
            shard=shard,
            region=region or rebuilt.region,
            attempt=attempt,
            scheduled_at=scheduled_at,
        )
        return await self._dag_lua.task_dispatch_commit(
            dispatch_input
        )

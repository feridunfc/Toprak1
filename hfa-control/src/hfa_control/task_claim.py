"""
hfa_control/task_claim.py
--------------------------
IRONCLAD Sprint 2 — Task claim service with fence tuple propagation.

Sprint 2 change: TaskClaimResult now exposes claim_epoch and scheduler_epoch
so the execution plane can carry the full fencing tuple to task_complete and
heartbeat calls.
"""
from __future__ import annotations

from dataclasses import dataclass

from hfa_control.dag_lua import (
    DagLua,
    TaskClaimResult,
    TASK_CLAIM_STATUS_RESERVATION_MISSING,
    TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH,
)
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore
from hfa.dag.schema import DagRedisKey

__all__ = ["TaskClaimResult", "TaskClaimService", "TaskClaimManager"]



def _decode_redis(value) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else (str(value) if value is not None else "")


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _get_task_reservation_owner(redis, task_id: str) -> str:
    """
    O(1) task-indexed reservation owner lookup.

    Canonical key:
      DagRedisKey.task_reservation_owner(task_id)

    This replaces the Sprint 55 worker-key compatibility bridge in the claim
    manager boundary. The Lua claim path is still the authority; this helper only
    normalizes old reservation_missing boundaries when an owner index exists.
    """
    key = DagRedisKey.task_reservation_owner(task_id)
    try:
        data = await _maybe_await(redis.hgetall(key))
    except Exception:
        return ""

    normalized = {
        _decode_redis(k): _decode_redis(v)
        for k, v in (data or {}).items()
    }

    indexed_task_id = normalized.get("task_id", "")
    if indexed_task_id and indexed_task_id != task_id:
        return ""

    return normalized.get("worker_id", "")

class TaskClaimService:
    """
    Thin wrapper around DagLua.claim_task() with optional event emission.
    """

    def __init__(self, dag_lua_or_redis=None, event_store=None) -> None:
        if isinstance(dag_lua_or_redis, DagLua):
            self._dag_lua: DagLua = dag_lua_or_redis
        elif dag_lua_or_redis is not None:
            self._dag_lua = DagLua(dag_lua_or_redis)
        else:
            self._dag_lua = None  # type: ignore[assignment]
        self._event_store = event_store

    async def claim(
        self,
        *,
        task_id: str,
        worker_id: str,
        tenant_id: str = "",
        now_ms: int = 0,
    ) -> TaskClaimResult:
        assert self._dag_lua is not None, "DagLua not configured"
        result = await self._dag_lua.claim_task(
            task_id=task_id,
            worker_instance_id=worker_id,
            claimed_at_ms=now_ms,
            tenant_id=tenant_id,
            allow_legacy_direct_claim=True,
        )
        if result.ok:
            emit_event_background(
                self._event_store,
                run_id=task_id,
                event_type=EventStore.EVENT_TASK_CLAIMED,
                worker_id=worker_id,
                details={"tenant_id": tenant_id} if tenant_id else None,
            )
        return result


class TaskClaimManager(TaskClaimService):
    """
    Production claim manager.

    claim_start() calls DagLua.task_claim_start() which executes
    task_claim_start.lua atomically and returns:
      - ok / status
      - claim_epoch    (Sprint 2: new fence generation)
      - scheduler_epoch
      - worker_id

    The caller must store claim_epoch and scheduler_epoch and pass them back
    on every task_complete and heartbeat call.
    """

    async def claim_start(
        self,
        *,
        task_id: str,
        tenant_id: str = "",
        worker_instance_id: str,
        claimed_at_ms: int,
        scheduler_epoch: str = "",
        allow_legacy_direct_claim: bool | None = None,
    ) -> TaskClaimResult:
        assert self._dag_lua is not None, "DagLua not configured"
        result = await self._dag_lua.task_claim_start(
            task_id=task_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            claimed_at_ms=claimed_at_ms,
            scheduler_epoch=scheduler_epoch,
            allow_legacy_direct_claim=allow_legacy_direct_claim,
        )

        # O(1) owner-index normalization:
        # The Lua script is the authority. If an older boundary still reports
        # reservation_missing while a task-indexed owner exists, normalize that
        # case without walking worker reservation keys.
        if (
            not result.ok
            and result.status == TASK_CLAIM_STATUS_RESERVATION_MISSING
            and scheduler_epoch
        ):
            redis = (
                getattr(self._dag_lua, "_redis", None)
                or getattr(self._dag_lua, "redis", None)
                or getattr(self._dag_lua, "_client", None)
            )
            if redis is not None:
                reserved_worker = await _get_task_reservation_owner(redis, task_id)
                if reserved_worker and reserved_worker != worker_instance_id:
                    result = TaskClaimResult(
                        ok=False,
                        status=TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH,
                        task_id=task_id,
                        worker_id=worker_instance_id,
                        claim_epoch="",
                        scheduler_epoch=scheduler_epoch,
                    )

        if result.ok:
            emit_event_background(
                self._event_store,
                run_id=task_id,
                event_type=EventStore.EVENT_TASK_CLAIMED,
                worker_id=worker_instance_id,
                details={"tenant_id": tenant_id, "claim_epoch": result.claim_epoch},
            )
        return result

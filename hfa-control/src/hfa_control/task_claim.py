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

from hfa_control.dag_lua import DagLua, TaskClaimResult
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


async def _find_reservation_worker_for_task(redis, task_id: str) -> str:
    """
    Compatibility-only lookup used after worker-key claim misses.

    This is not the final architecture. The final model should maintain a
    task-indexed reservation owner lookup instead of scanning worker reservations.
    """
    pattern_factory = getattr(DagRedisKey, "worker_reservation_pattern", None)
    pattern = pattern_factory() if callable(pattern_factory) else "hfa:dag:worker:*:reservation"

    async def _iter_keys():
        scan_iter = getattr(redis, "scan_iter", None)
        if scan_iter is not None:
            maybe_iter = await _maybe_await(scan_iter(match=pattern))

            if hasattr(maybe_iter, "__aiter__"):
                async for key in maybe_iter:
                    yield key
                return

            for key in maybe_iter:
                yield key
            return

        keys_fn = getattr(redis, "keys", None)
        if keys_fn is None:
            return

        keys = await _maybe_await(keys_fn(pattern))
        for key in keys:
            yield key

    async for key in _iter_keys():
        try:
            data = await _maybe_await(redis.hgetall(key))
        except Exception:
            continue

        normalized = {
            _decode_redis(k): _decode_redis(v)
            for k, v in (data or {}).items()
        }
        if normalized.get("task_id") == task_id:
            return normalized.get("worker_id", "")

    return ""

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
    ) -> TaskClaimResult:
        assert self._dag_lua is not None, "DagLua not configured"
        result = await self._dag_lua.task_claim_start(
            task_id=task_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            claimed_at_ms=claimed_at_ms,
            scheduler_epoch=scheduler_epoch,
        )

        # Compatibility-only normalization:
        # The Lua script checks the reservation key for the claiming worker.
        # If another worker owns the reservation for the same task, the direct
        # lookup returns reservation_missing. Normalize that boundary case to
        # reservation_worker_mismatch for legacy integration callers.
        if (
            not result.ok
            and result.status == "reservation_missing"
            and scheduler_epoch
        ):
            redis = (
                getattr(self._dag_lua, "_redis", None)
                or getattr(self._dag_lua, "redis", None)
                or getattr(self._dag_lua, "_client", None)
            )
            if redis is not None:
                reserved_worker = await _find_reservation_worker_for_task(redis, task_id)
                if reserved_worker and reserved_worker != worker_instance_id:
                    result = TaskClaimResult(
                        ok=False,
                        status="reservation_worker_mismatch",
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

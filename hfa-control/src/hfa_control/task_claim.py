"""
hfa_control/task_claim.py
-------------------------
Task claim service with fence tuple and explicit RUN identity propagation.
"""
from __future__ import annotations

from hfa_control.dag_lua import (
    DagLua,
    TaskClaimResult,
    TASK_CLAIM_STATUS_RESERVATION_MISSING,
    TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH,
)
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore
from hfa_control.task_claim_authority import (
    TASK_CLAIM_CONFIGURATION_CONFLICT_STATUS,
    TASK_CLAIM_EVIDENCE_CONFLICT_STATUS,
    TASK_CLAIM_PROJECTION_PENDING_STATUS,
    TaskClaimAuthorityBinding,
    TaskClaimAuthorityError,
    parse_task_claim_binding_flag,
    task_claim_status_allows_execution,
)
from hfa.dag.schema import DagRedisKey

__all__ = ["TaskClaimResult", "TaskClaimService", "TaskClaimManager"]


def _decode_redis(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value) if value is not None else ""


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _get_task_reservation_owner(redis, task_id: str) -> str:
    key = DagRedisKey.task_reservation_owner(task_id)
    try:
        data = await _maybe_await(redis.hgetall(key))
    except Exception:
        return ""
    normalized = {
        _decode_redis(key): _decode_redis(value)
        for key, value in (data or {}).items()
    }
    indexed_task_id = normalized.get("task_id", "")
    if indexed_task_id and indexed_task_id != task_id:
        return ""
    return normalized.get("worker_id", "")


def _binding_flag(value: bool | str | None) -> bool:
    if type(value) is bool:
        return value
    return parse_task_claim_binding_flag(value)


class TaskClaimService:
    """Compatibility boundary for legacy direct task claim behavior."""

    def __init__(
        self,
        dag_lua_or_redis=None,
        event_store=None,
        *,
        canonical_task_claim_binding: bool | str | None = False,
        canonical_task_admit_binding: bool = False,
        canonical_task_dispatch_binding: bool = False,
        canonical_authority_store=None,
    ) -> None:
        if isinstance(dag_lua_or_redis, DagLua):
            self._dag_lua: DagLua = dag_lua_or_redis
        elif dag_lua_or_redis is not None:
            self._dag_lua = DagLua(dag_lua_or_redis)
        else:
            self._dag_lua = None  # type: ignore[assignment]
        self._event_store = event_store
        self._canonical_task_claim_binding_enabled = _binding_flag(
            canonical_task_claim_binding
        )
        self._canonical_task_admit_binding_enabled = bool(
            canonical_task_admit_binding
        )
        self._canonical_task_dispatch_binding_enabled = bool(
            canonical_task_dispatch_binding
        )
        if self._canonical_task_claim_binding_enabled and not (
            self._canonical_task_admit_binding_enabled
            and self._canonical_task_dispatch_binding_enabled
        ):
            raise ValueError(
                "HFA_CANONICAL_TASK_CLAIM_BINDING requires both "
                "HFA_CANONICAL_TASK_ADMIT_BINDING and "
                "HFA_CANONICAL_TASK_DISPATCH_BINDING"
            )
        self._canonical_authority_store = canonical_authority_store
        self._task_claim_authority_binding = None

    async def claim(
        self,
        *,
        task_id: str,
        worker_id: str,
        tenant_id: str = "",
        now_ms: int = 0,
    ) -> TaskClaimResult:
        raise RuntimeError(
            "TaskClaimService.claim() is quarantined and must not be used by "
            "canonical runtime paths. Use TaskClaimManager.claim_start() with "
            "explicit run_id + scheduler_epoch, or use "
            "claim_legacy_direct_for_compatibility() only from explicit "
            "test/dev/manual compatibility surfaces."
        )

    async def claim_legacy_direct_for_compatibility(
        self,
        *,
        task_id: str,
        worker_id: str,
        tenant_id: str = "",
        now_ms: int = 0,
        run_id: str = "",
    ) -> TaskClaimResult:
        """Explicit compatibility surface; Lua still validates task/run identity."""
        assert self._dag_lua is not None, "DagLua not configured"
        result = await self._dag_lua.claim_task(
            task_id=task_id,
            run_id=run_id,
            worker_instance_id=worker_id,
            claimed_at_ms=now_ms,
            tenant_id=tenant_id,
            allow_legacy_direct_claim=True,
        )
        if result.ok:
            emit_event_background(
                self._event_store,
                run_id=run_id or task_id,
                event_type=EventStore.EVENT_TASK_CLAIMED,
                worker_id=worker_id,
                details={"tenant_id": tenant_id} if tenant_id else None,
            )
        return result


class TaskClaimManager(TaskClaimService):
    """Production claim manager using the atomic task_claim_start Lua path."""

    def _redis(self):
        assert self._dag_lua is not None, "DagLua not configured"
        return (
            getattr(self._dag_lua, "_redis", None)
            or getattr(self._dag_lua, "redis", None)
            or getattr(self._dag_lua, "_client", None)
        )

    def _canonical_binding(self) -> TaskClaimAuthorityBinding:
        if self._task_claim_authority_binding is None:
            redis = self._redis()
            if redis is None:
                raise TaskClaimAuthorityError(
                    status=TASK_CLAIM_CONFIGURATION_CONFLICT_STATUS,
                    detail="DagLua Redis client is unavailable",
                )
            self._task_claim_authority_binding = TaskClaimAuthorityBinding(
                redis=redis,
                store=self._canonical_authority_store,
            )
        return self._task_claim_authority_binding

    @staticmethod
    def _failed_result(
        *,
        status: str,
        task_id: str,
        worker_instance_id: str,
        scheduler_epoch: str,
    ) -> TaskClaimResult:
        return TaskClaimResult(
            ok=False,
            status=status,
            task_id=task_id,
            worker_id=worker_instance_id,
            claim_epoch="",
            scheduler_epoch=scheduler_epoch,
        )

    async def _claim_start_legacy(
        self,
        *,
        task_id: str,
        tenant_id: str,
        worker_instance_id: str,
        claimed_at_ms: int,
        scheduler_epoch: str,
        run_id: str,
        allow_legacy_direct_claim: bool | None,
    ) -> TaskClaimResult:
        assert self._dag_lua is not None, "DagLua not configured"
        result = await self._dag_lua.task_claim_start(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            claimed_at_ms=claimed_at_ms,
            scheduler_epoch=scheduler_epoch,
            allow_legacy_direct_claim=allow_legacy_direct_claim,
        )

        if (
            not result.ok
            and result.status == TASK_CLAIM_STATUS_RESERVATION_MISSING
            and scheduler_epoch
        ):
            redis = self._redis()
            if redis is not None:
                reserved_worker = await _get_task_reservation_owner(
                    redis, task_id
                )
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
                run_id=run_id or task_id,
                event_type=EventStore.EVENT_TASK_CLAIMED,
                worker_id=worker_instance_id,
                details={
                    "tenant_id": tenant_id,
                    "claim_epoch": result.claim_epoch,
                },
            )
        return result

    async def _claim_start_canonical(
        self,
        *,
        task_id: str,
        tenant_id: str,
        worker_instance_id: str,
        claimed_at_ms: int,
        scheduler_epoch: str,
        run_id: str,
        allow_legacy_direct_claim: bool | None,
    ) -> TaskClaimResult:
        assert self._dag_lua is not None, "DagLua not configured"
        if allow_legacy_direct_claim not in {None, False}:
            return self._failed_result(
                status=TASK_CLAIM_CONFIGURATION_CONFLICT_STATUS,
                task_id=task_id,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
            )
        try:
            prepared = await self._canonical_binding().prepare_claim(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
                claimed_at_ms=claimed_at_ms,
            )
        except TaskClaimAuthorityError as exc:
            return self._failed_result(
                status=exc.status,
                task_id=task_id,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
            )
        except (TypeError, ValueError):
            return self._failed_result(
                status=TASK_CLAIM_EVIDENCE_CONFLICT_STATUS,
                task_id=task_id,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
            )

        try:
            result = await self._dag_lua.task_claim_canonical_projection(
                prepared.projection
            )
        except Exception:
            return self._failed_result(
                status=TASK_CLAIM_PROJECTION_PENDING_STATUS,
                task_id=task_id,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=scheduler_epoch,
            )

        if task_claim_status_allows_execution(result.status):
            emit_event_background(
                self._event_store,
                run_id=run_id,
                event_type=EventStore.EVENT_TASK_CLAIMED,
                worker_id=worker_instance_id,
                details={
                    "tenant_id": tenant_id,
                    "claim_epoch": result.claim_epoch,
                },
            )
        return result

    async def claim_start(
        self,
        *,
        task_id: str,
        tenant_id: str = "",
        worker_instance_id: str,
        claimed_at_ms: int,
        scheduler_epoch: str = "",
        run_id: str = "",
        allow_legacy_direct_claim: bool | None = None,
    ) -> TaskClaimResult:
        if not self._canonical_task_claim_binding_enabled:
            return await self._claim_start_legacy(
                task_id=task_id,
                tenant_id=tenant_id,
                worker_instance_id=worker_instance_id,
                claimed_at_ms=claimed_at_ms,
                scheduler_epoch=scheduler_epoch,
                run_id=run_id,
                allow_legacy_direct_claim=allow_legacy_direct_claim,
            )
        return await self._claim_start_canonical(
            task_id=task_id,
            tenant_id=tenant_id,
            worker_instance_id=worker_instance_id,
            claimed_at_ms=claimed_at_ms,
            scheduler_epoch=scheduler_epoch,
            run_id=run_id,
            allow_legacy_direct_claim=allow_legacy_direct_claim,
        )

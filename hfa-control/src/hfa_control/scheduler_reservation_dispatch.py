from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

from hfa.dag.schema import DagRedisKey
from hfa_control.effect_config import get_dispatch_effect_ttl
from hfa_control.effect_ledger import EffectLedger
from hfa_control.effect_metrics import (
    DUPLICATE_DISPATCH_SUPPRESSED,
    increment,
)
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore

logger = logging.getLogger(__name__)

def _decode(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


@dataclass(frozen=True)
class ReservationDispatchResult:
    ok: bool
    status: str
    worker_id: str = ""
    task_id: str = ""
    run_id: str = ""
    tenant_id: str = ""
    scheduler_epoch: str = ""
    reason: Optional[str] = None
    committed_state: Optional[str] = None
    execution_token: Optional[str] = None
    idempotent_replay: bool = False


SchedulerReservationDispatchResult = ReservationDispatchResult


class SchedulerReservationDispatcher:
    def __init__(
        self,
        reservation_manager,
        dispatch_fn,
        event_store: EventStore | None = None,
        effect_ledger: EffectLedger | None = None,
        redis=None,
    ) -> None:
        self._reservation_manager = reservation_manager
        self._dispatch_fn = dispatch_fn
        self._event_store = event_store
        self._effect_ledger = effect_ledger
        # Redis is retained only for TASK-scoped dispatch-attempt metadata.
        # RUN lifecycle authority/prechecks belong to the canonical transition path.
        self._task_attempt_redis = redis

    @staticmethod
    def _dispatch_token(
        *,
        run_id: str,
        task_id: str,
        attempt: int,
        scheduler_epoch: str,
    ) -> str:
        return (
            f"dispatch:{run_id}:{task_id}:"
            f"attempt:{attempt}:epoch:{scheduler_epoch}"
        )

    async def _resolve_dispatch_attempt(
        self,
        task_id: str,
    ) -> int | None:
        if self._task_attempt_redis is None:
            return 1
        try:
            raw = await self._task_attempt_redis.hget(
                DagRedisKey.task_meta(task_id),
                "requeue_count",
            )
        except Exception as exc:
            logger.warning(
                "TASK_DISPATCH attempt read failed task=%s: %s",
                task_id,
                exc,
            )
            return None
        if raw in (None, b"", ""):
            return 1
        try:
            requeue_count = int(_decode(raw))
        except (TypeError, ValueError):
            return None
        if requeue_count < 0:
            return None
        return requeue_count + 1

    @staticmethod
    def _scheduled_at_ms(value: int | None) -> int | None:
        if value is None:
            return int(time.time() * 1000)
        if type(value) is not int or value < 0:
            return None
        return value

    async def _release_reservation(
        self,
        worker_id: str,
    ) -> None:
        release = getattr(
            self._reservation_manager,
            "release",
            None,
        )
        if release is not None:
            await release(worker_id)

    async def reserve_and_dispatch(
        self,
        *,
        task_id: str,
        worker_id: str,
        scheduler_epoch: str,
        dispatch_payload: dict,
        reserved_at_ms: int | None = None,
    ) -> ReservationDispatchResult:
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

        payload_task_id_present = "task_id" in payload
        payload_task_id = str(
            payload.get("task_id") or ""
        ).strip()
        run_id = str(payload.get("run_id") or "").strip()
        payload_epoch_present = (
            "scheduler_epoch" in payload
        )
        payload_scheduler_epoch = str(
            payload.get("scheduler_epoch") or ""
        ).strip()
        tenant_id = str(
            payload.get("tenant_id", "") or ""
        )

        if not task_id:
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_task_id_missing",
                worker_id=worker_id,
                task_id="",
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason="explicit_task_id_required",
            )

        if not run_id:
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_run_id_missing",
                worker_id=worker_id,
                task_id=task_id,
                run_id="",
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason="explicit_run_id_required",
            )

        if (
            payload_task_id_present
            and payload_task_id != task_id
        ):
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_task_id_mismatch",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason=(
                    "payload_task_id_differs_from_"
                    "dispatch_task_id"
                ),
            )

        if not worker_id:
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_worker_id_missing",
                worker_id="",
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason="explicit_worker_id_required",
            )

        if not scheduler_epoch or scheduler_epoch == "0":
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_scheduler_epoch_missing",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch="",
                reason="explicit_scheduler_epoch_required",
            )

        if (
            payload_epoch_present
            and payload_scheduler_epoch
            != scheduler_epoch
        ):
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_scheduler_epoch_mismatch",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason=(
                    "payload_scheduler_epoch_differs_"
                    "from_dispatch_epoch"
                ),
            )

        attempt = await self._resolve_dispatch_attempt(
            task_id
        )
        if attempt is None:
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_attempt_unavailable",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason="task_requeue_count_invalid_or_unavailable",
            )

        scheduled_at_ms = self._scheduled_at_ms(
            reserved_at_ms
        )
        if scheduled_at_ms is None:
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_timestamp_invalid",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason="reserved_at_ms_must_be_non_negative_int",
            )

        # User payload is not authoritative for attempt or dispatch time.
        payload["attempt"] = attempt
        payload["scheduled_at"] = scheduled_at_ms

        if self._effect_ledger is not None:
            token = self._dispatch_token(
                run_id=run_id,
                task_id=task_id,
                attempt=attempt,
                scheduler_epoch=scheduler_epoch,
            )
            receipt = (
                await self._effect_ledger.acquire_effect(
                    run_id=run_id,
                    token=token,
                    effect_type="dispatch",
                    owner_id=worker_id,
                    ttl_seconds=get_dispatch_effect_ttl(),
                )
            )
            if receipt.duplicate:
                receipt.reason = (
                    "duplicate_dispatch_suppressed"
                )
                increment(DUPLICATE_DISPATCH_SUPPRESSED)
                return ReservationDispatchResult(
                    ok=False,
                    status="duplicate_dispatch_suppressed",
                    worker_id=(
                        receipt.owner_id or worker_id
                    ),
                    task_id=task_id,
                    run_id=run_id,
                    tenant_id=tenant_id,
                    scheduler_epoch=scheduler_epoch,
                    reason=receipt.reason,
                    committed_state=(
                        receipt.committed_state
                    ),
                )

        try:
            reserved = (
                await self._reservation_manager.reserve(
                    worker_id=worker_id,
                    task_id=task_id,
                    scheduler_epoch=scheduler_epoch,
                    reserved_at_ms=scheduled_at_ms,
                )
            )
        except TypeError:
            try:
                reserved = (
                    await self._reservation_manager.reserve(
                        worker_id=worker_id,
                        run_id=task_id,
                        reserved_at_ms=scheduled_at_ms,
                    )
                )
            except TypeError:
                reserved = (
                    await self._reservation_manager.reserve(
                        worker_id,
                        task_id,
                    )
                )

        if not reserved.ok:
            return ReservationDispatchResult(
                ok=False,
                status=reserved.status,
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=getattr(
                    reserved,
                    "scheduler_epoch",
                    scheduler_epoch,
                ),
            )

        exec_token_obj = getattr(
            reserved,
            "execution_token",
            None,
        )
        exec_token_value = (
            exec_token_obj.token
            if exec_token_obj is not None
            else None
        )

        try:
            dispatched = await self._dispatch_fn(
                task_id=task_id,
                worker_id=worker_id,
                scheduler_epoch=scheduler_epoch,
                dispatch_payload=payload,
            )
        except Exception as exc:
            durable = bool(
                getattr(
                    exc,
                    "canonical_commit_durable",
                    False,
                )
            )
            if not durable:
                await self._release_reservation(worker_id)
            return ReservationDispatchResult(
                ok=False,
                status=str(
                    getattr(exc, "status", "")
                    or (
                        "canonical_projection_pending"
                        if durable
                        else "dispatch_failed"
                    )
                ),
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason=str(
                    getattr(exc, "detail", "")
                    or str(exc)
                ),
            )

        if hasattr(dispatched, "committed"):
            dispatch_ok = bool(
                getattr(dispatched, "committed", False)
            )
            dispatch_status = str(
                getattr(dispatched, "status", "") or ""
            )
            dispatch_reason = str(
                getattr(dispatched, "reason", "") or ""
            )
        else:
            dispatch_ok = bool(dispatched)
            dispatch_status = ""
            dispatch_reason = ""

        if not dispatch_ok:
            await self._release_reservation(worker_id)
            return ReservationDispatchResult(
                ok=False,
                status=(
                    dispatch_status or "dispatch_failed"
                ),
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason=dispatch_reason or None,
            )

        replay = dispatch_status == "already_projected"
        result = ReservationDispatchResult(
            ok=True,
            status=(
                "already_projected"
                if replay
                else "reserved_and_dispatched"
            ),
            worker_id=worker_id,
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            scheduler_epoch=scheduler_epoch,
            committed_state="scheduled",
            execution_token=exec_token_value,
            idempotent_replay=replay,
        )
        if not replay:
            emit_event_background(
                self._event_store,
                run_id=result.run_id,
                event_type=(
                    EventStore.EVENT_TASK_SCHEDULED
                ),
                worker_id=result.worker_id,
                details={
                    "tenant_id": result.tenant_id,
                    "task_id": result.task_id,
                    "scheduler_epoch": scheduler_epoch,
                    "dispatch_attempt": attempt,
                },
            )
        return result

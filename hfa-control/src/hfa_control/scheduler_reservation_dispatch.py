
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from hfa_control.effect_config import get_dispatch_effect_ttl
from hfa_control.effect_ledger import EffectLedger
from hfa_control.effect_metrics import DUPLICATE_DISPATCH_SUPPRESSED, increment
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore

logger = logging.getLogger(__name__)

# OCC: only dispatch if the run is in one of these states.
# Prevents double-assignment if a stale scheduler attempts to dispatch a run
# that was already picked up by a fresher scheduling decision.
_DISPATCHABLE_STATES = frozenset({"queued", "admitted"})


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
    # Execution ownership token value (hex string).
    # Populated on successful dispatch. Workers validate this before executing.
    execution_token: Optional[str] = None


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
        # Optional Redis client for OCC pre-check.
        # If not provided, OCC is skipped (safe degraded behaviour).
        self._redis = redis

    @staticmethod
    def _dispatch_token(
        *, run_id: str, task_id: str, attempt: int, scheduler_epoch: str
    ) -> str:
        return f"dispatch:{run_id}:{task_id}:attempt:{attempt}:epoch:{scheduler_epoch}"

    # ── OCC helpers ───────────────────────────────────────────────────────────

    async def _check_run_state_is_dispatchable(self, run_id: str) -> Optional[str]:
        """
        OCC pre-check: verify the run is still in a dispatchable state.

        Returns the current state string if dispatchable, or None if:
          - state is not in _DISPATCHABLE_STATES (already scheduled/running/done)
          - Redis read fails (safe: skip OCC, proceed with dispatch)
          - Redis client not available

        This is a best-effort check — the authoritative commit is the CAS
        transition in dispatch_commit / SchedulerLua. This check exits early
        to avoid unnecessary reservation attempts on obviously stale decisions.
        """
        if self._redis is None:
            # No Redis client injected — skip OCC, rely on downstream CAS
            return "queued"

        try:
            from hfa.config.keys import RedisKey
            state_key = RedisKey.run_state(run_id)
        except Exception:
            # hfa.config.keys unavailable — skip OCC
            return "queued"

        try:
            raw = await self._redis.get(state_key)
            state = (raw.decode() if isinstance(raw, bytes) else raw) or ""
            if state in _DISPATCHABLE_STATES:
                return state
            logger.info(
                "SchedulerReservationDispatcher: OCC pre-check rejected run=%s "
                "state=%s (not in %s)",
                run_id,
                state,
                _DISPATCHABLE_STATES,
            )
            return None
        except Exception as exc:
            logger.warning(
                "SchedulerReservationDispatcher: OCC pre-check failed for run=%s: %s "
                "— proceeding without pre-check",
                run_id,
                exc,
            )
            # Redis error: do not block dispatch — downstream CAS is the safety net
            return "queued"

    # ── Main dispatch path ────────────────────────────────────────────────────

    async def reserve_and_dispatch(
        self,
        *,
        task_id: str,
        worker_id: str,
        scheduler_epoch: str,
        dispatch_payload: dict,
        reserved_at_ms: int | None = None,
    ) -> ReservationDispatchResult:
        task_id = str(task_id or "").strip()
        scheduler_epoch = str(scheduler_epoch or "").strip()

        payload_task_id_present = "task_id" in dispatch_payload
        payload_task_id = str(
            dispatch_payload.get("task_id") or ""
        ).strip()

        run_id = str(
            dispatch_payload.get("run_id") or ""
        ).strip()

        payload_epoch_present = (
            "scheduler_epoch" in dispatch_payload
        )
        payload_scheduler_epoch = str(
            dispatch_payload.get("scheduler_epoch") or ""
        ).strip()
        tenant_id = str(dispatch_payload.get("tenant_id", ""))
        attempt = int(dispatch_payload.get("attempt", 1) or 1)

        # Sprint 77 canonical dispatch identity boundary.
        # task_id and scheduler_epoch are authoritative arguments.
        # run_id must be supplied explicitly; inference is forbidden.
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

        if payload_task_id_present and payload_task_id != task_id:
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_task_id_mismatch",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason="payload_task_id_differs_from_dispatch_task_id",
            )

        if not scheduler_epoch:
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
            and payload_scheduler_epoch != scheduler_epoch
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
                    "payload_scheduler_epoch_differs_from_dispatch_epoch"
                ),
            )

        # ── Step 1: OCC pre-check ─────────────────────────────────────────
        # Verify run is still queued before spending a reservation slot.
        # This is NOT the authoritative guard — downstream CAS is.
        current_state = await self._check_run_state_is_dispatchable(run_id)
        if current_state is None:
            return ReservationDispatchResult(
                ok=False,
                status="occ_state_conflict",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason="run_not_in_dispatchable_state",
            )

        # ── Step 2: Idempotency / duplicate suppression ───────────────────
        if self._effect_ledger is not None:
            token = self._dispatch_token(
                run_id=run_id,
                task_id=task_id,
                attempt=attempt,
                scheduler_epoch=scheduler_epoch,
            )
            receipt = await self._effect_ledger.acquire_effect(
                run_id=run_id,
                token=token,
                effect_type="dispatch",
                owner_id=worker_id,
                ttl_seconds=get_dispatch_effect_ttl(),
            )
            if receipt.duplicate:
                receipt.reason = "duplicate_dispatch_suppressed"
                increment(DUPLICATE_DISPATCH_SUPPRESSED)
                return ReservationDispatchResult(
                    ok=False,
                    status="duplicate_dispatch_suppressed",
                    worker_id=receipt.owner_id or worker_id,
                    task_id=task_id,
                    run_id=run_id,
                    tenant_id=tenant_id,
                    scheduler_epoch=scheduler_epoch,
                    reason=receipt.reason,
                    committed_state=receipt.committed_state,
                )

        # ── Step 3: Worker reservation + execution token generation ───────
        try:
            reserved = await self._reservation_manager.reserve(
                worker_id=worker_id,
                task_id=task_id,
                scheduler_epoch=scheduler_epoch,
                reserved_at_ms=reserved_at_ms,
            )
        except TypeError:
            try:
                reserved = await self._reservation_manager.reserve(
                    worker_id=worker_id,
                    run_id=task_id,
                    reserved_at_ms=reserved_at_ms,
                )
            except TypeError:
                reserved = await self._reservation_manager.reserve(worker_id, task_id)

        if not reserved.ok:
            return ReservationDispatchResult(
                ok=False,
                status=reserved.status,
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=getattr(reserved, "scheduler_epoch", scheduler_epoch),
            )

        # Extract execution token from reservation result (set by WorkerReservationManager)
        exec_token_obj = getattr(reserved, "execution_token", None)
        exec_token_value = exec_token_obj.token if exec_token_obj is not None else None

        # ── Step 4: Dispatch ──────────────────────────────────────────────
        dispatched = await self._dispatch_fn(
            task_id=task_id,
            worker_id=worker_id,
            scheduler_epoch=scheduler_epoch,
            dispatch_payload=dispatch_payload,
        )

        # Legacy callbacks return bool; canonical writers return a
        # structured result carrying committed/status/reason.
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
            release = getattr(self._reservation_manager, "release", None)
            if release is not None:
                await release(worker_id)
            return ReservationDispatchResult(
                ok=False,
                status=dispatch_status or "dispatch_failed",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
                reason=dispatch_reason or None,
            )

        result = ReservationDispatchResult(
            ok=True,
            status="reserved_and_dispatched",
            worker_id=worker_id,
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            scheduler_epoch=scheduler_epoch,
            committed_state="scheduled",
            execution_token=exec_token_value,
        )
        emit_event_background(
            self._event_store,
            run_id=result.run_id,
            event_type=EventStore.EVENT_TASK_SCHEDULED,
            worker_id=result.worker_id,
            details={
                "tenant_id": result.tenant_id,
                "task_id": result.task_id,
                "scheduler_epoch": scheduler_epoch,
            },
        )
        return result

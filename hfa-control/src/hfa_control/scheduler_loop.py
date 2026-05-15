from __future__ import annotations

import logging
import os
from typing import Any

from hfa.events.append_service import (
    AuthoritativeEventAppendError,
    AuthoritativeEventGate,
)
from hfa.state import transition_state
from hfa_control.backpressure import BackpressureGuard
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore

logger = logging.getLogger(__name__)

_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}


def is_scheduler_commit_seal_enabled() -> bool:
    return os.getenv("IRON_V3_SCHEDULER_SEAL", "0") not in _FALSE_VALUES


def is_proof_enforcement_enabled() -> bool:
    return os.getenv("IRON_V3_PROOF_ENFORCEMENT", "0") not in _FALSE_VALUES


_SCHEDULER_EPOCH_KEY = "hfa:scheduler:epoch"


class SchedulerLoop:
    """Persistent-state scheduler loop with event hooks and single commit authority."""

    def __init__(self, *args, **kwargs) -> None:
        if kwargs:
            self._dispatch_controller = kwargs.get("dispatch_controller")
            self._snapshot_builder = kwargs.get("snapshot_builder")
            self._worker_scorer = kwargs.get("worker_scorer")
            self._tenant_fairness = kwargs.get("tenant_fairness")
            self._config = kwargs.get("config")
            self._event_store = kwargs.get("event_store")
        else:
            self._dispatch_controller = args[7] if len(args) > 7 else None
            self._snapshot_builder = args[5] if len(args) > 5 else None
            self._worker_scorer = args[6] if len(args) > 6 else None
            self._tenant_fairness = args[4] if len(args) > 4 else None
            self._config = args[9] if len(args) > 9 else (args[-1] if args else None)
            self._event_store = None
        self._bp_guard = BackpressureGuard(self._config)
        self._epoch: str = "0"
        self._local_quarantine: dict[str, str] = {}

    async def _increment_epoch(self) -> str:
        redis = getattr(self._dispatch_controller, "redis", None)
        if redis is not None:
            try:
                # AUTHORITY_REVIEWED_LEASE_COUNTER:
                # Scheduler epoch is a fencing/lease counter, not run lifecycle truth.
                # It only scopes scheduler ownership decisions and is observable in TASK_SCHEDULED details.
                epoch_int = await redis.incr(_SCHEDULER_EPOCH_KEY)
                self._epoch = str(epoch_int)
                logger.info("SchedulerLoop: epoch incremented to %s", self._epoch)
                return self._epoch
            except Exception as exc:
                logger.warning(
                    "SchedulerLoop: epoch increment failed (Redis error): %s — using local fallback",
                    exc,
                )
        try:
            self._epoch = str(int(self._epoch) + 1)
        except ValueError:
            self._epoch = "1"
        logger.warning("SchedulerLoop: using local epoch fallback: %s", self._epoch)
        return self._epoch

    @property
    def current_epoch(self) -> str:
        return self._epoch

    async def on_leadership_gained(self) -> None:
        await self._increment_epoch()
        reset = getattr(self._tenant_fairness, "reset", None)
        if callable(reset):
            reset()
        initialise = getattr(self._dispatch_controller, "initialise", None)
        if initialise is not None:
            result = initialise()
            if hasattr(result, "__await__"):
                await result
        self._bp_guard.reset()

    async def on_leadership_lost(self) -> None:
        reset = getattr(self._tenant_fairness, "reset", None)
        if callable(reset):
            reset()
        self._bp_guard.reset()

    async def _quarantine_run(self, run_id: str, reason: str) -> None:
        """Mark a run as quarantined for scheduler enforcement.

        Sprint 6 keeps the existing method but gives it an enforced projection
        when the proof flag is enabled.  The in-memory marker keeps tests and
        degraded mode deterministic; the Redis marker lets workers observe the
        same quarantine boundary.
        """
        self._local_quarantine[run_id] = reason
        redis = getattr(self._dispatch_controller, "redis", None)
        if is_proof_enforcement_enabled() and redis is not None:
            try:
                if self._event_store is not None:
                    await AuthoritativeEventGate(self._event_store, enabled=True).append_before_authoritative_write(
                        run_id=run_id,
                        event_type="RUN_QUARANTINED",
                        worker_id=None,
                        details={
                            "run_id": run_id,
                            "reason": reason or "quarantined",
                            "scheduler_epoch": self._epoch,
                            "proof_enforcement": "IRON_V3_PROOF_ENFORCEMENT",
                        },
                        authority="SchedulerLoop.quarantine",
                    )
                # AUTHORITY_REVIEWED_PROJECTION_WRITE:
                # Quarantine Redis keys are enforced projections after RUN_QUARANTINED proof/event.
                await redis.set(f"hfa:quarantine:{run_id}", reason or "quarantined")
                # AUTHORITY_REVIEWED_PROJECTION_WRITE:
                # Compatibility quarantine marker; event/proof remains RUN_QUARANTINED.
                await redis.set(f"hfa:run:{run_id}:quarantined", "1")
            except AuthoritativeEventAppendError as exc:
                logger.error("SchedulerLoop quarantine event append blocked marker: run_id=%s error=%s", run_id, exc)
                return
            except Exception as exc:
                logger.error("SchedulerLoop quarantine marker failed: run_id=%s error=%s", run_id, exc)
        logger.warning("SchedulerLoop quarantine: run_id=%s reason=%s", run_id, reason)

    async def _is_run_quarantined(self, run_id: str) -> bool:
        if run_id in self._local_quarantine:
            return True
        redis = getattr(self._dispatch_controller, "redis", None)
        if redis is None:
            return False
        for key in (f"hfa:quarantine:{run_id}", f"hfa:run:{run_id}:quarantined"):
            exists = getattr(redis, "exists", None)
            if callable(exists):
                try:
                    if await exists(key):
                        return True
                except TypeError:
                    if exists(key):
                        return True
        get = getattr(redis, "get", None)
        if callable(get):
            state = await get(f"hfa:run:{run_id}:state")
            if isinstance(state, bytes):
                state = state.decode("utf-8")
            if state in {"quarantined", "blocked"}:
                return True
        return False

    async def commit_dispatch(
        self,
        run_id: str,
        *,
        expected_state: str = "queued",
        target_state: str = "scheduled",
    ) -> Any:
        if is_proof_enforcement_enabled() and await self._is_run_quarantined(run_id):
            logger.warning("SchedulerLoop blocked quarantined dispatch: run_id=%s", run_id)
            return False
        redis = getattr(self._dispatch_controller, "redis", None)
        if redis is None:
            return None
        return await transition_state(
            redis,
            run_id=run_id,
            expected_state=expected_state,
            target_state=target_state,
            state_key=run_id,
        )

    def _scheduled_event_details(self, *, tenant_id: Any | None = None) -> dict[str, Any]:
        details = {"scheduler_epoch": self._epoch}
        if tenant_id:
            details["tenant_id"] = tenant_id
        return details

    async def _seal_scheduler_commit(
        self,
        *,
        run_id: str,
        worker_id: str | None,
        details: dict[str, Any],
    ) -> bool:
        if not is_scheduler_commit_seal_enabled():
            emit_event_background(
                self._event_store,
                run_id=run_id,
                event_type=EventStore.EVENT_TASK_SCHEDULED,
                worker_id=worker_id,
                details=details,
            )
            return True

        try:
            await AuthoritativeEventGate(self._event_store, enabled=True).append_before_authoritative_write(
                run_id=run_id,
                event_type=EventStore.EVENT_TASK_SCHEDULED,
                worker_id=worker_id,
                details={**details, "event_gate": "IRON_V3_SCHEDULER_SEAL"},
                authority="SchedulerLoop.dispatch_commit",
            )
        except AuthoritativeEventAppendError as exc:
            logger.error(
                "SchedulerLoop blocked authoritative dispatch: run_id=%s worker_id=%s reason=%s",
                run_id,
                worker_id,
                exc,
            )
            return False
        return True

    async def _dispatch_once(self, snapshot: Any) -> bool:
        controller = self._dispatch_controller
        if controller is None:
            return False

        for name in ("dispatch_once", "try_dispatch_once", "run_once"):
            fn = getattr(controller, name, None)
            if fn is None:
                continue
            result = fn(snapshot=snapshot, worker_scorer=self._worker_scorer, scheduler_epoch=self._epoch)
            if hasattr(result, "__await__"):
                result = await result
            if isinstance(result, dict):
                run_id = result.get("run_id")
                worker_id = result.get("worker_id")
                tenant_id = result.get("tenant_id")
                if run_id:
                    if is_proof_enforcement_enabled() and await self._is_run_quarantined(str(run_id)):
                        logger.warning("SchedulerLoop suppressed dispatch for quarantined run=%s", run_id)
                        return False
                    sealed = await self._seal_scheduler_commit(
                        run_id=str(run_id),
                        worker_id=str(worker_id) if worker_id else None,
                        details=self._scheduled_event_details(tenant_id=tenant_id),
                    )
                    if not sealed:
                        return False
            return bool(result)
        return False

    async def run_cycle(self, max_dispatches: int | None = None) -> int:
        snapshot = await self._snapshot_builder.build_capacity_snapshot()
        decision = self._bp_guard.evaluate(
            inflight=getattr(snapshot, "total_inflight", 0),
            capacity=max(getattr(snapshot, "total_capacity", 0), 1),
            saturation=getattr(snapshot, "saturation", None),
            max_dispatches_requested=int(max_dispatches or getattr(snapshot, "max_dispatches_this_cycle", 0) or 0),
        )
        if decision.throttled:
            logger.info("SchedulerLoop throttled: reason=%s saturation=%.4f", decision.reason, decision.saturation)
            return 0

        permit = await self._dispatch_controller.current_permit()
        if not permit.allowed:
            return 0

        budget = int(max_dispatches or getattr(permit, "max_dispatches", 0) or 0)
        dispatched = 0
        for _ in range(max(budget, 0)):
            ok = await self._dispatch_once(snapshot)
            if not ok:
                break
            dispatched += 1
            consume = getattr(self._dispatch_controller, "try_consume", None)
            if consume is not None:
                consumed = consume(1)
                if hasattr(consumed, "__await__"):
                    await consumed
        return dispatched

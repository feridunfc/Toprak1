
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
    """Return True when scheduler commits require durable scheduled-event append."""
    return os.getenv("IRON_V3_SCHEDULER_SEAL", "0") not in _FALSE_VALUES


# Redis key for the persistent, monotonically-increasing scheduler epoch counter.
# Incremented each time this scheduler instance acquires leadership.
# Attached to every dispatch decision so workers can detect stale assignments.
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
        # Current scheduler epoch — set on leadership gain, attached to all dispatches.
        # "0" means epoch has not been initialised yet (pre-leadership or test mode).
        self._epoch: str = "0"

    # ── Scheduler Epoch ───────────────────────────────────────────────────────

    async def _increment_epoch(self) -> str:
        """
        Atomically increment the global scheduler epoch counter in Redis.

        Uses INCR which is atomic — safe for concurrent scheduler instances.
        The returned epoch string is stored in self._epoch and attached to
        every subsequent dispatch decision.

        If Redis is unavailable, falls back to a local monotonic value so the
        rest of the scheduler can continue operating in degraded mode.
        """
        redis = getattr(self._dispatch_controller, "redis", None)
        if redis is not None:
            try:
                epoch_int = await redis.incr(_SCHEDULER_EPOCH_KEY)
                self._epoch = str(epoch_int)
                logger.info("SchedulerLoop: epoch incremented to %s", self._epoch)
                return self._epoch
            except Exception as exc:
                logger.warning(
                    "SchedulerLoop: epoch increment failed (Redis error): %s — using local fallback",
                    exc,
                )
        # Fallback: use a local counter derived from the previous epoch value
        try:
            self._epoch = str(int(self._epoch) + 1)
        except ValueError:
            self._epoch = "1"
        logger.warning("SchedulerLoop: using local epoch fallback: %s", self._epoch)
        return self._epoch

    @property
    def current_epoch(self) -> str:
        """Current scheduler epoch. Attached to all dispatch decisions."""
        return self._epoch

    # ── Leadership Lifecycle ──────────────────────────────────────────────────

    async def on_leadership_gained(self) -> None:
        # Increment epoch so any in-flight decisions from the previous leader
        # (or previous incarnation of this leader) are invalidated.
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

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _quarantine_run(self, run_id: str, reason: str) -> None:
        logger.warning("SchedulerLoop quarantine: run_id=%s reason=%s", run_id, reason)

    async def commit_dispatch(
        self,
        run_id: str,
        *,
        expected_state: str = "queued",
        target_state: str = "scheduled",
    ) -> Any:
        """
        OCC state transition: queued → scheduled.

        Uses expected_state as a compare-and-set precondition.
        Returns None (no-op) if Redis is unavailable.
        Returns falsy if the CAS fails (another scheduler already committed).
        """
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
        """Append the durable scheduled event before a dispatch is authoritative.

        With IRON_V3_SCHEDULER_SEAL disabled, preserve legacy background
        emission behavior. With the flag enabled, the scheduler fails closed:
        a dispatch result is not counted as authoritative unless the scheduled
        event append succeeds.
        """
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
            await AuthoritativeEventGate(
                self._event_store,
                enabled=True,
            ).append_before_authoritative_write(
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
            # Pass current epoch to dispatch so it is propagated into
            # reservation and token generation downstream.
            result = fn(
                snapshot=snapshot,
                worker_scorer=self._worker_scorer,
                scheduler_epoch=self._epoch,
            )
            if hasattr(result, "__await__"):
                result = await result
            if isinstance(result, dict):
                run_id = result.get("run_id")
                worker_id = result.get("worker_id")
                tenant_id = result.get("tenant_id")
                if run_id:
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
            max_dispatches_requested=int(
                max_dispatches
                or getattr(snapshot, "max_dispatches_this_cycle", 0)
                or 0
            ),
        )
        if decision.throttled:
            logger.info(
                "SchedulerLoop throttled: reason=%s saturation=%.4f",
                decision.reason,
                decision.saturation,
            )
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

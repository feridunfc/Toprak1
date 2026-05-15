"""Worker effect runtime boundary for IRONCLAD v3 Sprint 5.

This module is intentionally small and additive.  It does not replace the
legacy worker consumer.  It provides a feature-flagged effect execution helper
that makes the worker plane explicit: workers may execute effects, but they do
not author terminal truth and they must not report/finalize an effect before the
event path acknowledges it.

Rollback:
    Set IRON_V3_WORKER_EFFECT_HYBRID to a false value.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from hfa.events.append_service import AuthoritativeEventAppendError, AuthoritativeEventGate
from hfa_control.effect_ledger import (
    EFFECT_COMMITTED,
    EFFECT_FAILED,
    EFFECT_REQUESTED,
    EFFECT_SUPPRESSED,
    EffectLedger,
    EffectReceipt,
)

logger = logging.getLogger(__name__)

_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}


def is_worker_effect_hybrid_enabled() -> bool:
    """Return True when Sprint 5 worker/effect closure is active."""
    return os.getenv("IRON_V3_WORKER_EFFECT_HYBRID", "0") not in _FALSE_VALUES


class EventStoreLike(Protocol):
    async def append_event(
        self,
        *,
        run_id: str,
        event_type: str,
        worker_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> bool: ...


@dataclass(frozen=True)
class WorkerEffectDecision:
    """Observable outcome of one worker-side effect attempt."""

    run_id: str
    worker_id: str
    token: str
    executed: bool
    event_acknowledged: bool
    finalized: bool
    status: str
    reason: str = ""
    receipt: EffectReceipt | None = None
    emitted_events: tuple[str, ...] = field(default_factory=tuple)
    output: Any = None
    error: str = ""


class WorkerRuntime:
    """Feature-flagged worker effect executor.

    In enabled mode, the runtime:
    * emits EFFECT_REQUESTED before execution,
    * rejects quarantined runs before execution,
    * uses EffectLedger to suppress duplicates when provided,
    * emits EFFECT_COMMITTED/EFFECT_FAILED after execution,
    * never marks terminal truth itself.

    The caller remains responsible for any legacy behavior when the feature flag
    is disabled.  This helper exists so worker consumers can close the effect
    loop without becoming the authority for terminal state.
    """

    def __init__(
        self,
        *,
        event_store: EventStoreLike | None,
        effect_ledger: EffectLedger | None = None,
        enabled: bool | None = None,
    ) -> None:
        self._event_gate = AuthoritativeEventGate(
            event_store,
            enabled=is_worker_effect_hybrid_enabled() if enabled is None else enabled,
        )
        self._effect_ledger = effect_ledger

    @property
    def enabled(self) -> bool:
        return self._event_gate.enabled

    async def is_quarantined(self, redis: Any, run_id: str) -> bool:
        """Best-effort quarantine read used before executing an effect.

        The scheduler is still the quarantine authority.  The worker only reads
        visible quarantine/projection markers and refuses to execute when any
        marker is present.
        """
        if redis is None:
            return False

        keys = (
            f"hfa:run:{run_id}:quarantined",
            f"hfa:quarantine:{run_id}",
        )
        for key in keys:
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

    async def execute_effect(
        self,
        *,
        run_id: str,
        worker_id: str,
        token: str,
        effect: Callable[[], Awaitable[Any]],
        redis: Any = None,
        effect_type: str = "worker",
    ) -> WorkerEffectDecision:
        if not self.enabled:
            output = await effect()
            return WorkerEffectDecision(
                run_id=run_id,
                worker_id=worker_id,
                token=token,
                executed=True,
                event_acknowledged=False,
                finalized=True,
                status="legacy_executed",
                reason="worker_effect_hybrid_disabled",
                output=output,
            )

        if await self.is_quarantined(redis, run_id):
            await self._append_effect_event(
                run_id=run_id,
                worker_id=worker_id,
                event_type=EFFECT_SUPPRESSED,
                details={"token": token, "reason": "quarantined_run_rejected"},
            )
            return WorkerEffectDecision(
                run_id=run_id,
                worker_id=worker_id,
                token=token,
                executed=False,
                event_acknowledged=True,
                finalized=False,
                status="suppressed",
                reason="quarantined_run_rejected",
                emitted_events=(EFFECT_SUPPRESSED,),
            )

        await self._append_effect_event(
            run_id=run_id,
            worker_id=worker_id,
            event_type=EFFECT_REQUESTED,
            details={"token": token, "effect_type": effect_type},
        )
        emitted = [EFFECT_REQUESTED]

        receipt: EffectReceipt | None = None
        if self._effect_ledger is not None:
            receipt = await self._effect_ledger.acquire_effect(
                run_id=run_id,
                token=token,
                effect_type=effect_type,
                owner_id=worker_id,
            )
            if not receipt.accepted:
                receipt.reason = receipt.reason or "duplicate_effect_suppressed"
                await self._append_effect_event(
                    run_id=run_id,
                    worker_id=worker_id,
                    event_type=EFFECT_SUPPRESSED,
                    details={"token": token, "reason": receipt.reason},
                )
                emitted.append(EFFECT_SUPPRESSED)
                return WorkerEffectDecision(
                    run_id=run_id,
                    worker_id=worker_id,
                    token=token,
                    executed=False,
                    event_acknowledged=True,
                    finalized=False,
                    status="suppressed",
                    reason=receipt.reason,
                    receipt=receipt,
                    emitted_events=tuple(emitted),
                )

        try:
            output = await effect()
        except Exception as exc:
            try:
                await self._append_effect_event(
                    run_id=run_id,
                    worker_id=worker_id,
                    event_type=EFFECT_FAILED,
                    details={"token": token, "error": str(exc)},
                )
            except AuthoritativeEventAppendError:
                logger.exception("worker effect failed and failure event was not acknowledged")
                return WorkerEffectDecision(
                    run_id=run_id,
                    worker_id=worker_id,
                    token=token,
                    executed=True,
                    event_acknowledged=False,
                    finalized=False,
                    status="unacknowledged_failure",
                    reason="effect_failed_event_not_acknowledged",
                    receipt=receipt,
                    emitted_events=tuple(emitted),
                    error=str(exc),
                )
            emitted.append(EFFECT_FAILED)
            return WorkerEffectDecision(
                run_id=run_id,
                worker_id=worker_id,
                token=token,
                executed=True,
                event_acknowledged=True,
                finalized=False,
                status="failed",
                reason="effect_failed_visible",
                receipt=receipt,
                emitted_events=tuple(emitted),
                error=str(exc),
            )

        try:
            await self._append_effect_event(
                run_id=run_id,
                worker_id=worker_id,
                event_type=EFFECT_COMMITTED,
                details={"token": token, "effect_type": effect_type},
            )
        except AuthoritativeEventAppendError:
            logger.exception("worker effect committed locally but event was not acknowledged")
            return WorkerEffectDecision(
                run_id=run_id,
                worker_id=worker_id,
                token=token,
                executed=True,
                event_acknowledged=False,
                finalized=False,
                status="unacknowledged_commit",
                reason="effect_commit_event_not_acknowledged",
                receipt=receipt,
                emitted_events=tuple(emitted),
                output=output,
            )

        emitted.append(EFFECT_COMMITTED)
        return WorkerEffectDecision(
            run_id=run_id,
            worker_id=worker_id,
            token=token,
            executed=True,
            event_acknowledged=True,
            finalized=False,
            status="committed_event_visible",
            reason="worker_does_not_author_terminal_truth",
            receipt=receipt,
            emitted_events=tuple(emitted),
            output=output,
        )

    async def _append_effect_event(
        self,
        *,
        run_id: str,
        worker_id: str,
        event_type: str,
        details: dict[str, Any],
    ) -> None:
        await self._event_gate.append_before_authoritative_write(
            run_id=run_id,
            event_type=event_type,
            worker_id=worker_id,
            details=details,
            authority="WorkerRuntime.effect",
        )

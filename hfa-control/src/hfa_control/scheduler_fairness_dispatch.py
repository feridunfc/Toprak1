
from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from hfa_control.fairness_manager import FairnessManager
from hfa_control.scheduler_reservation_dispatch import (
    ReservationDispatchResult,
    SchedulerReservationDispatcher,
)


@dataclass(frozen=True)
class FairDispatchCandidate:
    tenant_id: str
    task_id: str
    worker_id: str
    dispatch_payload: dict


@dataclass(frozen=True)
class SchedulerFairnessDecision:
    ok: bool
    status: str
    chosen_tenant_id: str = ""
    chosen_task_id: str = ""
    chosen_worker_id: str = ""


class SchedulerFairnessDispatcher:
    """
    Faz 3C:
    Integrates fairness selection with reservation-aware dispatch.

    Decision order:
    1. choose fairest tenant
    2. dispatch selected tenant/task to selected worker through reservation-aware dispatcher
    """

    def __init__(
        self,
        fairness_manager: FairnessManager,
        reservation_dispatcher: SchedulerReservationDispatcher,
    ) -> None:
        self._fairness_manager = fairness_manager
        self._reservation_dispatcher = reservation_dispatcher

    async def choose_and_dispatch(
        self,
        *,
        candidates: list[FairDispatchCandidate],
        scheduler_epoch: str,
        weights: dict[str, float] | None = None,
        reserved_at_ms: int | None = None,
    ) -> SchedulerFairnessDecision:
        if not candidates:
            return SchedulerFairnessDecision(ok=False, status="no_candidates")

        # pick fairest tenant first
        tenant_ids = sorted({c.tenant_id for c in candidates})
        chosen_snapshot = await self._fairness_manager.choose_fairest_tenant(
            tenant_ids,
            weights=weights or {},
        )

        # deterministic tie-break among same-tenant candidates by task_id then worker_id
        tenant_candidates = sorted(
            [c for c in candidates if c.tenant_id == chosen_snapshot.tenant_id],
            key=lambda c: (c.task_id, c.worker_id),
        )
        chosen = tenant_candidates[0]

        result = await self._reservation_dispatcher.reserve_and_dispatch(
            task_id=chosen.task_id,
            worker_id=chosen.worker_id,
            scheduler_epoch=scheduler_epoch,
            dispatch_payload=chosen.dispatch_payload,
            reserved_at_ms=reserved_at_ms,
        )

        return SchedulerFairnessDecision(
            ok=result.ok,
            status=result.status,
            chosen_tenant_id=chosen.tenant_id,
            chosen_task_id=chosen.task_id,
            chosen_worker_id=chosen.worker_id,
        )

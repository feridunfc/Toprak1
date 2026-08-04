from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from typing import Any, Mapping

from hfa_control.scheduler_capability_selector import (
    SchedulerCapabilitySelector,
    WorkerCandidate,
)
from hfa_control.scheduler_scoring import SchedulerScoring, ScoringCandidate


@dataclass(frozen=True)
class DagDispatchOnceResult:
    """Structured result for one canonical DAG dispatch attempt."""

    dispatched: bool
    status: str
    task_id: str = ""
    run_id: str = ""
    worker_id: str = ""
    reason: str = ""

    def __bool__(self) -> bool:
        return self.dispatched


class DagSchedulerDispatchController:
    """Canonical DAG dispatch orchestration without direct Redis mutation."""

    def __init__(
        self,
        *,
        ready_queue,
        tenant_fairness,
        dispatch_controller,
        reservation_dispatcher,
        shards,
        reservation_manager=None,
        dag_lua=None,
    ) -> None:
        self._ready_queue = ready_queue
        self._tenant_fairness = tenant_fairness
        self._dispatch_controller = dispatch_controller
        self._reservation_dispatcher = reservation_dispatcher
        self._shards = shards
        self._reservation_manager = reservation_manager
        self._dag_lua = dag_lua
        self._dependencies_initialised = False
        self._last_result = DagDispatchOnceResult(False, "no_task_available")

    @property
    def last_result(self) -> DagDispatchOnceResult:
        return self._last_result

    async def initialise(self) -> None:
        if not self._dependencies_initialised:
            for dependency in (self._dag_lua, self._reservation_manager):
                initialise = getattr(dependency, "initialise", None)
                if callable(initialise):
                    await self._maybe_await(initialise())
            self._dependencies_initialised = True
        initialise = getattr(self._dispatch_controller, "initialise", None)
        if callable(initialise):
            await self._maybe_await(initialise())

    async def current_permit(self):
        permit = await self._dispatch_controller.current_permit()
        if not getattr(permit, "allowed", False):
            self._last_result = DagDispatchOnceResult(
                False,
                "permit_unavailable",
                reason=str(getattr(permit, "reason", "") or ""),
            )
        return permit

    async def try_consume(self, n: int = 1) -> bool:
        return bool(await self._dispatch_controller.try_consume(n))

    async def on_dispatch_success(self) -> None:
        callback = getattr(self._dispatch_controller, "on_dispatch_success", None)
        if callable(callback):
            await self._maybe_await(callback())

    async def on_dispatch_failure(self, reason: str) -> None:
        callback = getattr(self._dispatch_controller, "on_dispatch_failure", None)
        if callable(callback):
            await self._maybe_await(callback(reason))

    async def dispatch_once(
        self,
        *,
        snapshot,
        worker_scorer=None,
        scheduler_epoch: str,
    ) -> DagDispatchOnceResult:
        try:
            return await self._dispatch_once_impl(
                snapshot=snapshot,
                worker_scorer=worker_scorer,
                scheduler_epoch=scheduler_epoch,
            )
        except asyncio.CancelledError:
            self._record("cancelled", reason="dispatch_cancelled")
            raise

    async def _dispatch_once_impl(
        self,
        *,
        snapshot,
        worker_scorer=None,
        scheduler_epoch: str,
    ) -> DagDispatchOnceResult:
        del worker_scorer  # existing SchedulerScoring is the canonical authority
        epoch = str(scheduler_epoch or "").strip()
        if not epoch or epoch == "0":
            return self._record("identity_invalid", reason="authoritative_epoch_required")

        tenants = await self._ready_queue.list_active_tenants()
        tenants = [str(value).strip() for value in tenants if str(value).strip()]
        if not tenants:
            return self._record("no_task_available")

        tenant_id = str(
            await self._maybe_await(self._tenant_fairness.pick_next(tenants))
        ).strip()
        if not tenant_id:
            return self._record("no_task_available", reason="tenant_selection_empty")

        peeked_task_id = str(await self._ready_queue.peek(tenant_id) or "").strip()
        if not peeked_task_id:
            return self._record("no_task_available", reason="ready_queue_empty")

        rebuilt = await self._ready_queue.rebuild_dispatch_input(
            peeked_task_id,
            tenant_id=tenant_id,
            worker_group="",
            shard=0,
        )
        if rebuilt is None:
            return self._record(
                "task_metadata_missing",
                task_id=peeked_task_id,
                reason="task_meta_not_found",
            )

        authoritative_task_id = str(getattr(rebuilt, "task_id", "") or "").strip()
        authoritative_run_id = str(getattr(rebuilt, "run_id", "") or "").strip()
        if not authoritative_task_id or authoritative_task_id != peeked_task_id:
            return self._record(
                "identity_invalid",
                task_id=peeked_task_id,
                run_id=authoritative_run_id,
                reason="peeked_task_id_differs_from_rebuilt_task_id",
            )
        if not authoritative_run_id:
            return self._record(
                "identity_invalid",
                task_id=authoritative_task_id,
                reason="explicit_run_id_required",
            )

        incoming_payload = self._payload_mapping(rebuilt)
        incoming_task_id = str(incoming_payload.get("task_id") or "").strip()
        incoming_run_id = str(incoming_payload.get("run_id") or "").strip()
        incoming_epoch = str(incoming_payload.get("scheduler_epoch") or "").strip()
        if incoming_task_id and incoming_task_id != authoritative_task_id:
            return self._record(
                "identity_invalid",
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                reason="payload_task_id_mismatch",
            )
        if incoming_run_id and incoming_run_id != authoritative_run_id:
            return self._record(
                "identity_invalid",
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                reason="payload_run_id_mismatch",
            )
        if incoming_epoch and incoming_epoch != epoch:
            return self._record(
                "identity_invalid",
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                reason="payload_scheduler_epoch_mismatch",
            )

        workers = self._worker_candidates(getattr(snapshot, "workers", ()) or ())
        if not workers:
            return self._record(
                "no_worker_available",
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                reason="worker_pool_empty",
            )

        required_capabilities = list(
            getattr(rebuilt, "required_capabilities", ()) or
            incoming_payload.get("required_capabilities", ()) or
            ()
        )
        selection = SchedulerCapabilitySelector.filter_workers(
            required_capabilities=[str(value) for value in required_capabilities],
            workers=workers,
        )
        if not selection.compatible:
            return self._record(
                "no_worker_available",
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                reason="no_compatible_workers",
            )

        vruntime = self._safe_float(getattr(rebuilt, "vruntime", 0.0), 0.0)
        inflight = self._safe_int(getattr(rebuilt, "inflight", 0), 0)
        scoring_candidates = [
            ScoringCandidate(
                tenant_id=tenant_id,
                worker_id=worker.worker_id,
                task_id=authoritative_task_id,
                vruntime=vruntime,
                inflight=inflight,
                worker_load=worker.current_load,
                capacity=worker.capacity,
            )
            for worker in selection.compatible
        ]
        best = SchedulerScoring.choose_best(scoring_candidates)
        if best is None:
            return self._record(
                "no_worker_available",
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                reason="scoring_returned_no_worker",
            )
        selected = next(
            (worker for worker in selection.compatible if worker.worker_id == best.worker_id),
            None,
        )
        if selected is None or not selected.worker_group:
            return self._record(
                "no_worker_available",
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                worker_id=str(getattr(best, "worker_id", "") or ""),
                reason="selected_worker_group_missing",
            )

        try:
            selected_shard = await self._maybe_await(
                self._shards.shard_for_group(selected.worker_group, authoritative_run_id)
            )
            if selected_shard is None or str(selected_shard).strip() == "":
                raise ValueError("shard ownership missing")
            selected_shard = int(selected_shard)
        except Exception as exc:
            return self._record(
                "no_worker_available",
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                worker_id=selected.worker_id,
                reason=f"shard_resolution_failed:{exc}",
            )

        dispatch_payload = {
            **incoming_payload,
            "task_id": authoritative_task_id,
            "run_id": authoritative_run_id,
            "scheduler_epoch": epoch,
            "tenant_id": tenant_id,
            "worker_group": selected.worker_group,
            "shard": selected_shard,
        }
        result = await self._reservation_dispatcher.reserve_and_dispatch(
            task_id=authoritative_task_id,
            worker_id=selected.worker_id,
            scheduler_epoch=epoch,
            dispatch_payload=dispatch_payload,
        )
        if not bool(getattr(result, "ok", False)):
            raw_status = str(getattr(result, "status", "") or "")
            reason = str(getattr(result, "reason", "") or raw_status)
            category = (
                "writer_rejected"
                if self._is_writer_status(raw_status)
                else "reservation_failed"
            )
            await self.on_dispatch_failure(raw_status or category)
            return self._record(
                category,
                task_id=authoritative_task_id,
                run_id=authoritative_run_id,
                worker_id=selected.worker_id,
                reason=reason,
            )

        idempotent_replay = bool(
            getattr(result, "idempotent_replay", False)
        )
        if not idempotent_replay:
            update = getattr(
                self._tenant_fairness,
                "update_on_dispatch",
                None,
            )
            if callable(update):
                cost = self._safe_float(
                    incoming_payload.get(
                        "estimated_cost_cents",
                        1.0,
                    ),
                    1.0,
                )
                await self._maybe_await(
                    update(tenant_id, cost)
                )
            await self.on_dispatch_success()
        return self._record(
            (
                "already_projected"
                if idempotent_replay
                else "committed"
            ),
            dispatched=True,
            task_id=authoritative_task_id,
            run_id=authoritative_run_id,
            worker_id=selected.worker_id,
        )

    def _record(
        self,
        status: str,
        *,
        dispatched: bool = False,
        task_id: str = "",
        run_id: str = "",
        worker_id: str = "",
        reason: str = "",
    ) -> DagDispatchOnceResult:
        self._last_result = DagDispatchOnceResult(
            dispatched=dispatched,
            status=status,
            task_id=task_id,
            run_id=run_id,
            worker_id=worker_id,
            reason=reason,
        )
        return self._last_result

    @staticmethod
    async def _maybe_await(value):
        if inspect.isawaitable(value):
            return await value
        return value

    @staticmethod
    def _payload_mapping(rebuilt) -> dict[str, Any]:
        empty_mapping: dict[str, Any] | None = None
        for name in ("dispatch_payload", "payload"):
            value = getattr(rebuilt, name, None)
            if isinstance(value, Mapping):
                copied = dict(value)
                if copied:
                    return copied
                empty_mapping = copied
        raw_json = str(getattr(rebuilt, "payload_json", "") or "").strip()
        if raw_json:
            try:
                decoded = json.loads(raw_json)
            except (TypeError, ValueError):
                return empty_mapping or {}
            if isinstance(decoded, Mapping):
                return dict(decoded)
        return empty_mapping or {}

    @classmethod
    def _worker_candidates(cls, workers) -> list[WorkerCandidate]:
        result: list[WorkerCandidate] = []
        for worker in workers:
            if not bool(getattr(worker, "schedulable", True)):
                continue
            worker_id = str(getattr(worker, "worker_id", "") or "").strip()
            worker_group = str(getattr(worker, "worker_group", "") or "").strip()
            capacity = cls._safe_int(getattr(worker, "capacity", 0), 0)
            current_load = cls._safe_int(
                getattr(worker, "current_load", getattr(worker, "inflight", 0)),
                0,
            )
            available = getattr(worker, "available_slots", None)
            if available is not None and cls._safe_int(available, 0) <= 0:
                continue
            if not worker_id or not worker_group or capacity <= 0 or current_load >= capacity:
                continue
            capabilities = [
                str(value).strip()
                for value in (getattr(worker, "capabilities", ()) or ())
                if str(value).strip()
            ]
            result.append(
                WorkerCandidate(
                    worker_id=worker_id,
                    worker_group=worker_group,
                    capabilities=capabilities,
                    capacity=capacity,
                    current_load=max(0, current_load),
                )
            )
        return result

    @staticmethod
    def _is_writer_status(status: str) -> bool:
        return status.startswith("dispatch_") or status in {
            "missing_task_meta",
            "task_missing",
            "task_state_conflict",
            "task_identity_mismatch",
            "scheduler_epoch_mismatch",
            "committed_rejected",
        }

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

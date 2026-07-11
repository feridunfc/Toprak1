from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from hfa.state import transition_state
from hfa_control.scheduler_loop import SchedulerLoop

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchedulerComposition:
    scheduler_loop: object
    dispatch_controller: object
    ready_queue: object
    dag_lua: object
    dispatch_writer: object
    reservation_manager: object
    reservation_dispatcher: object
    tenant_queue: object | None = None
    injected_dispatch_callback: object | None = None


class Scheduler:
    """Lifecycle facade for the single production SchedulerLoop authority."""

    def __init__(
        self,
        scheduler_loop: SchedulerLoop | None = None,
        *,
        composition: SchedulerComposition | None = None,
    ) -> None:
        self._scheduler_loop = scheduler_loop
        self._composition = composition
        self._lifecycle_lock = asyncio.Lock()
        self._cycle_task: asyncio.Task | None = None
        self._running = False
        self._closed = False
        self._active_epoch = ""
        self._last_failure: BaseException | None = None

    @property
    def running(self) -> bool:
        return self._running

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def cycle_task(self) -> asyncio.Task | None:
        return self._cycle_task

    @property
    def current_epoch(self) -> str:
        return self._active_epoch if self._running else ""

    @property
    def last_failure(self) -> BaseException | None:
        return self._last_failure

    @property
    def composition(self) -> SchedulerComposition | None:
        return self._composition

    async def start(self, *, scheduler_epoch: str) -> None:
        epoch = self._validate_epoch(scheduler_epoch)
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("scheduler is closed")
            if self._running:
                if epoch == self._active_epoch:
                    return
                raise RuntimeError("scheduler is already running under a different epoch")
            if self._scheduler_loop is None:
                raise RuntimeError("scheduler loop is not configured")

            self._last_failure = None
            try:
                await self._scheduler_loop.on_leadership_gained(epoch)
            except BaseException:
                self._running = False
                self._active_epoch = ""
                self._cycle_task = None
                try:
                    await self._scheduler_loop.on_leadership_lost()
                except Exception:
                    logger.exception("scheduler leadership cleanup failed after start error")
                raise

            self._active_epoch = epoch
            self._running = True
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._run_cycles(), name=f"scheduler.cycle.{epoch}")
            self._cycle_task = task
            task.add_done_callback(self._on_cycle_task_done)

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            await self._stop_locked()

    async def close(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            await self._stop_locked()

    async def tick(self, max_dispatches: int | None = None) -> int:
        if self._scheduler_loop is None:
            return 0
        return await self._scheduler_loop.run_cycle(max_dispatches=max_dispatches)

    async def _stop_locked(self) -> None:
        task = self._cycle_task
        was_running = self._running
        self._running = False
        self._active_epoch = ""
        self._cycle_task = None

        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if was_running and self._scheduler_loop is not None:
            await self._scheduler_loop.on_leadership_lost()

    async def _run_cycles(self) -> None:
        failures = 0
        config = getattr(self._scheduler_loop, "_config", None)
        idle_sleep = max(
            0.0,
            float(getattr(config, "scheduler_loop_idle_sleep_ms", 250) or 0) / 1000.0,
        )
        error_sleep = max(
            0.0,
            float(getattr(config, "scheduler_loop_error_sleep_ms", 1000) or 0) / 1000.0,
        )
        max_failures = max(
            1,
            int(getattr(config, "scheduler_loop_max_failures", 8) or 8),
        )

        while self._running:
            try:
                dispatched = await self._scheduler_loop.run_cycle()
                failures = 0
                if self._running and int(dispatched or 0) == 0 and idle_sleep > 0:
                    await asyncio.sleep(idle_sleep)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                self._last_failure = exc
                logger.exception("scheduler cycle failed (%d/%d)", failures, max_failures)
                if failures >= max_failures:
                    self._running = False
                    self._active_epoch = ""
                    raise
                if self._running and error_sleep > 0:
                    await asyncio.sleep(error_sleep)

    def _on_cycle_task_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        try:
            failure = task.exception()
        except asyncio.CancelledError:
            return
        if failure is not None:
            self._last_failure = failure
        if self._cycle_task is task:
            self._cycle_task = None
            self._running = False
            self._active_epoch = ""

    @staticmethod
    def _validate_epoch(value: Any) -> str:
        if value is None:
            raise ValueError("scheduler_epoch is required")
        epoch = str(value).strip()
        if not epoch or epoch == "0":
            raise ValueError("scheduler_epoch must represent acquired leadership authority")
        return epoch

    def _pick_best_worker(self, candidates):
        usable = []
        for candidate in candidates:
            if not getattr(candidate, "schedulable", True):
                continue
            if not getattr(candidate, "worker_id", None):
                continue
            if getattr(candidate, "decision_score", None) is None:
                continue
            usable.append(candidate)
        if not usable:
            return None
        usable.sort(key=lambda c: c.decision_score.as_sort_key())
        return usable[0]


def build_production_scheduler(
    *,
    redis,
    config,
    registry,
    shards,
    event_store=None,
) -> Scheduler:
    """Build the canonical production scheduler graph on shared Redis."""
    instance_id = str(getattr(config, "instance_id", "") or "").strip()
    if not instance_id:
        raise ValueError("ControlPlaneConfig.instance_id is required for production scheduler")
    ttl = int(getattr(config, "scheduler_reservation_ttl_seconds", 0) or 0)
    if ttl <= 0:
        raise ValueError("scheduler_reservation_ttl_seconds must be greater than zero")

    from hfa_control.dag_lua import DagLua
    from hfa_control.dag_scheduler_bridge import DagReadyQueue, DagSchedulerDispatchWriter
    from hfa_control.dag_scheduler_dispatch_controller import DagSchedulerDispatchController
    from hfa_control.dispatch_controller import DispatchController
    from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
    from hfa_control.scheduler_snapshot import SchedulerSnapshotBuilder
    from hfa_control.tenant_fairness import TenantFairnessTracker
    from hfa_control.worker_reservation import WorkerReservationManager

    dag_lua = DagLua(redis)
    ready_queue = DagReadyQueue(redis)
    dispatch_writer = DagSchedulerDispatchWriter(ready_queue=ready_queue, dag_lua=dag_lua)
    reservation_manager = WorkerReservationManager(
        redis,
        reservation_ttl_seconds=ttl,
        scheduler_id=instance_id,
    )
    reservation_dispatcher = SchedulerReservationDispatcher(
        reservation_manager,
        dispatch_writer,
        event_store=event_store,
    )
    pacing_controller = DispatchController(redis, config)
    tenant_fairness = TenantFairnessTracker()
    canonical_controller = DagSchedulerDispatchController(
        ready_queue=ready_queue,
        tenant_fairness=tenant_fairness,
        dispatch_controller=pacing_controller,
        reservation_dispatcher=reservation_dispatcher,
        shards=shards,
        reservation_manager=reservation_manager,
        dag_lua=dag_lua,
    )
    snapshot_builder = SchedulerSnapshotBuilder(
        redis,
        registry,
        None,
        tenant_fairness,
        config,
    )
    scheduler_loop = SchedulerLoop(
        redis=redis,
        dispatch_controller=canonical_controller,
        snapshot_builder=snapshot_builder,
        worker_scorer=None,
        tenant_fairness=tenant_fairness,
        tenant_queue=None,
        shards=shards,
        lua=dag_lua,
        config=config,
        event_store=event_store,
        explicit_epoch_required=True,
    )
    composition = SchedulerComposition(
        scheduler_loop=scheduler_loop,
        dispatch_controller=canonical_controller,
        ready_queue=ready_queue,
        dag_lua=dag_lua,
        dispatch_writer=dispatch_writer,
        reservation_manager=reservation_manager,
        reservation_dispatcher=reservation_dispatcher,
    )
    return Scheduler(scheduler_loop, composition=composition)

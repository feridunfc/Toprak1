from __future__ import annotations

import asyncio
import logging
import os
import uuid
from typing import Any, Dict

from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.events.schema import RunRequestedEvent
from hfa_control.dag_lua import DagLua
from hfa_control.task_claim import TaskClaimManager
from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_control.shard import OWNER_TTL, ShardOwnershipManager
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.drain import DrainManager
from hfa_worker.executor import BaseExecutor
from hfa_worker.executor_factory import build_executor
from hfa_worker.heartbeat import WorkerHeartbeatPublisher
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

logger = logging.getLogger(__name__)


class _BaseExecutorTaskAdapter(TaskExecutor):
    """Narrow compatibility adapter for the canonical TaskExecutor boundary."""

    def __init__(self, executor: BaseExecutor) -> None:
        self._executor = executor

    async def execute(self, ctx) -> TaskExecutionResult:
        event = RunRequestedEvent(
            task_id=ctx.task_id,
            run_id=ctx.run_id,
            tenant_id=ctx.tenant_id,
            agent_type=ctx.agent_type,
            payload=dict(ctx.payload or {}),
            scheduler_epoch=ctx.scheduler_epoch,
            trace_parent=ctx.trace_parent or None,
            trace_state=ctx.trace_state or None,
        )
        result = await self._executor.execute(event)
        return TaskExecutionResult(
            ok=str(getattr(result, "status", "")) == "done",
            output=dict(getattr(result, "payload", {}) or {}),
            error=str(getattr(result, "error", "") or ""),
        )


def _require_env(name: str) -> str:
    val = os.environ.get(name, "")
    if not val:
        raise RuntimeError(f"Required environment variable not set: {name}")
    return val


class WorkerService:
    def __init__(self, redis, config: Dict[str, Any]) -> None:
        self._redis = redis
        self._production = bool(config.get("production", False))

        configured_worker_id = str(config.get("worker_id") or "").strip()
        if self._production and not configured_worker_id:
            raise ValueError(
                "Production worker_id must be explicitly configured and non-empty"
            )
        self._worker_id = configured_worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._worker_group = str(config.get("worker_group") or "default-group")
        self._region = str(config.get("region") or "us-east-1")
        self._version = str(config.get("version") or "0.0.0")
        self._capabilities: list[str] = list(config.get("capabilities") or ["base"])
        self._shards: list[int] = list(config.get("shards") or [0])
        self._capacity = int(config.get("capacity") or 10)
        self._shard_renew_interval = float(
            config.get("shard_renew_interval", OWNER_TTL / 2)
        )
        maximum_renew_interval = OWNER_TTL / 2
        if (
            self._shard_renew_interval <= 0
            or self._shard_renew_interval > maximum_renew_interval
        ):
            raise ValueError(
                "shard_renew_interval must be greater than zero and no more "
                f"than half the shard lease TTL ({maximum_renew_interval:g}s)"
            )

        injected_shard_manager = config.get("shard_manager")
        if self._production:
            if injected_shard_manager is None:
                required_operations = ("set", "get", "expire", "hset", "eval")
                missing_operations = [
                    operation
                    for operation in required_operations
                    if not callable(getattr(redis, operation, None))
                ]
                if missing_operations:
                    raise RuntimeError(
                        "Production shard lease Redis dependency is incomplete; "
                        "missing operations: "
                        + ", ".join(missing_operations)
                    )
                self._shard_manager = ShardOwnershipManager(redis, config)
            else:
                self._shard_manager = injected_shard_manager
            self._manage_shard_leases = True
        else:
            self._shard_manager = injected_shard_manager
            self._manage_shard_leases = False

        self._shard_renewer_task: asyncio.Task | None = None

        executor: BaseExecutor | None = config.get("executor")
        if executor is None:
            executor_mode = str(
                config.get("executor_mode") or ""
            ).strip()
            if self._production and not executor_mode:
                raise ValueError(
                    "executor_mode is required when production=True and no "
                    "executor is explicitly injected"
                )

            executor = build_executor(config)
            logger.info(
                "WorkerService executor built from factory: mode=%s worker=%s",
                executor_mode or "fake",
                self._worker_id,
            )
        else:
            logger.info(
                "WorkerService using explicit executor: %s worker=%s",
                type(executor).__name__,
                self._worker_id,
            )

        self._dag_lua: DagLua | None = None
        self._task_claim_manager: TaskClaimManager | None = None
        self._task_heartbeat_manager: TaskHeartbeatManager | None = None
        self._task_consumer: TaskConsumer | None = None

        if self._production:
            task_executor = config.get("task_executor")
            if task_executor is None:
                task_executor = _BaseExecutorTaskAdapter(executor)

            task_heartbeat_interval_ms = int(
                config.get("task_heartbeat_interval_ms") or 5_000
            )
            task_stale_after_ms = int(
                config.get("task_stale_after_ms") or 30_000
            )
            if (
                task_heartbeat_interval_ms <= 0
                or task_stale_after_ms <= 0
                or task_heartbeat_interval_ms >= task_stale_after_ms
            ):
                raise ValueError(
                    "task heartbeat interval must be positive and strictly "
                    "below the stale-task threshold"
                )

            self._dag_lua = DagLua(redis)
            self._task_claim_manager = TaskClaimManager(self._dag_lua)
            self._task_heartbeat_manager = TaskHeartbeatManager(
                redis,
                policy=HeartbeatPolicy(
                    stale_after_ms=task_stale_after_ms,
                    heartbeat_interval_ms=task_heartbeat_interval_ms,
                ),
            )
            self._task_consumer = TaskConsumer(
                claim_manager=self._task_claim_manager,
                executor=task_executor,
                worker_capabilities=self._capabilities,
                heartbeat_manager=self._task_heartbeat_manager,
                heartbeat_interval_ms=task_heartbeat_interval_ms,
                completion_manager=self._dag_lua,
            )

        self._consumer = WorkerConsumer(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shards=self._shards,
            executor=executor,
            task_consumer=self._task_consumer,
        )

        self._heartbeat = WorkerHeartbeatPublisher(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            region=self._region,
            shards=self._shards,
            capacity=self._capacity,
            inflight_fn=lambda: self._consumer.inflight_count,
            is_draining_fn=lambda: self._consumer.is_draining,
            version=self._version,
            capabilities=self._capabilities,
        )

        self._drain_manager = DrainManager(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shards=self._shards,
            consumer=self._consumer,
        )

        self._lifecycle_lock = asyncio.Lock()
        self._started = False
        self._closed = False
        self._is_ready = False
        self._last_failure: BaseException | None = None
        self._failure_event = asyncio.Event()
        self._fatal_cleanup_task: asyncio.Task | None = None
        self._shutdown_grace_period = float(
            config.get("shutdown_grace_period", 30.0)
        )

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def _refresh_background_health(self) -> None:
        for component, task in (
            ("consumer", getattr(self._consumer, "_task", None)),
            ("claim_renewer", getattr(self._consumer, "_renewer_task", None)),
            ("heartbeat", getattr(self._heartbeat, "_task", None)),
            ("shard_renewer", self._shard_renewer_task),
        ):
            if (
                not isinstance(task, asyncio.Task)
                or not task.done()
                or task.cancelled()
            ):
                continue

            try:
                failure = task.exception()
            except asyncio.CancelledError:
                continue

            if failure is None:
                failure = RuntimeError(
                    f"{component} exited unexpectedly"
                )

            self._record_fatal_failure(
                failure,
                component=component,
            )
            return

    @property
    def is_ready(self) -> bool:
        self._refresh_background_health()
        return self._is_ready

    @property
    def last_failure(self) -> BaseException | None:
        self._refresh_background_health()
        return self._last_failure

    async def wait_for_failure(self) -> BaseException:
        """Wait until a fatal background component failure is observed."""
        self._refresh_background_health()
        if self._last_failure is not None:
            return self._last_failure

        await self._failure_event.wait()
        failure = self._last_failure
        if failure is None:
            raise RuntimeError(
                "WorkerService failure event set without an exception"
            )
        return failure

    def _record_fatal_failure(
        self,
        failure: BaseException,
        *,
        component: str,
    ) -> None:
        if not self._is_ready:
            return

        self._last_failure = failure
        self._is_ready = False
        self._failure_event.set()

        stop_pulling = getattr(self._consumer, "stop_pulling", None)
        if callable(stop_pulling):
            try:
                stop_pulling()
            except Exception as exc:
                logger.error(
                    "WorkerService failed to stop pulling after fatal "
                    "failure: worker_id=%s component=%s error=%s",
                    self._worker_id,
                    component,
                    exc,
                )

        logger.error(
            "WorkerService background component failed: "
            "worker_id=%s component=%s error=%s",
            self._worker_id,
            component,
            failure,
        )

        if (
            self._fatal_cleanup_task is None
            or self._fatal_cleanup_task.done()
        ):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            self._fatal_cleanup_task = loop.create_task(
                self._stop_after_fatal_failure(),
                name=f"worker-fatal-cleanup.{self._worker_id}",
            )

    async def _publish_unschedulable_heartbeat(self) -> None:
        publish_unschedulable_now = getattr(
            self._heartbeat,
            "publish_unschedulable_now",
            None,
        )
        if callable(publish_unschedulable_now):
            await publish_unschedulable_now()
            return

        # Compatibility path for test doubles and older publishers.
        publish_now = getattr(self._heartbeat, "publish_now", None)
        if callable(publish_now):
            await publish_now()

    async def _stop_after_fatal_failure(self) -> None:
        try:
            await self._publish_unschedulable_heartbeat()
        except BaseException as exc:
            logger.error(
                "WorkerService unschedulable heartbeat after fatal failure "
                "failed: worker_id=%s error=%s",
                self._worker_id,
                exc,
            )

        try:
            await self._heartbeat.close()
        except BaseException as exc:
            logger.error(
                "WorkerService heartbeat stop after fatal failure failed: "
                "worker_id=%s error=%s",
                self._worker_id,
                exc,
            )

        try:
            await self._stop_shard_renewer()
        except BaseException as exc:
            logger.error(
                "WorkerService shard renewer stop after fatal failure failed: "
                "worker_id=%s error=%s",
                self._worker_id,
                exc,
            )

    def _observe_background_task(
        self,
        task: object,
        *,
        component: str,
    ) -> None:
        if not isinstance(task, asyncio.Task):
            return

        def _on_done(completed: asyncio.Task) -> None:
            if completed.cancelled():
                return
            try:
                failure = completed.exception()
            except asyncio.CancelledError:
                return

            if failure is None:
                failure = RuntimeError(
                    f"{component} exited unexpectedly"
                )

            self._record_fatal_failure(
                failure,
                component=component,
            )

        task.add_done_callback(_on_done)

    def _attach_background_health_observers(self) -> None:
        self._observe_background_task(
            getattr(self._consumer, "_task", None),
            component="consumer",
        )
        self._observe_background_task(
            getattr(self._consumer, "_renewer_task", None),
            component="claim_renewer",
        )
        self._observe_background_task(
            getattr(self._heartbeat, "_task", None),
            component="heartbeat",
        )
        self._observe_background_task(
            self._shard_renewer_task,
            component="shard_renewer",
        )

    async def _prepare_consumer_groups(self) -> None:
        prepare = getattr(self._consumer, "prepare_consumer_groups", None)
        if callable(prepare):
            await prepare()

    async def _confirm_shard_leases(self) -> None:
        if not self._manage_shard_leases:
            return
        if self._shard_manager is None:
            raise RuntimeError("Shard lease manager is unavailable")

        for shard in self._shards:
            claimed = await self._shard_manager.claim_shard(
                shard,
                self._worker_group,
            )
            if claimed:
                continue

            # SET NX returns False for both a sibling-owned group lease and a
            # foreign lease. renew_shard() confirms the stored group owner.
            renewed = await self._shard_manager.renew_shard(
                shard,
                self._worker_group,
            )
            if not renewed:
                raise RuntimeError(
                    "Shard lease ownership conflict: "
                    f"shard={shard} requested_group={self._worker_group}"
                )

    async def _renew_shard_leases(self) -> None:
        if self._shard_manager is None:
            raise RuntimeError("Shard lease manager is unavailable")

        while True:
            await asyncio.sleep(self._shard_renew_interval)
            for shard in self._shards:
                renewed = await self._shard_manager.renew_shard(
                    shard,
                    self._worker_group,
                )
                if not renewed:
                    raise RuntimeError(
                        "Shard lease lost: "
                        f"shard={shard} worker_group={self._worker_group}"
                    )

    def _start_shard_renewer(self) -> None:
        if not self._manage_shard_leases:
            return
        if self._shard_renewer_task is not None:
            return

        loop = asyncio.get_running_loop()
        self._shard_renewer_task = loop.create_task(
            self._renew_shard_leases(),
            name=f"shard-renewer.{self._worker_id}",
        )

    async def _stop_shard_renewer(self) -> None:
        task = self._shard_renewer_task
        if task is None:
            return

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._shard_renewer_task = None

    def _prepare_consumer_for_start(self) -> None:
        prepare_for_start = getattr(
            self._consumer,
            "prepare_for_start",
            None,
        )
        if callable(prepare_for_start):
            prepare_for_start()
            return

        # Compatibility for narrow lifecycle probes that expose only the
        # historical boolean state used by their is_draining property.
        if isinstance(getattr(self._consumer, "_pulling", None), bool):
            setattr(self._consumer, "_pulling", True)

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("WorkerService is closed")
            if self._started:
                return

            logger.info(
                "WorkerService starting: worker_id=%s group=%s shards=%s capacity=%d",
                self._worker_id,
                self._worker_group,
                self._shards,
                self._capacity,
            )

            self._is_ready = False
            self._last_failure = None
            self._failure_event.clear()
            if (
                self._fatal_cleanup_task is not None
                and self._fatal_cleanup_task.done()
            ):
                self._fatal_cleanup_task = None
            heartbeat_started = False
            consumer_started = False
            try:
                if self._production and self._dag_lua is not None:
                    await self._dag_lua.initialise()

                await self._prepare_consumer_groups()
                await self._confirm_shard_leases()

                self._prepare_consumer_for_start()
                await self._heartbeat.start()
                heartbeat_started = True
                await self._consumer.start()
                consumer_started = True
                self._start_shard_renewer()
            except BaseException as exc:
                self._last_failure = exc
                await self._stop_shard_renewer()
                if consumer_started:
                    await self._consumer.close()
                if heartbeat_started:
                    try:
                        await self._publish_unschedulable_heartbeat()
                    except BaseException as projection_exc:
                        logger.error(
                            "WorkerService startup failure projection failed: "
                            "worker_id=%s error=%s",
                            self._worker_id,
                            projection_exc,
                        )
                    await self._heartbeat.close()
                raise

            self._started = True
            self._is_ready = True
            self._attach_background_health_observers()
            logger.info("WorkerService started: worker_id=%s", self._worker_id)

    async def _stop_locked(self, *, drain_timeout: float) -> None:
        if not self._started:
            self._is_ready = False
            return

        self._is_ready = False
        try:
            # Scheduler exclusion must be visible before waiting for inflight
            # work. DrainManager.stop_pulling() is intentionally idempotent.
            stop_pulling = getattr(
                self._consumer,
                "stop_pulling",
                None,
            )
            if callable(stop_pulling):
                stop_pulling()
            await self._publish_unschedulable_heartbeat()
            await self._drain_manager.start_drain(
                reason="shutdown",
                timeout=drain_timeout,
            )
        finally:
            try:
                await self._consumer.close()
            finally:
                try:
                    await self._heartbeat.close()
                finally:
                    await self._stop_shard_renewer()
                    self._started = False
                    reset = getattr(self._drain_manager, "reset", None)
                    if callable(reset):
                        reset()

    async def stop(self, drain_timeout: float | None = None) -> None:
        async with self._lifecycle_lock:
            timeout = (
                self._shutdown_grace_period
                if drain_timeout is None
                else float(drain_timeout)
            )
            await self._stop_locked(drain_timeout=timeout)

    async def close(self, drain_timeout: float | None = None) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                return
            timeout = (
                self._shutdown_grace_period
                if drain_timeout is None
                else float(drain_timeout)
            )
            await self._stop_locked(drain_timeout=timeout)
            self._closed = True

    async def graceful_shutdown(self, drain_timeout: float = 30.0) -> None:
        logger.info("WorkerService graceful_shutdown: worker_id=%s", self._worker_id)
        await self.close(drain_timeout=drain_timeout)
        logger.info("WorkerService shutdown complete: worker_id=%s", self._worker_id)

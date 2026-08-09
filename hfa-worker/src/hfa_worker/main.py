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
from hfa_control.task_terminal_authority import TaskTerminalAuthorityBinding
from hfa_control.run_terminate_authority import RunTerminateAuthorityBinding
from hfa_control.run_termination import RunTerminationCoordinator
from hfa_control.product_profile import (
    ProductMode,
    WorkerProductProfile,
    parse_product_mode,
    validate_worker_product_profile,
)
from hfa_control.shard import OWNER_TTL, ShardOwnershipManager
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.drain import DrainManager
from hfa_worker.executor import BaseExecutor
from hfa_worker.executor_factory import build_executor
from hfa_worker.heartbeat import WorkerHeartbeatPublisher
from hfa_worker.run_finalizing_runtime import (
    RunFinalizingTaskConsumer,
    RunFinalizingWorkerConsumer,
)
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_executor import TaskExecutionResult, TaskExecutor

logger = logging.getLogger(__name__)

_RESERVED_PRODUCT_CAPABILITY_PREFIXES = (
    "product:",
    "run-finalization:",
    "executor:",
)
_ALLOWED_EXECUTOR_CAPABILITIES = frozenset({
    "executor:configured",
    "executor:deterministic",
    "executor:external",
    "executor:cognitive",
})


def _sanitize_declared_capabilities(
    values: object,
) -> list[str]:
    raw_values = values if isinstance(values, (list, tuple, set)) else []
    sanitized = set()
    for value in raw_values:
        capability = str(value or "").strip()
        if not capability:
            continue
        if capability.startswith(
            _RESERVED_PRODUCT_CAPABILITY_PREFIXES
        ):
            continue
        sanitized.add(capability)
    return sorted(sanitized or {"base"})


def _executor_product_capability(
    executor: object,
) -> str:
    capability = str(
        getattr(
            executor,
            "product_executor_capability",
            "executor:configured",
        )
        or ""
    ).strip()
    if capability not in _ALLOWED_EXECUTOR_CAPABILITIES:
        return "executor:configured"
    return capability


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


class _CanonicalTaskTerminalCompletionGateway:
    """Adapt TaskConsumer completion calls to canonical TASK terminal authority."""

    def __init__(self, binding: TaskTerminalAuthorityBinding) -> None:
        self._binding = binding

    async def task_complete(
        self,
        *,
        task_id: str,
        run_id: str,
        tenant_id: str,
        terminal_state: str,
        finished_at_ms: int,
        reason_code: str,
        worker_instance_id: str,
        output_data: str,
        expected_scheduler_epoch: str,
        expected_claim_epoch: str | int,
    ):
        if terminal_state == "done":
            return await self._binding.complete(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                finished_at_ms=finished_at_ms,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=expected_scheduler_epoch,
                claim_epoch=expected_claim_epoch,
                output_data=output_data,
                reason_code=reason_code,
            )
        if terminal_state == "failed":
            return await self._binding.fail(
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                finished_at_ms=finished_at_ms,
                worker_instance_id=worker_instance_id,
                scheduler_epoch=expected_scheduler_epoch,
                claim_epoch=expected_claim_epoch,
                reason_code=reason_code,
            )
        raise RuntimeError(
            "canonical task terminal gateway accepts only done or failed"
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
        self._product_mode = parse_product_mode(
            config.get("product_mode")
        )

        run_termination_binding = config.get(
            "run_termination_binding_enabled",
            False,
        )
        if not isinstance(run_termination_binding, bool):
            raise ValueError(
                "run_termination_binding_enabled must be a boolean"
            )
        self._run_termination_binding_enabled = run_termination_binding
        if self._run_termination_binding_enabled and not self._production:
            raise ValueError(
                "run_termination_binding_enabled requires production=True"
            )

        canonical_binding_names = (
            "canonical_task_admit_binding",
            "canonical_task_dispatch_binding",
            "canonical_task_claim_binding",
        )
        canonical_bindings: dict[str, bool] = {}
        for name in canonical_binding_names:
            value = config.get(name, False)
            if type(value) is not bool:
                raise ValueError(f"{name} must be a boolean")
            canonical_bindings[name] = value

        self._canonical_task_admit_binding_enabled = canonical_bindings[
            "canonical_task_admit_binding"
        ]
        self._canonical_task_dispatch_binding_enabled = canonical_bindings[
            "canonical_task_dispatch_binding"
        ]
        self._canonical_task_claim_binding_enabled = canonical_bindings[
            "canonical_task_claim_binding"
        ]

        if (
            self._canonical_task_dispatch_binding_enabled
            and not self._canonical_task_admit_binding_enabled
        ):
            raise ValueError(
                "canonical_task_dispatch_binding requires "
                "canonical_task_admit_binding=True"
            )
        if self._canonical_task_claim_binding_enabled and not self._production:
            raise ValueError(
                "canonical_task_claim_binding requires production=True"
            )
        if self._canonical_task_claim_binding_enabled and not (
            self._canonical_task_admit_binding_enabled
            and self._canonical_task_dispatch_binding_enabled
        ):
            raise ValueError(
                "canonical_task_claim_binding requires both "
                "canonical_task_admit_binding=True and "
                "canonical_task_dispatch_binding=True"
            )

        canonical_task_terminal_binding = config.get(
            "canonical_task_terminal_binding",
            False,
        )
        if type(canonical_task_terminal_binding) is not bool:
            raise ValueError(
                "canonical_task_terminal_binding must be a boolean"
            )
        self._canonical_task_terminal_binding_enabled = (
            canonical_task_terminal_binding
        )
        if self._canonical_task_terminal_binding_enabled and not self._production:
            raise ValueError(
                "canonical_task_terminal_binding requires production=True"
            )
        if (
            self._canonical_task_terminal_binding_enabled
            and not self._canonical_task_claim_binding_enabled
        ):
            raise ValueError(
                "canonical_task_terminal_binding requires "
                "canonical_task_claim_binding=True"
            )
        if self._canonical_task_terminal_binding_enabled and not (
            self._canonical_task_admit_binding_enabled
            and self._canonical_task_dispatch_binding_enabled
        ):
            raise ValueError(
                "canonical_task_terminal_binding requires the canonical "
                "TASK_ADMIT/TASK_DISPATCH/TASK_CLAIM dependency chain"
            )
        configured_worker_id = str(config.get("worker_id") or "").strip()
        if self._production and not configured_worker_id:
            raise ValueError(
                "Production worker_id must be explicitly configured and non-empty"
            )
        self._worker_id = configured_worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        configured_worker_group = str(
            config.get("worker_group") or ""
        ).strip()
        self._worker_group = (
            configured_worker_group or "default-group"
        )
        self._region = str(config.get("region") or "us-east-1")
        self._version = str(config.get("version") or "0.0.0")
        self._capabilities = _sanitize_declared_capabilities(
            config.get("capabilities") or ["base"]
        )
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

        self._product_profile = validate_worker_product_profile(
            product_mode=self._product_mode,
            production=self._production,
            worker_id=configured_worker_id,
            worker_group=configured_worker_group,
            executor_configured=executor is not None,
            run_termination_binding_enabled=(
                self._run_termination_binding_enabled
            ),
        )

        self._dag_lua: DagLua | None = None
        self._task_claim_manager: TaskClaimManager | None = None
        self._task_heartbeat_manager: TaskHeartbeatManager | None = None
        self._task_consumer: TaskConsumer | None = None
        self._run_termination_coordinator: RunTerminationCoordinator | None = None
        self._run_terminate_authority_binding: RunTerminateAuthorityBinding | None = None
        self._task_terminal_authority_binding: (
            TaskTerminalAuthorityBinding | None
        ) = None
        self._task_terminal_completion_gateway: (
            _CanonicalTaskTerminalCompletionGateway | None
        ) = None
        worker_consumer_type = WorkerConsumer

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
            completion_manager: Any = self._dag_lua
            task_consumer_type = TaskConsumer

            if self._canonical_task_terminal_binding_enabled:
                self._task_terminal_authority_binding = (
                    TaskTerminalAuthorityBinding(redis)
                )
                self._task_terminal_completion_gateway = (
                    _CanonicalTaskTerminalCompletionGateway(
                        self._task_terminal_authority_binding
                    )
                )
                completion_manager = self._task_terminal_completion_gateway
                if self._run_termination_binding_enabled:
                    self._run_terminate_authority_binding = (
                        RunTerminateAuthorityBinding(redis)
                    )
                    self._run_termination_coordinator = RunTerminationCoordinator(
                        redis,
                        self._task_terminal_completion_gateway,
                        enabled=True,
                        authority_binding=self._run_terminate_authority_binding,
                    )
                    completion_manager = self._run_termination_coordinator
                    task_consumer_type = RunFinalizingTaskConsumer
                    worker_consumer_type = RunFinalizingWorkerConsumer
            elif self._run_termination_binding_enabled:
                self._run_termination_coordinator = RunTerminationCoordinator(
                    redis,
                    self._dag_lua,
                    enabled=True,
                )
                completion_manager = self._run_termination_coordinator
                task_consumer_type = RunFinalizingTaskConsumer
                worker_consumer_type = RunFinalizingWorkerConsumer

            self._task_claim_manager = TaskClaimManager(
                self._dag_lua,
                canonical_task_claim_binding=(
                    self._canonical_task_claim_binding_enabled
                ),
                canonical_task_admit_binding=(
                    self._canonical_task_admit_binding_enabled
                ),
                canonical_task_dispatch_binding=(
                    self._canonical_task_dispatch_binding_enabled
                ),
            )
            self._task_heartbeat_manager = TaskHeartbeatManager(
                redis,
                policy=HeartbeatPolicy(
                    stale_after_ms=task_stale_after_ms,
                    heartbeat_interval_ms=task_heartbeat_interval_ms,
                ),
            )
            self._task_consumer = task_consumer_type(
                claim_manager=self._task_claim_manager,
                executor=task_executor,
                worker_capabilities=self._capabilities,
                heartbeat_manager=self._task_heartbeat_manager,
                heartbeat_interval_ms=task_heartbeat_interval_ms,
                completion_manager=completion_manager,
            )

        self._derive_runtime_capabilities(
            executor=executor,
            worker_consumer_type=worker_consumer_type,
        )

        self._consumer = worker_consumer_type(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shards=self._shards,
            executor=executor,
            task_consumer=self._task_consumer,
            canonical_task_claim_binding_enabled=(
                self._canonical_task_claim_binding_enabled
            ),
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

    def _derive_runtime_capabilities(
        self,
        *,
        executor: object,
        worker_consumer_type: type,
    ) -> None:
        capabilities = set(self._capabilities)
        capabilities.add(
            _executor_product_capability(executor)
        )

        finalization_bound = (
            self._production
            and self._run_termination_binding_enabled
            and self._run_termination_coordinator is not None
            and isinstance(
                self._task_consumer,
                RunFinalizingTaskConsumer,
            )
            and worker_consumer_type
            is RunFinalizingWorkerConsumer
        )
        if finalization_bound:
            capabilities.add("run-finalization:v1")
        if (
            self._product_profile.single_task_alpha
            and finalization_bound
        ):
            capabilities.add("product:single-task-v1")

        self._capabilities[:] = sorted(capabilities)

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def run_termination_binding_enabled(self) -> bool:
        return self._run_termination_binding_enabled

    @property
    def canonical_task_claim_binding_enabled(self) -> bool:
        return self._canonical_task_claim_binding_enabled

    @property
    def canonical_task_terminal_binding_enabled(self) -> bool:
        return self._canonical_task_terminal_binding_enabled

    @property
    def product_profile(self) -> WorkerProductProfile:
        return self._product_profile

    @property
    def runtime_capabilities(self) -> tuple[str, ...]:
        return tuple(self._capabilities)

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

                if self._task_terminal_authority_binding is not None:
                    await self._task_terminal_authority_binding.initialise()

                if self._run_termination_coordinator is not None:
                    await self._run_termination_coordinator.initialise()

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

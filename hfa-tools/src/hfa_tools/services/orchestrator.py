"""
hfa-tools/src/hfa_tools/services/orchestrator.py
IRONCLAD Sprint 5 → Sprint 7 Faz 2 → Sprint 8 Mini 3

Lineage
-------
Sprint 5  (baseline)
  ✅ Ledger task tracking & drain
  ✅ Atomic budget gate
  ✅ Graceful shutdown: queue.join() drain before sentinels
  ✅ Memory leak protection: consume semantics on get_result / wait_for
  ✅ Start-guard on enqueue
  ✅ validate_run_id_format + tenant/run_id cross-check
  ✅ Per-tenant semaphore
  ✅ Background ledger task tracking with drain on close()

Sprint 7 Faz 2
  ✅ queue_depth incremented AFTER successful queue insertion (sprint7)
  ✅ queue_depth decremented in worker finally (cannot be skipped) (sprint7)
  ✅ HFAMetrics.inc_runs_total()     — on run completion
  ✅ HFAMetrics.record_run_latency() — on run completion

Sprint 8 Mini 3
  ✅ hfa.orchestrator.run span per run in _execute()
  ✅ Attributes: hfa.run_id, hfa.tenant_id, hfa.agent_type, hfa.worker_id
  ✅ Success path: hfa.status=done, span_ok()
  ✅ Failure path: record_exc(), hfa.status=failed
  ✅ Tracing never blocks or breaks business logic (no-op safe)
  ✅ All Sprint 5/7 hardened behaviors preserved

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop() where needed.
* close() drains queue then joins workers.
* Per-tenant semaphore prevents cross-tenant starvation.
* Monetary values: cost_cents: int (no float USD).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Set

from hfa.obs.run_graph import ExecutionGraph
from hfa.obs.metrics import HFAMetrics  # Sprint 7 Faz 2
from hfa.obs.tracing import get_tracer, HFATracing  # Sprint 8 Mini 3
from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError

logger = logging.getLogger(__name__)
_tracer = get_tracer("hfa.orchestrator")


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    REJECTED = "rejected"


@dataclass
class RunRequest:
    agent_type: str
    tenant_id: str
    run_id: str
    payload: Any
    priority: int = 5
    submitted_at: float = field(default_factory=time.time)

    def __lt__(self, other: "RunRequest") -> bool:
        return (self.priority, self.submitted_at) < (other.priority, other.submitted_at)


@dataclass
class RunResult:
    run_id: str
    tenant_id: str
    agent_type: str
    status: RunStatus
    payload: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: Optional[float] = None
    graph: Optional[Dict] = None
    tokens_used: int = 0
    cost_cents: int = 0  # integer cents — no float USD


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class OrchestratorError(Exception):
    """Base orchestrator error."""


class QueueFullError(OrchestratorError):
    def __init__(self, tenant_id: str, queue_size: int) -> None:
        super().__init__(f"Run queue full for tenant={tenant_id} (size={queue_size})")
        self.tenant_id = tenant_id
        self.queue_size = queue_size


class AgentNotRegisteredError(OrchestratorError):
    """No handler registered for the given agent_type."""


class OrchestratorNotStartedError(OrchestratorError):
    """Raised when enqueue is called before start()."""


# ---------------------------------------------------------------------------
# RunOrchestrator
# ---------------------------------------------------------------------------


class RunOrchestrator:
    """
    Bounded priority queue + worker pool orchestrator.

    Sprint 5 hardening: budget gate, ledger, shutdown drain, validate_run_id_format.
    Sprint 7 telemetry: queue depth / run totals / latency metrics.
    Sprint 8 tracing:  root span per run in _execute().
    """

    def __init__(
        self,
        max_queue_size: int = 500,
        worker_count: int = 8,
        per_tenant_concurrency: int = 5,
        worker_shutdown_timeout: float = 30.0,
        budget_guard: Optional[Any] = None,
        ledger: Optional[Any] = None,
    ) -> None:
        self._max_queue_size = max_queue_size
        self._worker_count = worker_count
        self._per_tenant_limit = per_tenant_concurrency
        self._shutdown_timeout = worker_shutdown_timeout
        self._budget_guard = budget_guard
        self._ledger = ledger

        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._dispatch_table: Dict[str, Callable] = {}
        self._tenant_sems: Dict[str, asyncio.Semaphore] = {}
        self._sems_lock = asyncio.Lock()

        self._active_graphs: Dict[str, ExecutionGraph] = {}
        self._results: Dict[str, RunResult] = {}
        self._result_events: Dict[str, asyncio.Event] = {}
        self._background_tasks: Set[asyncio.Task] = set()
        self._workers: list = []
        self._running = False

        logger.info(
            "RunOrchestrator created: workers=%d queue=%d per_tenant=%d budget=%s ledger=%s",
            worker_count,
            max_queue_size,
            per_tenant_concurrency,
            "enabled" if budget_guard else "disabled",
            "enabled" if ledger else "disabled",
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        loop = asyncio.get_running_loop()
        for i in range(self._worker_count):
            self._workers.append(loop.create_task(self._worker(i), name=f"orchestrator.worker.{i}"))
        logger.info("RunOrchestrator started: %d workers", self._worker_count)

    async def close(self) -> None:
        if not self._running and not self._workers:
            return
        self._running = False
        logger.info("RunOrchestrator shutting down…")

        # 1. Drain queue — wait for all currently-queued items to be dequeued
        if self._worker_count > 0:
            try:
                await asyncio.wait_for(
                    self._queue.join(),
                    timeout=self._shutdown_timeout / 2,
                )
            except asyncio.TimeoutError:
                logger.warning("Queue drain timeout during shutdown")

        # 2. Signal workers to exit via sentinels
        for _ in range(self._worker_count):
            try:
                await asyncio.wait_for(self._queue.put((0, None)), timeout=0.1)
            except (asyncio.TimeoutError, asyncio.QueueFull):
                pass

        if self._workers:
            done, pending = await asyncio.wait(self._workers, timeout=self._shutdown_timeout)
            for t in pending:
                t.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)

        # 3. Drain background ledger tasks
        if self._background_tasks:
            logger.info("Draining %d background ledger tasks…", len(self._background_tasks))
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        self._workers.clear()
        logger.info("RunOrchestrator closed")

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, agent_type: str, handler: Callable) -> None:
        self._dispatch_table[agent_type] = handler
        logger.info("RunOrchestrator registered: agent_type=%s", agent_type)

    def registered_agents(self) -> list:
        return list(self._dispatch_table.keys())

    def queue_size(self) -> int:
        return self._queue.qsize()

    def inflight_run_count(self) -> int:
        return len(self._active_graphs)

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    async def enqueue(self, request: RunRequest) -> str:
        """
        Validate, guard, and enqueue a run request.

        Validation order (Sprint 5):
          1. Orchestrator started
          2. run_id format check (validate_run_id_format)
          3. tenant/run_id cross-check
          4. agent_type registered
          5. queue not full

        Telemetry (Sprint 7):
          inc_queue_depth emitted AFTER successful queue insertion.
        """
        if not self._running:
            raise OrchestratorNotStartedError("Call start() before enqueue()")

        # Sprint 5: validate run_id format and tenant cross-check
        try:
            ext_tenant, _ = validate_run_id_format(request.run_id)
            if ext_tenant != request.tenant_id:
                raise OrchestratorError(
                    f"Tenant mismatch in run_id: "
                    f"run_id encodes {ext_tenant!r} but request.tenant_id={request.tenant_id!r}"
                )
        except TenantFormatError as exc:
            raise OrchestratorError(f"Invalid run_id format: {exc}") from exc

        if request.agent_type not in self._dispatch_table:
            raise AgentNotRegisteredError(
                f"No handler for agent_type={request.agent_type!r}. "
                f"Registered: {sorted(self._dispatch_table)}"
            )

        self._result_events[request.run_id] = asyncio.Event()
        self._active_graphs[request.run_id] = ExecutionGraph(
            run_id=request.run_id, tenant_id=request.tenant_id
        )

        try:
            self._queue.put_nowait((request.priority, request))
        except asyncio.QueueFull:
            # Roll back pre-allocated state on failure
            self._result_events.pop(request.run_id, None)
            self._active_graphs.pop(request.run_id, None)
            raise QueueFullError(request.tenant_id, self._max_queue_size)

        # Emit queue depth metric only after successful insertion.
        HFAMetrics.inc_queue_depth(request.tenant_id)

        logger.info(
            "RunOrchestrator.enqueue: tenant=%s run=%s agent=%s qsize=%d",
            request.tenant_id,
            request.run_id,
            request.agent_type,
            self._queue.qsize(),
        )
        return request.run_id

    # ------------------------------------------------------------------
    # Result retrieval
    # ------------------------------------------------------------------

    async def wait_for(
        self,
        run_id: str,
        timeout: float = 120.0,
        consume: bool = True,
    ) -> RunResult:
        event = self._result_events.get(run_id)
        if event is None:
            raise KeyError(f"Run ID {run_id!r} not enqueued")
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return self.get_result(run_id, consume=consume)

    def get_result(self, run_id: str, consume: bool = False) -> Optional[RunResult]:
        result = self._results.get(run_id)
        if result and consume:
            self._results.pop(run_id, None)
            self._result_events.pop(run_id, None)
        return result

    def get_graph(self, run_id: str) -> Optional[ExecutionGraph]:
        return self._active_graphs.get(run_id)

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def _worker(self, worker_id: int) -> None:
        logger.debug("Worker %d started", worker_id)
        while True:
            try:
                _, item = await self._queue.get()
            except asyncio.CancelledError:
                break

            if item is None:  # sentinel — shut down
                self._queue.task_done()
                break

            request: RunRequest = item

            # Sprint 7: dec in finally — guaranteed even on CancelledError or handler exception
            try:
                await self._execute(request, worker_id)
            finally:
                HFAMetrics.dec_queue_depth(request.tenant_id)
                self._queue.task_done()

        logger.debug("Worker %d stopped", worker_id)

    async def _execute(self, request: RunRequest, worker_id: int) -> None:
        sem = await self._get_tenant_sem(request.tenant_id)
        start = time.perf_counter()

        with _tracer.start_as_current_span("hfa.orchestrator.run") as span:
            HFATracing.set_attrs(
                span,
                {
                    "hfa.run_id": request.run_id,
                    "hfa.tenant_id": request.tenant_id,
                    "hfa.agent_type": request.agent_type,
                    "hfa.worker_id": str(worker_id),
                },
            )

            async with sem:
                if self._budget_guard:
                    try:
                        state = await self._budget_guard.get_state(
                            request.tenant_id, request.run_id
                        )
                        if state and state.status.value in ("exhausted", "frozen"):
                            run_result = RunResult(
                                run_id=request.run_id,
                                tenant_id=request.tenant_id,
                                agent_type=request.agent_type,
                                status=RunStatus.REJECTED,
                                error="Budget exhausted or frozen",
                            )
                            HFATracing.set_attrs(span, {"hfa.status": RunStatus.REJECTED.value})
                            HFATracing.record_exc(span, RuntimeError("Budget exhausted"))
                            await self._record_result(run_result)
                            self._active_graphs.pop(request.run_id, None)
                            return
                    except Exception as exc:
                        logger.error("Budget check failed: %s", exc)

                graph = self._active_graphs.get(request.run_id)
                if graph is None:
                    graph = ExecutionGraph(request.run_id, request.tenant_id)
                    self._active_graphs[request.run_id] = graph

                handler = self._dispatch_table[request.agent_type]
                node_id = graph.add_node(
                    agent_type=request.agent_type,
                    input_data={"run_id": request.run_id},
                )

                try:
                    await graph.start_node(node_id)
                    result_payload = await handler(request, graph)
                    duration_ms = (time.perf_counter() - start) * 1000

                    await graph.commit_node(
                        node_id,
                        tokens=getattr(result_payload, "total_tokens", 0),
                        cost_cents=getattr(result_payload, "cost_cents", 0),
                    )

                    run_result = RunResult(
                        run_id=request.run_id,
                        tenant_id=request.tenant_id,
                        agent_type=request.agent_type,
                        status=RunStatus.DONE,
                        payload=result_payload,
                        duration_ms=duration_ms,
                        graph=graph.snapshot(),
                        tokens_used=graph.total_tokens(),
                        cost_cents=graph.total_cost_cents(),
                    )
                    HFATracing.set_attrs(
                        span,
                        {
                            "hfa.status": RunStatus.DONE.value,
                            "hfa.duration_ms": duration_ms,
                        },
                    )
                    HFATracing.span_ok(span)

                except Exception as exc:
                    duration_ms = (time.perf_counter() - start) * 1000
                    await graph.fail_node(node_id, str(exc))

                    run_result = RunResult(
                        run_id=request.run_id,
                        tenant_id=request.tenant_id,
                        agent_type=request.agent_type,
                        status=RunStatus.FAILED,
                        error=str(exc),
                        duration_ms=duration_ms,
                        graph=graph.snapshot(),
                    )
                    HFATracing.record_exc(span, exc)
                    HFATracing.set_attrs(span, {"hfa.status": RunStatus.FAILED.value})

                await self._record_result(run_result)
                self._active_graphs.pop(request.run_id, None)

    async def _record_result(self, run_result: RunResult) -> None:
        """Store result, signal waiter, emit telemetry, append to ledger."""
        self._results[run_result.run_id] = run_result

        # Sprint 5: ledger append as fire-and-forget background task
        if self._ledger:
            task = asyncio.create_task(
                self._ledger.append(
                    run_result.tenant_id,
                    run_result.run_id,
                    "orchestrator_run",
                    {"status": run_result.status.value},
                )
            )
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

        event = self._result_events.get(run_result.run_id)
        if event:
            event.set()

        # Sprint 7: telemetry
        HFAMetrics.inc_runs_total(run_result.tenant_id, run_result.status.value)
        if run_result.duration_ms is not None:
            HFAMetrics.record_run_latency(run_result.tenant_id, run_result.duration_ms)

    async def _get_tenant_sem(self, tenant_id: str) -> asyncio.Semaphore:
        async with self._sems_lock:
            if tenant_id not in self._tenant_sems:
                self._tenant_sems[tenant_id] = asyncio.Semaphore(self._per_tenant_limit)
            return self._tenant_sems[tenant_id]

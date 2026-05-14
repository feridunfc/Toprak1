"""
hfa-worker/src/hfa_worker/cognitive_executor.py

IRONCLAD Sprint 9.5 + 9.6 — Cognitive Executor (Traced + Finalized Observability)

Sprint 9.5: trace_id propagation
  * trace_id generated once per execution (UUID16 hex)
  * trace_id attached to raw_event passed to SemanticBridge
  * trace_id included in all structured log calls
  * trace_id propagated in ExecutionResult.payload

Sprint 9.6: finalized observability counter naming
  * cognitive_started, cognitive_completed, cognitive_failed (canonical)
  * semantic_degraded counter on build
  * All log events carry run_id + trace_id
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from typing import Any

from hfa.events.schema import RunRequestedEvent
from hfa_worker.executor import BaseExecutor
from hfa_worker.models import ExecutionError, ExecutionResult

# Module-level imports for mock.patch compatibility
try:
    from hfa_agents.integration.semantic_bridge import SemanticBridge
except ImportError:
    SemanticBridge = None  # type: ignore[assignment,misc]

try:
    from hfa_agents.workflow.engine import WorkflowEngine
except ImportError:
    WorkflowEngine = None  # type: ignore[assignment,misc]

try:
    from hfa_worker.feedback_writer import FeedbackWriter
except ImportError:
    FeedbackWriter = None  # type: ignore[assignment,misc]


logger = logging.getLogger("hfa.cognitive_executor")

COGNITIVE_AGENT_TYPES = frozenset({"cognitive", "agent", "ai_task", "hfa_agent"})


def _new_trace_id() -> str:
    """Generate a short trace ID for a single cognitive execution."""
    return uuid.uuid4().hex[:16]


# ── CognitiveMetrics ──────────────────────────────────────────────────────────

class CognitiveMetrics:
    """Lightweight in-process counters for cognitive execution path."""

    def __init__(self) -> None:
        # Sprint 9.6: canonical counter names
        self.cognitive_started:   int = 0
        self.cognitive_completed: int = 0   # done (success or graceful fail)
        self.cognitive_failed:    int = 0   # crash or unexpected error
        self.filtered:            int = 0   # filtered_by_semantic
        self.semantic_degraded:   int = 0   # ran without semantic pipeline
        self._total_latency_ms:   float = 0.0
        self._latency_samples:    int = 0

    def inc_started(self)   -> None: self.cognitive_started += 1
    def inc_done(self)      -> None: self.cognitive_completed += 1
    def inc_failed(self)    -> None: self.cognitive_failed += 1
    def inc_filtered(self)  -> None: self.filtered += 1
    def inc_degraded(self)  -> None: self.semantic_degraded += 1

    def record_latency(self, ms: float) -> None:
        self._total_latency_ms += ms
        self._latency_samples += 1

    @property
    def avg_latency_ms(self) -> float:
        return (
            self._total_latency_ms / self._latency_samples
            if self._latency_samples > 0 else 0.0
        )

    def snapshot(self) -> dict:
        return {
            "cognitive_started":       self.cognitive_started,
            "cognitive_completed":     self.cognitive_completed,
            "cognitive_failed":        self.cognitive_failed,
            "cognitive_filtered":      self.filtered,
            "semantic_degraded":       self.semantic_degraded,
            "cognitive_avg_latency_ms": round(self.avg_latency_ms, 2),
        }


_metrics = CognitiveMetrics()


def get_cognitive_metrics() -> CognitiveMetrics:
    """Return the module-level CognitiveMetrics instance."""
    return _metrics


# ── CognitiveExecutor ─────────────────────────────────────────────────────────

class CognitiveExecutor(BaseExecutor):
    """
    Cognitive execution: RunRequestedEvent → SemanticBridge → WorkflowEngine → ExecutionResult.

    Sprint 9.5: trace_id generated per execution, propagated to all layers.
    Sprint 9.6: canonical observability counter names.
    """

    def __init__(
        self,
        semantic_pipeline: Any | None = None,
        max_budget_cents: int = 2000,
    ) -> None:
        self._semantic = semantic_pipeline
        self._max_budget = max_budget_cents
        self._workflow_engine: Any = None

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def build(cls, config: Any) -> "CognitiveExecutor":
        budget = int(
            config.get("cognitive_budget_cents", os.environ.get("COGNITIVE_BUDGET_CENTS", 2000))
        ) if isinstance(config, dict) else int(os.environ.get("COGNITIVE_BUDGET_CENTS", 2000))

        semantic_pipeline = None
        semantic_mode = "degraded"
        try:
            from hfa_semantic.runtime.factory import build_pipeline_sync
            import redis.asyncio as aioredis
            redis_url = (
                config.get("redis_url") if isinstance(config, dict) else None
            ) or os.environ.get("REDIS_URL", "redis://localhost:6379")
            if redis_url:
                redis_client = aioredis.from_url(redis_url, decode_responses=False)
                semantic_pipeline = build_pipeline_sync(redis_client=redis_client)
                semantic_mode = "pipeline"
                logger.info(
                    "CognitiveExecutor.build: semantic=pipeline budget=%d¢", budget
                )
            else:
                logger.info(
                    "CognitiveExecutor.build: no REDIS_URL — semantic=degraded budget=%d¢",
                    budget,
                )
        except ImportError:
            logger.warning(
                "CognitiveExecutor.build: hfa-semantic not installed — semantic=degraded"
            )
        except Exception as exc:
            logger.warning(
                "CognitiveExecutor.build: semantic init failed: %s — semantic=degraded", exc
            )

        if semantic_mode == "degraded":
            _metrics.inc_degraded()

        return cls(semantic_pipeline=semantic_pipeline, max_budget_cents=budget)

    # ── BaseExecutor protocol ─────────────────────────────────────────────────

    async def execute(self, run_event: RunRequestedEvent) -> ExecutionResult:
        agent_type = run_event.agent_type or ""
        run_id     = run_event.run_id

        if agent_type not in COGNITIVE_AGENT_TYPES:
            logger.debug(
                "CognitiveExecutor: non-cognitive run=%s agent_type=%s → pass-through",
                run_id, agent_type,
            )
            return ExecutionResult(
                status="done",
                payload={"run_id": run_id, "agent_type": agent_type, "note": "not_cognitive_type"},
                cost_cents=0,
                tokens_used=0,
            )

        # Sprint 9.5: generate trace_id once per execution
        trace_id = _new_trace_id()

        _metrics.inc_started()
        semantic_mode = "pipeline" if self._semantic is not None else "degraded"
        logger.info(
            "cognitive_started run=%s trace_id=%s agent_type=%s semantic=%s",
            run_id, trace_id, agent_type, semantic_mode,
        )
        t0 = time.perf_counter()

        try:
            result = await self._execute_cognitive(run_event, trace_id)
            duration_ms = round((time.perf_counter() - t0) * 1000)
            _metrics.record_latency(duration_ms)
            _metrics.inc_done()
            logger.info(
                "cognitive_completed run=%s trace_id=%s status=%s "
                "duration_ms=%d cost_cents=%d",
                run_id, trace_id, result.status, duration_ms, result.cost_cents,
            )
            return result

        except Exception as exc:
            duration_ms = round((time.perf_counter() - t0) * 1000)
            _metrics.inc_failed()
            _metrics.record_latency(duration_ms)
            logger.error(
                "cognitive_failed run=%s trace_id=%s duration_ms=%d error=%s",
                run_id, trace_id, duration_ms, exc, exc_info=True,
            )
            return ExecutionResult(
                status="failed",
                payload={"run_id": run_id, "trace_id": trace_id},
                error=f"cognitive_executor_crash: {exc}",
                cost_cents=0,
                tokens_used=0,
            )

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _execute_cognitive(
        self, run_event: RunRequestedEvent, trace_id: str
    ) -> ExecutionResult:
        run_id     = run_event.run_id
        tenant_id  = run_event.tenant_id
        agent_type = run_event.agent_type
        payload    = run_event.payload or {}
        goal       = payload.get("goal", "")

        t0 = time.perf_counter()

        # ── 1. Semantic enrichment ─────────────────────────────────────────
        import hfa_worker.cognitive_executor as _ce_mod
        _SB = _ce_mod.SemanticBridge
        if _SB is None:
            from hfa_agents.integration.semantic_bridge import SemanticBridge as _SB
        bridge = _SB(semantic_pipeline=self._semantic)
        enriched_dict = await bridge.enrich_event(
            raw_event={
                "event_id":       f"{run_id}:run",
                "event_type":     agent_type,
                "goal":           goal,
                "tenant_id":      tenant_id,
                "run_id":         run_id,
                "lineage_run_id": run_id,
                "trace_id":       trace_id,  # Sprint 9.5: inject trace
                **payload,
            },
            workflow_id=run_id,
            execution_id=run_id,
        )

        if enriched_dict is None:
            _metrics.inc_filtered()
            logger.info(
                "semantic_dropped run=%s trace_id=%s reason=filtered_by_semantic",
                run_id, trace_id,
            )
            return ExecutionResult(
                status="done",
                payload={"run_id": run_id, "status": "filtered_by_semantic", "trace_id": trace_id},
                cost_cents=0,
                tokens_used=0,
            )

        semantic_enriched = enriched_dict.get("semantic_enriched", False)
        logger.debug(
            "semantic_processed run=%s trace_id=%s semantic_enriched=%s",
            run_id, trace_id, semantic_enriched,
        )

        # ── 2. Agent orchestration ────────────────────────────────────────
        engine = self._get_engine()

        logger.info("workflow_started run=%s trace_id=%s goal=%r", run_id, trace_id, goal[:60])
        agent_result = await engine.run_workflow(enriched_dict)

        duration_ms = round((time.perf_counter() - t0) * 1000)
        step_count  = len(agent_result.reasoning_trace) if agent_result.reasoning_trace else 0
        cost_cents  = getattr(agent_result, "cost_cents", 0) or 0

        logger.info(
            "workflow_finished run=%s trace_id=%s agent_status=%s "
            "step_count=%d duration_ms=%d cost_cents=%d confidence=%.2f",
            run_id, trace_id, agent_result.status,
            step_count, duration_ms, cost_cents, agent_result.confidence,
        )

        # ── 3. Async feedback loop (non-blocking) ─────────────────────────
        import hfa_worker.cognitive_executor as _ce_mod
        _FW = _ce_mod.FeedbackWriter
        if _FW is None:
            from hfa_worker.feedback_writer import FeedbackWriter as _FW
        fw = _FW(semantic_pipeline=self._semantic)
        if agent_result.status == "success" and not agent_result.requires_hitl:
            _coro = fw.write(
                    execution_result=agent_result,
                    task_id=run_id,
                    run_id=run_id,
                    tenant_id=tenant_id,
                )
            if asyncio.iscoroutine(_coro):
                asyncio.create_task(_coro)
            logger.debug(
                "CognitiveExecutor: feedback_write scheduled run=%s trace_id=%s",
                run_id, trace_id,
            )
        else:
            logger.debug(
                "CognitiveExecutor: feedback_write skipped run=%s trace_id=%s "
                "agent_status=%s requires_hitl=%s",
                run_id, trace_id, agent_result.status, agent_result.requires_hitl,
            )

        # ── 4. Map result ──────────────────────────────────────────────────
        is_success = agent_result.status == "success"

        return ExecutionResult(
            status="done" if is_success else "failed",
            payload={
                "run_id":            run_id,
                "trace_id":          trace_id,    # Sprint 9.5: carry trace
                "agent_status":      agent_result.status,
                "confidence":        agent_result.confidence,
                "artifacts":         list(agent_result.output_data.keys()),
                "requires_hitl":     agent_result.requires_hitl,
                "reasoning_trace":   agent_result.reasoning_trace[-5:] if agent_result.reasoning_trace else [],
                "duration_ms":       duration_ms,
                "step_count":        step_count,
                "semantic_enriched": semantic_enriched,
            },
            error=(agent_result.suggested_feedback or "") if not is_success else None,
            cost_cents=cost_cents,
            tokens_used=0,
        )

    def _get_engine(self) -> Any:
        if self._workflow_engine is None:
            import hfa_worker.cognitive_executor as _ce_mod
            _WE = _ce_mod.WorkflowEngine
            if _WE is None:
                from hfa_agents.workflow.engine import WorkflowEngine as _WE
            self._workflow_engine = _WE(
                semantic_engine=self._semantic,
                cost_budget_cents=self._max_budget,
            )
        return self._workflow_engine

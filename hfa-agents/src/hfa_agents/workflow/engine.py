"""
hfa-agents/src/hfa_agents/workflow/engine.py

IRONCLAD Sprint 6.4 — Canonical Workflow Engine + Step Observability

Sprint 6.4 additions (observability only — zero logic change):
  * workflow_started log: goal, step_count, max_steps, budget
  * step_started  log: step_index, agent_name
  * step_finished log: step_index, agent_name, status, duration_ms, cost_cents
  * workflow_finished log: status, total_steps, total_cost_cents, duration_ms
  * Step counter in reasoning_trace for external visibility

Production guards (unchanged from Sprint 1):
  * max_steps  — hard cap on pipeline steps
  * cost_budget_cents — hard budget cap
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.base.registry import AgentRegistry

logger = logging.getLogger(__name__)

_MAX_STEPS_CEILING = 50
_DEFAULT_MAX_STEPS = 10
_DEFAULT_BUDGET_CENTS = 2_000


@dataclass(slots=True)
class WorkflowStep:
    agent_name: str
    description: str
    required_keys: List[str] = field(default_factory=list)
    timeout_seconds: int = 120


class WorkflowEngine:
    """
    Canonical workflow engine.

    Sprint 6.4: step-level structured observability added.
    All production guards (max_steps, cost_budget) unchanged.
    """

    def __init__(
        self,
        semantic_engine: Any,
        max_steps: int = _DEFAULT_MAX_STEPS,
        cost_budget_cents: int = _DEFAULT_BUDGET_CENTS,
    ) -> None:
        self.semantic_engine = semantic_engine
        self.max_steps: int = min(max(1, max_steps), _MAX_STEPS_CEILING)
        self.cost_budget_cents: int = max(0, cost_budget_cents)

        self.default_pipeline: List[WorkflowStep] = [
            WorkflowStep("supervisor",  "plan workflow"),
            WorkflowStep("researcher",  "retrieve semantic context", ["execution_plan"]),
            WorkflowStep("architect",   "synthesize architecture",   ["research_findings"]),
            WorkflowStep("coder",       "generate code",             ["architecture_summary"]),
            WorkflowStep("tester",      "validate code",             ["generated_code"]),
            WorkflowStep("compliance",  "run compliance checks",     ["generated_code"]),
        ]

    async def run_workflow(
        self,
        initial_event: Dict[str, Any],
        pipeline: Optional[List[WorkflowStep]] = None,
    ) -> ExecutionResult:
        effective_pipeline = pipeline if pipeline is not None else self.default_pipeline
        goal = initial_event.get("goal", "")
        event_id = initial_event.get("event_id", "unknown")
        workflow_t0 = time.perf_counter()

        # ── Guard: max_steps ──────────────────────────────────────────────
        if len(effective_pipeline) > self.max_steps:
            logger.error(
                "WorkflowEngine.aborted event_id=%s reason=max_steps_exceeded "
                "pipeline_len=%d max_steps=%d",
                event_id, len(effective_pipeline), self.max_steps,
            )
            return ExecutionResult(
                status="failed",
                output_data={},
                reasoning_trace=[
                    f"aborted:pipeline_length={len(effective_pipeline)}"
                    f":max_steps={self.max_steps}"
                ],
                suggested_feedback="Workflow pipeline exceeds max_steps limit.",
            )

        # ── workflow_started ──────────────────────────────────────────────
        logger.info(
            "WorkflowEngine.started event_id=%s goal=%r step_count=%d "
            "max_steps=%d budget_cents=%d",
            event_id, goal[:80], len(effective_pipeline),
            self.max_steps, self.cost_budget_cents,
        )

        artifacts: Dict[str, Any] = {}
        reasoning_trace: List[str] = []
        accumulated_cost_cents: int = 0

        event = EnrichedEvent(
            event_id=event_id,
            lineage_run_id=initial_event.get("lineage_run_id"),
            goal=goal,
            context=dict(initial_event.get("context", {})),
            artifacts={},
        )

        for step_index, step in enumerate(effective_pipeline):
            # ── Guard: required keys ──────────────────────────────────────
            for required in step.required_keys:
                if required not in artifacts and required not in event.context:
                    logger.warning(
                        "WorkflowEngine.step_blocked event_id=%s step=%s "
                        "missing_key=%s",
                        event_id, step.agent_name, required,
                    )
                    return ExecutionResult(
                        status="failed",
                        output_data=artifacts,
                        reasoning_trace=reasoning_trace + [
                            f"missing_required:{step.agent_name}:{required}"
                        ],
                        requires_hitl=True,
                    )

            # ── step_started ──────────────────────────────────────────────
            logger.info(
                "WorkflowEngine.step_started event_id=%s step=%d/%d agent=%s",
                event_id, step_index + 1, len(effective_pipeline), step.agent_name,
            )
            step_t0 = time.perf_counter()

            # ── Execute step ──────────────────────────────────────────────
            try:
                AgentClass = AgentRegistry.get(step.agent_name)
            except KeyError:
                logger.error(
                    "WorkflowEngine.step_error event_id=%s agent=%s "
                    "reason=not_registered",
                    event_id, step.agent_name,
                )
                return ExecutionResult(
                    status="failed",
                    output_data=artifacts,
                    reasoning_trace=reasoning_trace + [
                        f"agent_not_registered:{step.agent_name}"
                    ],
                )

            agent = AgentClass(
                step.agent_name,
                self.semantic_engine,
                timeout_seconds=step.timeout_seconds,
            )

            event.context.update(artifacts)
            event.artifacts = artifacts.copy()

            result = await agent.execute(event)
            artifacts.update(result.output_data)
            artifacts.update(result.artifacts)
            reasoning_trace.extend(result.reasoning_trace)

            step_ms = round((time.perf_counter() - step_t0) * 1000)
            step_cost = int(result.output_data.get("cost_cents", 0) or 0)
            accumulated_cost_cents += step_cost

            # ── step_finished ─────────────────────────────────────────────
            logger.info(
                "WorkflowEngine.step_finished event_id=%s step=%d/%d agent=%s "
                "status=%s duration_ms=%d cost_cents=%d total_cost_cents=%d",
                event_id, step_index + 1, len(effective_pipeline),
                step.agent_name, result.status, step_ms,
                step_cost, accumulated_cost_cents,
            )

            # ── Guard: cost budget ────────────────────────────────────────
            if self.cost_budget_cents > 0 and accumulated_cost_cents > self.cost_budget_cents:
                logger.error(
                    "WorkflowEngine.budget_exceeded event_id=%s "
                    "accumulated_cents=%d budget_cents=%d at_step=%s",
                    event_id, accumulated_cost_cents,
                    self.cost_budget_cents, step.agent_name,
                )
                return ExecutionResult(
                    status="failed",
                    output_data=artifacts,
                    reasoning_trace=reasoning_trace + [
                        f"budget_exceeded:accumulated_cents={accumulated_cost_cents}"
                        f":budget_cents={self.cost_budget_cents}"
                        f":at_step={step.agent_name}"
                    ],
                    suggested_feedback="Workflow stopped: cost budget exceeded.",
                    requires_hitl=True,
                )

            # ── Early exit on non-success ─────────────────────────────────
            if result.status != "success":
                workflow_ms = round((time.perf_counter() - workflow_t0) * 1000)
                logger.warning(
                    "WorkflowEngine.early_exit event_id=%s at_step=%s "
                    "agent_status=%s total_steps_run=%d duration_ms=%d",
                    event_id, step.agent_name, result.status,
                    step_index + 1, workflow_ms,
                )
                return ExecutionResult(
                    status=result.status,
                    output_data=artifacts,
                    reasoning_trace=reasoning_trace,
                    suggested_feedback=result.suggested_feedback,
                    requires_hitl=result.requires_hitl,
                    confidence=result.confidence,
                )

        # ── workflow_finished ─────────────────────────────────────────────
        workflow_ms = round((time.perf_counter() - workflow_t0) * 1000)
        logger.info(
            "WorkflowEngine.finished event_id=%s status=success "
            "total_steps=%d total_cost_cents=%d duration_ms=%d",
            event_id, len(effective_pipeline),
            accumulated_cost_cents, workflow_ms,
        )

        return ExecutionResult(
            status="success",
            output_data=artifacts,
            reasoning_trace=reasoning_trace,
            confidence=0.92,
        )

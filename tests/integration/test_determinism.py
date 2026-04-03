"""
tests/integration/test_determinism.py

IRONCLAD Sprint 9.3 — Determinism Test Suite

Validates that the cognitive pipeline produces consistent, predictable
results across multiple calls with the same input.

Tests:
  A. Same event → same status + same payload structure (idempotent shape)
  B. Retry determinism — same result on second call
  C. Degraded mode determinism — no Redis still produces consistent output
  D. Filtered event determinism — duplicate returns same "filtered" status
  E. FeedbackValidator determinism — same input always same accept/reject
  F. DegradedPipeline determinism — no-op components behave consistently
  G. WorkflowEngine determinism — same pipeline steps → same trace structure

No external services required. All mocks are deterministic.
"""

from __future__ import annotations

import pytest
from dataclasses import dataclass, field
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch


# ── Helpers ───────────────────────────────────────────────────────────────────

@dataclass
class _RunEvent:
    run_id:     str = "run-det-001"
    tenant_id:  str = "tenant-det"
    agent_type: str = "cognitive"
    payload:    Dict[str, Any] = field(default_factory=lambda: {"goal": "deterministic test"})


def _make_event(**kw) -> _RunEvent:
    ev = _RunEvent()
    for k, v in kw.items():
        object.__setattr__(ev, k, v)
    return ev


def _make_agent_result(status="success", confidence=0.88, cost=30):
    r = MagicMock()
    r.status = status
    r.confidence = confidence
    r.cost_cents = cost
    r.requires_hitl = False
    r.suggested_feedback = None
    r.reasoning_trace = ["step_1", "step_2", "step_3"]
    r.output_data = {"result": "ok", "type": "test"}
    r.artifacts = {}
    return r


# ── A: Same input → same output shape ────────────────────────────────────────

@pytest.mark.asyncio
async def test_same_input_produces_same_output_shape():
    """Two identical calls must produce ExecutionResult with the same structure."""
    from hfa_worker.cognitive_executor import CognitiveExecutor
    from hfa_worker.models import ExecutionResult

    agent_result = _make_agent_result("success", 0.88)

    async def _run():
        with patch("hfa_worker.cognitive_executor.SemanticBridge") as MB, \
             patch("hfa_worker.cognitive_executor.WorkflowEngine") as ME, \
             patch("hfa_worker.cognitive_executor.FeedbackWriter"):
            MB.return_value.enrich_event = AsyncMock(
                return_value={"event_id": "run-det-001:run", "goal": "test",
                              "semantic_enriched": True, "trace_id": "abc"}
            )
            ME.return_value.run_workflow = AsyncMock(return_value=agent_result)
            executor = CognitiveExecutor(semantic_pipeline=None)
            return await executor.execute(_make_event())

    r1 = await _run()
    r2 = await _run()

    assert isinstance(r1, ExecutionResult)
    assert isinstance(r2, ExecutionResult)

    # Status must be identical
    assert r1.status == r2.status, f"status mismatch: {r1.status} vs {r2.status}"

    # Payload keys must be identical
    assert set(r1.payload.keys()) == set(r2.payload.keys()), \
        f"payload key mismatch: {set(r1.payload.keys())} vs {set(r2.payload.keys())}"

    # Structural fields must match
    for field_name in ("agent_status", "confidence", "requires_hitl", "step_count"):
        assert r1.payload[field_name] == r2.payload[field_name], \
            f"payload[{field_name}] mismatch: {r1.payload[field_name]} vs {r2.payload[field_name]}"


# ── B: Retry determinism ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_retry_produces_same_result():
    """Simulated retry (same run_id, same payload) → same result."""
    from hfa_worker.cognitive_executor import CognitiveExecutor

    agent_result = _make_agent_result("success", 0.90)

    results = []
    for _ in range(3):
        with patch("hfa_worker.cognitive_executor.SemanticBridge") as MB, \
             patch("hfa_worker.cognitive_executor.WorkflowEngine") as ME, \
             patch("hfa_worker.cognitive_executor.FeedbackWriter"):
            MB.return_value.enrich_event = AsyncMock(
                return_value={"event_id": "run-retry:run", "goal": "retry test",
                              "semantic_enriched": True, "trace_id": "xyz"}
            )
            ME.return_value.run_workflow = AsyncMock(return_value=agent_result)
            executor = CognitiveExecutor(semantic_pipeline=None)
            r = await executor.execute(_make_event(run_id="run-retry"))
            results.append(r)

    statuses = {r.status for r in results}
    assert len(statuses) == 1, f"retry produced inconsistent statuses: {statuses}"

    confidences = {r.payload["confidence"] for r in results}
    assert len(confidences) == 1, f"retry produced inconsistent confidences: {confidences}"


# ── C: Degraded mode determinism ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_degraded_mode_is_deterministic():
    """semantic_pipeline=None always produces the same output shape."""
    from hfa_worker.cognitive_executor import CognitiveExecutor

    agent_result = _make_agent_result("success", 0.82)

    results = []
    for _ in range(2):
        with patch("hfa_worker.cognitive_executor.SemanticBridge") as MB, \
             patch("hfa_worker.cognitive_executor.WorkflowEngine") as ME, \
             patch("hfa_worker.cognitive_executor.FeedbackWriter"):
            MB.return_value.enrich_event = AsyncMock(
                return_value={"event_id": "run-deg:run", "goal": "degraded",
                              "semantic_enriched": False, "trace_id": "deg"}
            )
            ME.return_value.run_workflow = AsyncMock(return_value=agent_result)
            executor = CognitiveExecutor(semantic_pipeline=None)
            r = await executor.execute(_make_event(run_id="run-deg"))
            results.append(r)

    assert results[0].status == results[1].status
    assert results[0].payload["semantic_enriched"] == results[1].payload["semantic_enriched"]


# ── D: Filtered event determinism ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_filtered_event_is_always_done():
    """When semantic filters an event (returns None), result is always done/filtered."""
    from hfa_worker.cognitive_executor import CognitiveExecutor

    for _ in range(3):
        with patch("hfa_worker.cognitive_executor.SemanticBridge") as MB, \
             patch("hfa_worker.cognitive_executor.WorkflowEngine"):
            MB.return_value.enrich_event = AsyncMock(return_value=None)
            executor = CognitiveExecutor(semantic_pipeline=MagicMock())
            r = await executor.execute(_make_event())

        assert r.status == "done", f"filtered event must be done, got {r.status}"
        assert r.payload.get("status") == "filtered_by_semantic"


# ── E: FeedbackValidator determinism ─────────────────────────────────────────

def test_feedback_validator_is_deterministic():
    """Same input always produces same accept/reject decision."""
    from hfa_semantic.validation.feedback_validator import FeedbackValidator

    validator = FeedbackValidator(min_confidence=0.60)

    result_ok = MagicMock(
        status="success", confidence=0.85, requires_hitl=False,
        output_data={"x": 1}, reasoning_trace=["step"]
    )
    result_low = MagicMock(
        status="success", confidence=0.40, requires_hitl=False,
        output_data={"x": 1}, reasoning_trace=["step"]
    )

    # Run 5 times each — must be identical
    decisions_ok   = [validator.validate(result_ok).accepted for _ in range(5)]
    decisions_low  = [validator.validate(result_low).accepted for _ in range(5)]

    assert all(decisions_ok),   "high-confidence result must always be accepted"
    assert not any(decisions_low), "low-confidence result must always be rejected"


# ── F: DegradedPipeline determinism ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_degraded_pipeline_no_op_is_consistent():
    """DegradedPipeline always returns False for is_duplicate, never marks late."""
    from hfa_semantic.runtime.factory import DegradedPipeline

    p = DegradedPipeline()

    # Dedup: always False (accept everything)
    results = [await p.dedup_store.is_duplicate(f"event-{i}") for i in range(5)]
    assert all(r is False for r in results), "DegradedPipeline dedup must always return False"

    # Watermark: never late
    wm_results = [p.watermark.observe(float(i * 1000)) for i in range(5)]
    assert all(r[0] is False for r in wm_results), "DegradedPipeline watermark must never be late"


# ── G: WorkflowEngine step trace determinism ─────────────────────────────────

@pytest.mark.asyncio
async def test_workflow_engine_trace_is_deterministic():
    """Same pipeline steps → reasoning_trace ends with same agent names."""
    from hfa_agents.workflow.engine import WorkflowEngine, WorkflowStep

    class _StubAgent:
        def __init__(self, *a, **kw): pass
        async def execute(self, event):
            from hfa_agents.base.contracts import ExecutionResult
            return ExecutionResult(
                status="success",
                output_data={},
                reasoning_trace=[f"agent_ok"],
                confidence=0.9,
            )

    from hfa_agents.base.registry import AgentRegistry
    original_sup = AgentRegistry._agents.get("supervisor")
    original_res = AgentRegistry._agents.get("researcher")
    try:
        AgentRegistry._agents["supervisor"] = _StubAgent
        AgentRegistry._agents["researcher"] = _StubAgent

        results = []
        for _ in range(2):
            engine = WorkflowEngine(semantic_engine=MagicMock(), max_steps=5)
            r = await engine.run_workflow(
                {"event_id": "det-wf-01", "goal": "determinism test"},
                pipeline=[
                    WorkflowStep("supervisor", "plan"),
                    WorkflowStep("researcher", "research", ["execution_plan"]),
                ],
            )
            results.append(r)

        # Both runs must have same status and same trace length
        assert results[0].status == results[1].status
        assert len(results[0].reasoning_trace) == len(results[1].reasoning_trace)

    finally:
        if original_sup:
            AgentRegistry._agents["supervisor"] = original_sup
        else:
            AgentRegistry._agents.pop("supervisor", None)
        if original_res:
            AgentRegistry._agents["researcher"] = original_res
        else:
            AgentRegistry._agents.pop("researcher", None)

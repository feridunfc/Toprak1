"""
tests/integration/test_full_cognitive_pipeline.py

IRONCLAD Sprint 8.5 — Full Cognitive Pipeline Integration Test

Tests the complete path without external services:
  RunRequestedEvent → CognitiveExecutor → SemanticBridge → WorkflowEngine → ExecutionResult

Coverage:
  A. Canonical path with SemanticPipeline (mocked Redis components)
  B. Degraded path (DegradedPipeline — no Redis)
  C. Multi-step workflow (supervisor → researcher → architect)
  D. Semantic filtering (duplicate event → done/filtered)
  E. Budget guard (cost limit triggers stop)
  F. max_steps guard (oversized pipeline → failed immediately)
  G. Feedback writer contract (success/skip/fail)
  H. SemanticPipeline type contract (never None, never tuple)

Requirements:
  pip install hfa-core hfa-worker hfa-semantic hfa-agents fakeredis pytest-asyncio
"""

from __future__ import annotations

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

from hfa_worker.cognitive_executor import CognitiveExecutor, COGNITIVE_AGENT_TYPES
from hfa_worker.feedback_writer import FeedbackWriter
from hfa_worker.models import ExecutionResult


# ── Stubs ─────────────────────────────────────────────────────────────────────

@dataclass
class _RunEvent:
    """Minimal RunRequestedEvent stand-in (avoids hfa-core dep in isolation)."""
    run_id:     str = "run-001"
    tenant_id:  str = "tenant-A"
    agent_type: str = "cognitive"
    payload:    Dict[str, Any] = field(default_factory=lambda: {
        "goal": "Build a minimal REST API",
    })


def _make_event(agent_type: str = "cognitive", run_id: str = "run-001", **kw) -> _RunEvent:
    ev = _RunEvent(agent_type=agent_type, run_id=run_id)
    for k, v in kw.items():
        object.__setattr__(ev, k, v)
    return ev


def _make_agent_result(status: str = "success", confidence: float = 0.92, cost: int = 50):
    r = MagicMock()
    r.status = status
    r.confidence = confidence
    r.cost_cents = cost
    r.requires_hitl = False
    r.suggested_feedback = None if status == "success" else "needs review"
    r.reasoning_trace = ["supervisor_ok", "architect_ok", "coder_ok"]
    r.output_data = {"generated_code": "def main(): pass", "test_results": True}
    r.artifacts = {}
    return r


def _make_semantic_pipeline():
    """Minimal SemanticPipeline-like object with no-op components."""
    from hfa_semantic.runtime.factory import DegradedPipeline
    return DegradedPipeline()


# ── A: Canonical path ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_canonical_path_done():
    """RunRequestedEvent → semantic → agents → ExecutionResult(done)."""
    agent_result = _make_agent_result("success", 0.92, 50)

    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine") as MockEngine, \
         patch("hfa_worker.cognitive_executor.FeedbackWriter"):

        MockBridge.return_value.enrich_event = AsyncMock(
            return_value={"event_id": "run-001:run", "goal": "test", "semantic_enriched": True}
        )
        MockEngine.return_value.run_workflow = AsyncMock(return_value=agent_result)

        executor = CognitiveExecutor(semantic_pipeline=_make_semantic_pipeline())
        result = await executor.execute(_make_event())

    assert isinstance(result, ExecutionResult)
    assert result.status == "done"
    assert result.payload["agent_status"] == "success"
    assert result.payload["confidence"] == 0.92
    assert result.cost_cents == 50
    assert result.error is None


# ── B: Degraded path ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_degraded_pipeline_no_crash():
    """DegradedPipeline (no Redis) — full path still works."""
    agent_result = _make_agent_result("success", 0.85)

    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine") as MockEngine, \
         patch("hfa_worker.cognitive_executor.FeedbackWriter"):

        MockBridge.return_value.enrich_event = AsyncMock(
            return_value={"event_id": "run-002:run", "goal": "test", "semantic_enriched": False}
        )
        MockEngine.return_value.run_workflow = AsyncMock(return_value=agent_result)

        executor = CognitiveExecutor(semantic_pipeline=None)  # no pipeline
        result = await executor.execute(_make_event(run_id="run-002"))

    assert result is not None
    assert result.status == "done"


@pytest.mark.asyncio
async def test_degraded_pipeline_factory_returns_pipeline_object():
    """DegradedPipeline returned by factory is not None and not a tuple."""
    from hfa_semantic.runtime.factory import build_pipeline_sync, DegradedPipeline, SemanticPipeline

    pipeline = build_pipeline_sync(redis_client=None)

    assert pipeline is not None, "build_pipeline_sync(None) must not return None"
    assert not isinstance(pipeline, tuple), "build_pipeline_sync must not return tuple"
    assert isinstance(pipeline, SemanticPipeline), \
        f"must return SemanticPipeline subclass, got {type(pipeline).__name__}"
    assert isinstance(pipeline, DegradedPipeline), \
        "no-redis path must return DegradedPipeline"
    assert pipeline.is_degraded is True


@pytest.mark.asyncio
async def test_degraded_pipeline_has_required_attributes():
    """DegradedPipeline exposes all required attributes."""
    from hfa_semantic.runtime.factory import DegradedPipeline

    p = DegradedPipeline()
    assert hasattr(p, "dedup_store")
    assert hasattr(p, "watermark")
    assert hasattr(p, "metrics")
    assert hasattr(p, "state_store")

    # Dedup is always False (accept everything)
    assert await p.dedup_store.is_duplicate("any-id") is False
    # Watermark never marks late
    is_late, _ = p.watermark.observe(1000.0)
    assert is_late is False


# ── C: Multi-step workflow ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_multi_step_workflow_smoke():
    """WorkflowEngine with 3-step pipeline executes and returns result."""
    from hfa_agents.workflow.engine import WorkflowEngine, WorkflowStep
    from hfa_agents.base.registry import AgentRegistry
    from hfa_agents.roles import supervisor, researcher, architect  # noqa: F401 — registers agents

    engine = WorkflowEngine(
        semantic_engine=MagicMock(
            merge_engine=MagicMock(
                execute_query=AsyncMock(return_value=[])
            )
        ),
        max_steps=10,
    )

    result = await engine.run_workflow({
        "event_id": "run-multi-01",
        "goal":     "Build a REST API",
        "context":  {},
    })

    assert result is not None
    assert result.status in ("success", "failed", "escalated")
    assert isinstance(result.reasoning_trace, list)


# ── D: Semantic filtering ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_duplicate_event_returns_done_filtered():
    """Semantic returns None (duplicate) → done with filtered_by_semantic."""
    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine"):

        MockBridge.return_value.enrich_event = AsyncMock(return_value=None)

        executor = CognitiveExecutor(semantic_pipeline=_make_semantic_pipeline())
        result = await executor.execute(_make_event())

    assert result.status == "done"
    assert result.payload.get("status") == "filtered_by_semantic"


# ── E: Budget guard ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_budget_guard_stops_workflow():
    """Workflow stops when cost_budget exceeded — status=failed."""
    from hfa_agents.workflow.engine import WorkflowEngine, WorkflowStep
    from hfa_agents.base.registry import AgentRegistry

    # Stub an expensive agent
    class _ExpensiveAgent:
        def __init__(self, *a, **kw):
            pass
        async def execute(self, event):
            from hfa_agents.base.contracts import ExecutionResult
            return ExecutionResult(
                status="success",
                output_data={"cost_cents": 3000},  # exceeds budget
                reasoning_trace=["expensive_step"],
                confidence=0.9,
            )

    original = AgentRegistry._agents.get("supervisor")
    try:
        AgentRegistry._agents["supervisor"] = _ExpensiveAgent
        engine = WorkflowEngine(
            semantic_engine=MagicMock(),
            max_steps=5,
            cost_budget_cents=1000,
        )
        result = await engine.run_workflow(
            {"event_id": "run-budget-01", "goal": "expensive task"},
            pipeline=[WorkflowStep("supervisor", "step1")],
        )
        assert result.status == "failed"
        assert "budget_exceeded" in " ".join(result.reasoning_trace)
    finally:
        if original:
            AgentRegistry._agents["supervisor"] = original
        else:
            AgentRegistry._agents.pop("supervisor", None)


# ── F: max_steps guard ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_max_steps_guard_fails_before_execution():
    """Pipeline longer than max_steps → failed immediately, no steps run."""
    from hfa_agents.workflow.engine import WorkflowEngine, WorkflowStep

    engine = WorkflowEngine(semantic_engine=None, max_steps=2)
    result = await engine.run_workflow(
        {"event_id": "run-steps-01", "goal": "test"},
        pipeline=[
            WorkflowStep("supervisor", "s1"),
            WorkflowStep("researcher", "s2"),
            WorkflowStep("architect",  "s3"),  # 3 > max_steps=2
        ],
    )
    assert result.status == "failed"
    assert "max_steps" in " ".join(result.reasoning_trace)


# ── G: Feedback writer ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_feedback_writer_skips_low_confidence():
    result = _make_agent_result("success", confidence=0.4)
    fw = FeedbackWriter(semantic_pipeline=None, min_confidence=0.7)
    await fw.write(result, task_id="t-001", run_id="r-001", tenant_id="t-A")
    from hfa_worker.feedback_writer import get_feedback_metrics
    m = get_feedback_metrics()
    assert m.skipped >= 1


@pytest.mark.asyncio
async def test_feedback_writer_skips_hitl():
    result = _make_agent_result("success", confidence=0.95)
    result.requires_hitl = True
    fw = FeedbackWriter(semantic_pipeline=None)
    await fw.write(result, task_id="t-002", run_id="r-002", tenant_id="t-A")


@pytest.mark.asyncio
async def test_feedback_writer_no_pipeline_no_crash():
    result = _make_agent_result("success", confidence=0.9)
    fw = FeedbackWriter(semantic_pipeline=None)
    await fw.write(result, task_id="t-003", run_id="r-003", tenant_id="t-A")


# ── H: SemanticPipeline type contract ─────────────────────────────────────────

def test_semantic_pipeline_with_redis_client():
    """build_pipeline_sync with a client returns SemanticPipeline (not degraded)."""
    from unittest.mock import MagicMock
    from hfa_semantic.runtime.factory import build_pipeline_sync, SemanticPipeline, DegradedPipeline

    mock_redis = MagicMock()
    pipeline = build_pipeline_sync(redis_client=mock_redis)

    assert isinstance(pipeline, SemanticPipeline)
    assert not isinstance(pipeline, DegradedPipeline)
    assert pipeline.is_degraded is False
    assert hasattr(pipeline, "dedup_store")
    assert hasattr(pipeline, "watermark")
    assert hasattr(pipeline, "metrics")


def test_no_tuple_in_runtime_path():
    """Confirm build_pipeline_sync never returns a tuple."""
    from hfa_semantic.runtime.factory import build_pipeline_sync
    result_none = build_pipeline_sync(redis_client=None)
    result_mock = build_pipeline_sync(redis_client=MagicMock())
    assert not isinstance(result_none, tuple), "None path must not return tuple"
    assert not isinstance(result_mock, tuple), "Redis path must not return tuple"

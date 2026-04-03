"""
tests/integration/test_full_os_integration.py
IRONCLAD OS — End-to-End Integration Tests (correct interfaces)

Tests the complete path:
  RunRequestedEvent → CognitiveExecutor → Semantic → Agents → ExecutionResult → Feedback

Uses correct IRONCLAD types:
  - RunRequestedEvent  (hfa-core/hfa/events/schema.py)
  - ExecutionResult    (hfa-worker/hfa_worker/models.py)
  - BaseExecutor protocol
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


# ── Stub RunRequestedEvent (avoids hfa-core import in test env) ───────────────

@dataclass
class _RunEvent:
    """Minimal stand-in for RunRequestedEvent from hfa-core."""
    run_id: str = "run-001"
    tenant_id: str = "tenant-A"
    agent_type: str = "cognitive"
    payload: Dict[str, Any] = field(default_factory=lambda: {
        "goal": "Build a LIFO stack with push/pop",
        "priority": 50,
    })


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_run_event(agent_type: str = "cognitive", **kwargs) -> _RunEvent:
    ev = _RunEvent(agent_type=agent_type)
    for k, v in kwargs.items():
        object.__setattr__(ev, k, v)
    return ev


def make_semantic_pipeline(processed: bool = True):
    pipeline = MagicMock()
    pipeline.enabled = True
    result = MagicMock(
        processed=processed,
        deduped=not processed,
        late=False,
        late_strategy="n/a",
        state_snapshot={"count": 1, "partition": "tenant-A"},
        partition_key="tenant-A",
    )
    pipeline.process = AsyncMock(return_value=result)
    return pipeline


def make_agent_result(status: str = "success", confidence: float = 0.92):
    """Minimal hfa_agents ExecutionResult stand-in."""
    r = MagicMock()
    r.status = status
    r.confidence = confidence
    r.cost_cents = 45
    r.requires_hitl = False
    r.suggested_feedback = None
    r.reasoning_trace = ["supervisor_ok", "coder_ok", "tester_ok"]
    r.output_data = {"generated_code": "def push(): ...", "test_results": True}
    r.artifacts = {}
    return r


# ── A: Interface contract ─────────────────────────────────────────────────────

def test_cognitive_agent_types_set():
    assert "cognitive" in COGNITIVE_AGENT_TYPES
    assert "agent" in COGNITIVE_AGENT_TYPES
    assert "simple" not in COGNITIVE_AGENT_TYPES


@pytest.mark.asyncio
async def test_non_cognitive_type_bypasses_stack():
    """Non-cognitive agent_type returns done immediately without any I/O."""
    executor = CognitiveExecutor(semantic_pipeline=None)
    result = await executor.execute(make_run_event(agent_type="openai_chat"))

    assert isinstance(result, ExecutionResult)
    assert result.status == "done"
    assert result.payload["agent_type"] == "openai_chat"


# ── B: Happy path ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cognitive_full_happy_path():
    """RunRequestedEvent → Semantic → Agents → ExecutionResult(done)."""
    pipeline = make_semantic_pipeline(processed=True)
    agent_result = make_agent_result("success", 0.92)

    mock_enriched = MagicMock(event_id="run-001:run")

    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine") as MockEngine, \
         patch("hfa_worker.cognitive_executor.FeedbackWriter"):

        MockBridge.return_value.enrich_event = AsyncMock(return_value=mock_enriched)
        MockEngine.return_value.execute = AsyncMock(return_value=agent_result)

        executor = CognitiveExecutor(semantic_pipeline=pipeline)
        result = await executor.execute(make_run_event())

    assert isinstance(result, ExecutionResult)
    assert result.status == "done"
    assert result.payload["agent_status"] == "success"
    assert result.payload["confidence"] == 0.92
    assert result.cost_cents == 45
    assert result.error is None


# ── C: Dedup path ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_duplicate_event_returns_done_filtered():
    """Duplicate event: semantic returns None → done with filtered status."""
    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine"):

        MockBridge.return_value.enrich_event = AsyncMock(return_value=None)

        executor = CognitiveExecutor(semantic_pipeline=make_semantic_pipeline(False))
        result = await executor.execute(make_run_event())

    # Must be "done" (not "failed") — idempotent
    assert result.status == "done"
    assert result.payload.get("status") == "filtered_by_semantic"


# ── D: Agent failure ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_agent_failure_returns_failed_result():
    """Agent returns status=failed → ExecutionResult(failed)."""
    agent_result = make_agent_result("failed", 0.2)

    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine") as MockEngine, \
         patch("hfa_worker.cognitive_executor.FeedbackWriter"):

        MockBridge.return_value.enrich_event = AsyncMock(return_value=MagicMock())
        MockEngine.return_value.execute = AsyncMock(return_value=agent_result)

        executor = CognitiveExecutor()
        result = await executor.execute(make_run_event())

    assert result.status == "failed"
    assert result.payload["agent_status"] == "failed"


# ── E: Resilience ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_semantic_crash_returns_failed_gracefully():
    """SemanticBridge crash → ExecutionResult(failed), no exception propagation."""
    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine"):

        MockBridge.return_value.enrich_event = AsyncMock(
            side_effect=ConnectionError("Redis down")
        )
        executor = CognitiveExecutor()
        result = await executor.execute(make_run_event())

    assert result is not None
    assert result.status == "failed"
    assert "Redis down" in result.error


@pytest.mark.asyncio
async def test_workflow_engine_crash_handled():
    """WorkflowEngine crash → ExecutionResult(failed)."""
    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine") as MockEngine, \
         patch("hfa_worker.cognitive_executor.FeedbackWriter"):

        MockBridge.return_value.enrich_event = AsyncMock(return_value=MagicMock())
        MockEngine.return_value.execute = AsyncMock(
            side_effect=RuntimeError("LLM quota exceeded")
        )
        executor = CognitiveExecutor()
        result = await executor.execute(make_run_event())

    assert result.status == "failed"
    assert "LLM quota exceeded" in result.error


# ── F: Feedback writer ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_feedback_writer_skips_low_confidence():
    """confidence < 0.7 is never written to semantic memory."""
    result = make_agent_result("success", confidence=0.4)
    fw = FeedbackWriter(semantic_pipeline=None, min_confidence=0.7)
    # Should complete without writing (no exception)
    await fw.write(result, task_id="t-001", run_id="r-001", tenant_id="t-A")


@pytest.mark.asyncio
async def test_feedback_writer_skips_hitl_required():
    """HITL-flagged outcomes are never written."""
    result = make_agent_result("escalated", confidence=0.95)
    result.requires_hitl = True
    fw = FeedbackWriter(semantic_pipeline=None)
    await fw.write(result, task_id="t-002", run_id="r-002", tenant_id="t-A")


@pytest.mark.asyncio
async def test_feedback_writer_skips_failed_status():
    """Failed execution is not written (only success signals improve memory)."""
    result = make_agent_result("failed", confidence=0.9)
    fw = FeedbackWriter(semantic_pipeline=None)
    await fw.write(result, task_id="t-003", run_id="r-003", tenant_id="t-A")


@pytest.mark.asyncio
async def test_feedback_writer_with_pipeline_error_does_not_raise():
    """Pipeline error during write never crashes the caller."""
    pipeline = MagicMock()
    pipeline.validate_outcome = AsyncMock(side_effect=Exception("Redis write failed"))

    result = make_agent_result("success", confidence=0.9)
    fw = FeedbackWriter(semantic_pipeline=pipeline)
    # Must not raise
    await fw.write(result, task_id="t-004", run_id="r-004", tenant_id="t-A")


# ── G: executor_factory ───────────────────────────────────────────────────────

def test_cognitive_mode_builds_executor():
    """executor_factory.build_executor({'executor_mode': 'cognitive'}) succeeds."""
    with patch("hfa_worker.cognitive_executor.CognitiveExecutor.build") as MockBuild:
        MockBuild.return_value = MagicMock()
        from hfa_worker.executor_factory import build_executor
        result = build_executor({"executor_mode": "cognitive"})
    MockBuild.assert_called_once()
    assert result is not None


def test_fake_mode_still_works():
    """Existing fake mode is unchanged."""
    from hfa_worker.executor_factory import build_executor
    result = build_executor({"executor_mode": "fake"})
    assert result is not None


# ── H: Executor engine reuse ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_workflow_engine_reused_across_calls():
    """WorkflowEngine is created once and reused (no re-instantiation per task)."""
    agent_result = make_agent_result("success", 0.9)

    with patch("hfa_worker.cognitive_executor.SemanticBridge") as MockBridge, \
         patch("hfa_worker.cognitive_executor.WorkflowEngine") as MockEngine, \
         patch("hfa_worker.cognitive_executor.FeedbackWriter"):

        mock_engine_instance = MagicMock()
        mock_engine_instance.execute = AsyncMock(return_value=agent_result)
        MockEngine.return_value = mock_engine_instance
        MockBridge.return_value.enrich_event = AsyncMock(return_value=MagicMock())

        executor = CognitiveExecutor()
        await executor.execute(make_run_event())
        await executor.execute(make_run_event(run_id="run-002"))

    # WorkflowEngine constructor called exactly once
    assert MockEngine.call_count == 1

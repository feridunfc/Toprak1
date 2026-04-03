"""
IRONCLAD OS — Integration Contract (Canonical Types)

This file defines the strict interface between the three layers.
Any change to these contracts requires a version bump.

CONTRACT VERSION: 1.0.0
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

# ── Layer 1 → Layer 2: IRONCLAD → Semantic ───────────────────────────────────

@dataclass(frozen=True)
class SchedulerEvent:
    """
    Emitted by IRONCLAD scheduler after Lua dispatch commit.
    Consumed by hfa-semantic for pre-warming (fire-and-forget).
    """
    event_id: str           # f"sched:{run_id}"
    event_type: str         # f"scheduled:{agent_type}"
    tenant_id: str
    run_id: str
    timestamp_ms: int


# ── Layer 1+2 → Layer 3: Worker → Agents ─────────────────────────────────────

@dataclass(frozen=True)
class EnrichedTaskContext:
    """
    What CognitiveExecutor builds from TaskContext + semantic enrichment.
    Passed to WorkflowEngine as the EnrichedEvent.
    """
    event_id: str
    workflow_id: str         # = run_id from IRONCLAD
    execution_id: str        # = task_id from IRONCLAD
    lineage_run_id: str      # = run_id (IRONCLAD lineage correlation)
    tenant_id: str
    goal: str                # from task payload
    context: Dict[str, Any]  # full task payload
    semantic_ref: Optional[Dict[str, Any]]  # from hfa-semantic pipeline
    semantic_matches: List[Dict[str, Any]]  # from MergeEngine


# ── Layer 3 → Layer 1: Agents → Worker ───────────────────────────────────────

@dataclass(frozen=True)
class CognitiveTaskResult:
    """
    What CognitiveExecutor returns to IRONCLAD TaskConsumer.
    Converted from WorkflowEngine ExecutionResult.
    """
    ok: bool
    status: Literal["success", "failed", "escalated", "filtered_by_semantic"]
    confidence: float
    requires_hitl: bool
    artifact_keys: List[str]          # keys only — payloads go to PayloadStore
    reasoning_trace: List[str]        # last 5 steps
    duration_ms: int
    cost_cents: int
    error: str = ""


# ── Layer 3 → Layer 2: Agents → Semantic (Feedback) ──────────────────────────

@dataclass(frozen=True)
class FeedbackSignal:
    """
    What FeedbackWriter sends to hfa-semantic memory.
    Gated by OutcomeValidator (confidence >= 0.7, no flip-flop).
    """
    task_id: str
    run_id: str
    tenant_id: str
    status: Literal["success"]        # only success outcomes written
    confidence: float                 # >= 0.7 (enforced by OutcomeValidator)
    output_keys: List[str]
    reasoning_trace: List[str]        # last 3 steps
    validated_at_ms: int


# ── Failure modes ─────────────────────────────────────────────────────────────

FAILURE_BEHAVIORS: Dict[str, str] = {
    "semantic_down":      "CognitiveExecutor.fail-open → raw task executes",
    "agent_crash":        "CognitiveExecutor catches → TaskExecutionResult(ok=False)",
    "partial_dag":        "WorkflowEngine returns first-failed + completed artifacts",
    "redis_partition":    "SchedulerLoop leader election re-runs (Lua prevents split)",
    "semantic_redis_down": "Local dict fallback for dedup/watermark",
    "hitl_required":      "status=escalated, task stays RUNNING, heartbeat holds",
    "feedback_poisoning": "OutcomeValidator: confidence<0.7 → skip write",
    "budget_exceeded":    "WorkflowEngine returns escalated at over-budget step",
}

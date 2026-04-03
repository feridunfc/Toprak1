# IRONCLAD Integration Contract

**Version:** 1.1.0  
**Status:** CANONICAL — changes require a version bump  
**Owner:** Platform Engineering

---

## Overview

This document is the authoritative boundary specification for the four layers
of the IRONCLAD system. Every cross-layer call must conform to the interfaces
defined here. No layer may reach into another layer's internals.

---

## Layer Map

```
┌─────────────────────────────────────────────────────────┐
│  Layer 1 — Control Plane  (hfa-control + hfa-core)      │
│  Authority: scheduler, state transitions, admission      │
├─────────────────────────────────────────────────────────┤
│  Layer 2 — Semantic Runtime  (hfa-semantic)              │
│  Authority: enrichment, dedup, watermark, policy         │
├─────────────────────────────────────────────────────────┤
│  Layer 3 — Worker Execution  (hfa-worker)                │
│  Authority: task dispatch, executor routing              │
├─────────────────────────────────────────────────────────┤
│  Layer 4 — Agent Workflow  (hfa-agents)                  │
│  Authority: role execution, tool use, planning           │
└─────────────────────────────────────────────────────────┘
```

---

## Layer Boundaries

### Layer 1 → Layer 2: Control Plane → Semantic (fire-and-forget)

**Direction:** 1 → 2 only. Semantic never calls Control Plane.

```python
# hfa-worker/src/hfa_worker/cognitive_executor.py emits:
SchedulerEvent(
    event_id   = f"sched:{run_id}",
    event_type = f"scheduled:{agent_type}",
    tenant_id  = tenant_id,
    run_id     = run_id,
    timestamp_ms = int(time.time() * 1000),
)
```

**Contract:**
- Semantic enrichment is **advisory only** — Control Plane never blocks on it
- If semantic is unavailable, execution continues in degraded mode
- Semantic **never** writes to Control Plane state

---

### Layer 1+2 → Layer 3: Control/Semantic → Worker

**Direction:** Control Plane dispatches `RunRequestedEvent` to Worker via Redis Stream.

```python
# hfa-core/src/hfa/events/schema.py
RunRequestedEvent(
    run_id     = str,   # IRONCLAD run identifier
    tenant_id  = str,   # tenant UUIDv4
    agent_type = str,   # e.g. "cognitive", "openai", "fake"
    payload    = dict,  # task-specific data
)
```

**Canonical executor interface** (`hfa_worker.executor.BaseExecutor`):
```python
async def execute(self, run_event: RunRequestedEvent) -> ExecutionResult:
    ...
# ExecutionResult: status ("done"|"failed"), payload, error, cost_cents, tokens_used
```

**FORBIDDEN:**
- Worker must NOT call `transition_state` directly — only StateStore
- Worker must NOT write to semantic state — only FeedbackWriter (async)

---

### Layer 3 → Layer 4: Worker → Agents

**Direction:** CognitiveExecutor invokes WorkflowEngine.

```python
# CognitiveExecutor passes enriched_dict to:
from hfa_agents.workflow.engine import WorkflowEngine  # CANONICAL ENTRYPOINT

result = await engine.run_workflow(enriched_dict)
# enriched_dict must contain: event_id, goal, run_id, tenant_id
```

**Agent result** (`hfa_agents.base.contracts.ExecutionResult`):
```python
status:          "success" | "failed" | "escalated" | "delegated"
output_data:     dict
reasoning_trace: list[str]
confidence:      float  # 0.0–1.0
requires_hitl:   bool
```

**FORBIDDEN:**
- Agents must NOT persist state — they are stateless planners
- Agents must NOT call Redis directly
- Agents must NOT mutate the DAG at runtime (plan freezes at workflow start)
- WorkflowEngine.max_steps hard limit: 10 (ceiling: 50)
- WorkflowEngine.cost_budget_cents: 2000¢ ($20) default

---

### Layer 4 → Layer 2: Agents → Semantic (feedback loop)

**Direction:** FeedbackWriter writes outcome back to semantic memory. Non-blocking.

```python
# hfa_worker.feedback_writer.FeedbackWriter.write()
FeedbackSignal(
    task_id        = str,
    run_id         = str,
    tenant_id      = str,
    status         = "success",   # only success outcomes written
    confidence     = float,       # must be >= 0.70
    output_keys    = list[str],
    reasoning_trace = list[str],  # last 3 steps only
    validated_at_ms = int,
)
```

**Guards (all must pass before write):**
1. `status == "success"`
2. `confidence >= 0.70`
3. `requires_hitl == False`

**FORBIDDEN:**
- FeedbackWriter must NOT block the execution path (always `create_task`)
- FeedbackWriter must NOT write failed or escalated outcomes

---

## Canonical Import Paths

| What | Import from |
|------|-------------|
| Executor base class | `hfa_worker.executor.BaseExecutor` |
| Worker result model | `hfa_worker.models.ExecutionResult` |
| Worker exceptions | `hfa_worker.models.ExecutionError` et al. |
| Workflow entrypoint | `hfa_agents.workflow.engine.WorkflowEngine` |
| Semantic runtime | `hfa_semantic.runtime.*` (via `__init__.py`) |
| Watermark | `hfa_semantic.runtime.watermark.Watermark` |
| Dedup | `hfa_semantic.runtime.dedup_store.DedupStore` |
| Metrics aggregate | `hfa_semantic.observability.aggregate_snapshot` |
| Run state transition | `hfa.state.transition_state` |
| Redis keys | `hfa.config.keys.RedisKey` |

---

## Deprecated Paths (remove by Sprint 7)

| Deprecated | Use instead |
|------------|-------------|
| `hfa_worker.executor_base.Executor` | `hfa_worker.executor.BaseExecutor` |
| `hfa_worker.execution_types.ExecutionResult` | `hfa_worker.models.ExecutionResult` |
| `hfa_semantic.runtime.watermark_v2.WatermarkV2` | `hfa_semantic.runtime.watermark.Watermark` |
| `hfa_semantic.runtime.eviction_v2.EvictionV2` | `hfa_semantic.runtime.eviction.EvictionPolicy` |
| `hfa_semantic.memory.semantic_memory.SemanticMemory` | `hfa_semantic.memory.semantic_memory_v2.SemanticMemoryV2` |
| `hfa_agents.workflow.orchestrator.AgentOrchestrator` | `hfa_agents.workflow.engine.WorkflowEngine` |

---

## Failure Behavior Matrix

| Failure | Expected behavior |
|---------|------------------|
| Semantic unavailable | `SemanticBridge` degrades → pass-through, `semantic_enriched=False` |
| Agent crash | `CognitiveExecutor` catches → `ExecutionResult(status="failed")` |
| Partial DAG | `WorkflowEngine` returns first-failed + completed artifacts |
| Budget exceeded | `WorkflowEngine` returns `status="failed"` with `budget_exceeded` trace |
| max_steps exceeded | `WorkflowEngine` returns `status="failed"` before any step runs |
| Redis partition | Scheduler loop re-elects leader; Lua prevents double-dispatch |
| Feedback write failure | `FeedbackWriter` logs warning, swallows exception — never blocks |
| HITL required | `status="escalated"`, task stays RUNNING, heartbeat holds |
| Feedback poisoning | `confidence < 0.70` → skip write |
| Execution token mismatch | Worker rejects execution (Sprint 1 hfa-control) |

---

## Enforcement

- This contract is tested by `tests/integration/test_cognitive_pipeline.py`
- CI `test-cognitive` job runs the canonical cognitive path on every push
- Violations cause build failure, not just warnings
- Contract version is checked at import time by `hfa_worker.integration_contract`

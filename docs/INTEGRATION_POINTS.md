# IRONCLAD OS — Exact Integration Points

## The Single Critical Change

The entire integration reduces to **one new if-branch** in an existing factory:

```python
# hfa-worker/src/hfa_worker/executor_factory.py  — ADD 4 LINES

if mode == "cognitive":
    from hfa_worker.cognitive_executor import CognitiveExecutor
    return CognitiveExecutor.build(config)
```

That's it. Everything else flows from this.

---

## Files Changed

### NEW — add to hfa-worker/src/hfa_worker/

| File | Lines | Purpose |
|------|-------|---------|
| `cognitive_executor.py` | ~130 | `RunRequestedEvent → Semantic → Agents → ExecutionResult` |
| `feedback_writer.py`    | ~120 | Async validated feedback to semantic memory |

### PATCHED — minimal change to existing file

| File | Change | Risk |
|------|--------|------|
| `executor_factory.py` | +4 lines, new if-branch | Zero — existing modes untouched |

### NEW — add to hfa-control/src/hfa_control/api/

| File | Purpose |
|------|---------|
| `hitl_router.py` | Human-in-the-loop approve/reject API |
| `scheduler_semantic_hook.py` | Fire-and-forget pre-warm (optional) |

---

## Files Sealed (UNTOUCHED)

```
hfa-worker/src/hfa_worker/task_consumer.py    ← UNTOUCHED
hfa-worker/src/hfa_worker/consumer.py         ← UNTOUCHED
hfa-worker/src/hfa_worker/task_context.py     ← UNTOUCHED
hfa-worker/src/hfa_worker/executor_base.py    ← UNTOUCHED
hfa-worker/src/hfa_worker/executor.py         ← UNTOUCHED
hfa-worker/src/hfa_worker/models.py           ← UNTOUCHED
hfa-control/ (entire package)                 ← UNTOUCHED
hfa-core/    (entire package)                 ← UNTOUCHED
```

---

## Real Interface (corrected from initial analysis)

**Executor receives `RunRequestedEvent`** (not `TaskContext`):
```python
# hfa-core/hfa/events/schema.py
class RunRequestedEvent:
    run_id:     str
    tenant_id:  str
    agent_type: str
    payload:    dict   # contains: goal, priority, ...
```

**Executor returns `ExecutionResult`** (from `hfa_worker/models.py`):
```python
@dataclass
class ExecutionResult:
    status:      Literal["done", "failed"]
    payload:     dict    # output data
    error:       str | None
    cost_cents:  int
    tokens_used: int
```

**WorkflowEngine receives `EnrichedEvent`** (from `hfa_agents/base/contracts.py`):
```python
class EnrichedEvent(BaseModel):
    event_id:       str
    workflow_id:    str   # = run_id
    execution_id:   str   # = run_id
    tenant_id:      str
    goal:           str
    context:        dict  # full payload
    semantic_ref:   dict | None  # from hfa-semantic pipeline
```

---

## Call Chain

```
WorkerConsumer
  └─ BaseExecutor.execute(run_event: RunRequestedEvent)
       └─ CognitiveExecutor.execute(run_event)
            ├─ SemanticBridge.enrich_event()
            │    ├─ pipeline.process(event)    ← dedup / watermark / lateness
            │    └─ returns EnrichedEvent | None
            │
            ├─ if None: return ExecutionResult(done, filtered_by_semantic)
            │
            ├─ WorkflowEngine.execute(enriched)
            │    ├─ Supervisor → DAGPlan
            │    ├─ Researcher (hfa-semantic MergeEngine)
            │    ├─ Lawmaker → Invariants
            │    ├─ Architect → Spec
            │    ├─ Coder → Code + Sandbox
            │    ├─ Tester → AST + pytest
            │    └─ Compliance → SOC2 audit
            │
            ├─ asyncio.create_task(FeedbackWriter.write())  ← non-blocking
            │
            └─ return ExecutionResult(
                   status = "done" | "failed",
                   payload = {agent_status, confidence, artifacts, ...},
                   cost_cents = total_llm_spend,
               )
```

---

## Environment Variables

```bash
# Activate cognitive mode
EXECUTOR_MODE=cognitive

# hfa-semantic
SEMANTIC_ENABLED=true
SEMANTIC_MAX_PARTITIONS=10000
SEMANTIC_WINDOW_TTL_MS=300000
SEMANTIC_LATENESS_STRATEGY=drop    # drop | accept_with_correction
SEMANTIC_PARTITION_FIELD=tenant_id

# Budget
COGNITIVE_BUDGET_CENTS=2000        # max LLM spend per workflow

# LLM (hfa-agents LLMRouter)
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...              # fallback provider

# Optional graph/vector (semantic degrades without these)
NEO4J_URI=bolt://neo4j:7687
NEO4J_AUTH=neo4j/password
QDRANT_HOST=qdrant
QDRANT_PORT=6333

# Infrastructure
REDIS_URL=redis://redis:6379
```

---

## HITL (Human-In-The-Loop) Flow

When an agent workflow escalates (compliance violation, low confidence, etc.):

```
1. WorkflowEngine returns ExecutionResult(status="escalated", requires_hitl=True)
2. CognitiveExecutor calls:  POST /v1/hitl/{run_id}/request
3. Task stays RUNNING — heartbeat holds it (DrainManager unaffected)
4. Human queries:            GET  /v1/hitl/pending
5. Human decides:            POST /v1/hitl/{run_id}/approve   (or /reject)
6. On approve:  CognitiveExecutor retries with modified_context
7. On reject:   ExecutionResult(failed) returned → IRONCLAD marks FAILED
8. Timeout:     48 hours → auto-fail

Storage: semantic:hitl:{run_id} in semantic Redis (TTL 48h)
```

---

## State Written Per Run

```
After admit:
  hfa:state:{run_id}               = "queued"    (IRONCLAD Redis)

After schedule:
  hfa:state:{run_id}               = "scheduled" (IRONCLAD Redis)
  semantic:state:scheduler_prewarm:{tenant_id}   (optional, hfa-semantic)

After claim:
  hfa:state:{run_id}               = "running"   (IRONCLAD Redis)
  semantic:dedup:{run_id}:run      = "1" TTL 60s (hfa-semantic Redis)

After cognitive execution:
  hfa:state:{run_id}               = "completed" (IRONCLAD Redis)
  hfa:output:{task_id}             = {payload}   (IRONCLAD Redis)
  hfa:lineage:{run_id}             = {lineage}   (IRONCLAD Redis)
  semantic:memory:feedback:{tenant} = {outcome}  (hfa-semantic Redis, TTL 24h)
```

---

## Deployment Topology

```
┌─────────────┐     ┌─────────────┐     ┌───────────────┐
│   Client    │────►│  hfa-control│────►│  Redis :6379  │◄───┐
│             │     │  :8000      │     │  (shared)     │    │
└─────────────┘     └─────────────┘     └───────────────┘    │
                                                              │
                    ┌─────────────┐                           │
                    │  hfa-worker │───────────────────────────┘
                    │  (×N pods)  │
                    │  cognitive  │     ┌──────────────┐
                    │  executor   │────►│  Neo4j :7687 │ (optional)
                    └─────────────┘     └──────────────┘
                         │
                         │              ┌──────────────┐
                         └─────────────►│  Qdrant :6333│ (optional)
                                        └──────────────┘
Workers scale independently.
Control plane: 2+ replicas with leader election.
Semantic: sidecar inside worker process (no separate service needed).
```

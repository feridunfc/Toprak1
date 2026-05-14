# IRONCLAD OS — Production Architecture

## 1. SYSTEM DIAGRAM

```
┌──────────────────────────────────────────────────────────────────────┐
│  IRONCLAD COGNITIVE OS                                                │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  CONTROL PLANE  (hfa-control — SEALED, DETERMINISTIC)           │ │
│  │                                                                  │ │
│  │  Client ─► FastAPI /v1 ─► Admission ─► EventStore               │ │
│  │                                  │                               │ │
│  │                          SchedulerLoop (Lua CAS)                 │ │
│  │                                  │                               │ │
│  │               [HOOK A] emit_event_background()                   │ │
│  │                       Task Queue (Redis sorted set)              │ │
│  └──────────────────────────┬──────────────────────────────────────┘ │
│                             │ task_id + payload                       │
│                             ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  WORKER PLANE  (hfa-worker)                                     │ │
│  │                                                                  │ │
│  │  TaskConsumer ─► claim_start                                    │ │
│  │                       │                                         │ │
│  │           [HOOK B]  CognitiveExecutor                           │ │
│  │                       │                                         │ │
│  │        ┌──────────────┴──────────────┐                          │ │
│  │        ▼                             ▼                          │ │
│  │  SemanticBridge              DirectExecutor                     │ │
│  │  (agent_type=cognitive)      (agent_type=simple)                │ │
│  └───────────────┬─────────────────────────────────────────────────┘ │
│                  │                                                    │
│     ┌────────────┴────────────────────┐                              │
│     ▼                                 ▼                              │
│  ┌──────────────────┐    ┌───────────────────────────────────────┐   │
│  │  hfa-semantic    │    │  hfa-agents                          │   │
│  │  SIDECAR BRAIN   │◄──►│  EXECUTION PLANE                     │   │
│  │                  │    │                                       │   │
│  │  DedupStore      │    │  WorkflowEngine                      │   │
│  │  Watermark       │    │    Supervisor → DAGPlan               │   │
│  │  StateStore      │    │    Researcher → MergeEngine           │   │
│  │  RuleEngine      │    │    Lawmaker → Invariants              │   │
│  │  MergeEngine     │    │    Architect → Spec                   │   │
│  │  PolicyEngine    │    │    Coder → SecureSandbox              │   │
│  │  SemanticMemory  │    │    Tester → AST + pytest              │   │
│  └─────────┬────────┘    │    Compliance → SOC2 audit            │   │
│            │             └───────────────────────────────────────┘   │
│            │ validated_outcome                                        │
│            ▼                                                         │
│  ┌──────────────────┐                                               │
│  │  FEEDBACK LOOP   │                                               │
│  │  OutcomeValidator│                                               │
│  │  ConfidenceScore │                                               │
│  │  SemanticMemory  │──► future reasoning                          │
│  └──────────────────┘                                               │
└──────────────────────────────────────────────────────────────────────┘
```

## 2. DATA FLOW

```
SUBMIT → SCHEDULE → ENRICH → EXECUTE → FEEDBACK → COMPLETE

1. POST /v1/runs  {tenant, goal, agent_type=cognitive, payload}
2. Admission → QUEUED in Redis
3. SchedulerLoop Lua CAS: QUEUED → SCHEDULED
4. HOOK A: emit_event_background(TASK_SCHEDULED) — non-blocking
5. TaskConsumer.claim_start (Lua atomic)
6. HOOK B: CognitiveExecutor.execute(ctx)
   a. SemanticBridge.enrich_event()
      - semantic.process_event()  [dedup/watermark/lateness]
      - if filtered: return raw idempotent complete
      - attach semantic_ref to EnrichedEvent
   b. WorkflowEngine.execute(enriched_event)
      - Supervisor → DAGPlan
      - Researcher (queries hfa-semantic MergeEngine)
      - Lawmaker → Invariants
      - Architect → Spec
      - Coder → Code (sandbox validated)
      - Tester → Tests
      - Compliance → Audit
   c. FeedbackWriter.write(result)  — async, non-blocking
      - OutcomeValidator: confidence + flip-flop guard
      - SemanticMemory.write(validated_outcome)
7. StateStore.store_task_output() — IRONCLAD lineage
8. emit_event_background(TASK_COMPLETED)
```

## 3. STATE AUTHORITY

| Layer | Owns |
|-------|------|
| IRONCLAD/Redis | Task state machine, effect ledger, event log, lineage, payload store, worker reservations, tenant vruntime |
| hfa-semantic/Redis | Window state, semantic memory, policy cooldowns, HITL flags, graph cache |
| Agents | NOTHING persistent. Read-only EnrichedEvent consumers. Output via ExecutionResult only. |

## 4. ASYNC BOUNDARIES

**Synchronous (awaited):**
- TaskConsumer.consume_once()
- CognitiveExecutor.execute()
- WorkflowEngine.execute()
- SemanticBridge.enrich_event()

**Fire-and-forget (asyncio.create_task):**
- FeedbackWriter.write()  ← AFTER execution completes
- emit_event_background() ← AFTER scheduler dispatch
- Scheduler enrichment hook ← BEFORE task enters queue

**NEVER blocking SchedulerLoop:**
- No semantic calls inside scheduler tick
- No LLM calls inside scheduler tick
- Scheduler = pure Redis Lua CAS operations only

## 5. FAILURE MODES

| Failure | Behavior | Recovery |
|---------|----------|----------|
| semantic down | SemanticBridge fail-open → raw task executes | no semantic context, task completes |
| agent crash | CognitiveExecutor catches → TaskExecutionResult(ok=False) | IRONCLAD requeues |
| partial DAG | Return first failed step + completed artifacts | HITL if requires_hitl=True |
| Redis partition | Leader election re-runs, Lua prevents split brain | standby takes over |
| semantic Redis down | Local dict fallback for dedup/watermark | eventual consistency on reconnect |
| feedback poisoning | OutcomeValidator blocks confidence<0.7 + flip-flop | skip write, log warning |
| HITL escalation | Task stays RUNNING, heartbeat owns it | POST /v1/hitl/{run_id}/approve |

## 6. PERFORMANCE TARGETS

| Operation | Target | Note |
|-----------|--------|------|
| Scheduler tick | < 5ms p99 | Lua atomic only |
| SemanticBridge.enrich | < 50ms p99 | Redis ops + dedup check |
| WorkflowEngine.execute | < 120s p95 | full DAG, timeout per step |
| FeedbackWriter.write | < 200ms | async, never blocks execution |
| Semantic memory lookup | < 10ms | Redis GET |

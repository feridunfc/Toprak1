"""
hfa-semantic/INTEGRATION_GUIDE.md

IRONCLAD Semantic Layer — Integration with Core System

⚠️  CRITICAL RULES
=================

1. Scheduler is SEALED → do not modify scheduling logic
2. Semantic is OPTIONAL → system must work without it
3. All outputs are ADDITIVE → no mutation of core events
4. Graceful degradation → semantic failure → raw mode continues
5. Determinism preserved → no randomness, no hidden state

## Integration Points (Minimal & Safe)

### 1. Event Flow Hook (OPTIONAL, ASYNC, NON-BLOCKING)

Location: hfa-control/src/hfa_control/scheduler.py (optional)

Before dispatching to worker:
  * Attach semantic metadata reference
  * DO NOT block dispatch
  * DO NOT modify event
  * Failure → continue without semantic enrichment

Pattern:
```python
async def _schedule(self, event: RunAdmittedEvent) -> None:
    # ... existing scheduler logic ...

    # OPTIONAL: asynchronous semantic enrichment (non-blocking)
    if self._semantic_enricher:
        try:
            await asyncio.wait_for(
                self._semantic_enricher.enrich_async(event),
                timeout=10  # max 10ms, don't block dispatch
            )
        except Exception:
            # Semantic failure → continue, raw mode
            pass

    # ... existing dispatch logic continues ...
```

### 2. Semantic Metadata in RunRequestedEvent (OPTIONAL)

Location: hfa-core/src/hfa/events/schema.py (optional)

Additive field (already exists):
```python
@dataclass
class RunRequestedEvent(HFAEvent):
    # ... existing fields ...
    semantic_ref: Optional[str] = None  # e.g., "rule:pattern_a:partition_x"
    reasoned_ref: Optional[str] = None  # e.g., "match:12345"
```

Scheduler uses it ONLY for logging/observability.
Scheduler does NOT depend on these fields.

### 3. Semantic Sidecar Process

The semantic layer runs as a SEPARATE process:
  * Consumes events from stream (asynchronously)
  * Runs dedup + watermark + reasoning
  * Writes outcomes to Redis (non-blocking)
  * Never blocks scheduler

```
┌─────────────────┐
│   Scheduler     │
│   (sealed)      │
└────────┬────────┘
         │
    Event Stream
         │
┌────────▼────────────────────┐
│  Semantic Sidecar Process   │
│  (async, non-blocking)      │
│  - Dedup                    │
│  - Watermark                │
│  - Reasoning                │
│  - Outcomes → Redis         │
└─────────────────────────────┘
```

### 4. Agent Access to Semantic Results (OPTIONAL)

Location: hfa-tools/src/hfa_tools/agent_executor.py (optional)

Agent can optionally query semantic layer for enrichment:

```python
async def execute(self, run_id: str, payload: dict) -> dict:
    # ... existing agent logic ...

    # OPTIONAL: check if semantic layer has reasoning for this run
    semantic_results = None
    if self._semantic_client:
        try:
            semantic_results = await self._semantic_client.get_reasoning(run_id)
        except Exception:
            # Semantic unavailable → continue with raw logic
            pass

    # Agent makes decision using both raw payload and optional semantic insights
    # Semantic is ADVISORY, not mandatory

    # ... existing execution logic ...
```

### 5. Observability Hooks (OPTIONAL)

Semantic layer exports Prometheus metrics:
  * `/metrics` endpoint available on semantic service
  * Metrics prefixed with `semantic_*`
  * Scheduler metrics unaffected

No scheduler changes needed. Observability is ADDITIVE.

## Files That MAY Be Modified (Minimal)

✅ Optional, non-breaking:

1. hfa-control/src/hfa_control/scheduler.py
   - Add optional async semantic enrichment hook
   - Must timeout after 10ms
   - Failure must not affect dispatch

2. hfa-core/src/hfa/events/schema.py
   - Already has optional fields (semantic_ref, reasoned_ref)
   - No changes needed if not used

3. hfa-tools/src/hfa_tools/agent_executor.py
   - Add optional semantic query client
   - Must have safe fallback to raw payload
   - Failure must not affect execution

## Files That MUST NOT Be Modified

❌ SEALED, do not touch:

1. hfa-control/src/hfa_control/scheduler_loop.py
   - Dispatch logic is deterministic
   - No semantic dependencies

2. hfa-core/src/hfa/runtime/
   - State management is sealed
   - Lineage integrity is cryptographic

3. Lua scripts (scheduling logic)
   - CAS-based atomic operations
   - Deterministic

## Deployment Model

### Development/Testing
- In-memory semantic state store
- Single-process (scheduler + semantic in same app)

### Production
- Redis-backed semantic state store
- Semantic runs as separate sidecar process
- Horizontal scaling via Redis sharding
- Automatic TTL eviction

### Failover/Degradation
- Scheduler works without semantic
- Semantic failure → raw mode continues
- No cascading failures

## Configuration

Environment variables (optional):

```bash
# Semantic layer enable
SEMANTIC_ENABLED=true

# State backend
SEMANTIC_STATE_BACKEND=redis  # or "memory" for dev
SEMANTIC_REDIS_URL=redis://...

# Eviction policies
SEMANTIC_MAX_PARTITIONS_PER_RULE=10000
SEMANTIC_STATE_TTL_MS=3600000  # 1 hour

# Policy bounds
SEMANTIC_POLICY_MIN_CONFIDENCE=0.5
SEMANTIC_POLICY_COOLDOWN_MS=3600000  # 1 hour between changes
```

## Testing

Existing core tests MUST continue to pass:
```bash
pytest tests/core/ -v
pytest tests/scheduler/ -v
```

Semantic layer has separate test suite:
```bash
pytest hfa-semantic/tests/ -v
```

No core tests should depend on semantic layer.

## Summary

The semantic layer is:
  * ✅ Additive (non-destructive)
  * ✅ Optional (can be disabled)
  * ✅ Non-blocking (async only)
  * ✅ Fault-tolerant (graceful degradation)
  * ✅ Observable (Prometheus metrics)
  * ✅ Deterministic (no randomness)

IRONCLAD core remains sealed. Semantic is a sidecar intelligence layer.
"""


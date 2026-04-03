"""
hfa-semantic/SCHEDULER_INTEGRATION.md

IRONCLAD Semantic Layer — Scheduler Integration Guide

This guide shows HOW to integrate semantic layer with scheduler.
All changes are OPTIONAL and NON-BREAKING.

## Critical Rule

Scheduler is SEALED. Do not modify scheduling logic.
Semantic is OPTIONAL. System must work without it.

## Integration Pattern 1: Optional Enrichment Hook (Async)

Location: hfa-control/src/hfa_control/scheduler.py

Add OPTIONAL, ASYNC, NON-BLOCKING enrichment:

```python
from hfa_semantic import RuntimeFactory

class Scheduler:
    def __init__(self, ...):
        self._semantic_enricher = None
        self._semantic_runtime = None

    async def start(self) -> None:
        # ... existing startup code ...

        # OPTIONAL: Initialize semantic layer
        try:
            self._semantic_runtime = await RuntimeFactory.create_all()
        except Exception as e:
            logger.warning("Semantic layer disabled: %s", e)
            self._semantic_runtime = None

    async def _schedule(self, event: RunAdmittedEvent) -> None:
        # ... existing scheduling logic ...

        # OPTIONAL: Async semantic enrichment (NON-BLOCKING)
        if self._semantic_runtime:
            try:
                await asyncio.wait_for(
                    self._enrich_semantic(event),
                    timeout=0.01  # max 10ms, don't block dispatch
                )
            except asyncio.TimeoutError:
                # Timeout is OK, continue
                pass
            except Exception:
                # Semantic failure → continue, raw mode
                logger.exception("Semantic enrichment failed")

        # ... existing dispatch logic continues ...

    async def _enrich_semantic(self, event: RunAdmittedEvent) -> None:
        '''Semantic enrichment (optional, non-blocking).'''
        dedup = self._semantic_runtime["dedup_store"]
        state_store = self._semantic_runtime["state_store"]

        # Check for duplicate
        if await dedup.is_duplicate(event.event_id):
            logger.info("Duplicate detected: run=%s", event.run_id)
            return

        # Mark as processed
        await dedup.mark_processed(event.event_id, event.timestamp * 1000)

        # Store semantic metadata
        # (reasoning happens in async sidecar, not here)
```

## Integration Pattern 2: Semantic Metadata in Event

Location: hfa-core/src/hfa/events/schema.py

These fields already exist (no changes needed):

```python
@dataclass
class RunRequestedEvent(HFAEvent):
    # ... existing fields ...
    semantic_ref: Optional[str] = None  # e.g., "rule:pattern_a:partition_x"
    reasoned_ref: Optional[str] = None  # e.g., "match:12345"
    semantic_metadata: Dict[str, Any] = field(default_factory=dict)
```

Usage (optional):

```python
# In scheduler, when enriching
event.semantic_ref = f"rule:{rule_id}:{partition}"
event.semantic_metadata = {
    "dedup_checked": True,
    "watermark_lag_ms": 100,
}
```

Usage (in agent):

```python
# In agent executor
run_request = event
semantic_hint = run_request.semantic_ref  # optional
if semantic_hint:
    logger.info("Semantic hint: %s", semantic_hint)
```

## Integration Pattern 3: Query Semantic Results (In Agent)

Location: hfa-tools/src/hfa_tools/agent_executor.py

Query semantic layer for reasoning (optional):

```python
from hfa_semantic import RedisStateStore

class AgentExecutor:
    def __init__(self, ...):
        self._semantic_client = None

    async def setup(self) -> None:
        # OPTIONAL: Initialize semantic client
        try:
            import redis.asyncio as redis
            redis_client = redis.from_url("redis://localhost:6379")
            self._semantic_client = RedisStateStore(redis_client)
        except Exception:
            logger.warning("Semantic client disabled")

    async def execute(self, run_id: str, payload: dict) -> dict:
        # ... existing agent logic ...

        # OPTIONAL: Check if semantic layer has reasoning
        semantic_hints = None
        if self._semantic_client:
            try:
                # Query semantic state (future reasoning output)
                semantic_hints = await self._semantic_client.get(
                    "reasoning", f"run:{run_id}"
                )
            except Exception:
                # Semantic unavailable → continue
                pass

        # Make decision using both raw payload and optional semantic insights
        decision = await self._make_decision(payload, semantic_hints)

        # Semantic is ADVISORY, not mandatory
        # Raw logic always works

        # ... execute decision ...
```

## Integration Pattern 4: Expose Semantic Metrics

Location: hfa-tools/src/hfa_tools/metrics.py (optional)

Expose semantic metrics alongside regular metrics:

```python
from hfa_semantic.runtime import RuntimeMetrics

# In main app initialization
metrics = RuntimeMetrics()

# Later, when handling events
metrics.record_event_processed(rule_id="pattern_a")
metrics.record_dedup_hit()
metrics.set_watermark_lag(lag_ms=100)
```

Prometheus will scrape all metrics on `/metrics` endpoint.

## Integration Checklist

- [ ] Decide: semantic layer needed? (yes/no)
- [ ] If NO: skip all below, scheduler works as-is
- [ ] If YES:
  - [ ] Add optional enrichment hook in scheduler (max 10ms)
  - [ ] Set `SEMANTIC_ENABLED=true` env var
  - [ ] Set `SEMANTIC_STATE_BACKEND=redis` env var
  - [ ] Set `SEMANTIC_REDIS_URL=...` env var
  - [ ] Run tests: `pytest tests/core/` (must still pass)
  - [ ] Run tests: `pytest hfa-semantic/tests/` (new layer)
  - [ ] Deploy semantic sidecar process (separate from scheduler)

## Testing Integration

### Existing Tests (Must Pass)

```bash
# All core tests still pass
pytest tests/core/ -v
pytest tests/scheduler/ -v
pytest tests/agent/ -v
```

### Semantic Tests (New)

```bash
# New semantic layer tests
pytest hfa-semantic/tests/ -v
```

### Integration Tests

```bash
# Test with semantic enabled
export SEMANTIC_ENABLED=true
export SEMANTIC_STATE_BACKEND=memory  # for quick test
pytest tests/core/ tests/integration/ hfa-semantic/tests/ -v
```

### Degradation Test

```python
# Test that scheduler works WITHOUT semantic
import os
os.environ["SEMANTIC_ENABLED"] = "false"

# Run scheduler tests
pytest tests/scheduler/ -v  # must all pass
```

## Deployment Options

### Option A: Disabled (Default)

```bash
# Semantic layer not used
SEMANTIC_ENABLED=false
# or not set (defaults to true but will skip if unavailable)
```

Scheduler works 100% as before.

### Option B: Single-Node Dev

```bash
SEMANTIC_ENABLED=true
SEMANTIC_STATE_BACKEND=memory
```

Semantic runs in-process, no external dependencies.

### Option C: Redis-Backed Prod

```bash
SEMANTIC_ENABLED=true
SEMANTIC_STATE_BACKEND=redis
SEMANTIC_REDIS_URL=redis://prod-redis:6379
```

Semantic runs as separate sidecar process, shares Redis state.

## Safety Guarantees

✅ **Backward Compatible**: Old scheduler code works unchanged
✅ **Graceful Degradation**: Semantic failure doesn't break scheduler
✅ **Minimal Changes**: <50 lines total in scheduler
✅ **Non-Blocking**: Semantic enrichment times out at 10ms
✅ **No Replay Risk**: Semantic doesn't affect replay logic

## Example: Complete Minimal Integration

In scheduler:

```python
class Scheduler:
    async def start(self):
        try:
            from hfa_semantic import RuntimeFactory
            self._semantic = await RuntimeFactory.create_all()
        except Exception:
            self._semantic = None

    async def dispatch(self, event):
        # Existing dispatch logic...
        
        # NEW: Optional semantic enrichment
        if self._semantic:
            try:
                dedup = self._semantic["dedup_store"]
                if not await dedup.is_duplicate(event.event_id):
                    await dedup.mark_processed(
                        event.event_id,
                        event.timestamp * 1000
                    )
            except:
                pass  # Ignore semantic errors
        
        # Continue with normal dispatch...
```

That's it! Everything else is optional.

## Monitoring Integration

View semantic metrics:

```bash
# If semantic_metrics exposed on port 8000
curl http://localhost:8000/metrics | grep semantic_

# Or in Prometheus dashboard
# Select metric: semantic_events_processed_total
# Select metric: semantic_dedup_hits_total
# etc.
```

## Questions?

1. **Q**: Will semantic slow down scheduler?
   **A**: No. Enrichment times out at 10ms, doesn't block dispatch.

2. **Q**: What if Redis is down?
   **A**: Scheduler continues. Semantic degrades gracefully.

3. **Q**: How do I disable semantic?
   **A**: Set `SEMANTIC_ENABLED=false` or just don't initialize it.

4. **Q**: Will it affect existing tests?
   **A**: No. All core tests pass without semantic layer.

5. **Q**: How to monitor semantic health?
   **A**: Check Prometheus metrics or `SEMANTIC_ENABLED` env var.
"""


# IRONCLAD v6 - STAGING DEPLOYMENT PLAYBOOK

**Status:** STAGING GO APPROVED  
**Date:** 2026-03-31  
**Production Status:** NOT YET (See: Production Blockers)

---

## Phase 1: Staging Deployment

### Environment Setup

```bash
# Navigate to workspace
cd /path/to/IRONCLAD

# Set environment variables
export EXECUTOR_MODE=cognitive
export REDIS_URL=redis://localhost:6379
export ANTHROPIC_API_KEY=sk-ant-[your-key]
export PYTHONPATH="hfa-worker/src:hfa-agents/src:hfa-semantic/src:hfa-core/src"

# Optional: Enable debug logging
export LOGLEVEL=INFO
```

### Install Dependencies

```bash
# Ensure all packages are installed
pip install -e hfa-worker/
pip install -e hfa-agents/
pip install -e hfa-semantic/
pip install -e hfa-core/

# Verify Redis is running
redis-cli ping
# Expected: PONG
```

### Start Worker (Cognitive Mode)

```bash
# Start the worker
python -m hfa_worker.main

# Expected logs:
# - "Building executor: mode=cognitive"
# - "CognitiveExecutor built: budget=5000"
# - "Worker started on port 8000"
# - No errors or exceptions
```

---

## Phase 2: Integration Test

### Run Integration Test Suite

```bash
# Run the integration test
pytest tests/integration/test_cognitive_integration.py -v

# Expected output:
# - test_cognitive_integration.py::test_single_task PASSED
# - test_cognitive_integration.py::test_concurrent_tasks PASSED
# - test_cognitive_integration.py::test_semantic_degradation PASSED
# - test_cognitive_integration.py::test_result_contract PASSED
# All tests should PASS
```

### Test Coverage

Integration tests should verify:
- ✓ RunRequestedEvent → ExecutionResult
- ✓ Semantic enrichment flow
- ✓ Agent orchestration
- ✓ Feedback loop (async)
- ✓ Error handling
- ✓ Concurrent execution

---

## Phase 3: Canary Deployment

### Small-Scale Test Configuration

```
Tenant: 1 (test-tenant)
Traffic: Low (5-10 tasks/minute)
Task Type: Simple (supervisor, researcher only)
Payload Size: <1MB
Expected Duration: 30 seconds - 2 minutes per task
Duration: 4-8 hours (initial validation)
```

### Canary Test Steps

1. **Send single task**
   ```bash
   curl -X POST http://localhost:8000/run \
     -H "Content-Type: application/json" \
     -d '{
       "run_id": "canary-1",
       "tenant_id": "test-tenant",
       "agent_type": "supervisor",
       "payload": {"goal": "validate staging setup"}
     }'
   ```

2. **Monitor worker logs**
   - Check for "CognitiveExecutor built"
   - Check for "SemanticBridge: event enriched"
   - Check for "FeedbackWriter validated"
   - No errors or exceptions

3. **Check result**
   - Status: "done" or "failed"
   - Payload: valid dict
   - No null/missing fields

4. **Send 5 more tasks**
   - Space them 30 seconds apart
   - Monitor for consistency
   - Check timing

5. **Run for 4-8 hours**
   - Keep sending low-volume tasks
   - Monitor metrics (see next section)
   - Watch for anomalies

---

## Phase 4: Monitoring Checklist (4-8 Hour Window)

### Metric 1: Worker Stability

```
Check every 30 minutes:
[ ] No worker crashes
[ ] No unhandled exceptions in logs
[ ] Memory usage: stable (no growth)
[ ] CPU usage: <50% baseline
[ ] Process still running: ps aux | grep hfa_worker
```

Log pattern to watch:
```
ERROR     → ALERT (worker may crash)
Exception → ALERT (needs immediate fix)
INFO      → Normal
DEBUG     → Verbose but ok
```

### Metric 2: Semantic Degradation Fallback

```
Watch for:
[ ] "CognitiveExecutor: semantic pipeline init failed" (acceptable)
[ ] "SemanticBridge: degraded mode" (acceptable)
[ ] "FeedbackWriter: memory write failed (graceful)" (acceptable)

Count occurrences:
- Expected: 0 (or minimal if Redis unavailable)
- Acceptable: <5 in 4 hours
- Alert: >10 means semantic instability
```

Log example (OK):
```
[INFO] SemanticBridge: degraded mode (no pipeline)
[INFO] CognitiveExecutor: event processed (degraded)
```

### Metric 3: Task Completion Latency

```
Baseline (local testing):
- Supervisor: 500ms - 2s
- Researcher: 1s - 3s
- Architect: 2s - 5s

Staging Acceptable:
- Single task: <30s (allow network overhead)
- 5 concurrent: <60s average
- 10 concurrent: <90s average

ALERT if:
- Single task >60s (latency spike)
- Average >120s (degradation)
- Increasing trend (semantic issue)
```

Logging to check:
```
[INFO] CognitiveExecutor: duration_ms=1234
```

### Metric 4: Duplicate/Retry Anomalies

```
Watch for patterns:
[ ] Same run_id processed twice (should not happen)
[ ] Retry loops (seen in logs)
[ ] Idempotency failures (run-xyz appears >1 time)

Expected:
- Each run_id processed exactly once
- No automatic retries (should fail-fast)

ALERT if:
- run_id appears 2+ times with different results
- "retry" mentioned in logs
- Feedback loop exceptions
```

Log pattern:
```
[INFO] SemanticBridge: duplicate event filtered: run-xyz
    → This is OK (dedup working)

[ERROR] Retrying task run-xyz
    → This is BAD (should not retry)
```

### Metric 5: Resource Usage (Memory/CPU)

```
Monitor process:
ps aux | grep hfa_worker

Expected baseline:
- RSS (resident memory): 100-300 MB
- CPU: <10% idle, <50% under load

ALERT if:
- RSS grows >500 MB (memory leak)
- RSS > 1 GB (definite leak, stop worker)
- CPU consistently >80% (efficiency issue)

Command to monitor:
watch -n 5 'ps aux | grep hfa_worker'
```

---

## Red Lines (Production Blockers)

If ANY of these occur, STOP staging and investigate:

### 1. Unexpected Failed Rate Spike
```
Normal: <5% of tasks fail
ALERT: >20% of tasks fail
Action: Check logs, identify root cause, fix, redeploy

Expected failures:
- Network timeout
- Agent error (recoverable)
- Payload validation

Unexpected failures:
- Worker crash
- Semantic corruption
- Deadlock
```

### 2. Latency Spike When Semantic is Active

```
Normal: Single task <30s
ALERT: Single task >120s AND semantic is available
Action: Semantic layer is too slow, needs optimization

This suggests:
- Redis slow/unresponsive
- Semantic enrichment bottleneck
- Graph/vector DB timeout
```

### 3. Cognitive Mode Stuck Run

```
Symptom: Task hangs indefinitely
Check: Is worker still running?
Check: Is it in executor?
Check: Is it in semantic?
Check: Is it in agent?

Action if stuck >5 minutes:
- Kill and restart worker
- Check for deadlock in logs
- Verify semantic pipeline
```

### 4. Concurrent Load Timeout

```
Scenario: Send 10 concurrent tasks
Expected: All complete within 2 minutes
ALERT: Tasks timeout or queue builds up

This suggests:
- Worker can't handle concurrency
- Executor is blocking
- Agent orchestration bottleneck
```

### 5. FeedbackWriter Event Loop Warning/Error

```
Watch logs for:
[WARNING] coroutine 'write' was never awaited
[ERROR] FeedbackWriter: write failed

This suggests:
- Async context issue
- Event loop blocking
- Fire-and-forget pattern broken

Action: Investigate FeedbackWriter async pattern
```

---

## Go/No-Go Decision Points

### After 1 Hour
- [ ] No worker crashes
- [ ] First 10 tasks completed
- [ ] Latency <30s per task
- [ ] Memory stable

**Decision:** Continue or escalate?

### After 4 Hours
- [ ] >50 tasks completed
- [ ] No red lines triggered
- [ ] Latency consistent
- [ ] Semantic degradation <5 times
- [ ] CPU/Memory stable

**Decision:** Canary SUCCESS or escalate?

### After 8 Hours
- [ ] >100 tasks completed
- [ ] All metrics green
- [ ] Zero red lines
- [ ] Staging STABLE

**Decision:** Ready for next phase or production?

---

## Production Readiness (NOT YET)

### Current Status: STAGING ✓ | PRODUCTION ✗

### Blockers for Production

1. **Graph Truth Validation** (NOT IMPLEMENTED)
   - Vector results not validated against Neo4j
   - Risk: Hallucinatory agent decisions
   - Needed for: v6.1 (Week 2)

2. **Hard Policy Bounds** (NOT IMPLEMENTED)
   - Adaptive policies unbounded
   - Risk: System self-blinds via feedback poisoning
   - Needed for: v6.2 (Week 3)

3. **HITL Gate** (NOT IMPLEMENTED)
   - Large decisions not escalated to humans
   - Risk: Autonomous system divergence
   - Needed for: v6.2 (Week 3)

4. **Production Observability** (MINIMAL)
   - Metrics: basic only
   - Alerting: manual only
   - Risk: Silent failures
   - Needed for: v6.2 (Week 3)

### Staging Sufficiency

Staging does NOT need these because:
- Small tenant count (control group)
- Low traffic (easy to monitor)
- Human oversight active
- Rollback easy
- Blast radius: minimal

### Production Insufficiency

Production CANNOT launch without these because:
- Large tenant count (uncontrollable)
- High traffic (hard to monitor)
- Autonomous behavior
- Rollback complex
- Blast radius: massive

---

## Next Steps

### Week 1 (Staging)
- [ ] Day 1-2: Canary 4-8 hours
- [ ] Day 3-4: Expand to 3 tenants
- [ ] Day 5-7: Full staging load test

### Week 2 (v6.1)
- [ ] Implement graph truth validation
- [ ] Integrate Neo4j checks
- [ ] Validate intersection merge

### Week 3 (v6.2)
- [ ] Implement policy bounds
- [ ] Add HITL escalation
- [ ] Production observability

### Week 4
- [ ] Final chaos tests
- [ ] Production certification
- [ ] Launch to prod

---

## Status Summary

```
STAGING:     GO ✓
PRODUCTION:  LATER (v6.1 + v6.2 required)
CONFIDENCE:  HIGH (9/9 tests passed)
RISK LEVEL:  LOW (staging only, small blast radius)
```

**Proceed to Phase 1: Staging Deployment** ✓


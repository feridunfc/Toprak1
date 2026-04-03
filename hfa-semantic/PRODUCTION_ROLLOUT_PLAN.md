"""
IRONCLAD v6 — PRODUCTION ROLLOUT PLAN

Status: READY FOR CONTROLLED DEPLOYMENT
Phases: Shadow → Passive → Canary (10%) → Full

═══════════════════════════════════════════════════════════════════════════════

PHASE 0: SHADOW MODE (Week 1)
─────────────────────────────────

Semantic layer runs but output is NOT used.
Purpose: Verify correctness without risk.

1. Deploy code
2. Semantic processes all events
3. Metrics collected but not acted upon
4. Zero behavior change in scheduler/agent
5. Monitoring: 100% of semantic operations logged

Exit criteria:
  ✅ Zero panics/crashes (1M events)
  ✅ Metrics clean
  ✅ Dedup accuracy 100% (test: duplicate_storm)
  ✅ Watermark monotonic (no regressions)
  ✅ Memory stable (no leaks)

Rollback: Delete semantic sidecar pod


PHASE 1: PASSIVE MODE (Week 2)
──────────────────────────────────

Semantic_ref attached to events but NO decision changes.

1. semantic_ref tag added to RunAdmittedEvent
2. Dedup runs but result not enforced (logging only)
3. Policy engine runs but no policy changes
4. Agent gets semantic hints but ignores them

Exit criteria:
  ✅ semantic_ref populated 100%
  ✅ Dedup matches (external validation against event duplication)
  ✅ Policy engine suggestions logged, 0 applied
  ✅ Operator confidence high

Rollback: Stop attaching semantic_ref


PHASE 2: CANARY (10% traffic) (Week 3)
────────────────────────────────────────

10% of events use semantic decision.

1. Sample rule selects 10% of runs
2. Those runs use semantic_ref for dedup enforcement
3. Policy suggestions reviewed before applying
4. Metrics compared: canary vs control

Exit criteria:
  ✅ Canary latency p99 <5ms (semantic overhead)
  ✅ Dedup enforcement matches behavior
  ✅ Policy suggestions valid (manual review)
  ✅ No user-facing regressions
  ✅ Monitoring shows expected metrics

Rollback: Return to 0% canary


PHASE 3: FULL DEPLOYMENT (Week 4+)
───────────────────────────────────

100% traffic through semantic.

1. semantic decision enforced on all events
2. Policy adaptation enabled (with guardrails)
3. Feedback loop active
4. Continuous monitoring


DETAILED DEPLOYMENT SCRIPT
═══════════════════════════════════════════════════════════════════════════════

PRE-DEPLOYMENT CHECKLIST
───────────────────────────────────

Code:
  [ ] lua_scripts.py: STATE_MERGE increments (not overwrites) ✅
  [ ] dedup_store.py: SETNX atomic ✅
  [ ] watermark_v2.py: Redis-backed per-rule ✅
  [ ] eviction_v2.py: Batch delete + backpressure ✅
  [ ] operational_metrics.py: All gauges exposed ✅

Tests:
  [ ] test_concurrent_correctness.py: All pass
  [ ] test_dedup_duplicate_flood.py: Pass
  [ ] test_watermark_monotonic.py: Pass
  [ ] test_memory_explosion.py: Pass (100k partitions)

Infrastructure:
  [ ] Redis cluster ready (production mode)
  [ ] Monitoring dashboards created
  [ ] Alert thresholds defined
  [ ] Runbooks written
  [ ] Rollback procedure tested

Operators:
  [ ] Training completed
  [ ] On-call schedule assigned
  [ ] Escalation procedures clear


PHASE 0 DEPLOYMENT STEPS
───────────────────────────────────

1. Create semantic-shadow namespace
   kubectl create namespace semantic-shadow

2. Deploy semantic sidecar (shadow mode)
   - SEMANTIC_ENABLED=true
   - SEMANTIC_DECISION_ENFORCEMENT=false
   - SEMANTIC_LOG_LEVEL=INFO
   - SEMANTIC_METRICS_PORT=8000

3. Configure event tap (100% sampling)
   - All events logged to semantic
   - No scheduler changes

4. Create monitoring dashboard
   - semantic_events_processed_total
   - semantic_dedup_hits_total
   - semantic_watermark_lag_ms
   - semantic_state_size_bytes
   - semantic_merge_conflicts_total

5. Run baseline tests
   pytest tests/concurrent_correctness.py -v


PHASE 1 DEPLOYMENT STEPS
───────────────────────────────────

1. Enable semantic_ref attachment
   - Modify event schema attachment
   - Test with 1% of events first

2. Add semantic_ref to RunAdmittedEvent
   - semantic_ref = f"rule:{rule}:partition:{partition}"
   - semanticmetadata = { ... }

3. Verify attachment
   - Log 1000 events
   - Grep for semantic_ref
   - Confirm 100% present

4. Update monitoring
   - semantic_ref_attached_ratio
   - semantic_metadata_size


PHASE 2 DEPLOYMENT STEPS (CANARY)
───────────────────────────────────

1. Select canary rules (deterministic)
   CANARY_HASH = hash(rule_id) % 100
   IF CANARY_HASH < 10: USE_SEMANTIC = true

2. Deploy semantic decision enforcer
   - if USE_SEMANTIC: enforce dedup + watermark
   - else: use raw mode

3. Create canary dashboard
   - metric_name{canary="true"} vs {canary="false"}
   - latency comparison
   - accuracy comparison

4. Set up alerts
   - Canary latency p99 > 10ms → page
   - Canary error rate > 0.1% → alert
   - Dedup mismatch > 0.01% → alert

5. Daily review
   - Compare metrics
   - Review policy suggestions
   - Check logs for errors

6. Gradual ramp
   - After 3 days: 20%
   - After 1 week: 50%
   - After 2 weeks: 100%


MONITORING & ALARMS
═══════════════════════════════════════════════════════════════════════════════

CRITICAL METRICS & THRESHOLDS

Metric                             Threshold      Action
───────────────────────────────────────────────────────────────
semantic_events_processed_total   < rate/2       P0: Check input
semantic_dedup_hits_ratio          > 0.5          P1: Investigate
semantic_watermark_lag_ms          > 30_000       P1: Order anomaly
semantic_state_size_bytes          > 500MB        P1: Eviction issue
semantic_merge_conflicts_total     > 100/sec      P0: Contention
partition_pressure                 > 0.95         P0: Eviction failing
semantic_late_event_ratio          > 0.1          P1: Stream disorder

LATENCY HISTOGRAMS

Metric                    P50       P95       P99
─────────────────────────────────────────────────
state_store_get_latency   0.5ms     1ms       5ms
state_store_put_latency   1ms       2ms       10ms
dedup_latency             0.1ms     0.5ms     1ms
watermark_latency         0.1ms     0.5ms     1ms

If exceeded:
  - Check Redis latency (redis-cli --latency)
  - Check Lua script execution time
  - Check GC pauses


BACKPRESSURE SIGNALS

Signal                     Meaning
─────────────────────────────────────
partition_pressure=0.8     WARNING: Eviction ramping up
partition_pressure=0.95    CRITICAL: Emergency eviction
backpressure_active=true   System under pressure

Action:
  - Monitor for 5 minutes
  - If > 10 mins: page on-call
  - Trigger eviction batch increase


ROLLBACK PROCEDURE
═══════════════════════════════════════════════════════════════════════════════

If P0 alert fires:

1. IMMEDIATE (0-2 mins)
   kubectl scale deployment semantic-sidecar --replicas=0
   (Scheduler continues in raw mode automatically)

2. Check logs (2-5 mins)
   kubectl logs deployment/semantic-sidecar --tail=1000

3. Analyze (5-15 mins)
   - What failed?
   - Root cause?
   - Can we fix in code?

4. Decision
   If code fix < 30 mins: fix + redeploy
   Else: stay in raw mode, schedule post-mortem

5. Post-mortem (next day)
   - What happened?
   - How to prevent?
   - Update runbook


MONITORING DASHBOARD (Prometheus)
═══════════════════════════════════════════════════════════════════════════════

Row 1: Throughput
  - semantic_events_processed_total
  - semantic_matches_emitted_total
  - semantic_dedup_hits_total

Row 2: Latency (histograms)
  - state_store_get_latency (p50, p95, p99)
  - state_store_put_latency (p50, p95, p99)
  - dedup_latency

Row 3: Pressure
  - partition_pressure (by rule)
  - active_partitions (by rule)
  - backpressure_active

Row 4: Correctness
  - watermark_lag_ms
  - late_event_ratio
  - dedup_accuracy

Row 5: Errors
  - merge_conflicts_total
  - redis_fallback_total
  - eviction_rate


DURING DEPLOYMENT
═══════════════════════════════════════════════════════════════════════════════

Check every 5 minutes:

- [ ] Semantic sidecar running (kubectl get pods)
- [ ] Events being processed (tail -f logs)
- [ ] Metrics flowing (prometheus scrape works)
- [ ] No alerts firing (AlertManager)
- [ ] Latency normal (dashboard p99 < threshold)

Check every hour:

- [ ] Dedup accuracy (compare to expected)
- [ ] Watermark progression (should be monotonic)
- [ ] Memory not growing (state size stable)
- [ ] No backpressure signals
- [ ] Policy suggestions sensible (manual review)

Check every 8 hours:

- [ ] Compare canary vs control group
- [ ] Review all policy suggestions
- [ ] Check for subtle issues in logs
- [ ] Verify no user complaints


METRICS COLLECTION (Push to Prometheus)
═══════════════════════════════════════════════════════════════════════════════

# HELP semantic_events_processed_total Events processed by semantic layer
# TYPE semantic_events_processed_total counter
semantic_events_processed_total{rule_id="rule1"} 1000000

# HELP semantic_state_store_latency_ms State store operation latency
# TYPE semantic_state_store_latency_ms histogram
semantic_state_store_latency_ms_bucket{operation="get",le="0.1"} 500000
semantic_state_store_latency_ms_bucket{operation="get",le="1"} 950000
semantic_state_store_latency_ms_bucket{operation="get",le="10"} 1000000
semantic_state_store_latency_ms_sum{operation="get"} 500000
semantic_state_store_latency_ms_count{operation="get"} 1000000

... (all operational metrics pushed every 10 seconds)


INCIDENT RESPONSE
═══════════════════════════════════════════════════════════════════════════════

Scenario 1: Dedup not working
  → Check: SETNX returning both 1 and 0 for same event_id
  → Fix: Likely race condition in try_accept(), rollback

Scenario 2: State growing unbounded
  → Check: eviction_rate = 0, partition_pressure → 1.0
  → Fix: Likely max_partitions too low or eviction failing
  → Action: Increase TTL, increase batch size, escalate

Scenario 3: Watermark lag increasing
  → Check: watermark stalled?
  → Fix: Lua script error in WATERMARK_OBSERVE?
  → Action: Check Lua error logs, rollback

Scenario 4: High latency (p99 > 10ms)
  → Check: Redis latency? Lua script slow? GC pause?
  → Action: Check Redis metrics, profile Lua, adjust batch size


═══════════════════════════════════════════════════════════════════════════════
"""


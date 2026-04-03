"""
hfa-semantic/ARCHITECTURE.md

IRONCLAD Semantic Layer — Production Architecture

## Overview

The semantic layer is a **distributed truth engine** that runs as a sidecar to IRONCLAD.

It solves three critical problems:

1. **Event Correctness**: Dedup, ordering, lateness handling
2. **State Safety**: Bounded memory, TTL eviction, cardinality control
3. **Result Consistency**: Graph truth, vector validation, deterministic merge

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    Event Input Stream                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────┐
        │    Dedup Check (event_id)      │ ◄─── DedupStore
        │   DROP if duplicate             │
        └─────────────┬──────────────────┘
                      │
                      ▼
        ┌────────────────────────────────┐
        │    Watermark / Lateness Check  │ ◄─── Watermark
        │   event_time vs processing_time│
        └─────────────┬──────────────────┘
                      │
                      ▼
        ┌────────────────────────────────┐
        │  StateStore-backed Runtime     │ ◄─── StateStore
        │  (compute reasoning state)     │      (Redis/InMemory)
        └─────────────┬──────────────────┘
                      │
                      ▼
        ┌────────────────────────────────┐
        │    Reasoning (DSL + Rules)     │ ◄─── DSL, Graph, Vector
        │   Generate match candidates    │
        └─────────────┬──────────────────┘
                      │
                      ▼
        ┌────────────────────────────────┐
        │   Agent / Execution Target     │
        │   (with semantic enrichment)   │
        └─────────────┬──────────────────┘
                      │
                      ▼
        ┌────────────────────────────────┐
        │   Validated Outcome            │ ◄─── OutcomeValidator
        │   (truth-checked feedback)     │
        └─────────────┬──────────────────┘
                      │
                      ▼
        ┌────────────────────────────────┐
        │    Policy Engine Guard         │ ◄─── Bounds, Cooldown, HITL
        │   (future Sprint 16+)          │
        └────────────────────────────────┘
```

## Components (Sprint R1-R3)

### 1. StateStore (Abstract Interface)

**File**: `runtime/state_store.py`

Provides backend-agnostic state storage.

Implementations:
- `InMemoryStateStore`: Dev/test (single-node, non-durable)
- `RedisStateStore`: Production (durable, scalable, restart-safe)

Operations:
```python
state = await store.get(rule_id, partition)
await store.put(rule_id, partition, state_dict, ttl_ms)
await store.delete(rule_id, partition)
evicted = await store.evict(rule_id, max_partitions)
```

### 2. DedupStore (Event Idempotency)

**File**: `runtime/dedup_store.py`

Prevents duplicate events from producing duplicate matches.

Operations:
```python
is_dup = await dedup.is_duplicate(event_id)
await dedup.mark_processed(event_id, event_time_ms, ttl_ms)
```

**TTL**: Configurable, default 1 hour. After expiry, same event_id is reprocessed.

### 3. Watermark (Event Ordering)

**File**: `runtime/watermark.py`

Tracks event time progress and detects late arrivals.

```python
watermark = Watermark(allowed_lateness_ms=10_000)
is_late, should_accept = watermark.observe(event_time_ms)

# Or check policy
accept = watermark.should_accept(event_time_ms, LatenessPolicy.DROP)
```

Policies:
- `DROP`: Late events ignored (default, safe)
- `ACCEPT_WITH_CORRECTION`: Process with retry
- `SIDE_CHANNEL`: Route to repair queue

### 4. Partitioning

**File**: `runtime/partitioning.py`

Determines how events map to state partitions.

Strategies:
- `ENTITY_ID`: One partition per entity
- `TENANT_ID`: One partition per tenant
- `EVENT_TYPE`: One partition per type
- `CUSTOM`: User-defined function

**Purpose**: Scale reasoning state while preventing O(n²) memory explosion.

### 5. Eviction Policy

**File**: `runtime/eviction.py`

Controls when state is removed (memory safety).

Strategies:
- `TTL_ONLY`: Expire by age
- `LRU`: Least recently used
- `WATERMARK`: Beyond event window
- `HYBRID`: Combine approaches

**Max partitions per rule**: Hard boundary (prevents runaway state).

### 6. RuntimeMetrics (Observability)

**File**: `runtime/runtime_metrics.py`

Prometheus-compatible metrics:
- `events_processed_total`: Events seen
- `matches_emitted_total`: Rule firing rate
- `dedup_hits_total`: Duplicate events
- `late_events_total`: Ordering anomalies
- `state_size_bytes`: Memory pressure
- `eviction_rate`: Partition pressure

## Configuration

Environment variables (all optional with sane defaults):

```bash
# Enable/disable entire layer
SEMANTIC_ENABLED=true

# State backend
SEMANTIC_STATE_BACKEND=redis         # or "memory" for dev
SEMANTIC_REDIS_URL=redis://...

# Memory bounds
SEMANTIC_MAX_PARTITIONS_PER_RULE=10000
SEMANTIC_STATE_TTL_MS=3600000        # 1 hour

# Event handling
SEMANTIC_ALLOWED_LATENESS_MS=10000   # 10 seconds
SEMANTIC_LATE_EVENT_POLICY=drop

# Dedup
SEMANTIC_DEDUP_TTL_MS=3600000

# Observability
SEMANTIC_METRICS_PORT=8000
SEMANTIC_LOG_LEVEL=INFO

# Partitioning
SEMANTIC_PARTITION_STRATEGY=entity_id
```

## Production Deployment

### Single-Node (Development)

```bash
export SEMANTIC_STATE_BACKEND=memory
python -m hfa_semantic.service
```

### Redis-Backed (Production)

```bash
export SEMANTIC_STATE_BACKEND=redis
export SEMANTIC_REDIS_URL=redis://prod-redis:6379
python -m hfa_semantic.service
```

### Horizontal Scaling

1. Each semantic sidecar connects to same Redis
2. Events routed by partition key
3. State is shared via Redis
4. No coordination needed

```
Scheduler 1 ──┐
              ├─→ Event Stream ──┐
Scheduler 2 ──┘                   │
                                  ▼
                        ┌──────────────────┐
                        │ Semantic Sidecar │
                        │  (instance 1)    │
                        └────────┬─────────┘
                                 │
                        ┌────────▼─────────┐
                        │  Shared Redis    │
                        │  State Store     │
                        └─────────────────┘
                                 ▲
                        ┌────────┴─────────┐
                        │ Semantic Sidecar │
                        │  (instance 2)    │
                        └──────────────────┘
```

## Failover & Degradation

### Semantic Failure

If semantic layer crashes:
1. Scheduler continues (semantic is optional)
2. Raw mode: agent works without enrichment
3. Metrics show missing semantic data
4. No cascading failures

### Redis Degradation

If Redis is slow/unavailable:
1. Fallback to in-memory cache (if configured)
2. Dedup still works (local cache)
3. Watermark still works (in-process)
4. Graceful degradation

## Security

- ✅ No scheduler modification
- ✅ No authentication to scheduler
- ✅ Semantic results are advisory (not mandatory)
- ✅ No lateral movement risk

## Observability

### Prometheus Metrics

Available at `http://localhost:8000/metrics`:

```
semantic_events_processed_total{rule_id="rule1"} 1000
semantic_matches_emitted_total{rule_id="rule1",severity="HIGH"} 5
semantic_dedup_hits_total 42
semantic_late_events_total{policy="drop"} 3
semantic_state_size_bytes{rule_id="rule1"} 1048576
semantic_active_partitions{rule_id="rule1"} 25
```

### Tracing

Semantic spans follow OpenTelemetry convention.

### Logs

Structured logs with context (rule_id, event_id, partition).

## Future Sprints

| Sprint | Component | Purpose |
|--------|-----------|---------|
| R4 | Redis backend hardening | Production-ready, tested failures |
| R5 | Chaos tests | Duplicate storms, high cardinality |
| S1 | DSL + Reasoning | Temporal rules, pattern matching |
| S2 | Query Engine | Graph + vector merge |
| S3 | Memory Layer | Validated outcomes |
| S4 | Policy Engine | Bounded adaptation |
| S5 | Production gates | All components tested, gated |

## Performance Characteristics

### Latency

- Dedup: O(1) Redis lookup
- Watermark: O(1) in-memory update
- State get/put: O(1) Redis operation
- Eviction: O(log n) per partition

**Total per event**: <1ms (Redis local) to 10ms (network latency)

### Memory

- Per partition state: O(1) to O(n) depending on rule
- Dedup entries: O(1) per event_id (capped by TTL)
- Watermark: O(1) in-memory
- Metrics: O(num_rules) Prometheus series

### Throughput

- Events/sec: Limited by event stream input rate
- State updates: Limited by Redis throughput (100k+ ops/sec typical)
- Dedup hits: No impact on latency

## Testing

```bash
# Unit tests (in-memory)
pytest hfa-semantic/tests/test_runtime_foundation.py -v

# Integration tests (Redis)
pytest hfa-semantic/tests/ -v -k integration

# Chaos tests (future)
pytest hfa-semantic/tests/chaos/ -v
```

## Summary

The semantic layer provides:

✅ **Correctness**: Dedup, ordering, deterministic merge
✅ **Safety**: Bounded state, TTL eviction, max partitions
✅ **Scalability**: Redis backend, horizontal scaling
✅ **Observability**: Prometheus metrics, OpenTelemetry
✅ **Optionality**: Scheduler works without it
✅ **Simplicity**: Minimal, focused components
"""


# Run Inspector — Retention & Archive Policy
**HFA IRONCLAD Sprint 7 · Mini Paket 2**
*`docs/ops/retention_policy.md`*

---

## Overview

The Run Inspector stores execution data in two tiers:

| Tier | Backend | Scope | Eviction |
|---|---|---|---|
| **Live** | In-process RAM | Active + recently completed runs | Automatic (cleanup loop) |
| **Historical** | Redis (JSON, sorted set) | Completed runs post-eviction | Redis TTL |

A run exists in exactly one tier at any time:
* Runs enter the **Live** tier on creation (`RunRegistry.register()`).
* Runs move to the **Historical** tier atomically when the cleanup loop archives them.
* Runs exit the system when the Redis TTL expires.

---

## Configuration

All parameters are set in `RunRegistry.__init__()`:

| Parameter | Default | Description |
|---|---|---|
| `max_age` | `3600.0` s | Minimum age before a completed run is archive-eligible |
| `cleanup_interval` | `300.0` s | How often the cleanup loop runs |
| `history_ttl_seconds` | `604800` s | Redis TTL for archived snapshots (7 days) |
| `redis_client` | `None` | Async Redis client; `None` = memory-only mode |

**Example — production (30 day history):**
```python
registry = RunRegistry(
    max_age=3600,
    cleanup_interval=300,
    history_ttl_seconds=2_592_000,   # 30 days
    redis_client=aioredis.from_url("redis://redis:6379"),
)
```

**Example — test (fast cycle):**
```python
registry = RunRegistry(
    max_age=0.05,
    cleanup_interval=0.05,
    history_ttl_seconds=60,
    redis_client=mock_redis,
)
```

---

## Redis Key Schema

```
hfa:run:{run_id}                STRING   JSON snapshot, TTL = history_ttl_seconds
hfa:tenant_runs:{tenant_id}     ZSET     member = run_id, score = created_at (float)
```

* The ZSET index is **not** TTL-controlled and may accumulate stale members after
  snapshot keys expire. This is acceptable: `mget` returns `None` for expired keys,
  and the tenant list silently skips `None` entries.
* Future Sprint 8 work may add a ZSET pruning step if index growth becomes a concern.

---

## Archive Flow

```
Cleanup loop (every cleanup_interval seconds)
  │
  ├── Identify stale: is_complete() == True AND age > max_age
  │
  ├── For each stale graph:
  │     ├── pipeline(transaction=True):
  │     │     SET  hfa:run:{run_id}          JSON snapshot  EX history_ttl_seconds
  │     │     ZADD hfa:tenant_runs:{tenant}  score=created_at  member=run_id
  │     │     EXECUTE
  │     │
  │     ├── archive returned True  →  evict from RAM (_graphs, _added_at)
  │     └── archive returned False →  keep in RAM, retry next cleanup cycle
  │
  └── Log archived+evicted count
```

### Atomicity guarantee

`SET` and `ZADD` are issued inside a single `pipeline(transaction=True)`.
Either both are committed or neither is. A partial write cannot occur.

### Failure behaviour

| Failure | Result |
|---|---|
| Pipeline `execute()` raises | `_archive_run()` returns `False`, run stays in RAM |
| Redis network error | Same as above |
| Any unexpected exception | Same as above — never propagated to cleanup loop |

The cleanup loop will retry the same run on its next cycle.

---

## Expired History Semantics

### Decision (Sprint 7)

**Expired (TTL elapsed) = 404. No distinction from never-existed.**

Rationale:
* Implementing a 410 Gone response requires tracking a "tombstone" per run_id.
  This adds state, complexity, and a new Redis key type.
* The primary consumer (UI) has no known use-case for distinguishing
  "expired 30 days ago" from "never existed".
* This decision is reversible: tombstone support can be added in Sprint 8
  without breaking existing API consumers (404 → 410 is a status-code narrowing).

### Consequence

```
GET /v1/inspector/runs/{expired_run_id}
→ 404 Not Found

GET /v1/inspector/runs/{never_existed_run_id}
→ 404 Not Found
```

Clients must not rely on the difference between these two cases.

---

## Idempotency

Archiving the same `run_id` twice is safe:

* `SET` overwrites the snapshot (last-write-wins).
* `ZADD` overwrites the ZSET score for the existing member — no duplicate members.
* The tenant list will show the run exactly once.

This means a retry after a network timeout cannot corrupt the index.

---

## Degrade Behaviour (Redis Unavailable)

| Operation | Redis available | Redis unavailable |
|---|---|---|
| `get_snapshot(run_id)` in RAM | RAM hit, no Redis call | RAM hit, no Redis call |
| `get_snapshot(run_id)` not in RAM | Redis JSON fetch | `None` → endpoint returns 404 |
| `get_tenant_run_summaries()` | RAM + Redis merged | RAM-only list |
| `_archive_run()` | Atomic pipeline write | Returns `False`, no eviction |
| `list_by_tenant()` (sync) | RAM-only | RAM-only (never uses Redis) |

**The system never raises an unhandled exception due to Redis being unavailable.**
**Live (RAM) runs are always served regardless of Redis state.**

---

## JSON Decode Errors

If a stored snapshot is corrupt (non-UTF-8 bytes or invalid JSON):

* `get_snapshot()` logs an error at ERROR level.
* Returns `None` → endpoint surfaces as 404.
* The corrupt entry is not deleted automatically (TTL will eventually clean it).
* No exception propagates to the caller.

---

## Memory-Only Mode

If `redis_client=None`:

* All runs remain in RAM indefinitely until cleanup.
* `_archive_run()` returns `True` immediately (no-op success).
* The cleanup loop still runs and evicts from RAM after `max_age` seconds.
* Evicted runs are permanently lost (no persistence).
* Suitable for development, testing, and single-process deployments
  where run history is not required across restarts.

---

## Operational Checklist

| Check | How to verify |
|---|---|
| TTL set correctly | `redis-cli TTL hfa:run:{run_id}` after archive |
| ZSET populated | `redis-cli ZRANGE hfa:tenant_runs:{tenant_id} 0 -1 WITHSCORES` |
| Archive loop running | `inspector.registry.cleanup` task visible in `asyncio` task list |
| Degrade working | Kill Redis; `GET /runs/{live_run_id}` must still return 200 |
| Cleanup interval | Set `cleanup_interval=10` in staging to observe fast eviction |

---

## Sprint 8 Roadmap Items

* **Stale ZSET pruning** — periodically remove ZSET members whose snapshot keys have expired
* **410 Gone support** — tombstone per run_id with configurable tombstone TTL
* **Archive-on-complete** — immediate archive when graph transitions to DONE/FAILED,
  without waiting for `max_age` (reduces RAM pressure for high-throughput deployments)
* **Compression** — gzip snapshot payload before Redis write for large graphs

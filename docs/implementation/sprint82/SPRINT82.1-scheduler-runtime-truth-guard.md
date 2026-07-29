# Sprint 82.1 — Scheduler Runtime Truth Guard

## Scope

Sprint 82.1 guards exactly the existing atomic `TASK_DISPATCH` commit. It does not modify Sprint 80 frozen diagnostics, does not call the canonical-authority conflict adapter, and does not use the `hfa:authority:v1` namespace.

## Authoritative order

`task_dispatch_commit.lua` receives the RUN state key as `KEYS[8]` and the global runtime-truth conflict index/stream as `KEYS[9]` and `KEYS[10]`. The Lua script validates RUN truth and, on rejection, records durable conflict evidence before any task state, metadata, ready queue, scheduled/running zset, control stream, or shard stream mutation.

## RUN truth classification

Permitted nonterminal states are exactly `admitted`, `queued`, `scheduled`, `running`, and `rescheduled`. Terminal states are exactly `done`, `failed`, `rejected`, and `dead_lettered`.

| Condition | Status |
|---|---|
| RUN key missing | `run_truth_missing` |
| Known terminal truth | `run_truth_terminal_conflict` |
| Wrong Redis type, empty, corrupt, or unknown truth | `run_truth_corruption_conflict` |
| Evidence pair unavailable or corrupt | `truth_conflict_evidence_store_unavailable` |

## Durable observation store

The centralized keys are:

- `RedisKey.runtime_truth_conflict_index()` → `hfa:runtime-truth:v1:conflicts:index`
- `RedisKey.runtime_truth_conflict_stream()` → `hfa:runtime-truth:v1:conflicts:stream`

The index/stream pair follows the authority conflict integrity pattern: Redis type checks, reserved cardinality, deterministic length-prefixed identity, `redis.sha1hex`, `HSETNX`, first-payload-wins, and one stream append only for the first observation.

## Correctness boundary

The Lua guard is authoritative. Any Python OCC/pre-check remains an optimization only. Accepted RUN truth preserves the previous successful dispatch result fields and mutation order.

## Governance

This sprint does not authorize production cutover or claim that runtime truth is globally reconciled. It adds one fail-closed atomic scheduler guard and durable reconciliation evidence.

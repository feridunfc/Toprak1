# Sprint 82.1 — Scheduler Runtime Truth Guard

## Scope

Sprint 82.1 guards exactly the existing atomic `TASK_DISPATCH` commit. It does not modify Sprint 80 frozen diagnostics, does not call the canonical-authority conflict adapter, and does not use the `hfa:authority:v1` namespace.

## Authoritative order

`task_dispatch_commit.lua` receives the RUN state key as `KEYS[8]` and the global runtime-truth conflict index/stream as `KEYS[9]` and `KEYS[10]`. Before the supplied RUN key is read or classified, the Lua script reads task state without mutation, requires task metadata, verifies authoritative `task_id` and `run_id`, and compares the authoritative `run_id` with the explicit dispatch `run_id`. Only an exact task/run identity match may proceed to RUN truth classification or durable conflict observation.

A mismatched explicit `run_id` returns `identity_run_id_mismatch`, records no runtime-truth conflict evidence, and performs no lifecycle mutation. The mismatched RUN key is not treated as the task's counterpart authority.

After exact identity validation, RUN truth rejection records durable conflict evidence before any task state, metadata, ready queue, scheduled/running zset, control stream, or shard stream mutation.

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

Every observation is bound to the exact operation identity `TASK_DISPATCH`. The fixed deterministic identity order is `operation`, `run_id`, `task_id`, `status`, `detail_code`, and `observed_run_state`. Operation is also persisted in conflict JSON and conflict stream fields and is verified when an existing duplicate record is reused. Consequently, otherwise identical conflicts from different operations cannot deduplicate to one record.

The index/stream pair follows the authority conflict integrity pattern: Redis type checks, reserved cardinality, deterministic length-prefixed identity, `redis.sha1hex`, `HSETNX`, first-payload-wins, duplicate identity validation, and one stream append only for the first observation.

## Correctness boundary

The Lua guard is authoritative. Any Python OCC/pre-check remains an optimization only. Accepted RUN truth preserves the previous successful dispatch result fields and mutation order.

## Governance

This sprint does not authorize production cutover or claim that runtime truth is globally reconciled. It adds one fail-closed atomic scheduler guard and durable reconciliation evidence.

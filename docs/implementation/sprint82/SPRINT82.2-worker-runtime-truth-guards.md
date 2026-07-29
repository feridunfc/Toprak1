# Sprint 82.2 — Worker Lifecycle Runtime Truth Guards

## Scope

Sprint 82.2 adds atomic fail-closed RUN truth guards to the canonical worker lifecycle operations:

- `TASK_CLAIM`
- `TASK_HEARTBEAT`
- `TASK_COMPLETE`
- `TASK_FAIL`

It does not modify task requeue, run recovery, task recovery mutation, control API read models, canonical transition persistence, automatic repair, or production feature-flag cutover.

## Identity contract

Canonical production calls propagate explicit `run_id` end to end:

```text
TaskContext.run_id
→ TaskConsumer
→ TaskClaimManager / HeartbeatLoop / DagLua.task_complete
→ Lua
```

Compatibility-only callers may pre-read `task_meta.run_id` to construct the RUN key. The Lua script still compares the supplied `run_id` with authoritative task metadata before reading RUN truth or mutating lifecycle state.

Wrong explicit RUN identity returns `identity_run_id_mismatch`, writes no runtime-truth observation against the wrong RUN, and performs zero lifecycle mutation.

## RUN truth classification

Permitted nonterminal states are exactly:

- `admitted`
- `queued`
- `scheduled`
- `running`
- `rescheduled`

Terminal states are exactly:

- `done`
- `failed`
- `rejected`
- `dead_lettered`

Structured runtime-truth rejection statuses are:

- `run_truth_missing`
- `run_truth_terminal_conflict`
- `run_truth_corruption_conflict`
- `truth_conflict_evidence_store_unavailable`

## Atomic operation contracts

### TASK_CLAIM

RUN truth is validated after exact task/run identity and actionable task-state classification, but before claim epoch increment, task state/meta mutation, scheduled/running zset mutation, or reservation deletion.

A rejected claim leaves the reservation and task-indexed reservation owner intact.

### TASK_HEARTBEAT

Task state must be `running`. Exact task/run identity and RUN truth are validated before worker/claim fences and before heartbeat metadata or running-zset refresh.

Every rejected heartbeat is returned as `ok=False`. `HeartbeatLoop` treats it as ownership loss and stops retrying, allowing `TaskConsumer` to cancel active execution through the existing ownership-loss path.

The mock/fakeredis compatibility path may authorize only an accepted nonterminal RUN. A conflict that cannot be durably observed atomically fails closed as `truth_conflict_evidence_store_unavailable`.

### TASK_COMPLETE / TASK_FAIL

Existing invalid-terminal, missing-task, already-terminal, and non-running task behavior is preserved. For a running task, exact identity and RUN truth are validated before claim/scheduler/worker fences and before parent state, metadata, output, running-zset, dependency counter, child state, ready marker, or ready-queue mutation.

`terminal_state=done` records operation `TASK_COMPLETE`; `terminal_state=failed` records operation `TASK_FAIL`.

## Durable runtime-truth observation

All operations reuse:

- `RedisKey.runtime_truth_conflict_index()`
- `RedisKey.runtime_truth_conflict_stream()`

The deterministic conflict identity has the exact fixed order:

```text
operation
run_id
task_id
status
detail_code
observed_run_state
```

Every Lua operation preserves:

- Redis index/stream type validation;
- pair existence and cardinality validation;
- length-prefixed deterministic material;
- `redis.sha1hex` conflict identity;
- `HSETNX` first-payload-wins behavior;
- one `XADD` only for the first observation;
- duplicate identity validation;
- zero lifecycle mutation if the evidence store is unavailable or corrupt.

Different operations never deduplicate into the same record. Repeated observations of the same operation and same logical conflict deduplicate.

## ACK boundary

`WorkerConsumer` ACK code is not changed. Existing behavior remains authoritative:

- claim rejection: no ACK;
- ownership loss: no committed completion and no ACK;
- completion/failure rejection: no ACK;
- committed completion: ACK permitted;
- explicit terminal duplicate cleanup remains a separate transport policy.

## Verification

The Sprint 82.2 package includes:

- static/unit contract coverage;
- real Redis claim, heartbeat, completion and failure coverage;
- cross-operation noncollision and same-operation deduplication;
- worker execution cancellation on heartbeat ownership loss;
- completion suppression when RUN becomes terminal after execution;
- existing worker composition, claim-path and ACK regressions.

## Governance

Sprint 82.2 does not authorize:

- `TASK_REQUEUE` truth migration;
- recovery mutation changes;
- canonical authority records for worker operations;
- automatic reconciliation repair;
- `HFA_CANONICAL_TASK_ADMIT_BINDING` enablement;
- production-readiness claims.

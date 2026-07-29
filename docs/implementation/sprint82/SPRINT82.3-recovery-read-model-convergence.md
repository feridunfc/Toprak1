# Sprint 82.3 — Recovery and Read-Model Convergence

## Scope

Sprint 82.3 closes the remaining automatic recovery callers that could mutate or clean projections without first classifying both lifecycle truth planes.

Covered surfaces:

- `TASK_REQUEUE`;
- stale RUN reschedule/dead-letter decision;
- control-plane RUN state query enrichment.

The slice is deliberately fail-closed. It does not introduce an automatic reconciliation worker or authorize repair of a terminal/nonterminal contradiction.

## TASK_REQUEUE contract

Production callers propagate explicit `run_id` into `TaskRecoveryManager.requeue_stale_task()`.

Compatibility callers may pre-read `task_meta.run_id` only to construct the RUN and run-task-index Redis keys. `task_requeue.lua` revalidates:

1. task/run identity from task metadata; or, when task metadata is absent, exact membership in `DagRedisKey.run_tasks(run_id)`;
2. task state type and value;
3. RUN state type and value;
4. conflict-evidence pair integrity;
5. requeue mutation preconditions.

Known nonterminal RUN states permit requeue:

- `admitted`;
- `queued`;
- `scheduled`;
- `running`;
- `rescheduled`.

Missing, terminal, unknown, empty or wrong-type RUN truth blocks:

- task state mutation;
- task metadata mutation;
- running/ready ZSET mutation;
- completion-stream emission.

The durable conflict operation is exactly `TASK_REQUEUE`.

## RUN recovery contract

`RecoveryService._find_stale_runs()` is now detection-only. It reads the stale running projection and performs no cleanup, state selection or metadata mutation.

`run_recovery_commit.lua` owns the mutation boundary. The Python caller pre-reads sorted task IDs only to construct dynamic `KEYS`; Lua revalidates:

- RUN state and metadata type;
- optional stored RUN identity;
- run-task index type and cardinality;
- every task membership;
- every task ID and RUN ID binding;
- every task state type and terminal class;
- the expected reschedule count;
- runtime-truth evidence-store integrity.

A compatible nonterminal RUN/task aggregate may be atomically changed to `rescheduled`, with the reschedule count and running projection updated in the same script.

A terminal RUN is never silently removed from the running projection. Missing/corrupt authority and terminal/nonterminal cross-plane contradictions produce durable operation-bound evidence under `RUN_RECOVERY` and perform zero mutation.

### Automatic dead-letter boundary

The old recovery path could make RUN terminal while leaving TASK nonterminal. That would create a new cross-plane contradiction.

Sprint 82.3 therefore blocks automatic `DEAD_LETTER` when coordinated TASK terminalization would be required. The observation uses:

```text
task_truth_terminal_conflict
detail_code=dead_letter_requires_explicit_task_terminalization
```

Coordinated terminalization belongs to a later explicit reconciliation command. This sprint does not silently repair it.

## Durable observation identity

Both recovery operations reuse:

- `RedisKey.runtime_truth_conflict_index()`;
- `RedisKey.runtime_truth_conflict_stream()`.

Conflict identity is deterministic and fixed-order:

```text
operation
run_id
task_id
status
detail_code
observed_run_state
```

Observation timestamps and requested action metadata are not identity members. Repeated logical observations deduplicate and preserve the first payload. Different operations do not collide.

## RUN query read model

`ControlPlaneService.get_run_state()` remains strictly read-only.

The response keeps RUN state as the scoped answer and adds:

- `task_count`;
- `task_truth`;
- `truth_status`;
- `truth_conflict`;
- `truth_conflicts`.

TASK state never overwrites RUN state. Missing and wrong-type Redis keys are classified into explicit metadata rather than leaking raw Redis `WRONGTYPE` failures.

When RUN metadata is missing but all task metadata agrees on one tenant, that tenant may be used for request authorization while the missing RUN metadata is still reported as a conflict. Tenant disagreement is explicitly reported and never silently resolved.

## Out of scope

Sprint 82.3 does not authorize:

- automatic conflict repair;
- coordinated task/run terminalization;
- DLQ replay redesign;
- cancellation lifecycle migration;
- dependency-process authority wiring;
- canonical authority records for recovery operations;
- feature-flag cutover;
- production-readiness claims.

## Exit criteria

- stale detection is read-only;
- TASK requeue is atomically guarded by RUN truth;
- RUN recovery revalidates the complete indexed task set before mutation;
- terminal/missing/corrupt contradictions leave every lifecycle and projection surface unchanged;
- automatic dead-letter cannot create a new RUN/TASK contradiction;
- run query returns scoped RUN truth and explicit task conflict metadata;
- real Redis unit, integration and convergence E2E tests pass without skip or deselection;
- Sprint 81.1–82.2 regressions, compileall and whitespace checks remain green.

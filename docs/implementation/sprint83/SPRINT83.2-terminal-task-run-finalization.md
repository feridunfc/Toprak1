# Sprint 83.2 — Terminal TASK to RUN Finalization

## Status

```yaml
sprint: 83.2
name: Terminal TASK to RUN Finalization
base: 8adb8fc8b6e54fd7e15c6599823569d1356602cb
feature_default: DISABLED
production_cutover: NOT_AUTHORIZED
automatic_repair: false
self_healing: false
```

## Purpose

Sprint 83.1 proves that the canonical TASK product path executes and stores a
TASK output, but the RUN remains nonterminal. Sprint 83.2 adds an explicit,
operation-scoped `RUN_TERMINATE` coordination boundary after terminal TASK
commit.

```text
TASK_COMPLETE commit
→ read aggregate TASK truth
→ RUN_TERMINATE
→ RUN state/result/meta/event projection commit
→ message ACK
```

The feature is explicit opt-in through `RunTerminationCoordinator(..., enabled=True)` and the `RunFinalizingTaskConsumer` / `RunFinalizingWorkerConsumer` adapters. Existing `DagLua`, `TaskConsumer`, and `WorkerConsumer` composition remains unchanged. This is an internal acceptance binding, not a production cutover.

## Aggregate ownership

The accepted Sprint 80 matrix keeps the operations separate:

```text
TASK_COMPLETE  → TASK_AGGREGATE
RUN_TERMINATE  → RUN_AGGREGATE
```

`task_complete.lua` and the existing DAG gateway are not expanded into cross-aggregate writers. `run_terminate_from_tasks.lua` never writes TASK state, metadata, output, dependency, queue, claim, reservation, or heartbeat data.

## RUN_TERMINATE policy

The RUN finalizer reads the immutable run-to-task membership set and validates
every TASK identity and state.

```text
all TASK states done/skipped
→ RUN done

any terminal failure state
→ RUN failed

any TASK nonterminal
→ not_ready
→ no RUN mutation
→ ACK remains safe for the completed TASK message

missing/corrupt/unknown TASK or RUN truth
→ fail closed
→ durable RUN_TERMINATE conflict evidence
→ no RUN mutation
→ no ACK
```

Terminal failure states in this runtime boundary are:

```text
failed
blocked_by_failure
dead_lettered
rejected
cancelled
```

No task is automatically converted to a terminal state. Failure propagation,
dependency repair, cancellation, and self-healing are outside this sprint.

## Atomic RUN projection commit

After all input and Redis-key types are validated, one Lua execution commits:

- RUN terminal state;
- RUN metadata convergence;
- RUN result projection;
- active RUN ZSET cleanup;
- one `RunCompleted` or `RunFailed` result-stream event.

A duplicate invocation validates the existing terminal result and returns
`already_finalized` without emitting another event.

## ACK contract

Normal delivery:

```text
TASK completion rejected
→ no ACK

TASK committed + RUN_TERMINATE finalized/already_finalized/not_ready
→ ACK allowed

TASK committed + RUN_TERMINATE conflict/unavailable
→ no ACK
```

Terminal duplicate delivery:

- claim, execution, and TASK completion remain suppressed;
- the opt-in `RunFinalizingTaskConsumer` invokes only the idempotent RUN_TERMINATE operation;
- ACK occurs only when that operation reports `ack_allowed=true`;
- compatibility consumers without the opt-in binding retain their historical
  behavior.

This closes the process-crash window between TASK terminal commit and RUN
finalization for the opt-in product path.

## Explicit non-goals

- global canonical authority wiring;
- canonical RUN operation receipts/revisions;
- automatic reconciliation or repair;
- task failure propagation;
- product cancellation;
- product retry/requeue;
- external executor cutover;
- production enablement.

The runtime operation is designed to be migrated into the global canonical
RUN authority during the authority-wiring phase.

## Acceptance

Real Redis tests prove:

1. one successful TASK finalizes RUN `done`;
2. any terminal failure finalizes RUN `failed`;
3. a nonterminal sibling produces mutation-free `not_ready`;
4. the last terminal TASK converges the RUN;
5. duplicate RUN_TERMINATE emits one terminal event;
6. corrupt TASK truth blocks RUN mutation with evidence;
7. wrong-type mutation targets produce no partial lifecycle write;
8. TASK_COMPLETE invokes RUN_TERMINATE when opt-in is enabled;
9. `not_ready` completion remains ACK-safe;
10. terminal duplicate delivery repairs missing RUN finalization before ACK;
11. terminal duplicate finalization conflict remains pending;
12. the one-command product acceptance path exposes RUN result and event;
13. the acceptance artifact is deterministic;
14. the CLI writes the exact acceptance artifact.

## Exit flags

```yaml
runtime_alpha_testable: true
run_finalization_supported: true
run_termination_binding_default_enabled: false
product_alpha_ready: false
production_ready: false
automatic_repair_authorized: false
production_cutover_authorized: false
```

# Sprint 83.4 — Production RUN finalization composition binding

## Boundary

```text
production WorkerService
→ strict default-off configuration boundary
→ existing Sprint 83.2 RUN finalization coordinator
→ existing canonical TASK_COMPLETE
→ separate idempotent RUN_TERMINATE
```

Sprint 83.4 does not introduce a new lifecycle writer. It binds the already
implemented and tested Sprint 83.2 finalization boundary into the production
`WorkerService` composition behind an explicit default-disabled flag.

```yaml
canonical_lifecycle_authority_changed: false
new_lifecycle_writer: false
run_termination_binding_supported: true
run_termination_binding_default_enabled: false
production_cutover_authorized: false
Loop_Plane_dependency: NONE
```

## Configuration contract

The production worker process root reads:

```text
WORKER_RUN_TERMINATION_BINDING
```

Accepted true values:

```text
1
true
yes
on
```

Accepted false values:

```text
0
false
no
off
```

Parsing is case-insensitive and trims surrounding whitespace.

A missing value resolves to `false`.

Any unknown or empty explicit value fails startup. The worker does not silently
interpret an unsupported value.

The internal configuration key is:

```text
run_termination_binding_enabled
```

It must be a boolean. Enabling it outside `production=True` is rejected.

## Disabled composition

The default production composition remains:

```text
DagLua
├── TaskClaimManager
├── TaskConsumer
│   └── completion_manager = DagLua
└── WorkerConsumer
    └── task_consumer = TaskConsumer
```

Therefore an existing deployment that does not set
`WORKER_RUN_TERMINATION_BINDING` preserves the historical production graph.

```yaml
task_consumer: TaskConsumer
worker_consumer: WorkerConsumer
completion_manager: DagLua
run_termination_coordinator: absent
```

## Enabled composition

When the strict flag is enabled, production composition becomes:

```text
DagLua
├── TaskClaimManager
├── RunTerminationCoordinator
│   └── task_completion_gateway = DagLua
├── RunFinalizingTaskConsumer
│   └── completion_manager = RunTerminationCoordinator
└── RunFinalizingWorkerConsumer
    └── task_consumer = RunFinalizingTaskConsumer
```

The coordinator delegates TASK completion to the existing canonical `DagLua`
gateway and invokes the existing `RUN_TERMINATE` operation only after committed
terminal TASK completion.

Sprint 83.4 does not modify:

- `run_termination.py`;
- `run_finalizing_runtime.py`;
- `run_terminate_from_tasks.lua`;
- TASK claim authority;
- TASK completion authority;
- RUN termination semantics.

## Startup ordering

When the binding is enabled, startup ordering is:

```text
DagLua.initialise()
→ RunTerminationCoordinator.initialise()
→ prepare consumer groups
→ confirm shard leases
→ start heartbeat
→ start worker consumption
→ start shard lease renewer
→ mark worker ready
```

If RUN termination script initialization fails:

```yaml
worker_ready: false
consumer_groups_prepared: false
work_consumption_started: false
heartbeat_started: false
shard_renewer_started: false
startup_exception_propagated: true
```

The worker fails closed before advertising readiness or accepting work.

When the binding is disabled, the RUN finalization coordinator is neither
constructed nor initialized.

## Verified real-Redis behavior

The Sprint 83.4 production integration path constructs the real production
`WorkerService` with the binding enabled and verifies:

1. the worker starts successfully;
2. canonical TASK admission succeeds;
3. worker reservation succeeds;
4. canonical TASK dispatch commits;
5. the production worker claims and executes the TASK;
6. TASK state becomes `done`;
7. TASK output remains readable;
8. RUN state becomes `done`;
9. RUN metadata records `RUN_TERMINATE`;
10. RUN result records terminal aggregate evidence;
11. one `RunCompleted` event is emitted;
12. the running projection is cleared;
13. the stream pending count becomes zero;
14. the executor is invoked exactly once.

The acceptance report is written to:

```text
local_out/sprint83/sprint83_4_production_run_finalization_binding.json
```

The report is deterministic for a fixed acceptance identity.

## Verified compatibility

The verification suite covers:

- strict environment parsing;
- missing flag defaults to disabled;
- invalid explicit values fail startup;
- non-boolean internal configuration rejection;
- non-production enablement rejection;
- exact disabled composition preservation;
- enabled composition selection;
- startup initialization order;
- initialization failure before work intake;
- real-Redis production WorkerService execution;
- Sprint 83.2 RUN termination behavior;
- terminal duplicate recovery;
- terminal duplicate conflict ACK blocking;
- deterministic one-command acceptance;
- existing production composition;
- process-root ownership;
- worker lifecycle;
- startup failure projection;
- ACK authority.

## Known limitation: terminal heartbeat race

A TASK may complete before its heartbeat loop performs a subsequent renewal.
That renewal can observe the already-terminal TASK state and return
`illegal_transition`. The heartbeat loop stops fail-closed.

This behavior is visible in existing Sprint 83.2 acceptance as well as the
Sprint 83.4 production composition acceptance. Sprint 83.4 does not change the
heartbeat protocol or claim fencing contract.

It is not treated as proof of production readiness.

## Explicit exclusions

Sprint 83.4 does not add or authorize:

- RUN cancellation;
- TASK cancellation;
- user-facing retry or requeue;
- automatic reconciliation;
- automatic repair;
- lifecycle authority migration;
- external executor cutover;
- default enablement;
- production deployment;
- product-alpha readiness;
- Loop Plane integration.

## Honest exit state

```yaml
status: PASS_WITH_LIMITATIONS
production_worker_composition_supported: true
run_termination_binding_supported: true
run_termination_binding_default_enabled: false
opt_in_end_to_end_run_terminalization: true
terminal_duplicate_recovery_supported: true
idempotent_terminal_event_supported: true
product_alpha_ready: false
production_ready: false
cancel_command_supported: false
retry_command_supported: false
automatic_repair_authorized: false
production_cutover_authorized: false
```

# Sprint 84.1 — Canonical TASK_DISPATCH Binding

## Status

```yaml
sprint: 84.1
name: Canonical TASK_DISPATCH Binding
baseline: 660e7dbc32372ec06640009490c00519599aa3c9
product_status: IMPLEMENTATION_CANDIDATE
production_ready: false
production_cutover_authorized: false
```

## Purpose

Bind the existing production `TASK_DISPATCH` writer to the canonical
TASK authority store without replacing the existing atomic Redis/Lua
projection.

```text
canonical TASK_ADMIT
→ TASK authority state ready / revision 1
→ scheduler worker reservation
→ canonical TASK_DISPATCH commit
→ existing task_dispatch_commit.lua projection
→ TASK legacy state scheduled
→ TaskScheduled and TaskRequested messages
```

## Feature flags

```yaml
HFA_CANONICAL_TASK_ADMIT_BINDING:
  default: false

HFA_CANONICAL_TASK_DISPATCH_BINDING:
  default: false
  requires:
    - HFA_CANONICAL_TASK_ADMIT_BINDING
```

Enabling canonical dispatch without canonical task admission fails
startup. No silent legacy backfill is permitted.

## Canonical operation

```yaml
operation_type: TASK_DISPATCH
aggregate_type: TASK
previous_state: ready
next_state: scheduled
consumes_revision: true
receipt_required: true
writer_id: hfa-control/task-dispatch-writer:v1
fence_required: true
```

The scheduler epoch remains trusted-adapter fence evidence under the
existing trusted-process authority model.

## Operation identity

```text
task-dispatch:v1:<task-aggregate-sha256>:attempt:<attempt>
```

The logical attempt is derived from durable task metadata:

```text
attempt = requeue_count + 1
```

User payload cannot choose `attempt` or `scheduled_at`.

The operation ID binds TASK identity and logical dispatch attempt.
The canonical command hash additionally binds:

- RUN and tenant identity;
- selected worker and worker group;
- scheduler epoch;
- attempt and stable scheduled timestamp;
- shard, priority and agent type;
- payload and trace context;
- ready, scheduled and stream projection targets.

Therefore:

```yaml
same_attempt_same_exact_command: ALREADY_APPLIED
same_attempt_changed_worker: IDEMPOTENCY_CONFLICT
same_attempt_changed_epoch: IDEMPOTENCY_CONFLICT
new_attempt: NEW_OPERATION_ID
```

## Commit and projection boundary

The existing `task_dispatch_commit.lua` remains the sole legacy
projection writer.

```text
canonical commit
→ legacy projection
```

The projection atomically writes:

- TASK state `scheduled`;
- TASK dispatch metadata;
- scheduled zset membership;
- `TaskScheduled`;
- `TaskRequested`;
- canonical transition proof fields.

Proof fields stored in task metadata:

```yaml
canonical_transition_id: required_when_binding_enabled
canonical_record_hash: required_when_binding_enabled
canonical_command_hash: required_when_binding_enabled
canonical_revision: required_when_binding_enabled
canonical_operation_id: required_when_binding_enabled
dispatch_attempt: required
dispatch_worker_id: required
```

Because proof fields and all legacy projection effects are written in
one Lua transaction, an exact proof match permits an
`already_projected` no-op without stream re-emission.

## Failure semantics

### Canonical commit rejected or unavailable

```yaml
canonical_mutation: 0
legacy_projection_mutation: 0
stream_writes: 0
worker_reservation_release: true
```

### Canonical commit durable, projection unavailable

```yaml
canonical_commit_durable: true
legacy_projection_completed: false
result: canonical_projection_pending
worker_reservation_release: false
automatic_reassignment: false
automatic_repair: false
```

A durable canonical dispatch command names one worker and scheduler
epoch. Releasing the reservation and selecting another worker would
contradict the durable command.

### Exact retry after projection

```yaml
result: already_projected
stream_reemission: false
event_hook_reemission: false
fairness_accounting_repeated: false
dispatch_success_hook_repeated: false
```

### Legacy state regressed to ready after projection

```yaml
result: canonical_projection_regressed
retry_safe: false
automatic_redispatch: false
next_path: explicit_reconciliation
```

## Requeue boundary

Canonical `TASK_REQUEUE` is not implemented in this sprint.

A new attempt after a legacy-only requeue sees canonical TASK state
`scheduled`, not `ready`, and fails closed as an illegal canonical
transition. Sprint 84.1 does not silently advance canonical state or
perform automatic repair.

## Exact source scope

Exactly 18 files:

```text
.github/workflows/sprint84-1-canonical-task-dispatch-binding.yml
docs/implementation/sprint84/SPRINT84.1-canonical-task-dispatch-binding.md
hfa-control/src/hfa_control/dag_lua.py
hfa-control/src/hfa_control/dag_scheduler_bridge.py
hfa-control/src/hfa_control/dag_scheduler_dispatch_controller.py
hfa-control/src/hfa_control/scheduler.py
hfa-control/src/hfa_control/scheduler_reservation_dispatch.py
hfa-control/src/hfa_control/task_dispatch_authority.py
hfa-core/src/hfa/dag/schema.py
hfa-core/src/hfa/lua/task_dispatch_commit.lua
scripts/runtime_alpha_acceptance_84_1.py
tests/integration/test_task_dispatch_authority_binding_integration.py
tests/integration/test_dag_task_dispatch_integration.py
tests/integration/test_scheduler_dag_dispatch_writer_integration.py
tests/integration/test_scheduler_reservation_dispatch_integration.py
tests/integration/test_task_dispatch_identity_contract_integration.py
tests/unit/test_task_dispatch_authority_binding.py
tests/unit/test_scheduler_runtime_truth_guard.py
```

`local_out/` remains untracked deterministic evidence.

## Baseline fixture alignment

The exact baseline snapshot was executed against the same Redis regression selection before these fixture changes. It produced the same five failing test identities as the feature branch. The failures were pre-existing fixture debt:

- three dispatch fixtures admitted TASK state without creating required RUN runtime truth;
- one reservation/claim fixture created TASK state without the required task identity metadata and RUN truth.

Sprint 84.1 aligns only those fixtures with contracts that already existed on the baseline. It does not weaken RUN truth, TASK identity, reservation, claim, or authority checks.

## Explicit exclusions

```yaml
canonical_RUN_CREATE_binding: false
canonical_TASK_CLAIM_binding: false
canonical_TASK_COMPLETE_binding: false
canonical_TASK_FAIL_binding: false
canonical_TASK_REQUEUE_binding: false
automatic_reassignment: false
automatic_repair: false
reconciliation_worker: false
Loop_Plane_dependency: false
production_ready: false
production_cutover_authorized: false
```

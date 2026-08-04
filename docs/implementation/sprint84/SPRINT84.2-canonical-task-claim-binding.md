# Sprint 84.2 — Canonical TASK_CLAIM Binding

## Current tranche

```yaml
sprint: 84.2
base: 1ec723fd74a0aeca77fdfeb12be9b30a59994fee
branch: sprint/84-2-canonical-task-claim-binding
status: COMMAND_CONTRACT_IMPLEMENTED
runtime_binding_enabled: false
production_effect: NONE
production_ready: false
production_cutover_authorized: false
```

## Accepted transition

```yaml
operation: TASK_CLAIM
aggregate: TASK
previous_state: scheduled
next_state: running
revision_consumed: true
operation_receipt_required: true
projection_intent:
  - RUNNING_SET
writer_id: hfa-worker/task-claim-writer:v1
```

## Operation identity

```text
task-claim:v1:<task-aggregate-sha256>:attempt:<dispatch-attempt>
```

The operation ID excludes worker identity and observation time.

Worker identity, scheduler epoch, claim time, dispatch proof and claim
epoch are canonical-command-hash members. Reusing the same operation
ID with changed values is an idempotency conflict.

## Claim epoch

The authority command binds both:

```yaml
previous_claim_epoch: REQUIRED
claim_epoch: previous_claim_epoch + 1
```

The legacy projection must eventually set the accepted claim epoch
exactly. It must not generate a second epoch when replaying an already
committed canonical operation.

## Duplicate execution boundary

```yaml
first_projection:
  status: task_claimed
  task_execution_allowed: true

exact_duplicate_after_projection:
  status: canonical_claim_already_projected
  task_execution_allowed: false
  duplicate_claim_event: false
  second_claim_epoch_increment: false
```

A retry after the canonical commit but before the legacy projection may
complete the missing projection once and then allow execution.

A retry after the projection has already committed must not execute the
task again.

## Required runtime dependency

The future runtime binding must require:

```yaml
HFA_CANONICAL_TASK_ADMIT_BINDING: true
HFA_CANONICAL_TASK_DISPATCH_BINDING: true
HFA_CANONICAL_TASK_CLAIM_BINDING: true
```

The claim path must validate the exact canonical dispatch head before
committing TASK_CLAIM.

## Deferred from this tranche

- runtime feature-flag wiring;
- canonical authority persistence invocation;
- Lua projection proof and replay handling;
- reservation prevalidation;
- production WorkerService composition;
- acceptance and real-Redis integration;
- TASK_HEARTBEAT;
- TASK_COMPLETE / TASK_FAIL;
- TASK_REQUEUE;
- automatic repair;
- production cutover.

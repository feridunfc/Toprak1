# Sprint 84.3 — Canonical TASK_CLAIM Manager Binding

```yaml
sprint: 84.3
base: fd9883bb4ae153b3acb57cfbc7ce0f6231ea96ba
feature_flag: HFA_CANONICAL_TASK_CLAIM_BINDING
default_enabled: false
requires:
  HFA_CANONICAL_TASK_ADMIT_BINDING: true
  HFA_CANONICAL_TASK_DISPATCH_BINDING: true

ordering:
  - canonical evidence validation
  - reservation validation
  - canonical TASK_CLAIM commit
  - replay-safe Lua projection
  - execution authorization
  - event emission

worker_composition_files_changed: false
task_consumer_changed: false
lua_changed: false
production_cutover_authorized: false
automatic_repair: false
automatic_reassignment: false
run_create_binding: unresolved
```

`TaskClaimManager.claim_start()` retains its legacy behavior while the feature
flag is disabled. The canonical path is enabled only by explicit dependency
injection in this tranche; worker composition and production defaults are not
changed.

## Durable commit versus projection state

`canonical_projection_pending` means the canonical `TASK_CLAIM` transition is
durable, but the Redis/Lua running-state projection did not complete. Execution
is not authorized, the worker reservation is not released or rewritten, and no
automatic repair or reassignment is attempted. An exact retry reuses the stored
claim timestamp, dispatch proof, claim epoch and canonical transition proof,
then retries only the existing Lua projection.

`canonical_claim_already_projected` means both the canonical transition and the
claim projection already exist. The call is an idempotent replay: it does not
create another canonical transition, increment the claim epoch, authorize a
second execution, or emit another claim event.

## Authority and evidence boundaries

The caller supplies only task, run, tenant, worker, scheduler epoch and claim
time. Dispatch transition IDs, hashes, revision, operation identity and attempt
are derived from immutable canonical `TASK_DISPATCH` evidence. The legacy task
metadata, scheduled state, worker reservation and task owner index are treated
as projection/precondition evidence and must agree with canonical dispatch
truth before the claim commit.

Exact claim receipt replay remains valid after the aggregate head advances past
`running`, provided the immutable claim record and receipt remain valid. This
allows terminal duplicate proof without reopening execution.

No heartbeat, completion, failure, requeue, ACK, pending-message, worker
execution, dispatch, `RUN_CREATE`, reconciliation or production-cutover behavior
is changed.

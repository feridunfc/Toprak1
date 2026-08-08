# Sprint 84.5 — Canonical RUN_TERMINATE Authority Binding

```yaml
sprint: 84.5
base: cb2555925d2cc1ef4b87c56a2cd6edb8af8a50d5

canonical_RUN_TERMINATE:
  authority: canonical
  operation: RUN_TERMINATE
  manager_binding: implemented_pending_mandatory_validation
  runtime_composition: opt_in_dependency_injection_through_RunTerminationCoordinator
  default_enabled: false

terminal_outcomes:
  success: done
  failure: failed

projection:
  existing_run_termination_projection_reused: false
  legacy_run_terminate_from_tasks_lua_changed: false
  canonical_projection_preserves_existing_public_RUN_result_state_event_contract: true
  replay_safe: implementation_complete_validation_pending
  canonical_transition_proof_bound: true

real_redis:
  status: not_run
  note: mandatory before Sprint 84.5 may be reported PASS

resource_settlement:
  implemented: false
  owner: unresolved_for_future_sprint_84_6

worker_task_claim_cutover:
  changed: false

automatic_repair: false
production_ready: false
production_cutover_authorized: false
```

## Authority discovery

The exact parent already registers `OperationType.RUN_TERMINATE` for the `RUN`
aggregate. It is revision-consuming, receipt-bearing, and requires the
`RUN_RESULT_PROJECTION` projection intent. The accepted terminal outcomes are
`done` and `failed`; the accepted previous canonical states are `pending` and
`running`.

The legacy runtime owner is `RunTerminationCoordinator` plus
`run_terminate_from_tasks.lua`. That Lua script atomically derives terminal RUN
truth from the `RUN -> TASK` set and writes the legacy RUN state, metadata,
result, `cp_running` projection, and `RunCompleted`/`RunFailed` event. Sprint
84.5 leaves that file byte-for-byte unchanged for the legacy path.

## Immutable terminal aggregate proof

Canonical RUN termination does not hash mutable TASK state in Python. The new
`run_terminate_terminal_proof.lua` executes the TASK aggregate read and proof
capture atomically in Redis. The proof identity freezes:

```text
schema_version
run_id
tenant_id
sorted(task_id + terminal_state)
task_count
done_count
failed_count
skipped_count
final_state
proof_sha256
```

The SHA-256 is computed over a deterministic proof payload inside the same Lua
execution that captures the aggregate. The following stable operational
metadata is persisted with the proof but excluded from proof identity:

```text
finalized_at_ms
worker_instance_id
trigger_task_id
trigger_terminal_state
canonical_expected_revision
canonical_previous_state
```

A nonterminal sibling returns `not_ready` and creates no proof. An exact retry
of the same terminal aggregate returns the existing immutable proof. If a proof
exists but the live aggregate differs before canonical durability, the operation
fails closed with `terminal_proof_changed_after_capture`.

A legacy terminal RUN footprint without a matching durable canonical
RUN_TERMINATE receipt is never silently backfilled.

## Canonical command identity

The operation identity is deterministic from only:

```text
RUN_TERMINATE
run_id
terminal_proof_sha256
```

Timestamp, worker identity, trigger task, and "last TASK" do not participate in
operation identity. The canonical command carries the immutable terminal proof
and requests `RUN_RESULT_PROJECTION`.

## Commit and crash ordering

The canonical path is:

```text
atomic terminal aggregate proof capture
→ deterministic AuthorityCommand
→ RedisCanonicalAuthorityStore authority evaluation
→ durable canonical RUN_TERMINATE commit
→ proof-bound legacy RUN projection
→ ACK
```

The projection does not read TASK state. If the canonical commit is durable but
the process exits before projection, retry resolves the same canonical receipt
and replays projection from the persisted immutable proof. Mutable TASK state is
not reinterpreted after canonical durability.

Ambiguous authority writes are resolved only by rereading the exact canonical
operation receipt. There is no legacy-authority fallback.

## Projection

The canonical projection uses the separate
`run_terminate_projection.lua`. It preserves the existing public terminal RUN
contract:

```text
RUN state: done | failed
RUN metadata finalization_operation: RUN_TERMINATE
RUN result finalization_source: terminal_task_aggregate
RunCompleted | RunFailed
cp_running member removed
```

It additionally binds projection evidence to:

```text
terminal_proof_sha256
canonical_transition_id
canonical_record_hash
canonical_revision
```

The projection transaction writes state, metadata, result, terminal event, and
its projection receipt atomically. Exact retry validates the full projected
footprint and returns without emitting a second terminal event. Conflicting or
corrupt footprints fail closed.

## Compatibility and scope

`RunTerminationCoordinator` uses the canonical path only when a
`RunTerminateAuthorityBinding` is explicitly injected. Without that dependency,
the exact Sprint 83.2 legacy finalization path remains active and
`run_terminate_from_tasks.lua` remains unchanged.

This sprint does **not** implement operation-scoped resource settlement,
TASK_CLAIM worker cutover, TASK_REQUEUE, automatic repair, reconciliation, or a
production cutover. No resource counter is decremented by RUN_TERMINATE in this
sprint.

## Mandatory validation before PASS

Sprint 84.5 must not be reported PASS until the supplied validation script is
run in the user's exact checkout with real Redis and returns zero mandatory
skips. Required validation includes focused unit/import/package tests, the new
real-Redis authority/projection suite, Sprint 83.2 and Sprint 83.4 RUN
finalization regressions, Sprint 84.4 RUN_CREATE and canonical authority
regressions, the repository authority audit with `banned = 0`, `compileall`,
`git diff --check`, and an exact changed-file audit.

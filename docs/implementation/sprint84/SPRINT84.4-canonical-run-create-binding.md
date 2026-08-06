# Sprint 84.4 — Canonical RUN_CREATE Runtime Binding

```yaml
sprint: 84.4
base: f2e7fbcce3b2e952933080e4c448e37b5ced2be3

canonical_RUN_CREATE:
  authority: canonical
  manager_binding: complete
  runtime_composition: available_flagged
  feature_flag: HFA_CANONICAL_RUN_CREATE_BINDING
  default_enabled: false
  requires_canonical_TASK_ADMIT: true

resource_reservation:
  operation_scoped: true
  canonical_commit_ordered: true
  protected_resources:
    - concurrent_runs
    - budget_reserved_cents
    - tenant_inflight
  terminal_release_owner: unresolved

admitted_projection:
  replay_safe_design: true
  mechanism: atomic_Redis_Lua_receipt_state_XADD
  supported_topology: standalone_non_cluster_Redis
  exactly_once_event_real_redis_proof: pending

submission_recovery:
  automatic: false

artifact_verification:
  focused_unit_core_packaging: passed
  real_redis: pending
  authority_gate_complete: pending

production_ready: false
production_cutover_authorized: false
```

## Runtime order

The enabled binding executes:

```text
request validation
→ non-refundable tenant rate gate
→ operation-scoped resource reserve_once
→ canonical RUN_CREATE commit
→ resource finalize_once
→ replay-safe admitted projection
```

The operation identity is stable and shared across the resource receipt,
canonical authority receipt and admitted projection receipt:

```text
run-create:v1:<canonical RUN aggregate identity SHA-256>
```

The reservation receipt's persistent `created_at_ms` is the sole canonical
commit timestamp. Exact retries never mint a new commit timestamp.

## Authority and release rules

The canonical RUN aggregate transitions from no aggregate at revision zero to
`pending` at revision one. The legacy `admitted` RUN state and
`RunAdmittedEvent` are projections, not authority.

Automatic resource release is permitted only when the current invocation made
the first resource mutation and canonical absence is positively proven. A
canonical commit exception is treated as ambiguous until the aggregate snapshot
and exact operation receipt are re-read.

After canonical durability, no finalization or projection failure releases the
resource receipt. A durable canonical RUN with a RESERVED resource receipt is an
interrupted operation that exact retry may finalize.

## Projection contract

`run_create_projection.lua` validates all participating Redis key types before
its first write. On first exact application it atomically:

1. appends one stable `RunAdmittedEvent` to the configured control stream;
2. writes the legacy RUN state with the established RUN state TTL;
3. persists an immutable projection receipt containing canonical and resource
   proof plus the original stream entry ID.

An exact duplicate returns the stored stream entry ID and performs no `SET` or
`XADD`. Changed proof, corrupt receipt, a pre-existing legacy RUN state, or a
pre-existing admitted event without the exact receipt fails closed. The script
uses three Redis keys and is therefore scoped to the repository's established
standalone Redis topology; Redis Cluster cross-slot execution is not claimed.

## Feature-disabled compatibility

When `HFA_CANONICAL_RUN_CREATE_BINDING` is false, `AdmissionController` invokes
the unchanged legacy admission method and neither the canonical authority store
nor the operation-scoped resource manager is initialized or touched.

## Limit sources

```yaml
concurrent_run_limit:
  authoritative_source: unavailable_in_exact_parent
  accounting_active: true
  enforcement: unavailable
budget_limit:
  authoritative_source: unavailable_in_exact_parent
  accounting_active: true
  enforcement: unavailable
tenant_inflight_limit:
  authoritative_source: TenantRegistry.get_config(tenant_id).max_inflight_runs
  enforcement: atomic_in_resource_reservation_lua
tenant_rate_limit:
  authoritative_source: TenantRegistry.get_config(tenant_id).max_runs_per_second
  accounting: non_refundable_request_attempt
```

Missing concurrent-run and budget limit providers are represented as `None`.
No environment defaults or zero limits are invented.

## Explicit non-claims

This sprint does not provide terminal resource release ownership, submission
idempotency takeover, automatic recovery, worker TASK_CLAIM cutover, production
readiness, or production cutover authorization.

## Verification status of this artifact

The implementation and focused unit/core/wheel contracts pass in the isolated
exact-parent checkout. The producing sandbox does not provide `redis-server`,
`redis-py`, or `fakeredis`; therefore the mandatory real-Redis crash-point suite
and the complete Authority Gate remain pending. This document deliberately does
not claim that the exactly-once event proof has passed until those tests execute
on the user validation environment.

# Sprint 83.9 — Idempotency Reservation Diagnostics

## Status

```yaml
sprint: 83.9
name: Idempotency Reservation Diagnostics
baseline: b0f66c03b5f64977b6c4cf3b4f8d32f306505075
product_status: PASS_WITH_LIMITATIONS
production_ready: false
production_cutover_authorized: false
```

## Problem

Sprint 83.8 intentionally keeps an existing `IN_PROGRESS` idempotency
reservation fail-closed. A control process may disappear after reservation but
before finalization, leaving the caller with a 409 response until the
idempotency record expires.

Blind owner takeover is not safe. The crash may have occurred before RUN
admission, after RUN admission, after TASK admission, or after both lifecycle
writes but before idempotency finalization. Re-entering the lifecycle path
without authoritative reconciliation could duplicate work, collide with the
existing TASK identity, or introduce an implicit repair writer.

## Product claim

For a same-tenant, same-key, same-request `IN_PROGRESS` replay, the tenant API
returns safe reservation diagnostics while preserving the existing fail-closed
zero-lifecycle-write behavior.

```yaml
http_status: 409
failure_code: IDEMPOTENCY_IN_PROGRESS
reservation_created_at_ms: exposed
reservation_updated_at_ms: exposed
reservation_ttl_seconds: exposed
owner_token: not_exposed
recovery_safe: false
run_admission_calls: 0
task_admit_calls: 0
```

## API additions

`POST /control/v1/runs` responses add:

```yaml
idempotency_reservation_created_at_ms: integer_or_null
idempotency_reservation_updated_at_ms: integer_or_null
idempotency_reservation_ttl_seconds: integer_or_null
idempotency_recovery_safe: boolean
```

The timestamp and TTL fields are populated only for a valid
`IDEMPOTENCY_IN_PROGRESS` response. Other responses retain null diagnostics and
`idempotency_recovery_safe=false`.

## Evidence validation

An `IN_PROGRESS` record is accepted as diagnostic evidence only when:

- existing schema, tenant, fingerprint, RUN, TASK, state, and owner fields
  remain valid;
- `created_at_ms` is a non-negative integer;
- `updated_at_ms` is not earlier than `created_at_ms`;
- the Redis key has a positive remaining TTL.

Missing, malformed, contradictory, wrong-type, or non-expiring evidence fails
closed as `IDEMPOTENCY_STORE_FAILED`.

## Architecture boundary

```yaml
idempotency_store_role: SUBMISSION_GATE
lifecycle_authority: UNCHANGED
new_RUN_writer: false
new_TASK_writer: false
stale_owner_takeover: false
automatic_release: false
automatic_retry: false
automatic_repair: false
Loop_Plane_dependency: false
owner_token_exposed: false
```

## Exact source scope

Exactly 9 files:

```text
.github/workflows/sprint83-9-idempotency-reservation-diagnostics.yml
docs/implementation/sprint83/SPRINT83.9-idempotency-reservation-diagnostics.md
hfa-core/src/hfa/lua/submission_idempotency.lua
hfa-control/src/hfa_control/api/models.py
hfa-control/src/hfa_control/run_submission.py
hfa-control/src/hfa_control/submission_idempotency.py
scripts/runtime_alpha_acceptance_83_9.py
tests/integration/test_product_alpha_idempotency_diagnostics_e2e_83_9.py
tests/unit/test_submission_idempotency.py
```

`local_out/` remains deterministic local/CI evidence and is excluded from
source control.

## Acceptance

The real-Redis acceptance must prove:

- 409 `IDEMPOTENCY_IN_PROGRESS`;
- same stored RUN and TASK identities;
- created and updated timestamps exposed;
- positive remaining TTL bounded by configured retention;
- owner token absent from the HTTP response;
- zero RUN admission and TASK admission calls;
- zero keyspace mutation during replay;
- corrupt diagnostics fail closed with HTTP 503;
- no takeover, release, retry, repair, or new lifecycle writer.

## Explicit exclusions

```yaml
direct_public_tenant_authentication: false
stale_owner_takeover: false
automatic_release: false
automatic_retry_or_requeue: false
automatic_repair: false
multi_task_support: false
cancel_supported: false
external_executor_cutover: false
durable_archive: false
production_ready: false
production_cutover_authorized: false
```

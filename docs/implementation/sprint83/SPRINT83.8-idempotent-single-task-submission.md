# Sprint 83.8 — Trusted-Gateway Idempotent Single-Task Submission

## Status

```yaml
sprint: 83.8
implementation_status: IMPLEMENTED_LOCALLY
acceptance_status: PASS_WITH_LIMITATIONS
merge_authorized: false
production_ready: false
production_cutover_authorized: false
```

## Product claim

Within the existing trusted-gateway tenant-identity boundary, repeated submission
of the same canonical single-task request with the same tenant-scoped
`Idempotency-Key` creates at most one canonical RUN/TASK pair.

After the first submission is finalized, the same tenant, key, and canonical
request replay the stored submission response with the same `run_id` and
`task_id` and perform zero additional lifecycle writes.

Reuse of the same tenant-scoped key for a different canonical request fails
closed.

## HTTP contract

### Required headers

```text
X-Tenant-ID: <trusted gateway tenant>
Idempotency-Key: <1..128 visible non-whitespace characters>
```

The idempotency key:

- is mandatory for `POST /control/v1/runs`;
- rejects surrounding whitespace;
- rejects control characters;
- is never embedded raw in a Redis key;
- is hashed before Redis namespacing.

### Response behavior

```yaml
first_submission:
  http_status: 202
  status: ACCEPTED
  idempotent_replay: false

final_same_request_replay:
  http_status: 200
  status: ACCEPTED
  idempotent_replay: true
  same_run_id: true
  same_task_id: true
  additional_lifecycle_writes: 0

same_key_different_request:
  http_status: 409
  failure_code: IDEMPOTENCY_KEY_REUSED
  lifecycle_writes: 0

same_key_submission_in_progress:
  http_status: 409
  failure_code: IDEMPOTENCY_IN_PROGRESS
  lifecycle_writes: 0

missing_or_invalid_key:
  http_status: 400
  lifecycle_writes: 0

invalid_or_corrupt_store_evidence:
  http_status: 503
  failure_code: IDEMPOTENCY_STORE_FAILED
  lifecycle_writes: 0
```

## Canonical request fingerprint

The request fingerprint is SHA-256 over compact, sorted canonical JSON containing:

```text
run_shape
payload
agent_type
priority
estimated_cost_cents
preferred_region
preferred_placement
required_capabilities
```

`trace_parent` and `trace_state` are intentionally excluded so a transport retry
with a different tracing context remains the same product request.

Tenant identity is excluded from the fingerprint because it is already part of
the Redis namespace.

## Redis contract

Key shape:

```text
hfa:tenant:{tenant_id}:submission:idempotency:{sha256(idempotency_key)}
```

Stored fields:

```yaml
schema_version: "1"
tenant_id: "<tenant>"
request_fingerprint: "<sha256>"
run_id: "<canonical run id>"
task_id: "<canonical task id>"
state: IN_PROGRESS | FINAL
owner_token: "<reservation owner>"
result_json: "<final submission response>"
created_at_ms: "<integer>"
updated_at_ms: "<integer>"
```

Retention is aligned with the existing canonical RUN result contract:

```yaml
submission_idempotency_ttl_seconds: 86400
run_result_ttl_seconds: 86400
```

## Atomicity and ordering

```text
normalize request
→ validate Idempotency-Key
→ compute canonical request fingerprint
→ allocate candidate RUN/TASK identities
→ atomically reserve tenant-scoped idempotency record
   ├─ RESERVED: owner may continue
   ├─ FINAL_SAME_REQUEST: replay stored result
   ├─ IN_PROGRESS_SAME_REQUEST: fail closed
   ├─ DIFFERENT_REQUEST: reject key reuse
   └─ INVALID_EVIDENCE: fail closed
→ existing RUN admission authority
→ existing canonical TASK_ADMIT authority
→ atomically finalize idempotency result
```

The gate executes before `AdmissionController.admit()`. This prevents a replay
from re-entering quota, budget, rate-limit, tenant-inflight, RUN admission, or
TASK admission side effects.

The Lua script mutates only the idempotency hash and its TTL. It does not write
RUN state, RUN metadata, RUN result, TASK state, queues, streams, claims,
reservations, heartbeats, or terminal events.

## Failure behavior

A failure before RUN admission may release the reservation only when the owner
token still matches.

After RUN admission commits, every result, including
`SUBMISSION_INCOMPLETE`, is finalized and replayable. The key is not released,
because a retry must not create another RUN.

If finalization cannot be proven, the public result fails closed with
`IDEMPOTENCY_STORE_FAILED`; it does not claim a safely replayable accepted
submission.

`FINAL` replay validates that stored result JSON agrees with the idempotency
record's tenant, RUN, and TASK identity. Contradictory evidence fails closed.

An `IN_PROGRESS` record remains fail-closed until expiration. Sprint 83.8 does
not implement stale-owner takeover or automatic repair.

## Acceptance evidence

Observed locally on 2026-08-01:

```yaml
dedicated_unit_contracts: 74_passed
sprint_83_8_real_redis_e2e: 2_passed
combined_83_7_and_83_8_real_redis_e2e: 4_passed
complete_regression_set: 196_passed
compileall: PASS
git_diff_check: PASS
deterministic_acceptance: PASS_WITH_LIMITATIONS
```

The deterministic acceptance proves:

```yaml
same_key_same_request_replay: true
same_key_different_request_conflict: true
concurrent_in_progress_fail_closed: true
composition_restart_durability: true
corrupt_evidence_fail_closed: true
missing_key_fail_closed: true
invalid_key_fail_closed: true
tenant_scoped_namespace: true
raw_idempotency_key_persisted: false
zero_replay_lifecycle_writes: true
retention_seconds: 86400
```

## Exact source scope

Exactly 16 files:

```text
.github/workflows/sprint83-7-trusted-gateway-single-task-alpha.yml
.github/workflows/sprint83-8-idempotent-single-task-submission.yml
docs/implementation/sprint83/SPRINT83.8-idempotent-single-task-submission.md
hfa-core/src/hfa/config/keys.py
hfa-core/src/hfa/lua/submission_idempotency.lua
hfa-control/src/hfa_control/api/models.py
hfa-control/src/hfa_control/api/router.py
hfa-control/src/hfa_control/run_submission.py
hfa-control/src/hfa_control/service.py
hfa-control/src/hfa_control/submission_idempotency.py
scripts/runtime_alpha_acceptance_83_7.py
scripts/runtime_alpha_acceptance_83_8.py
tests/integration/test_product_alpha_closure_e2e_83_7.py
tests/integration/test_product_alpha_idempotent_submission_e2e_83_8.py
tests/unit/test_product_alpha_readiness.py
tests/unit/test_submission_idempotency.py
```

`local_out/` is deterministic local/CI evidence and is excluded from source
control.

## Explicit exclusions

```yaml
direct_public_tenant_authentication: false
multi_task_support: false
cancel_supported: false
retry_or_requeue_supported: false
stale_owner_takeover: false
automatic_repair: false
external_executor_cutover: false
durable_archive: false
new_lifecycle_writer: false
production_ready: false
production_cutover_authorized: false
```

## Honest interpretation

Sprint 83.8 closes duplicate RUN creation caused by trusted-gateway retries for
the canonical single-task submission surface.

It does not make the system production-ready. In particular, it does not
provide direct public authentication, stale reservation recovery, cancellation,
retry/requeue, multi-task aggregation, external executor cutover, durable
archive, automatic repair, or production cutover.

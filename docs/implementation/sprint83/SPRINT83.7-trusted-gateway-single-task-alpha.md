# Sprint 83.7 — Trusted-Gateway Single-Task Product Alpha

## Status

```yaml
status: IMPLEMENTED_WITH_LIMITATIONS
target_claim: TRUSTED_GATEWAY_SINGLE_TASK_PRODUCT_ALPHA_READY
local_runtime_acceptance: PASS_WITH_LIMITATIONS
local_dedicated_redis_e2e: 2_PASSED
local_combined_regression: 174_PASSED
ci_gate: REQUIRED
production_ready: false
```

## Honest product claim

Within an explicitly trusted tenant-identity boundary, a tenant can submit,
observe, and retrieve the successful or failed result of one canonical
single-task RUN through the production composition.

Incompatible alpha configuration fails closed. A committed terminal result
remains readable across replacement control-plane and worker composition
instances while the declared Redis retention evidence survives.

This claim does **not** authorize direct public multi-tenant use or production
cutover.

## Product boundary

```yaml
product_mode: SINGLE_TASK_ALPHA
tenant_identity_boundary: TRUSTED_GATEWAY_HEADER

direct_public_tenant_authentication: false
submission_idempotency: false
multi_task_support: false
cancel_supported: false
retry_supported: false
external_executor_cutover: false
automatic_repair: false
archive_available: false
production_ready: false
```

The `X-Tenant-ID` value is trusted only because an upstream gateway is assumed
to authenticate the caller and authoritatively set the header. The application
does not provide a direct public tenant-authentication boundary in this sprint.

## Fail-closed startup profiles

### Control-plane alpha profile

`SINGLE_TASK_ALPHA` requires:

```yaml
strict_cas_mode: true
canonical_task_admit_binding: true
single_task_submission_surface: true
tenant_identity_boundary: TRUSTED_GATEWAY_HEADER
result_retention_seconds: 86400
```

Unknown product modes, unknown tenant boundaries, or incompatible alpha
configuration stop construction before the service can claim product
readiness.

Control-plane startup does not require a local worker composition. Product
readiness remains false until a compatible live worker is observed.

### Worker alpha profile

`SINGLE_TASK_ALPHA` requires:

```yaml
production: true
worker_id: nonempty
worker_group: nonempty
executor_configured: true
run_termination_binding_enabled: true
```

Internal/default mode remains backward-compatible, including globally disabled
RUN finalization unless explicitly configured.

## Derived worker capabilities

Product capabilities are derived from the actual worker composition rather
than trusted from arbitrary configuration.

Reserved capability prefixes cannot be spoofed through the configured
capability list:

```text
product:
run-finalization:
executor:
```

The alpha worker publishes:

```yaml
product:single-task-v1
run-finalization:v1
executor:configured | executor:deterministic | valid executor marker
```

The same derived, sorted, unique capability list is shared with the canonical
task consumer and worker heartbeat.

## Product readiness and capability APIs

The existing infrastructure readiness endpoint remains unchanged.

Sprint 83.7 adds:

```text
GET /control/v1/product/readiness
GET /control/v1/product/capabilities
```

Product readiness is true only when all of the following are true on the
serving control-plane instance:

```yaml
alpha_profile_valid: true
redis_reachable: true
leader_available: true
local_instance_is_leader: true
scheduler_running: true
compatible_worker_count: greater_than_zero
run_finalization_available: true
executor_available: true
```

Readiness response fields:

```yaml
product_mode
ready
tenant_identity_boundary
redis_reachable
leader_available
scheduler_running
compatible_worker_count
run_finalization_available
executor_available
result_retention_seconds
```

Capability response:

```json
{
  "supported_run_shapes": ["SINGLE_TASK"],
  "multi_task_result_supported": false,
  "cancel_supported": false,
  "retry_supported": false,
  "submission_idempotency_supported": false,
  "external_executor_cutover": false,
  "archive_available": false,
  "production_ready": false
}
```

Registry and Redis failures are converted to sanitized not-ready responses
rather than exposing internal exception text.

## Submission contract

The request supports one explicit shape:

```yaml
run_shape: SINGLE_TASK
```

Omitting `run_shape` preserves backward compatibility and defaults to
`SINGLE_TASK`.

Unsupported, blank, or incorrectly cased shapes fail before UUID generation,
clock access, RUN admission, DAG initialization, TASK admission, dispatch,
rollback, retry, or repair:

```yaml
http_status: 400
submission_status: REJECTED
failure_code: UNSUPPORTED_RUN_SHAPE
run_id: empty
task_id: empty
writes: 0
```

Duplicate valid POST requests intentionally produce distinct RUNs because
submission idempotency is not supported.

## Public failure contract

Raw executor, provider, connection, path, credential, and stack information is
internal evidence and is not part of the product response.

The canonical public failure envelope is:

```json
{
  "code": "EXECUTOR_FAILED",
  "message": "Task execution failed.",
  "retryable": false
}
```

For failed canonical tasks:

- the public RUN error uses the stable envelope;
- the public TASK output uses the same stable envelope;
- the failed TASK output key is not read by the public combined reader;
- malformed or secret-bearing failed output cannot alter the public response;
- successful TASK output remains unchanged;
- legacy result reads expose only `Task execution failed.`;
- all public read paths remain read-only.

The real-Redis acceptance observed that private failure material was absent
from both durable public TASK output and the HTTP response.

## Lifecycle authority

The canonical authority sequence proven by this sprint is:

```text
HTTP POST
-> RUN admitted
-> root TASK admitted
-> production scheduler dispatch
-> canonical TASK claim
-> TASK state running
-> executor
-> canonical TASK terminal commit
-> RUN_TERMINATE aggregate commit
-> HTTP terminal read
```

The generic RUN read model maps RUN `admitted` to public `QUEUED`.

During executor activity, canonical TASK state is `running`, but RUN authority
remains `admitted`; therefore the public RUN response remains `QUEUED` until
terminal RUN finalization.

```yaml
canonical_task_running_observed: true
public_run_status_while_task_running: QUEUED
public_run_running_supported: false
new_run_lifecycle_writer_added: false
```

Sprint 83.7 intentionally does not derive RUN `RUNNING` from TASK truth and
does not introduce a new RUN lifecycle writer merely to manufacture that
status.

RUN terminal state and the complete public result projection may become
observable on adjacent read turns. Acceptance therefore polls until the
terminal RUN has `TERMINAL_WITH_RESULT`, canonical terminal TASK state, and
`task_output_status=AVAILABLE`; it does not treat a transient terminal,
incomplete projection as the final product response.

## Terminal read and retention

Successful single-task RUN:

```yaml
status: COMPLETED
outcome: SUCCESS
task_state: done
task_output_status: AVAILABLE
task_output: canonical durable success output
```

Failed single-task RUN:

```yaml
status: FAILED
outcome: FAILURE
task_state: failed
task_output_status: AVAILABLE
error:
  code: EXECUTOR_FAILED
  message: Task execution failed.
  retryable: false
task_output:
  code: EXECUTOR_FAILED
  message: Task execution failed.
  retryable: false
```

Current retention contract:

```yaml
run_state_ttl_seconds: 86400
run_meta_ttl_seconds: 86400
run_result_ttl_seconds: 86400
durable_archive: false
```

Restart durability is proven by replacing the control-plane service
composition, then replacing the worker service composition, against the same
Redis database. Stable terminal semantics remain identical; naturally
decreasing TTL fields are excluded from semantic equality.

This is an in-process composition restart proof, not a claim of separate
operating-system process crash recovery.

## Real-Redis acceptance

The one-command acceptance uses:

```yaml
redis: real Redis 7
http_application: real FastAPI/ASGI application
http_transport: in-process ASGI transport
scheduler: production scheduler
worker: production WorkerService
executor: controlled deterministic executor
```

The scheduler is paused briefly only to make the initial public `QUEUED`
observation deterministic. The real production scheduler performs dispatch.

Acceptance proves:

```yaml
initial_product_readiness: READY
success_submit: 202_ACCEPTED
success_initial_public_status: QUEUED
success_task_running: true
success_public_status_during_task_running: QUEUED
success_terminal: COMPLETED
success_output_available: true

failure_submit: 202_ACCEPTED
failure_task_running: true
failure_public_status_during_task_running: QUEUED
failure_terminal: FAILED
failure_sanitized: true
private_failure_exposed: false

unsupported_MULTI_TASK: 400_UNSUPPORTED_RUN_SHAPE
unsupported_MULTI_TASK_new_keys: 0
duplicate_GET_writes: 0
duplicate_POST_distinct_RUN: true
cross_tenant_read: 403
unknown_RUN: 404

control_composition_restart_semantics_stable: true
worker_composition_restart_semantics_stable: true
replacement_executor_calls_for_terminal_runs: 0
terminal_event_count_per_RUN: 1
worker_pending_count: 0
```

Local observed evidence:

```yaml
dedicated_real_redis_e2e: 2 passed
combined_alpha_regression: 174 passed
compileall: PASS
git_diff_check: PASS
```

## Architecture exclusions

```yaml
new_lifecycle_writer: false
direct_lifecycle_write_used: false
direct_dispatch_call_used: false
Loop_Plane_dependencies: 0
network_socket_http_used: false

direct_public_multitenant_alpha_ready: false
production_ready: false
production_cutover_authorized: false
```

## CI authority gate

The Sprint 83.7 workflow enforces:

- exact PR HEAD checkout;
- exact 24-file feature scope on the sprint branch;
- no `local_out`, cache, or bytecode files in the commit;
- 73 dedicated alpha contract tests;
- 2 real-Redis integration tests;
- 174 combined regression tests, including 6 prior durable read-model integration cases;
- no skipped, deselected, xfailed, or xpassed tests;
- authority and architecture scans;
- two byte-identical one-command acceptance reports;
- compile and whitespace checks;
- uploaded deterministic evidence artifacts.

Future branches that modify protected runtime files execute the same workflow
in runtime-intersection mode.

## Exact source scope

```text
.github/workflows/sprint83-7-trusted-gateway-single-task-alpha.yml
docs/implementation/sprint83/SPRINT83.7-trusted-gateway-single-task-alpha.md
hfa-control/src/hfa_control/api/models.py
hfa-control/src/hfa_control/api/router.py
hfa-control/src/hfa_control/models.py
hfa-control/src/hfa_control/product_profile.py
hfa-control/src/hfa_control/run_status_read_model.py
hfa-control/src/hfa_control/run_submission.py
hfa-control/src/hfa_control/service.py
hfa-control/src/hfa_control/user_facing_run_result.py
hfa-worker/src/hfa_worker/executor.py
hfa-worker/src/hfa_worker/fake_executor.py
hfa-worker/src/hfa_worker/main.py
hfa-worker/src/hfa_worker/process_root.py
scripts/runtime_alpha_acceptance_83_7.py
tests/integration/test_product_alpha_closure_e2e_83_7.py
tests/integration/test_run_status_result_read_model_integration.py
tests/unit/test_product_alpha_failure_contract.py
tests/unit/test_product_alpha_profile.py
tests/unit/test_product_alpha_readiness.py
tests/unit/test_product_alpha_run_shape.py
tests/unit/test_product_alpha_worker_capabilities.py
tests/unit/test_run_status_result_read_model.py
tests/unit/test_user_facing_run_result.py
```

`local_out/` contains disposable local or CI evidence and is explicitly not
part of the source scope.

## Exit statement

```yaml
status: PASS_WITH_LIMITATIONS
trusted_gateway_product_alpha_ready: true
internal_product_alpha_ready: true
product_alpha_ready: true

direct_public_multitenant_alpha_ready: false
production_ready: false

canonical_task_admit_used: true
canonical_task_running_observed: true
public_run_running_supported: false
run_finalization_supported: true
sanitized_failure_contract: true
control_service_restart_durability: true
worker_service_restart_durability: true
duplicate_get_zero_writes: true
duplicate_post_distinct_run: true
tenant_isolation: true
unsupported_multi_task_zero_writes: true

submission_idempotency_supported: false
multi_task_support: false
cancel_supported: false
retry_supported: false
external_executor_cutover: false
automatic_repair: false
archive_available: false

new_lifecycle_writer: false
direct_lifecycle_write_used: false
direct_dispatch_call_used: false
Loop_Plane_dependencies: 0
production_cutover_authorized: false
```

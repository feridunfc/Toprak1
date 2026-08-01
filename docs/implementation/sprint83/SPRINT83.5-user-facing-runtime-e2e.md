# Sprint 83.5 — User-Facing Runtime End-to-End Closure

## Status

```yaml
status: IMPLEMENTED_PENDING_FINAL_ACCEPTANCE
target_claim: CONTROL_PLANE_CAN_ACCEPT_AND_READ_A_CANONICAL_SINGLE_TASK_RUN
```

## Scope

Sprint 83.5 binds a tenant-scoped HTTP submission surface to the existing RUN
admission authority and the production scheduler's existing `DagLua` instance.

```text
POST /control/v1/runs
  -> ControlPlaneService.submit_single_task_run
  -> AdmissionController.admit
  -> production SchedulerComposition.dag_lua.task_admit
  -> production scheduler
  -> production WorkerService
  -> TASK_COMPLETE
  -> RUN_TERMINATE

GET /control/v1/runs/{run_id}
  -> DurableRunStatusResultReader
```

No second scheduler graph, second `DagLua`, direct Redis lifecycle writer, direct
dispatch call, automatic retry, rollback, or repair is introduced.

## HTTP contract

### Submit

```yaml
endpoint: POST /control/v1/runs
tenant_authority: X-Tenant-ID
accepted: 202
submission_incomplete: 409
invalid_request: 400
runtime_admission_failure: 503
```

The server generates the canonical RUN and TASK identifiers. A successful RUN
admission followed by failed TASK admission is reported honestly as
`SUBMISSION_INCOMPLETE`; the RUN is not silently rolled back or repaired.

### Combined status/result

```yaml
endpoint: GET /control/v1/runs/{run_id}
known_owned_run: 200
malformed_run_id: 400
tenant_mismatch: 403
unknown_run: 404
```

The read surface remains read-only and is backed by the Sprint 83.3 durable RUN
status/result adapter.

## Proven acceptance path

```text
ASGI HTTP POST
-> canonical RUN admission
-> canonical root TASK admission
-> production scheduler reservation and dispatch
-> production WorkerService execution
-> TASK done
-> RUN done
-> ASGI HTTP GET returns COMPLETED
```

Acceptance also proves:

- exactly one executor call;
- exactly one `RunCompleted` event;
- zero pending worker stream messages;
- running projection removed;
- durable TASK output;
- no direct lifecycle Redis write or direct dispatch invocation in the
  acceptance script.

## Explicit limitation

The combined HTTP RUN result currently exposes the terminal aggregate payload
(task counts), not the durable TASK executor output. Therefore:

```yaml
task_output_durable: true
task_output_exposed_in_http_result: false
product_alpha_ready: false
production_ready: false
external_executor_enabled: false
production_cutover_authorized: false
```

## Baseline test debt

`tests/core/test_sprint13_service_queries.py` still constructs the modern
`Scheduler` with its historical positional constructor. That baseline fixture
failure predates Sprint 83.5 and is not repaired in this sprint.

## Honest exit

```yaml
status: PASS_WITH_LIMITATIONS
user_facing_http_submit_supported: true
user_facing_http_status_result_supported: true
production_scheduler_used: true
production_worker_used: true
canonical_task_admit_used: true
run_finalization_supported: true
terminal_http_read_supported: true
task_output_durable: true
task_output_exposed_in_http_result: false
new_lifecycle_writer: false
direct_lifecycle_write_used: false
direct_dispatch_call_used: false
Loop_Plane_dependencies: 0
product_alpha_ready: false
production_ready: false
cancel_command_supported: false
retry_command_supported: false
automatic_repair_authorized: false
production_cutover_authorized: false
```

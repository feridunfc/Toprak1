# Sprint 83.6 — Canonical Single-Task Output Read Binding

## Status

```yaml
status: IMPLEMENTED_PENDING_FINAL_ACCEPTANCE
target_claim: CONTROL_PLANE_CAN_RETURN_THE_DURABLE_OUTPUT_OF_A_CANONICAL_SINGLE_TASK_RUN
```

## Scope

Sprint 83.6 closes the remaining user-facing gap after Sprint 83.5:

```text
POST /control/v1/runs
  -> canonical RUN admission
  -> canonical root TASK admission
  -> production scheduler
  -> production WorkerService
  -> TASK terminal output
  -> RUN terminal aggregate

GET /control/v1/runs/{run_id}
  -> UserFacingSingleTaskRunReader
     -> DurableRunStatusResultReader
     -> DagRedisKey.run_tasks(run_id)
     -> DagRedisKey.task_meta(task_id)
     -> DagRedisKey.task_state(task_id)
     -> DagRedisKey.task_output(task_id)
```

The Sprint 83.3 RUN reader remains unchanged and remains the generic RUN
status/result authority. Sprint 83.6 adds a thin read-only adapter for the
explicit single-task product contract.

## Additive HTTP contract

The existing response fields are preserved. The following fields are added:

```yaml
task_output_status: AVAILABLE | fail-closed status
task_id: canonical TASK id or null
task_state: canonical TASK state or null
task_output: decoded durable executor output or null
task_output_issues: deterministic issue list
```

Successful terminal example:

```json
{
  "status": "COMPLETED",
  "terminal": true,
  "completeness": "TERMINAL_WITH_RESULT",
  "task_output_status": "AVAILABLE",
  "task_id": "task-...",
  "task_state": "done",
  "task_output": {
    "output_text": "..."
  },
  "task_output_issues": []
}
```

Tenant authorization and run-id validation still occur before the read. Existing
`400`, `403`, and `404` behavior is preserved.

## Fail-closed matrix

```yaml
unknown_run: RUN_UNKNOWN
nonterminal_run: NOT_TERMINAL
terminal_run_evidence_incomplete: RUN_EVIDENCE_INCOMPLETE
missing_task_membership: TASK_MEMBERSHIP_MISSING
wrong_type_task_membership: TASK_MEMBERSHIP_WRONG_TYPE
multiple_tasks: NOT_A_SINGLE_TASK_RUN
missing_task_identity: TASK_IDENTITY_UNAVAILABLE
task_identity_mismatch: TASK_IDENTITY_CONFLICT
missing_task_state: TASK_STATE_UNAVAILABLE
run_task_state_mismatch: TASK_STATE_CONFLICT
missing_terminal_output: TERMINAL_OUTPUT_MISSING
wrong_type_output: OUTPUT_WRONG_TYPE
malformed_output: OUTPUT_MALFORMED
valid_canonical_output: AVAILABLE
```

No output is guessed, repaired, retried, reconstructed, or copied from another
record.

## Proven acceptance path

```text
ASGI HTTP POST
-> production RUN admission
-> production scheduler
-> production WorkerService
-> durable TASK output
-> RUN terminal result
-> ASGI HTTP GET
-> task_output_status == AVAILABLE
-> HTTP task_output exactly equals durable TASK output
```

Acceptance also proves:

- exactly one canonical TASK membership;
- exact `task_meta.task_id` and `task_meta.run_id` identity;
- terminal TASK/RUN state agreement;
- exactly one executor call;
- exactly one `RunCompleted` event;
- zero pending worker stream messages;
- running projection removed;
- deterministic acceptance output;
- zero Redis writes from the output reader;
- no direct dispatch or lifecycle invocation in the acceptance script.

## Explicit limitations

```yaml
single_task_contract_only: true
multi_task_output_aggregation: false
external_executor_enabled: false
cancel_command_supported: false
retry_command_supported: false
automatic_repair_authorized: false
product_alpha_ready: false
production_ready: false
production_cutover_authorized: false
```

The acceptance still uses in-process ASGI transport, disposable Redis data, and
a deterministic executor. Production deployment and external executor cutover
remain outside this sprint.

## Exact scope

```text
.github/workflows/sprint83-6-single-task-output-read.yml
docs/implementation/sprint83/SPRINT83.6-single-task-output-read.md
hfa-control/src/hfa_control/api/models.py
hfa-control/src/hfa_control/service.py
hfa-control/src/hfa_control/user_facing_run_result.py
scripts/runtime_alpha_acceptance_83_6.py
tests/integration/test_user_facing_run_output_e2e_83_6.py
tests/unit/test_user_facing_run_result.py
tests/unit/test_user_facing_run_result_http_binding.py
```

## Honest exit target

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
task_output_exposed_in_http_result: true
new_lifecycle_writer: false
direct_lifecycle_write_used: false
direct_dispatch_call_used: false
Loop_Plane_dependencies: 0
product_alpha_ready: false
production_ready: false
production_cutover_authorized: false
```

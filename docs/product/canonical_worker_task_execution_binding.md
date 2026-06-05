# Canonical Worker Task Execution Binding

## Purpose

This contract defines Sprint 38: binding the task execution product path to the repository's real worker/runtime submit-claim-execute-complete path.

Sprint 37 proved a tenant-scoped task execution flow through a safe artifact-backed adapter. Sprint 38 must reduce or remove fallback and prove that real repository worker/runtime code participates in task execution.

## Discovery result

Sprint 38 discovery found a real worker/runtime path:

- `hfa-worker/src/hfa_worker/task_consumer.py`
  - `TaskConsumer`
  - `consume_once`
- `hfa-worker/src/hfa_worker/consumer.py`
  - `WorkerConsumer`
  - `StateStore`
  - `try_claim_and_mark_running`
  - `store_result`
  - `mark_completed`
- `hfa-worker/src/hfa_worker/executor.py`
  - `BaseExecutor`
- `hfa-worker/src/hfa_worker/fake_executor.py`
  - `FakeExecutor`
- `hfa-worker/src/hfa_worker/models.py`
  - `ExecutionResult`
- `hfa-core/src/hfa/runtime/state_store.py`
  - `StateStore`
  - `claim_execution`
  - `mark_running`
  - `store_result`
  - `mark_completed`

This means Sprint 38 must not repeat the Sprint 37 script-only adapter pattern.

## Sprint goal

Bind E2E task execution to the canonical worker/runtime path.

Required final evidence should prefer:

    {
      "canonical_worker_consumer_used": true,
      "task_consumer_consume_once_used": true,
      "worker_consumer_runtime_path_used": true,
      "fake_executor_used": true,
      "worker_executor_invoked": true,
      "state_store_result_written": true,
      "state_store_mark_completed_called": true,
      "result_readable": true,
      "worker_claim_execute_complete_fallback_used": false
    }

If a fully canonical submit API does not exist yet, the allowed transitional shape is:

    {
      "submit_binding": "safe_local_adapter",
      "worker_binding": "TaskConsumer.consume_once",
      "completion_binding": "StateStore.store_result/mark_completed"
    }

## Required behavior

The implementation must prove:

- tenant-scoped task submission or synthetic runtime task context
- real worker/consumer code is imported and executed
- worker consumes exactly one task
- safe executor is invoked through worker contract
- task reaches terminal completed state
- result is written through StateStore-compatible completion path
- result is readable
- no production LLM call is attempted
- no deployment is attempted
- no release tag is created
- no noncanonical Redis mutation is attempted

## Must touch worker/runtime code

Sprint 38 must touch repository worker/runtime/core code if the current APIs do not expose a stable testable binding.

Script-only fake queue or standalone adapter-only execution is not sufficient.

## Allowed mutation

Controlled task lifecycle mutation is allowed only through canonical runtime/worker/state abstractions.

Allowed:

- construct tenant-scoped task context
- claim task through worker/runtime path
- execute safe executor through worker executor path
- mark task completed through runtime state path
- write/read result through StateStore-compatible path

## Forbidden operations

The sprint must not:

- bypass worker claim/execute/complete with script-only fake logic
- mutate Redis outside canonical abstractions
- call production LLMs
- call external side-effecting services
- deploy
- create release tags
- expose operator requeue/retry/auto-resume buttons
- assert production-ready status

## Required tests

Required test:

- `tests/integration/test_canonical_worker_task_execution.py`

The test must verify:

- `TaskConsumer.consume_once` or `WorkerConsumer` path is used
- FakeExecutor or safe executor is invoked through worker contract
- StateStore-compatible result/write completion path is used
- task completes
- result is readable
- tenant isolation is preserved
- terminal state is stable/idempotent
- worker claim/execute/complete fallback is false
- production LLM call is false
- deployment/release flags are false
- noncanonical Redis mutation flag is false

## Output artifact

The sprint should write:

- `docs/dashboard/artifacts/latest_canonical_worker_task_execution.json`

Minimum successful shape:

    {
      "source": "canonical_worker_task_execution",
      "status": "PASS",
      "canonical_worker_consumer_used": true,
      "task_consumer_consume_once_used": true,
      "fake_executor_used": true,
      "worker_executor_invoked": true,
      "state_store_result_written": true,
      "state_store_mark_completed_called": true,
      "result_readable": true,
      "worker_claim_execute_complete_fallback_used": false,
      "production_llm_call_attempted": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "noncanonical_redis_mutation_attempted": false
    }

## Non-goals

This sprint does not:

- implement production LLM execution
- implement production deployment
- implement release tagging
- implement operator requeue UI
- implement auto-resume UI
- implement long-running production worker service
- assert production-ready status

## Governance rule

Sprint 38 is not another script-only demo.

It must bind product task execution to real repository worker/runtime code or explicitly fail until that path is found or created.

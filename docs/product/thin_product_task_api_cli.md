# Thin Product Task API and CLI

## Sprint

Sprint 43 — Thin Product Task API and CLI

## Purpose

Sprint 43 exposes the proven Sprint 42 runtime path through user-facing commands.

The system must allow a user/operator to:

1. submit a tenant-scoped task
2. receive a run_id
3. read the completed result by run_id

This is the first thin product shell over the runtime engine. The goal is not another proof-only artifact. The goal is a visible task execution flow.

## Runtime requirement

The CLI/API must use the existing Sprint 42 runtime path:

tenant submit/enqueue
-> production Lua/EVALSHA dispatch
-> Redis stream
-> worker consume loop
-> FakeExecutor
-> StateStore-compatible result/completion
-> result read

## Product claim

`USER_VISIBLE_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND`

## Required commands

### Submit

    python scripts/ironclad_submit.py --tenant demo --message "hello" --redis-url redis://localhost:6389/0

Required output:

    {
      "status": "SUBMITTED",
      "tenant_id": "demo",
      "task_id": "...",
      "run_id": "...",
      "message": "hello"
    }

### Result

    python scripts/ironclad_result.py --run-id "..." --redis-url redis://localhost:6389/0

Required output:

    {
      "status": "COMPLETED",
      "tenant_id": "demo",
      "run_id": "...",
      "result": {
        "output_text": "..."
      }
    }

### Demo

    python scripts/ironclad_demo.py --tenant demo --message "hello" --redis-url redis://localhost:6389/0 --json

Required output:

    {
      "source": "thin_product_task_cli_demo",
      "status": "PASS",
      "product_visible": true,
      "submitted": {
        "tenant_id": "demo",
        "task_id": "...",
        "run_id": "..."
      },
      "result": {
        "status": "COMPLETED",
        "output_text": "FAKE_RESPONSE: hello"
      }
    }

## Must use existing runtime

Sprint 43 must reuse the Sprint 42 runtime path. It must not create a separate fake execution path.

PASS requires:

- submit CLI accepts tenant and message
- run_id and task_id are returned
- production Lua/EVALSHA dispatch is used
- Redis stream is used
- WorkerConsumer stream consume loop is used
- FakeExecutor is used
- StateStore-compatible result path is used
- result is readable by run_id
- demo CLI returns a user-visible completed result

## Forbidden as PASS

Sprint 43 must not report PASS if any of these are used:

- new fake runtime path
- artifact-only adapter as the product path
- direct `_process_message` injection as product path
- SchedulerLua Python fallback
- production LLM call
- deployment
- release tag
- operator action buttons
- noncanonical Redis mutation
- production-ready claim

## Required artifact

Artifact:

- `docs/dashboard/artifacts/latest_thin_product_task_cli_demo.json`

Minimum PASS shape:

    {
      "source": "thin_product_task_cli_demo",
      "status": "PASS",
      "product_visible": true,
      "submit_cli_available": true,
      "result_cli_available": true,
      "demo_cli_available": true,
      "tenant_id": "demo",
      "run_id": "...",
      "task_id": "...",
      "runtime_claim": "USER_VISIBLE_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND",
      "uses_sprint_42_runtime": true,
      "tenant_submit_used": true,
      "run_id_returned": true,
      "result_readable": true,
      "production_lua_evalsha_path_used": true,
      "scheduler_lua_python_fallback_used": false,
      "worker_stream_consume_loop_used": true,
      "direct_process_message_call_used": false,
      "fake_executor_used": true,
      "production_llm_call_attempted": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "operator_action_buttons": false,
      "noncanonical_redis_mutation_attempted": false,
      "failing_reasons": []
    }

## Tests

Sprint 43 should add:

- `tests/integration/test_thin_product_task_cli.py`

Minimum tests:

- `ironclad_demo.py` completes task and returns readable result
- `ironclad_submit.py` returns run_id and task_id
- `ironclad_result.py` reads completed result
- unavailable Redis degrades or fails safely
- CLI does not use production LLM
- CLI does not deploy or release
- CLI does not bypass Sprint 42 runtime

## Known limitations

- FakeExecutor only.
- No production LLM call.
- No HTTP API yet.
- No multi-tenant fairness/load proof.
- No production deployment readiness claim.

# Minimal Product Task HTTP API

## Sprint

Sprint 44 — Minimal Product Task HTTP API

## Purpose

Sprint 44 exposes the Sprint 43 thin product task flow through a minimal local/dev HTTP API.

Sprint 43 provides user-visible CLI commands for:

- task submit
- result read
- demo execution

Sprint 44 adds HTTP endpoints so an API client can submit a task and read the result.

This is a product API surface, not a production deployment claim.

## Target claim

`HTTP_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND`

## Required endpoints

### GET /health

Returns API health and runtime configuration.

Required response:

    {
      "status": "ok",
      "service": "ironclad_minimal_product_task_api",
      "runtime": "sprint_43"
    }

### POST /tasks

Accepts:

    {
      "tenant_id": "demo",
      "message": "hello"
    }

Returns:

    {
      "status": "SUBMITTED",
      "tenant_id": "demo",
      "task_id": "...",
      "run_id": "...",
      "result_readable": true
    }

For the Sprint 44 MVP, `POST /tasks` may execute the full Sprint 43 demo path synchronously and persist the result for `GET /runs/{run_id}`.

### GET /runs/{run_id}

Returns:

    {
      "status": "COMPLETED",
      "tenant_id": "demo",
      "task_id": "...",
      "run_id": "...",
      "result_readable": true,
      "result": {
        "output_text": "FAKE_RESPONSE: hello"
      }
    }

Unknown runs should return HTTP 404 or a structured `NOT_FOUND` response.

### POST /demo

Optional convenience endpoint.

It may run the full submit -> worker stream -> result read flow and return the completed output in one request.

## Runtime requirement

The API must use the existing Sprint 43 product runtime path.

Required runtime path:

tenant submit/enqueue
-> production Lua/EVALSHA dispatch
-> Redis stream
-> worker consume loop
-> FakeExecutor
-> StateStore-compatible result/completion
-> result read

## PASS requirements

Sprint 44 PASS requires:

- HTTP API app is importable
- `GET /health` works
- `POST /tasks` accepts tenant_id and message
- `POST /tasks` returns run_id and task_id
- `GET /runs/{run_id}` returns a readable result
- result output includes the submitted message
- Sprint 43 runtime path is used
- production Lua/EVALSHA dispatch is used
- worker stream consume loop is used
- FakeExecutor is used
- direct `_process_message` product bypass is not used
- production LLM is not called
- deployment/release actions are not attempted

## Forbidden as PASS

Sprint 44 must not report PASS if any of these are used:

- new fake runtime path
- bypassing Sprint 43 runtime
- direct `_process_message` product bypass
- SchedulerLua Python fallback as PASS
- production LLM call
- deployment
- release tag
- operator action buttons
- production-ready claim

## Required artifact

Artifact:

- `docs/dashboard/artifacts/latest_minimal_product_task_http_api.json`

Minimum PASS shape:

    {
      "source": "minimal_product_task_http_api",
      "status": "PASS",
      "target_claim_supported": true,
      "http_api_available": true,
      "health_endpoint_available": true,
      "post_tasks_available": true,
      "get_run_result_available": true,
      "uses_sprint_43_runtime": true,
      "tenant_id": "demo",
      "run_id": "...",
      "task_id": "...",
      "result_readable": true,
      "result_output_includes_submitted_message": true,
      "production_lua_evalsha_path_used": true,
      "worker_stream_consume_loop_used": true,
      "direct_process_message_call_used": false,
      "fake_executor_used": true,
      "production_llm_call_attempted": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "operator_action_buttons": false,
      "failing_reasons": []
    }

## Test file

Sprint 44 should add:

- `tests/integration/test_minimal_product_task_http_api.py`

Required tests:

- `GET /health` returns ok
- `POST /tasks` returns run_id and task_id
- `GET /runs/{run_id}` returns completed/readable result
- result output includes submitted message
- unknown run_id returns 404 or NOT_FOUND
- safety flags remain false
- API uses Sprint 43 runtime path

## Dependencies

Sprint 44 may use:

- `fastapi`
- `uvicorn`
- `httpx`

## Known limitations

- Local/dev HTTP API only.
- FakeExecutor only.
- No auth.
- No production LLM.
- No deployment/release claim.
- No production-ready API claim.
- No multi-tenant rate limiting.
- No async job polling architecture guarantee yet.

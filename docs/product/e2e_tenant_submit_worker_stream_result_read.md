# End-to-End Tenant Submit to Worker Stream Result Read

## Sprint

Sprint 42 — End-to-End Tenant Submit to Worker Stream Result Read

## Purpose

This contract proves that a tenant-scoped task can enter the system through a submit/enqueue boundary, be dispatched through the production Redis Lua/EVALSHA path, be consumed from Redis stream by the canonical worker consume loop, complete through StateStore-compatible lifecycle mutation, and be read back as a result by `run_id`.

Sprint 41 proved:

- production Lua/EVALSHA dispatch writes `RunRequested` to Redis stream
- `WorkerConsumer` reads from Redis stream through its consume loop
- `FakeExecutor` executes through the worker path
- StateStore-compatible result/completion paths are called
- stream message is acknowledged

Sprint 42 adds:

- tenant submit/enqueue proof at the beginning
- result read proof at the end

## Target claim

`TENANT_SUBMIT_TO_LUA_DISPATCH_WORKER_STREAM_RESULT_READ_BOUND`

## PASS requirements

A PASS artifact must prove all of the following:

- tenant submit/enqueue boundary used
- canonical enqueue used
- production Lua/EVALSHA dispatch used
- SchedulerLua Python fallback not used
- Lua dispatch wrote `RunRequested` to the shard stream
- worker stream consume loop consumed the message
- direct `_process_message` call was not used as final proof
- FakeExecutor executed through the worker path
- IdempotencyGuard claim path was used
- StateStore-compatible result path was written
- StateStore-compatible transition path was called
- StateStore-compatible completion path was called
- stream message was acknowledged
- result was read by `run_id`
- artifact was written

## Required artifact

The sprint writes:

- `docs/dashboard/artifacts/latest_e2e_tenant_submit_worker_stream_result_read.json`

Minimum PASS shape:

    {
      "source": "e2e_tenant_submit_worker_stream_result_read",
      "status": "PASS",
      "target_claim_supported": true,
      "redis_backend": "real_redis",
      "tenant_id": "tenant-demo",
      "task_id": "task-e2e-tenant-submit-demo",
      "run_id": "run-e2e-tenant-submit-demo",
      "agent_type": "fake",
      "executor": "FakeExecutor",
      "tenant_submit_used": true,
      "tenant_submit_entrypoint": "canonical_submit_or_service",
      "tenant_task_submitted": true,
      "canonical_enqueue_used": true,
      "scheduler_lua_initialised": true,
      "production_lua_evalsha_path_used": true,
      "scheduler_lua_python_fallback_used": false,
      "dispatch_output_created": true,
      "run_requested_event_written": true,
      "dispatch_message_from_lua": true,
      "worker_stream_consume_loop_used": true,
      "run_requested_event_consumed": true,
      "direct_process_message_call_used": false,
      "worker_consumer_process_message_used": true,
      "fake_executor_used": true,
      "worker_executor_invoked": true,
      "idempotency_guard_claimed": true,
      "state_store_result_written": true,
      "state_store_transition_state_called": true,
      "state_store_mark_completed_called": true,
      "message_acknowledged": true,
      "result_read_api_used": true,
      "result_readable": true,
      "result_status": "done",
      "artifact_backed_safe_local_adapter_used": false,
      "production_llm_call_attempted": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "operator_action_buttons": false,
      "noncanonical_redis_mutation_attempted": false,
      "failing_reasons": []
    }

## Forbidden as PASS

Sprint 42 must not report PASS if any of the following are required:

- artifact-backed safe local adapter
- SchedulerLua Python fallback
- direct `_process_message` injection
- script-only fake queue/result path
- production LLM call
- deployment
- release tag
- operator action buttons
- noncanonical Redis mutation

## DEGRADED behavior

The artifact may report DEGRADED if:

- real Redis is unavailable
- no acceptable tenant submit/enqueue boundary is available
- production Lua/EVALSHA dispatch is unavailable
- worker stream consume loop is unavailable
- result cannot be read by `run_id`

DEGRADED must set:

    {
      "status": "DEGRADED",
      "target_claim_supported": false
    }

## Known limitations

- FakeExecutor only.
- No production LLM call.
- No deployment.
- No release tag.
- No multi-tenant fairness/load proof.
- No production readiness claim.

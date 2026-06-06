# Worker Stream Consume Loop Binding

## Purpose

This contract ensures that tasks emitted by the production Lua dispatch path are consumed by the worker through the canonical Redis stream consume loop, not by direct `_process_message` calls.

Sprint 40 proved:

- real Redis backend
- `SchedulerLua.initialise()` loads Lua scripts
- `dispatch_commit.lua` executes through production Lua/EVALSHA
- Lua dispatch emits `RunRequested` to the shard stream
- direct worker processing can complete the task

Sprint 41 must prove the next boundary:

- worker reads the Lua-produced `RunRequested` message from Redis stream through its consume loop

## Target claim

`TENANT_SCOPED_TASK_EXECUTION_BOUND_TO_WORKER_STREAM_CONSUME_LOOP`

## Goals

- Prove `SchedulerLua.dispatch_commit_detailed` writes `RunRequested` events to Redis stream.
- Prove `WorkerConsumer` reads messages from the stream through its canonical consume loop.
- Prove task lifecycle completes through canonical worker flow:
  - claim through `IdempotencyGuard`
  - execute through executor
  - write result through StateStore-compatible path
  - mark task completed through StateStore-compatible path
  - acknowledge stream message
- Prove the final artifact is not based on direct `_process_message` injection.

## Allowed mutation

Canonical task lifecycle mutation only:

- enqueue/admit through scheduler runtime path
- dispatch through production Lua/EVALSHA path
- stream read through WorkerConsumer consume loop
- claim via IdempotencyGuard
- execute task via executor
- write result via StateStore
- mark completed via StateStore
- acknowledge stream message

## Forbidden mutation

Sprint 41 must not:

- call `_process_message` directly as final proof
- use script-only `xadd` as PASS
- bypass IdempotencyGuard
- bypass StateStore
- call production LLMs
- deploy
- create release tags
- perform noncanonical Redis mutation
- expose operator action buttons
- claim production readiness

## Required artifact

The sprint writes:

- `docs/dashboard/artifacts/latest_worker_stream_consume_loop.json`

Minimum PASS shape:

    {
      "source": "worker_stream_consume_loop",
      "status": "PASS",
      "target_claim_supported": true,
      "redis_backend": "real_redis",
      "production_lua_evalsha_path_used": true,
      "scheduler_lua_python_fallback_used": false,
      "dispatch_message_from_lua": true,
      "run_requested_event_written": true,
      "run_requested_event_consumed": true,
      "worker_stream_consume_loop_used": true,
      "direct_process_message_call_used": false,
      "worker_consumer_process_message_used": true,
      "idempotency_guard_claimed": true,
      "executor_invoked": true,
      "fake_executor_used": true,
      "state_store_result_written": true,
      "state_store_mark_completed_called": true,
      "message_acknowledged": true,
      "production_llm_call_attempted": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "operator_action_buttons": false,
      "noncanonical_redis_mutation_attempted": false,
      "tenant_id": "tenant-demo",
      "run_id": "...",
      "failing_reasons": []
    }

## Degraded behavior

If real Redis is unavailable, the artifact may report DEGRADED.

DEGRADED must not support the Sprint 41 target claim.

If the worker cannot consume through the stream loop, the artifact must not report PASS.

## Required tests

Required integration test:

- `tests/integration/test_worker_stream_consume_loop.py`

Recommended core contract test:

- `tests/core/test_worker_stream_consume_loop_contract.py`

Tests must verify:

- Lua dispatch writes `RunRequested`.
- Worker stream consume loop consumes the message.
- Direct `_process_message` call is not used as final proof.
- FakeExecutor is invoked.
- StateStore-compatible result/completion paths are called.
- stream ack happens.
- safety flags remain false.
- artifact writes `latest_worker_stream_consume_loop.json`.

## Non-goals

This sprint does not:

- call production LLMs
- prove multi-tenant fairness under load
- prove production deployment readiness
- deploy
- create release tags

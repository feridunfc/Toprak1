# Production Lua Dispatch Path Binding

## Purpose

Sprint 40 binds scheduler-dispatched task execution to the production Redis Lua/EVALSHA dispatch path.

Sprint 39 proved:

    tenant enqueue/admit
    -> SchedulerLua dispatch output
    -> RunRequested shard stream message
    -> WorkerConsumer process path
    -> FakeExecutor
    -> StateStore-compatible result/completion
    -> ack

But Sprint 39 used the SchedulerLua Python fallback path in fakeredis/test mode.

Sprint 40 requires production Lua/EVALSHA dispatch for PASS.

## Target claim

`TENANT_SCOPED_TASK_EXECUTION_BOUND_TO_PRODUCTION_LUA_DISPATCH_AND_WORKER_COMPLETION`

## Required PASS evidence

PASS requires:

- real Redis backend
- `SchedulerLua.initialise()` succeeds
- dispatch commit Lua script is loaded
- dispatch uses Lua/EVALSHA path
- Python fallback is not used
- dispatch output is created
- `RunRequested` is emitted from Lua dispatch to shard stream
- `WorkerConsumer` processes the Lua-produced message
- `FakeExecutor` executes through worker path
- StateStore-compatible result write is verified
- StateStore-compatible transition/completion is verified
- stream message ack is verified
- result is readable

Minimum PASS shape:

    {
      "source": "production_lua_dispatch_path",
      "status": "PASS",
      "redis_backend": "real_redis",
      "scheduler_lua_initialised": true,
      "dispatch_commit_loader_used": true,
      "dispatch_commit_sha_loaded": true,
      "production_lua_evalsha_path_used": true,
      "scheduler_lua_python_fallback_used": false,
      "dispatch_output_created": true,
      "run_requested_event_from_lua_dispatch": true,
      "manual_worker_message_injection_used": false,
      "worker_consumer_process_message_used": true,
      "state_store_result_written": true,
      "state_store_mark_completed_called": true,
      "message_acknowledged": true,
      "fake_executor_used": true,
      "production_llm_call_attempted": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "operator_action_buttons": false,
      "noncanonical_redis_mutation_attempted": false,
      "failing_reasons": []
    }

## Degraded behavior

If real Redis is unavailable, the artifact may report DEGRADED.

DEGRADED must not support the Sprint 40 target claim.

Required degraded shape:

    {
      "source": "production_lua_dispatch_path",
      "status": "DEGRADED",
      "redis_backend": "unavailable",
      "target_claim_supported": false,
      "production_lua_evalsha_path_used": false,
      "scheduler_lua_python_fallback_used": false,
      "failing_reasons": [
        "real Redis unavailable; production Lua/EVALSHA path not proven"
      ]
    }

If SchedulerLua Python fallback is used, the artifact must not report PASS:

    {
      "status": "DEGRADED",
      "scheduler_lua_python_fallback_used": true,
      "target_claim_supported": false
    }

## Allowed mutation

Controlled task lifecycle mutation is allowed only through canonical scheduler/worker/runtime abstractions.

Allowed:

- canonical enqueue/admit through SchedulerLua
- canonical dispatch through production Lua/EVALSHA path
- canonical shard stream `RunRequested` emission
- canonical WorkerConsumer process path
- canonical result/transition/completion path
- canonical stream ack

## Forbidden operations

Sprint 40 must not:

- report PASS through SchedulerLua Python fallback
- report PASS through direct script-only `xadd`
- report PASS through manual worker message injection
- call production LLMs
- deploy
- create release tags
- expose operator action buttons
- perform noncanonical Redis mutation
- assert production-ready status

## Required tests

Required integration test:

- `tests/integration/test_production_lua_dispatch_path.py`

Recommended core contract test:

- `tests/core/test_production_lua_dispatch_path_contract.py`

Tests must verify:

- real Redis Lua/EVALSHA path can produce PASS
- fallback path cannot produce PASS
- missing Redis degrades without target claim
- Lua-produced `RunRequested` reaches shard stream
- WorkerConsumer completion path is verified
- safety flags remain false
- artifact writes `latest_production_lua_dispatch_path.json`

## Output artifact

The sprint writes:

- `docs/dashboard/artifacts/latest_production_lua_dispatch_path.json`

## Non-goals

This sprint does not:

- call production LLMs
- prove multi-tenant fairness under load
- prove worker stream consume loop as long-running service
- assert production deployment readiness
- deploy
- create release tags

## Governance rule

Scheduler dispatch PASS must come from real Redis Lua/EVALSHA path, not Python fallback.

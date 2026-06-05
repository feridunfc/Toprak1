# Production Lua Dispatch Inventory

## Purpose

This document records Sprint 40B discovery for proving SchedulerLua dispatch through the production Redis Lua/EVALSHA path.

Sprint 40 target claim:

`TENANT_SCOPED_TASK_EXECUTION_BOUND_TO_PRODUCTION_LUA_DISPATCH_AND_WORKER_COMPLETION`

## Baseline from Sprint 39

Sprint 39 proved scheduler-dispatched task execution through the repository scheduler helper and worker completion path, but it used the SchedulerLua Python fallback in fakeredis/test mode.

Sprint 40 requires real Redis Lua/EVALSHA dispatch for PASS.

## Discovery summary

The repository already has the core assets required for Sprint 40:

- `hfa-core/src/hfa/lua/dispatch_commit.lua`
  - performs CAS from admitted/queued to scheduled
  - writes run metadata
  - writes running ZSET entry
  - writes control stream event
  - writes shard stream `RunRequested` event
- `hfa-core/src/hfa/lua/enqueue_admitted.lua`
  - atomically admits/enqueues task metadata and payload
  - writes queued state
  - writes tenant active index
- `hfa-core/src/hfa/lua/loader.py`
  - `LuaScriptLoader`
  - loads scripts via `SCRIPT LOAD`
  - executes via `EVALSHA`
  - retries on `NOSCRIPT`
  - supports fakeredis fallback for tests
- `hfa-control/src/hfa_control/scheduler_lua.py`
  - `SchedulerLua.initialise()`
  - loads `enqueue_admitted.lua`
  - loads `dispatch_commit.lua`
  - `dispatch_commit_detailed(...)`
  - uses `LuaScriptLoader.run(...)`
  - falls back only when Lua/Redis support is unavailable

## Required PASS distinction

Sprint 40 PASS must prove:

- real Redis backend
- `SchedulerLua.initialise()` succeeded
- `dispatch_commit.lua` was loaded into Redis
- dispatch commit ran through `EVALSHA`
- SchedulerLua Python fallback was not used
- Lua dispatch created `RunRequested` output on shard stream
- WorkerConsumer processed the Lua-produced message
- FakeExecutor executed through worker contract
- StateStore-compatible result/completion path completed
- stream ack happened

## Fallback/degraded rule

Python fallback may exist, but it cannot produce PASS for Sprint 40.

If fallback is used, the artifact must report DEGRADED and set:

- `target_claim_supported=false`
- `production_lua_evalsha_path_used=false`
- `scheduler_lua_python_fallback_used=true`

If real Redis is unavailable, the artifact must report DEGRADED and set:

- `redis_backend=unavailable`
- `target_claim_supported=false`

## Implementation direction

Sprint 40 should add:

- `scripts/production_lua_dispatch_path.py`
- `tests/integration/test_production_lua_dispatch_path.py`
- optional `tests/core/test_production_lua_dispatch_path_contract.py`

CI should run with a real Redis service:

- `redis:7`
- `REDIS_URL=redis://localhost:6379/0`

## Expected artifact

The output artifact should be:

- `docs/dashboard/artifacts/latest_production_lua_dispatch_path.json`

PASS requires:

- `status=PASS`
- `redis_backend=real_redis`
- `scheduler_lua_initialised=true`
- `dispatch_commit_loader_used=true`
- `dispatch_commit_sha_loaded=true`
- `production_lua_evalsha_path_used=true`
- `scheduler_lua_python_fallback_used=false`
- `dispatch_output_created=true`
- `run_requested_event_from_lua_dispatch=true`
- `manual_worker_message_injection_used=false`
- `worker_consumer_process_message_used=true`
- `state_store_result_written=true`
- `state_store_mark_completed_called=true`
- `message_acknowledged=true`
- `production_llm_call_attempted=false`
- `noncanonical_redis_mutation_attempted=false`

# Scheduler Dispatch Inventory

## Purpose

This document records Sprint 39B discovery for scheduler-dispatched task execution.

Sprint 39 target claim:

`TENANT_SCOPED_TASK_EXECUTION_BOUND_TO_SCHEDULER_DISPATCH_AND_WORKER_COMPLETION`

Sprint 39 must prove that the worker message is produced by scheduler/dispatch output, not manually injected into `WorkerConsumer._process_message`.

## Discovery summary

The repository contains canonical scheduler/dispatch entrypoints.

Relevant files discovered:

- `hfa-control/src/hfa_control/scheduler_lua.py`
  - `SchedulerLua`
  - `enqueue_admitted`
  - `dispatch_commit`
  - `dispatch_commit_detailed`
  - shard stream `xadd`
- `hfa-control/src/hfa_control/scheduler_loop.py`
  - `SchedulerLoop`
  - `commit_dispatch`
  - `_dispatch_once`
  - `run_cycle`
- `hfa-control/src/hfa_control/scheduler_reservation_dispatch.py`
  - `SchedulerReservationDispatcher`
  - `reserve_and_dispatch`
- `hfa-control/src/hfa_control/scheduler_capability_fair_dispatch.py`
  - `SchedulerCapabilityFairDispatcher`
  - `dispatch_task`
- `hfa-control/src/hfa_control/observable_scheduler_capability_fair_dispatch.py`
  - `ObservableSchedulerCapabilityFairDispatcher`
  - `dispatch_task`
- `hfa-control/src/hfa_control/dag_lua.py`
  - `task_dispatch_commit`
- `hfa-control/src/hfa_control/dag_scheduler_bridge.py`
  - `rebuild_dispatch_input`
- `hfa-control/src/hfa_control/tenant_queue.py`
  - `enqueue`
- `hfa-core/src/hfa/config/keys.py`
  - `RedisKey.stream_shard`
  - `hfa:stream:runs:{shard}`
- `hfa-core/src/hfa/events/schema.py`
  - `RunRequestedEvent`
- `hfa-core/src/hfa/events/codec.py`
  - `serialize_event`
  - `deserialize_run_requested`

## Most likely Sprint 39 binding

The strongest candidate is:

1. create tenant-scoped task/run input
2. use canonical scheduler dispatch helper or `SchedulerLua.dispatch_commit_detailed`
3. verify dispatch output writes a `RunRequestedEvent` to `RedisKey.stream_shard(shard)`
4. pass the stream-produced message to `WorkerConsumer` consume/process path
5. verify `FakeExecutor`, `StateStore.store_result`, `transition_state`, `mark_completed`, result event write, and ack

## Important distinction from Sprint 38

Sprint 38 manually created a serialized `RunRequestedEvent` and called `WorkerConsumer._process_message`.

Sprint 39 must not use manual worker message injection as the final claim.

A PASS artifact must include:

- `scheduler_dispatch_used=true`
- `dispatch_output_created=true`
- `run_requested_event_from_dispatch=true`
- `manual_worker_message_injection_used=false`
- `worker_consumer_process_message_used=true`

## Acceptable transitional implementation

If a full scheduler loop is too broad for this sprint, a canonical dispatch helper is acceptable if it is repository code and produces the worker stream message.

Acceptable:

- `SchedulerLua.dispatch_commit` or `dispatch_commit_detailed`
- `SchedulerLoop.commit_dispatch`
- `SchedulerReservationDispatcher.reserve_and_dispatch`
- existing scheduler dispatch abstraction that writes `RunRequestedEvent` to shard stream

Not acceptable as PASS:

- script-created `RunRequestedEvent` directly passed to worker
- ad-hoc Redis `xadd` in the proof script without repository scheduler/dispatch helper
- script-only fake queue

## Tests discovered

Existing test areas mention:

- scheduler dispatch
- `RunRequestedEvent`
- shard stream writes such as `hfa:stream:runs:5`
- exactly-once dispatch
- DAG task dispatch contract

These should be used as implementation references before adding Sprint 39 tests.

## Next implementation target

Sprint 39C should add:

- `scripts/scheduler_dispatched_task_execution.py`
- `tests/core/test_scheduler_dispatched_task_execution.py`

The script must produce:

- `docs/dashboard/artifacts/latest_scheduler_dispatched_task_execution.json`

PASS requires scheduler/dispatch-created worker message, worker completion, and no manual worker message injection.

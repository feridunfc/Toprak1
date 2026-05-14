# V23.1 Production-Ready DAG Schema

## Design goals

This schema is built for an IRONCLAD V22 base:
- single scheduling authority remains `SchedulerLoop.dispatch_once()`
- single state authority remains centralized
- DAG unlock must be atomic
- no recursive graph mutation in Redis Lua
- no giant graph JSON read-modify-write cycles

## Non-negotiable rules

1. **Run and task are distinct entities**
   - Run is the user-visible orchestration envelope.
   - Task is the schedulable unit.

2. **Scheduler sees only READY tasks**
   - Scheduler must not traverse the graph.
   - Graph semantics live in DAG state + Lua unlock.

3. **Task completion unlocks only direct children**
   - Recursive descendant walks inside Lua are forbidden.

4. **Failure propagation is lazy**
   - Task failure marks the task terminal.
   - Full descendant cancellation is not performed inside Lua.
   - Higher-layer evaluation decides whether descendants become `blocked_by_failure` or remain inert.

5. **Fairness remains tenant-scoped**
   - DAG fan-out must not bypass tenant fairness.

## Redis key schema

### Run-level
- `hfa:run:<run_id>:graph`
  - HASH
  - metadata: `run_id`, `tenant_id`, `root_task_count`, `task_count`, `graph_version`
- `hfa:run:<run_id>:state`
  - STRING
- `hfa:run:<run_id>:tasks`
  - SET of task ids
- `hfa:run:<run_id>:ready_count`
  - STRING counter
- `hfa:run:<run_id>:terminal_count`
  - STRING counter

### Tenant-level
- `hfa:dag:tenant:<tenant_id>:ready`
  - ZSET of ready task members
  - score = ready timestamp / fairness timestamp
  - member = `dagtask:<task_id>`

### Task-level
- `hfa:task:<task_id>:state`
  - STRING
- `hfa:task:<task_id>:meta`
  - HASH
  - required fields:
    - `task_id`
    - `run_id`
    - `tenant_id`
    - `agent_type`
    - `priority`
    - `preferred_region`
    - `preferred_placement`
    - `admitted_at`
    - `estimated_cost_cents`
- `hfa:task:<task_id>:payload`
  - STRING / JSON
- `hfa:task:<task_id>:parents`
  - SET
- `hfa:task:<task_id>:children`
  - SET
- `hfa:task:<task_id>:remaining_deps`
  - STRING integer
- `hfa:task:<task_id>:ready_emitted`
  - STRING marker
- `hfa:task:<task_id>:attempts`
  - STRING integer

## Task state machine

Allowed states:
- `pending`
- `ready`
- `scheduled`
- `running`
- `done`
- `failed`
- `skipped`
- `blocked_by_failure`
- `dead_lettered`

Core legal transitions for V23.1:
- `pending -> ready`
- `ready -> scheduled`
- `scheduled -> running`
- `scheduled -> done`
- `running -> done`
- `running -> failed`
- `ready -> blocked_by_failure`
- `pending -> blocked_by_failure`

Terminal states:
- `done`
- `failed`
- `skipped`
- `blocked_by_failure`
- `dead_lettered`

## Readiness invariant

A task is READY iff:
- `remaining_deps == 0`
- task state is not terminal
- readiness marker not already emitted

## Why no single giant DAG JSON

Forbidden pattern:
- read full run graph JSON
- mutate one task
- write full JSON back

This creates:
- giant contention windows
- non-atomic graph updates
- hidden merge races
- Redis CPU blowups on large DAGs

Instead, graph topology is normalized into per-task keys.

## Atomic unlock engine

`task_complete.lua` is the V23.1 unlock engine.

### On terminal `done`
- mark task terminal
- update completion metadata
- iterate direct children
- decrement each child remaining deps
- emit READY exactly once when remaining deps reaches zero

### On terminal `failed`
- mark only the current task terminal
- do not recursively mutate descendants

## Why failure propagation is lazy

Recursive descendant cancellation inside Lua is prohibited because large DAGs would block Redis.

Instead:
- current task becomes `failed`
- run-level / higher-level orchestration can later infer blocked descendants
- dequeue-time checks may convert descendants to `blocked_by_failure`

## Scheduler contract for V23.2

SchedulerLoop must not inspect child topology.
It must only pull from the tenant READY queue and dispatch READY tasks.

## Required next scripts

### V23.2
- `task_admit.lua`
  - create task graph atomically
  - seed root tasks into READY queue

### V23.3
- `task_schedule.lua`
  - task-level dispatch commit

### V23.4
- `task_block_by_failure.lua`
  - lazy failure marking helper

## Operational alarms

Must alert on:
- READY queue growth without dispatch progress
- tasks stuck in `scheduled` or `running`
- run with zero READY tasks and zero terminal progress for too long
- repeated `child_missing` from unlock script
- repeated `already_terminal` completion retries beyond threshold

## Final stance

V23.1 should not attempt full DAG orchestration.
It should establish:
- normalized graph schema
- task state machine
- atomic direct-child unlock engine
- strict idempotency

That is the smallest production-safe vertical slice.

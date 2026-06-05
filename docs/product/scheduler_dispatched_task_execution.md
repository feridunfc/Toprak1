# Scheduler-Dispatched Task Execution Binding

## Purpose

Sprint 39 binds tenant-scoped task execution to scheduler/dispatch output before the worker consumes the task.

Sprint 38 proved:

    serialized RunRequestedEvent -> WorkerConsumer._process_message -> FakeExecutor -> StateStore result/completion -> ack

Sprint 39 must prove:

    tenant task submit/enqueue -> scheduler/dispatch output -> WorkerConsumer processes dispatched message -> FakeExecutor -> StateStore result/completion -> result readable

The key difference is that the worker must not receive a manually injected message as the final product proof.

## Target claim

`TENANT_SCOPED_TASK_EXECUTION_BOUND_TO_SCHEDULER_DISPATCH_AND_WORKER_COMPLETION`

## Required lifecycle

The verified lifecycle must include:

1. tenant task accepted
2. task queued through canonical path
3. scheduler/dispatch output created
4. `RunRequestedEvent` appears as dispatch/stream message
5. `WorkerConsumer` processes dispatched message
6. `FakeExecutor` runs through worker contract
7. `StateStore` result/completion path is used
8. result is readable

## Required successful evidence

A PASS artifact must include:

    {
      "status": "PASS",
      "tenant_task_submitted": true,
      "canonical_enqueue_used": true,
      "scheduler_dispatch_used": true,
      "dispatch_output_created": true,
      "run_requested_event_from_dispatch": true,
      "manual_worker_message_injection_used": false,
      "worker_consumer_process_message_used": true,
      "state_store_result_written": true,
      "state_store_mark_completed_called": true,
      "message_acknowledged": true,
      "result_readable": true,
      "fallback_used": false,
      "production_llm_call_attempted": false,
      "noncanonical_redis_mutation_attempted": false
    }

## Allowed mutation

Controlled task lifecycle mutation is allowed only through canonical task/runtime abstractions.

Allowed:

- canonical submit/enqueue
- canonical scheduler/dispatch output
- canonical worker claim/execute/complete
- canonical result/event write

## Forbidden operations

Sprint 39 must not:

- use direct `_process_message` message injection as the final product claim
- use ad-hoc Redis `xadd` outside canonical dispatch/helper path as the final claim
- use script-only fake queue as the final claim
- call production LLMs
- deploy
- create release tags
- expose operator action buttons
- perform noncanonical Redis mutation
- assert production-ready status

## Fallback/degraded handling

If no stable canonical scheduler dispatch API exists, the artifact must not claim PASS.

It must instead report a degraded state such as:

    {
      "status": "DEGRADED",
      "scheduler_dispatch_used": false,
      "manual_worker_message_injection_used": false,
      "fallback_used": true,
      "fallback_reason": "No stable canonical scheduler dispatch API discovered",
      "failing_reasons": [
        "scheduler dispatch entrypoint unavailable"
      ]
    }

## Required tests

Required test file:

- `tests/core/test_scheduler_dispatched_task_execution.py`

Tests must verify:

- PASS when dispatch path creates a `RunRequestedEvent` and `WorkerConsumer` completes it
- degraded/fail when dispatch output is missing
- safety violation if `manual_worker_message_injection_used=true`
- safety violation if production LLM call is attempted
- safety violation if noncanonical Redis mutation is attempted
- dashboard artifact is written

## Output artifact

The sprint writes:

- `docs/dashboard/artifacts/latest_scheduler_dispatched_task_execution.json`

## Non-goals

This sprint does not:

- run production LLM execution
- prove multi-tenant fairness under load
- prove production deployment readiness
- create release tags
- expose operator action controls
- implement long-running production worker service

## Governance rule

The worker must process scheduler/dispatch output.

If the proof manually injects a message into `WorkerConsumer._process_message` without scheduler/dispatch involvement, Sprint 39 must not claim PASS.

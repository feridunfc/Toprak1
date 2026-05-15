# Command / Event / Effect Taxonomy

## Command

A command is an intent to change system state.  Commands may validate input,
reserve capacity, or request execution.  A command does not directly become
truth until the authoritative event path accepts it.

Examples:

- admit a task
- schedule a task
- claim a task
- complete a task
- fail a task

## Event

An event is durable authoritative history.  Replay consumes events to rebuild
truth.  Terminal truth requires an event in gated paths.

Examples:

- `TASK_ADMITTED`
- `TASK_SCHEDULED`
- `TASK_CLAIMED`
- `TASK_COMPLETED`
- `TASK_FAILED`
- `TASK_REQUEUED`

## Effect

An effect is external or operational work performed by a worker, LLM, sandbox,
semantic runtime, or sidecar.  Effects may produce outputs and observations, but
are not terminal truth until accepted by the authoritative event path.

Examples:

- LLM call
- code execution
- test execution
- semantic query
- payload persistence
- feedback observation

## Sprint 1 sealing rule

With `IRON_V3_EVENT_GATE=1`, terminal completion in `StateStore.complete_once`
uses an event-before-projection gate.  If the event cannot be appended, the
projection write is blocked and the result is reported as `event_gate_blocked`.

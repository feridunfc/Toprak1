# Worker/Scheduler Health Signal Artifacts

## Purpose

This contract defines read-only worker heartbeat and scheduler/control visibility artifacts for dashboard runtime health.

Sprint 35 introduced the Dashboard Runtime Health Panel. Sprint 36 adds explicit worker and scheduler signal artifacts so the runtime health panel can rely on canonical dashboard evidence rather than indirect inference.

Worker/scheduler health signals are read-only dashboard evidence artifacts. They must not connect to Redis for mutation, mutate runtime state, requeue tasks, auto-resume workers, deploy, create release tags, or expose operator actions.

## Scope

This contract defines two read-only signal artifacts:

- worker health signal
- scheduler/control signal

These artifacts may summarize existing read-only evidence from recovery, drill, guardrail, and dashboard artifacts.

They do not authorize runtime actions.

## Outputs

The worker health signal should write:

- `docs/dashboard/artifacts/latest_worker_health_signal.json`

The scheduler/control signal should write:

- `docs/dashboard/artifacts/latest_scheduler_control_signal.json`

## Worker health signal shape

Minimum expected shape:

    {
      "source": "worker_health_signal",
      "status": "PASS",
      "signal_status": "WORKER_HEARTBEAT_VISIBLE",
      "worker_heartbeat_visible": true,
      "evidence_source": "artifacts",
      "actionable": false,
      "actions": [],
      "redis_mutation_attempted": false,
      "runtime_mutation_attempted": false,
      "requeue_attempted": false,
      "auto_resume_attempted": false,
      "failing_reasons": []
    }

## Scheduler/control signal shape

Minimum expected shape:

    {
      "source": "scheduler_control_signal",
      "status": "PASS",
      "signal_status": "SCHEDULER_CONTROL_VISIBLE",
      "scheduler_control_visible": true,
      "evidence_source": "artifacts",
      "actionable": false,
      "actions": [],
      "redis_mutation_attempted": false,
      "runtime_mutation_attempted": false,
      "requeue_attempted": false,
      "auto_resume_attempted": false,
      "failing_reasons": []
    }

## Inputs

Initial worker health signal may read:

- `docs/dashboard/artifacts/latest_recovery_audit.json`
- `docs/dashboard/artifacts/latest_recovery_requeue.json`
- `docs/dashboard/artifacts/latest_recovery_requeue_drill.json`
- `docs/dashboard/artifacts/latest_zombie_completion_drill.json`

Initial scheduler/control signal may read:

- `docs/dashboard/artifacts/latest_recovery_auto_resume_guardrail.json`
- `docs/dashboard/artifacts/latest_cold_restart_drill.json`
- `docs/dashboard/artifacts/latest_zombie_completion_drill.json`

Future sprints may replace indirect evidence with dedicated heartbeat/control artifacts.

## Signal statuses

Worker signal statuses:

- `WORKER_HEARTBEAT_VISIBLE`
- `WORKER_HEARTBEAT_DEGRADED`
- `WORKER_HEALTH_INVALID`
- `SAFETY_VIOLATION`

Scheduler/control signal statuses:

- `SCHEDULER_CONTROL_VISIBLE`
- `SCHEDULER_CONTROL_DEGRADED`
- `SCHEDULER_CONTROL_INVALID`
- `SAFETY_VIOLATION`

## Status mapping

The signal artifacts must map evidence states as follows:

- sufficient PASS or acceptable SKIPPED source evidence -> visible
- missing source evidence -> degraded
- malformed source evidence -> invalid
- source FAIL -> degraded
- `actions` present -> `SAFETY_VIOLATION`
- `actionable=true` -> `SAFETY_VIOLATION`
- `redis_mutation_attempted=true` -> `SAFETY_VIOLATION`
- `runtime_mutation_attempted=true` -> `SAFETY_VIOLATION`
- `requeue_attempted=true` -> `SAFETY_VIOLATION`
- `auto_resume_attempted=true` -> `SAFETY_VIOLATION`

Historical drill artifacts that report controlled dry-run or proof-mode behavior must be treated as evidence, not action authorization.

## Safety invariants

The signal artifacts are read-only.

They must always expose:

- `actionable=false`
- `actions=[]`
- `redis_mutation_attempted=false`
- `runtime_mutation_attempted=false`
- `requeue_attempted=false`
- `auto_resume_attempted=false`

They must not:

- connect to Redis for mutation
- mutate Redis
- mutate runtime state
- mutate canonical state
- requeue tasks
- auto-resume workers or queues
- deploy
- create release tags
- expose retry, requeue, recovery, deploy, release, tag, promote, approval, mutation, or auto-enforcement actions
- assert production-ready status

## Non-goals

This contract does not:

- implement live worker control
- implement live scheduler control
- implement task requeue
- implement auto-resume
- implement recovery execution
- implement deployment
- create release tags
- authorize runtime mutation
- enable production auto-enforcement
- assert production-ready status

## Governance rule

Worker/scheduler health signals are dashboard evidence only.

They may inform operator visibility but must never become an authority source, command path, or mutation path.

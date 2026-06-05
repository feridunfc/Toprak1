# Dashboard Runtime Health Panel

## Purpose

This contract defines a read-only dashboard panel for runtime health visibility.

Sprint 35 moves the operator dashboard beyond release-candidate evidence display and into runtime observability. The panel summarizes runtime health evidence so an operator can understand whether key runtime signals are visible, current, and healthy.

The dashboard may display runtime health evidence, but it must not trigger recovery, requeue, auto-resume, deployment, release tagging, Redis mutation, runtime mutation, or canonical state mutation.

## Scope

The Runtime Health Panel is a read-only presentation and artifact summarization surface.

It may display:

- Redis reachability status
- worker heartbeat visibility
- scheduler/control-plane visibility
- recovery audit status
- replay status
- authority status
- stale, missing, malformed, skipped, or failing evidence reasons
- read-only safety badges

It must not expose operator actions.

## Inputs

The panel should prefer existing dashboard artifacts and read-only evidence files.

Initial input artifacts may include:

- `docs/dashboard/artifacts/latest_authority.json`
- `docs/dashboard/artifacts/latest_replay.json`
- `docs/dashboard/artifacts/latest_redis_failover_smoke.json`
- `docs/dashboard/artifacts/latest_recovery_audit.json`
- `docs/dashboard/artifacts/latest_recovery_requeue.json`
- `docs/dashboard/artifacts/latest_recovery_requeue_drill.json`
- `docs/dashboard/artifacts/latest_cold_restart_drill.json`
- `docs/dashboard/artifacts/latest_zombie_completion_drill.json`
- `docs/dashboard/artifacts/latest_recovery_auto_resume_guardrail.json`

The panel may also include future read-only worker heartbeat or scheduler/control artifacts when they exist.

The panel must not connect to Redis directly unless a future sprint explicitly defines a read-only smoke/artifact flow. Runtime health visibility should come from artifacts, not direct mutation-capable runtime access.

## Output

The panel read model should expose a dashboard-facing object with this minimum shape:

    {
      "source": "dashboard_runtime_health_panel",
      "status": "PASS",
      "title": "Runtime Health",
      "panel_status": "RUNTIME_HEALTH_VISIBLE",
      "redis_reachable": true,
      "worker_heartbeat_visible": true,
      "scheduler_control_visible": true,
      "recovery_audit_status": "PASS",
      "replay_status": "PASS",
      "authority_status": "PASS",
      "actionable": false,
      "actions": [],
      "badges": [
        "READ_ONLY",
        "NO_RUNTIME_MUTATION",
        "NO_OPERATOR_ACTIONS"
      ],
      "failing_reasons": []
    }

## Panel statuses

The panel may emit these status values:

- `RUNTIME_HEALTH_VISIBLE`
- `RUNTIME_HEALTH_DEGRADED`
- `PANEL_UNAVAILABLE`
- `PANEL_INVALID`
- `SAFETY_VIOLATION`

## Status mapping

The panel must map evidence states as follows:

- all required visible runtime evidence PASS or acceptable read-only SKIPPED -> `RUNTIME_HEALTH_VISIBLE`
- missing authority artifact -> `RUNTIME_HEALTH_DEGRADED`
- malformed authority artifact -> `PANEL_INVALID`
- missing replay artifact -> `RUNTIME_HEALTH_DEGRADED`
- malformed replay artifact -> `PANEL_INVALID`
- replay FAIL -> `RUNTIME_HEALTH_DEGRADED`
- missing recovery audit artifact -> `RUNTIME_HEALTH_DEGRADED`
- malformed recovery audit artifact -> `PANEL_INVALID`
- recovery audit FAIL -> `RUNTIME_HEALTH_DEGRADED`
- Redis smoke FAIL -> `RUNTIME_HEALTH_DEGRADED`
- Redis smoke missing -> `RUNTIME_HEALTH_DEGRADED`
- worker heartbeat unavailable or missing -> `RUNTIME_HEALTH_DEGRADED`
- scheduler/control visibility unavailable or missing -> `RUNTIME_HEALTH_DEGRADED`
- any action exposed -> `SAFETY_VIOLATION`
- any mutation/deploy/release-tag flag true -> `SAFETY_VIOLATION`

## Safety invariants

The panel is read-only.

The panel:

- does not trigger recovery
- does not requeue tasks
- does not auto-resume workers or queues
- does not deploy
- does not create release tags
- does not mutate Redis
- does not mutate runtime state
- does not mutate canonical state
- does not expose retry, requeue, deploy, release, tag, promote, approval, mutation, or auto-enforcement actions
- does not assert production-ready status

The panel must always expose:

- `actionable=false`
- `actions=[]`
- `READ_ONLY`
- `NO_RUNTIME_MUTATION`
- `NO_OPERATOR_ACTIONS`

## Forbidden UI actions

The panel must not render action buttons, links, forms, or controls for:

- Deploy
- Promote
- Promote to production
- Create tag
- Create release tag
- Release
- Auto resume
- Requeue
- Retry
- Retry mutation
- Recover
- Run recovery
- Approve deployment
- Enable production
- Run release
- Mutate Redis
- Restart worker
- Resume scheduler

## Degraded rendering rule

If runtime health evidence is missing, malformed, stale, skipped, or failing, the dashboard must render a degraded, non-actionable panel.

The panel must not hide missing or failing evidence. It must show failing reasons to the operator.

## Non-goals

This contract does not:

- implement runtime recovery
- implement task requeue
- implement auto-resume
- implement direct Redis health mutation
- implement deployment
- create release tags
- authorize runtime mutation
- enable production auto-enforcement
- assert production-ready status

## Governance rule

The Runtime Health Panel is a read-only dashboard surface.

The panel may summarize runtime health evidence, but it must never become an authority source or mutation path.

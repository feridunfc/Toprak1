# Sprint 22A — Recovery Auto-Resume Guardrail Contract

Sprint 22A locks the recovery auto-resume production guardrail.

## Goal

The recovery pipeline now has:

- read-only recovery audit
- proof-gated recovery requeue command
- artifact-backed proof decision
- Redis-backed single-task mutation drill
- cold restart drill
- zombie completion rejection drill

However, an automatic recovery daemon must remain disabled until an explicit production enablement sprint.

## Required invariants

- No background auto-recovery daemon is enabled by default.
- No startup path may automatically requeue tasks without an explicit operator command.
- Single-task recovery remains available only through explicit proof-gated commands.
- Production auto-enable requires a future dedicated sprint and checklist approval.
- CI may emit recovery artifacts, but CI must not perform real Redis mutation drills unless explicitly configured with real Redis.

## Allowed recovery entry points

Allowed:

- `scripts/recovery_audit.py`
- `scripts/recovery_requeue.py`
- `scripts/recovery_requeue_drill.py`
- `scripts/cold_restart_drill.py`
- `scripts/zombie_completion_drill.py`

Not allowed by default:

- background recovery loop
- startup auto-resume
- automatic stale-task requeue daemon
- production auto-enable flag

## Production readiness status

Recovery is drill-proven but not auto-enabled.

The next production enablement decision must be explicit and reviewable.

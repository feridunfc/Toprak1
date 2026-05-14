
# Faz 4A — Scheduler-Side Capability Filtering

## Goal
Filter incompatible workers at the scheduler layer before fairness scoring and reservation dispatch.

## What this pack adds
- `SchedulerCapabilitySelector`
- deterministic compatible worker filtering
- rejected-worker diagnostics (`missing_by_worker`)
- core + integration tests

## Rules
- incompatible workers never enter candidate selection
- compatible workers are preserved deterministically
- worker-side reject remains a fallback, not the primary routing mechanism

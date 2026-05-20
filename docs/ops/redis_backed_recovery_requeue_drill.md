# Sprint 19 — Redis-Backed Recovery Requeue Drill Contract

Sprint 19 verifies the real Redis-backed recovery requeue mutation path for a single stale running task.

## Goal

Validate that artifact-backed proof can authorize one controlled recovery requeue mutation through the canonical recovery manager path.

This sprint does not enable an automatic recovery daemon.

## Required preconditions

A recovery requeue drill may mutate Redis only when all of the following are true:

1. A stale running candidate exists.
2. Recovery audit classifies the run/task as stale, missing-claim, or expired-claim.
3. Artifact-backed proof allows auto-resume.
4. Replay evidence is clean/pass.
5. Authority artifact is clean/pass and has no banned findings.
6. The requested tenant matches the candidate metadata.
7. Mutation is routed only through `TaskRecoveryManager.requeue_stale_task(...)`.

## Required drill behavior

The drill must:

- Seed or use a single stale running candidate.
- Generate/consume proof artifacts.
- Execute `recovery_requeue.py --proof-mode artifacts` without dry-run.
- Emit a drill artifact.
- Confirm that mutation was attempted only after proof passed.
- Confirm that fail-closed paths perform no mutation.

## Required artifact

`docs/dashboard/artifacts/latest_recovery_requeue_drill.json` must include:

- source
- mode
- run_id/task_id
- tenant_id
- proof decision
- candidate status
- mutation attempted
- requeue result
- pre-state summary
- post-state summary
- notes

## Non-goals

- No automatic recovery loop.
- No background daemon.
- No Redis Sentinel/cluster failover.
- No production auto-enable.

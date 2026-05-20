# Sprint 21 — Zombie Completion Rejection Drill Contract

Sprint 21 verifies that stale worker completion is rejected after recovery requeue.

## Goal

Validate fencing behavior across recovery:

1. A task is running with an old worker identity and claim epoch.
2. Recovery requeue moves the task back to ready.
3. `claim_epoch` remains monotonic and is not reset by requeue.
4. A stale/zombie completion from the old owner/fence must be rejected.
5. No automatic recovery daemon is enabled.

## Required invariants

- Requeue must not reset `claim_epoch`.
- Requeue must clear stale worker identity fields.
- Old owner completion must fail after requeue.
- The rejection reason must be captured in an artifact.
- Mutation path remains canonical.
- Recovery audit and proof gates remain fail-closed.

## Required artifact

`docs/dashboard/artifacts/latest_zombie_completion_drill.json` must include:

- source
- status
- task id
- tenant id
- pre-requeue claim epoch
- post-requeue claim epoch
- stale worker identity
- zombie completion attempted
- zombie completion accepted/rejected
- rejection reason/status
- notes

## Non-goals

- No automatic recovery daemon.
- No production auto-enable.
- No background worker orchestration.

# Sprint 20 — Staging Cold Restart Drill Contract

Sprint 20 validates the recovery path under a controlled cold restart scenario.

## Goal

Verify that a stale in-flight task can be recovered after restart using the already established chain:

1. recovery audit
2. artifact-backed proof
3. proof-gated requeue
4. Redis-backed canonical mutation path

This sprint does not enable an automatic recovery daemon.

## Required scenario

The drill must model:

- a task in `running`
- a stale worker identity
- a persisted claim/fence generation
- control/worker restart boundary
- recovery audit candidate detection
- artifact-backed proof approval
- canonical requeue mutation
- stale/zombie owner completion rejection

## Required invariants

- Recovery audit remains read-only.
- Requeue mutation uses `TaskRecoveryManager.requeue_stale_task(...)`.
- `claim_epoch` must remain monotonic across requeue.
- Old owner/fence completion must not be accepted after requeue.
- No automatic daemon is enabled.
- The drill must emit an artifact.

## Required artifact

`docs/dashboard/artifacts/latest_cold_restart_drill.json` must include:

- source
- status
- run/task id
- tenant id
- pre-restart state
- recovery audit result
- proof decision
- requeue result
- post-requeue state
- zombie completion rejection result
- notes

## Non-goals

- No production auto-enable.
- No background recovery loop.
- No Redis Sentinel/cluster failover.

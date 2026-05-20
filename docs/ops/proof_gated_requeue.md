# Sprint 17 — Proof-Gated Recovery Requeue Contract

Sprint 17 introduces a controlled single-task recovery requeue command.

## Scope

This sprint does not enable an automatic recovery loop.

It only allows an explicit, proof-gated requeue path:

1. Read recovery candidates from the existing recovery audit model.
2. Require replay/runtime proof to allow auto-resume.
3. If proof is ambiguous or dirty, do not mutate Redis.
4. If proof allows auto-resume, call the existing canonical `TaskRecoveryManager.requeue_stale_task(...)` path.
5. Emit a recovery requeue artifact.

## Required invariants

- Recovery audit remains read-only.
- Requeue mutation must go through existing Lua-backed `TaskRecoveryManager.requeue_stale_task(...)`.
- Proof denial must be fail-closed.
- Missing candidate must be fail-closed.
- Artifact must include:
  - source
  - mode
  - status
  - run/task id
  - proof decision
  - mutation attempted
  - requeue result

## Non-goals

- No background/automatic recovery daemon.
- No Redis Sentinel/cluster failover.
- No bypass of replay evidence gate.
- No direct Redis state mutation outside canonical requeue path.

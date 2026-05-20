# Sprint 18 — Artifact-Backed Recovery Proof Contract

Sprint 18 connects proof-gated recovery requeue decisions to generated evidence artifacts.

## Goal

`recovery_requeue.py` must be able to evaluate recovery proof from existing dashboard artifacts instead of relying only on manual flags.

Required proof inputs:

- `docs/dashboard/artifacts/latest_replay.json`
- `docs/dashboard/artifacts/latest_authority.json`
- `docs/dashboard/artifacts/latest_recovery_audit.json`

## Required behavior

The proof decision must fail closed.

Recovery requeue is allowed only when:

1. Replay artifact exists and reports a clean/pass status.
2. Authority artifact exists and reports no banned/dirty authority findings.
3. Recovery audit artifact exists and contains the requested recovery candidate.
4. The requested run/task is still classified as stale, missing-claim, or expired-claim.
5. No proof artifact is missing, malformed, or ambiguous.

## Non-goals

- No automatic recovery loop.
- No direct Redis mutation outside `TaskRecoveryManager.requeue_stale_task(...)`.
- No production auto-enable.
- No staging drill mutation yet; that is Sprint 19.

## Artifact-backed proof output

`latest_recovery_requeue.json` must include:

- proof source mode: manual or artifact-backed
- replay artifact status
- authority artifact status
- recovery audit candidate status
- final proof decision
- fail-closed reason when blocked

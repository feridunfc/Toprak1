# Dashboard-B — Read-only Evidence Panels

## Goal

Dashboard-B extends Dashboard-A with additional read-only evidence panels:

- Replay Integrity
- Quarantine Snapshot
- Artifact / Claim-Check Vault

No mutation, approval, rejection, event append, or Redis write is introduced.

## Backend Endpoints

New GET-only endpoints:

- `GET /dashboard/replay`
- `GET /dashboard/quarantine`
- `GET /dashboard/artifacts`

These read models are intentionally conservative. They expose readiness and
snapshot metadata without executing replay, mutating quarantine state, or reading
sealed artifact payload bodies.

## Frontend

The React UI adds three evidence cards above the existing authority heatmap:

- Replay Integrity
- Quarantine Snapshot
- Artifact Vault

All action text remains disabled/read-only.

## Invariants

- GET-only endpoints
- no Redis writes
- no event append
- no approve/reject/action buttons
- no runtime/control-plane mutation imports
- no artifact payload body reads in the MVP

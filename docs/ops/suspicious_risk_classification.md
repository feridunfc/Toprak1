# Sprint 10 â€” Suspicious Risk Classification

## Goal

Sprint 10 does not reduce suspicious findings by marking them allowed. It adds
review-oriented classes so the remaining authority audit backlog is easier to
understand, prioritize, and display in the dashboard.

## Classification Field

Each `AuditFinding` now includes `suspicious_class`.

Allowed and banned findings use:

- `not_applicable`

Suspicious findings can be classified as:

- `governance_local`
- `observability_only`
- `lua_atomic_boundary`
- `lease_or_fencing`
- `rate_limit_or_admission`
- `recovery_or_reconciliation`
- `worker_or_effect_telemetry`
- `false_positive_static`
- `true_authority_review`

## Invariants

- No runtime/control-plane behavior changes.
- Banned count must remain zero on the current baseline.
- Suspicious count may remain unchanged.
- Classification is not an authority waiver.
- Dashboard displays the class but does not expose actions.

## Intended Follow-up

Sprint 10B can use these classes to burn down false positives and low-risk
observability findings without hiding true authority-review work.

## Sprint 10B — Static Noise Burn-down

`false_positive_static` findings remain `suspicious` so they stay visible in
audit output, but they no longer contribute to `risk_score`.

Dashboard payload now includes:

- `noise_count`
- `risk_bearing_suspicious`

This keeps the backlog transparent while separating broad static scanner noise
from risk-bearing authority review work.

## Sprint 10C — Observability-only Risk Separation

`observability_only` findings remain `suspicious` because they are real writes,
but they are non-terminal telemetry/observability paths. They now contribute a
reduced `risk_score` of `1` instead of `5`.

Dashboard payload now includes:

- `telemetry_risk_count`

`risk_bearing_suspicious` excludes both `false_positive_static` and
`observability_only`, while `suspicious` still includes all visible backlog.

## Sprint 10D — Lua Atomic Boundary Classification

Sprint 10D splits broad `lua_atomic_boundary` findings into more actionable
review buckets without changing risk score:

- `lua_authority_transition`
- `lua_scheduler_fallback`
- `lua_atomic_projection`

The goal is classification only. Lua findings remain visible and risk-bearing
until a later sprint reviews invariants per bucket.

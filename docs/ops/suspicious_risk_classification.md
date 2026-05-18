# Sprint 10 — Suspicious Risk Classification

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

# Sprint 9 — Authority Risk Burn-down

## Phase 1 Scope

This phase reduces only clearly reviewable banned findings:

- projection writes
- non-truth local retry/circuit state
- governance-local budget state
- Lua atomic/fallback boundaries
- worker registry projection state

## Explicitly Not Cleared In Phase 1

The following remain intentionally reviewed later because they affect scheduling
or task eligibility directly:

- scheduler quarantine writes
- failure_sweeper blocked_by_failure writes

## Acceptance

- Audit tool and smoke wrapper tests pass.
- Banned count decreases from the Sprint 8 baseline.
- Remaining banned findings are documented for follow-up.

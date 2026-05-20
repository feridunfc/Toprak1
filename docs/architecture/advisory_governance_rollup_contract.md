# Sprint 26 — Advisory Governance Rollup Contract

Sprint 26 consolidates advisory/cognitive governance proof artifacts into one rollup artifact.

## Goal

Create a single advisory governance rollup that confirms all advisory governance surfaces remain non-authoritative and pass their dedicated checks.

## Included artifacts

The rollup must evaluate:

- cognitive governance audit
- semantic advisory contract
- FeedbackWriter governance
- SemanticBridge gate enforcement

## Required invariants

- Cognitive/semantic/feedback surfaces remain advisory or feedback-only.
- FeedbackWriter remains non-authoritative and locally governed.
- SemanticBridge gate decisions remain structured and fail-closed.
- Canonical runtime truth authority remains in runtime/Lua/control-plane approved paths.
- The rollup must not mutate Redis, runtime task state, run state, scheduler state, recovery state, replay truth, or worker ownership.

## Output

The rollup writes:

`docs/dashboard/artifacts/latest_advisory_governance_rollup.json`

## Pass criteria

The rollup status is `PASS` only when:

- cognitive governance audit status is `PASS`
- semantic advisory contract status is `PASS`
- FeedbackWriter governance status is `PASS`
- SemanticBridge gate status is `PASS`
- no canonical authority write permission is granted to advisory surfaces

## Non-goals

- No new runtime behavior.
- No production auto-enforcement.
- No HITL workflow implementation.
- No canonical runtime state mutation.

# Sprint 28 — Production Readiness Decision Gate Contract

Sprint 28 converts the production readiness rollup artifact into an explicit decision artifact.

## Goal

Create a small, human-readable decision artifact that answers whether the current proof state is production-ready.

## Input

The decision gate consumes:

`docs/dashboard/artifacts/latest_production_readiness_rollup.json`

## Output

The decision gate writes:

`docs/dashboard/artifacts/latest_production_readiness_decision.json`

## Decision states

Allowed decision states:

- `READY`
- `NOT_READY`

## Required invariants

- The decision gate is read-only.
- The decision gate must not mutate Redis.
- The decision gate must not mutate runtime task state, run state, scheduler state, recovery state, replay truth, worker ownership, advisory/cognitive state, or production configuration.
- The decision must fail closed to `NOT_READY` when the rollup artifact is missing, malformed, or non-PASS.
- The decision must include explicit reasons and component summaries.
- Canonical runtime truth authority remains in runtime/Lua/control-plane approved paths.

## READY criteria

The decision is `READY` only when:

- production readiness rollup is present
- production readiness rollup status is `PASS`
- required rollup components are present
- no required component has a failing status

## Non-goals

- No production auto-deployment.
- No production auto-enforcement.
- No HITL workflow implementation.
- No automatic recovery daemon.
- No canonical runtime state mutation.

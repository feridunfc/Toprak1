# Sprint 30 — Staging Release Candidate Gate

Sprint 30 converts the production readiness decision and frozen evidence set into a staging release-candidate decision artifact.

## Goal

Create a read-only release-candidate gate that answers whether a staging release candidate may be opened.

## Inputs

The RC gate consumes:

- `docs/dashboard/artifacts/latest_production_readiness_decision.json`
- `docs/dashboard/artifacts/latest_production_readiness_evidence_freeze.json`

## Output

The RC gate writes:

`docs/dashboard/artifacts/latest_staging_release_candidate_gate.json`

## Decision states

Allowed RC states:

- `RC_ALLOWED`
- `RC_BLOCKED`

## Required invariants

- The RC gate is read-only.
- The RC gate must not mutate Redis.
- The RC gate must not mutate runtime task state, run state, scheduler state, recovery state, replay truth, worker ownership, advisory/cognitive state, release state, or production configuration.
- The RC gate must not deploy anything.
- The RC gate must not tag a release.
- The RC gate must not publish an artifact outside the dashboard evidence directory.
- Missing or malformed inputs fail closed to `RC_BLOCKED`.
- `decision != READY` fails closed to `RC_BLOCKED`.
- `evidence_freeze.status != PASS` fails closed to `RC_BLOCKED`.
- Missing evidence manifest hash fails closed to `RC_BLOCKED`.

## RC_ALLOWED criteria

The RC gate emits `RC_ALLOWED` only when:

- production readiness decision exists and reports `decision=READY`
- production readiness decision status is `PASS`
- evidence freeze exists and reports `status=PASS`
- evidence freeze reports `decision=READY`
- evidence freeze reports `required_artifacts_complete=true`
- evidence freeze includes a non-empty `evidence_manifest_hash`
- evidence freeze reports `mutation_attempted=false`

## Non-goals

- No deployment.
- No release tagging.
- No production auto-enforcement.
- No HITL workflow implementation.
- No automatic recovery daemon.
- No canonical runtime state mutation.

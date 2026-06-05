# Staging RC Readiness Boundary

## Purpose

This contract defines the boundary between an evidence-backed staging release candidate declaration and any production-ready or deployment claim.

Sprint 30 introduced the staging release candidate gate. That gate may produce `RC_ALLOWED` only when production readiness decision evidence and frozen evidence artifacts satisfy the required checks.

This boundary contract makes the scope explicit:

- `RC_ALLOWED` is not a production-ready claim.
- `RC_ALLOWED` is not a deployment.
- `RC_ALLOWED` does not create a release tag.
- `RC_ALLOWED` does not mutate Redis.
- `RC_ALLOWED` does not mutate runtime or canonical state.

The only valid interpretation is:

> evidence-backed staging release candidate declaration

## Inputs

The boundary depends on the following artifacts:

- `docs/dashboard/artifacts/latest_production_readiness_decision.json`
- `docs/dashboard/artifacts/latest_production_readiness_evidence_freeze.json`
- `docs/dashboard/artifacts/latest_staging_release_candidate_gate.json`

## Required upstream state

The boundary may pass only when:

- production readiness decision status is `PASS`
- production readiness decision is `READY`
- production readiness evidence freeze status is `PASS`
- production readiness evidence freeze decision is `READY`
- production readiness evidence freeze has a non-empty evidence manifest hash
- production readiness evidence freeze reports required artifacts complete
- staging release candidate gate status is `RC_ALLOWED`

## Safety invariants

The boundary must fail closed if any of the following is true:

- production-ready claim is asserted
- deployment is attempted
- release tag is created
- Redis mutation is attempted
- runtime mutation is attempted
- canonical state mutation is attempted
- required evidence is missing
- required evidence is malformed
- required evidence is non-PASS
- staging RC gate is missing or not `RC_ALLOWED`

## Output

The boundary artifact should be:

- `docs/dashboard/artifacts/latest_staging_rc_readiness_boundary.json`

Expected PASS shape:

    {
      "status": "PASS",
      "decision_scope": "EVIDENCE_BACKED_STAGING_RELEASE_CANDIDATE_ONLY",
      "production_ready_claim": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "redis_mutation_attempted": false,
      "runtime_mutation_attempted": false,
      "canonical_state_mutation_attempted": false
    }

## Non-goals

This contract does not:

- deploy to production
- create a release tag
- mutate Redis
- mutate runtime state
- mutate canonical state
- enable auto-enforcement
- convert staging RC declaration into a production-ready claim

## Governance rule

Authority Gate may publish this boundary artifact only after:

1. production readiness decision artifact
2. production readiness evidence freeze artifact
3. staging release candidate gate artifact

The boundary artifact is read-only evidence. It is not a release action.

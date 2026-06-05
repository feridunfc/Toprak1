# Staging RC Evidence Index

## Purpose

This contract defines a read-only evidence index for an evidence-backed staging release candidate declaration.

The index summarizes the existing readiness and release-candidate artifact chain in one audit-friendly artifact. It does not create new runtime authority and does not convert a staging release candidate declaration into a production-ready claim.

The only valid interpretation is:

> evidence-backed staging release candidate declaration indexed for audit

## Scope

The Staging RC Evidence Index reads existing artifacts and produces a single summary artifact.

It may read:

- `docs/dashboard/artifacts/latest_production_readiness_rollup.json`
- `docs/dashboard/artifacts/latest_production_readiness_decision.json`
- `docs/dashboard/artifacts/latest_production_readiness_evidence_freeze.json`
- `docs/dashboard/artifacts/latest_staging_release_candidate_gate.json`
- `docs/dashboard/artifacts/latest_staging_rc_readiness_boundary.json`

It may write:

- `docs/dashboard/artifacts/latest_staging_rc_evidence_index.json`

## Required upstream state

The index may pass only when:

- production readiness rollup status is `PASS`
- production readiness decision status is `PASS`
- production readiness decision is `READY`
- production readiness evidence freeze status is `PASS`
- production readiness evidence freeze decision is `READY`
- production readiness evidence freeze required artifacts are complete
- production readiness evidence freeze has a non-empty evidence manifest hash
- production readiness evidence freeze mutation flag is false
- staging RC gate status is `PASS`
- staging RC gate decision is `RC_ALLOWED`
- staging RC readiness boundary status is `PASS`
- staging RC readiness boundary decision scope is `EVIDENCE_BACKED_STAGING_RELEASE_CANDIDATE_ONLY`

## Safety invariants

The index must fail closed if any required artifact is:

- missing
- malformed
- non-PASS
- NOT_READY
- RC_BLOCKED
- scope-violating

The index must also fail closed if any generated or observed release-safety flag indicates:

- production-ready claim
- deployment attempted
- release tag created
- Redis mutation attempted
- runtime mutation attempted
- canonical state mutation attempted

## Output

The output artifact should be:

- `docs/dashboard/artifacts/latest_staging_rc_evidence_index.json`

Expected PASS shape:

    {
      "source": "staging_rc_evidence_index",
      "status": "PASS",
      "decision_scope": "EVIDENCE_BACKED_STAGING_RELEASE_CANDIDATE_ONLY",
      "staging_rc_indexed": true,
      "production_ready_claim": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "redis_mutation_attempted": false,
      "runtime_mutation_attempted": false,
      "canonical_state_mutation_attempted": false,
      "production_readiness_rollup_status": "PASS",
      "production_readiness_decision_status": "PASS",
      "production_readiness_decision": "READY",
      "evidence_freeze_status": "PASS",
      "evidence_freeze_decision": "READY",
      "staging_rc_gate_status": "PASS",
      "staging_rc_decision": "RC_ALLOWED",
      "staging_rc_boundary_status": "PASS",
      "evidence_manifest_hash": "...",
      "indexed_artifacts_count": 0,
      "indexed_artifacts": []
    }

## Indexed artifacts

The index should include deterministic entries for required upstream artifacts.

Each entry should include at minimum:

- artifact name
- artifact path
- present
- valid_json
- status or decision fields when applicable
- sha256

The list must be stable-sorted by artifact name or explicit contract order.

## Non-goals

This contract does not:

- deploy to production
- create a release tag
- mutate Redis
- mutate runtime state
- mutate canonical state
- enable automatic production enforcement
- assert production-ready status

## Governance rule

Authority Gate may publish this index artifact only after:

1. production readiness rollup artifact
2. production readiness decision artifact
3. production readiness evidence freeze artifact
4. staging RC gate artifact
5. staging RC readiness boundary artifact

The index artifact is read-only evidence. It is not a release action.

# Operator Dashboard RC Evidence Panel

## Purpose

This contract defines a read-only operator dashboard panel for the indexed staging release candidate evidence chain.

Sprint 32 introduced the Staging RC Evidence Index. Sprint 33 exposes that index as an operator-facing read model so operators can understand the current staging RC evidence state without triggering deployment, release tagging, Redis mutation, runtime mutation, or canonical state mutation.

The panel answers these operator questions:

- Is the staging RC evidence indexed?
- Is the production readiness decision READY?
- Is the staging RC gate RC_ALLOWED?
- Did the staging RC readiness boundary pass?
- Is an evidence manifest hash present?
- Is this explicitly not a production deployment?
- If the panel is degraded or blocked, which evidence reason explains it?

## Single source of truth

The panel has exactly one input artifact:

- `docs/dashboard/artifacts/latest_staging_rc_evidence_index.json`

The panel must not directly read or recompute:

- production readiness rollup
- production readiness decision
- production readiness evidence freeze
- staging RC gate
- staging RC readiness boundary
- Redis state
- runtime state
- canonical state

All dashboard state must be derived from the staging RC evidence index.

## Output

The panel read model should expose an operator-facing object with this minimum shape:

    {
      "source": "operator_rc_evidence_panel",
      "status": "PASS",
      "panel_status": "RC_ALLOWED_INDEXED",
      "decision_scope": "EVIDENCE_BACKED_STAGING_RELEASE_CANDIDATE_ONLY",
      "badges": [
        "READY",
        "RC_ALLOWED",
        "BOUNDARY_PASS",
        "INDEXED",
        "NOT_PRODUCTION_DEPLOYMENT"
      ],
      "operator_message": "Staging RC evidence is indexed and allowed. This is not a production deployment.",
      "actionable": false,
      "input_artifact": "docs/dashboard/artifacts/latest_staging_rc_evidence_index.json",
      "production_ready_claim": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "redis_mutation_attempted": false,
      "runtime_mutation_attempted": false,
      "evidence_manifest_hash": "...",
      "indexed_artifacts_count": 5,
      "failing_reasons": []
    }

## Panel statuses

The panel may emit these status values:

- `RC_ALLOWED_INDEXED`
- `PANEL_UNAVAILABLE`
- `PANEL_INVALID`
- `RC_BLOCKED_OR_NOT_READY`
- `RC_NOT_INDEXED`
- `SAFETY_VIOLATION`

## Status mapping

The panel must map evidence states as follows:

- missing index artifact -> `PANEL_UNAVAILABLE`
- malformed index artifact -> `PANEL_INVALID`
- index status `FAIL` -> `RC_BLOCKED_OR_NOT_READY`
- `staging_rc_indexed=false` -> `RC_NOT_INDEXED`
- missing evidence manifest hash -> `RC_BLOCKED_OR_NOT_READY`
- invalid decision scope -> `SAFETY_VIOLATION`
- `production_ready_claim=true` -> `SAFETY_VIOLATION`
- `deployment_attempted=true` -> `SAFETY_VIOLATION`
- `release_tag_created=true` -> `SAFETY_VIOLATION`
- `redis_mutation_attempted=true` -> `SAFETY_VIOLATION`
- `runtime_mutation_attempted=true` -> `SAFETY_VIOLATION`
- `canonical_state_mutation_attempted=true` -> `SAFETY_VIOLATION`
- complete indexed RC evidence chain -> `RC_ALLOWED_INDEXED`

## Safety invariants

The panel is read-only.

The panel:

- reads only `latest_staging_rc_evidence_index.json`
- does not recompute production readiness
- does not trigger deployment
- does not create release tags
- does not mutate Redis
- does not mutate runtime state
- does not mutate canonical state
- does not expose approval, retry, release, deploy, tag, mutation, or auto-enforcement actions

The panel must display `NOT_PRODUCTION_DEPLOYMENT` whenever RC evidence is shown.

The panel must surface failing artifact reasons when the index is unavailable, invalid, failing, or safety-violating.

The panel must fail closed into a non-actionable degraded state when evidence is missing or malformed.

## Operator UX requirements

When the index passes, the panel should show:

- `READY`
- `RC_ALLOWED`
- `BOUNDARY_PASS`
- `INDEXED`
- `NOT_PRODUCTION_DEPLOYMENT`

When the index fails or is degraded, the panel should show:

- the panel status
- the operator message
- the failing reasons from the index when available
- `actionable=false`

## Non-goals

This contract does not:

- deploy to production
- create a release tag
- mutate Redis
- mutate runtime state
- mutate canonical state
- authorize runtime mutation
- enable automatic production enforcement
- assert production-ready status

## Governance rule

The operator panel is a read-only presentation surface over the Staging RC Evidence Index.

The panel may be rendered only after `latest_staging_rc_evidence_index.json` exists and is parsed.

The panel must never become an authority source. The single source of truth remains the evidence index artifact.

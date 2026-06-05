# Operator Dashboard RC Panel UI Wiring

## Purpose

This contract defines how the Operator Dashboard renders the operator RC evidence panel as a read-only UI surface.

Sprint 33 introduced the operator RC evidence panel read model. Sprint 34 wires that read model into the dashboard so an operator can understand staging RC evidence status without triggering deployment, release tagging, Redis mutation, runtime mutation, canonical state mutation, or any operator-triggered release action.

The dashboard may display staging RC evidence, but it must not create a production-ready claim, deployment permission, release tag, runtime mutation, or operator-triggered release action.

## Single source of truth

The UI has exactly one input artifact:

- `docs/dashboard/artifacts/latest_operator_rc_evidence_panel.json`

The UI must not directly read or recompute:

- production readiness rollup
- production readiness decision
- production readiness evidence freeze
- staging RC gate
- staging RC readiness boundary
- staging RC evidence index
- Redis state
- runtime state
- canonical state

All dashboard UI state for this panel must be derived from `latest_operator_rc_evidence_panel.json`.

## Required UI fields

The dashboard panel should render:

- title: `Staging RC Evidence`
- `panel_status`
- `badges`
- `evidence_manifest_hash`
- `indexed_artifacts_count`
- `operator_message`
- `actionable`
- `failing_reasons`

Minimum rendered model:

    {
      "title": "Staging RC Evidence",
      "panel_status": "RC_ALLOWED_INDEXED",
      "badges": [
        "READY",
        "RC_ALLOWED",
        "BOUNDARY_PASS",
        "INDEXED",
        "NOT_PRODUCTION_DEPLOYMENT"
      ],
      "evidence_manifest_hash": "...",
      "indexed_artifacts_count": 5,
      "operator_message": "Staging RC evidence is indexed and allowed. This is not a production deployment.",
      "actionable": false,
      "failing_reasons": []
    }

## Required badge

The dashboard must always show:

- `NOT_PRODUCTION_DEPLOYMENT`

whenever operator RC evidence is rendered.

## Degraded states

The UI must render degraded states as read-only and non-actionable.

The UI must support:

- `PANEL_UNAVAILABLE`
- `PANEL_INVALID`
- `RC_BLOCKED_OR_NOT_READY`
- `RC_NOT_INDEXED`
- `SAFETY_VIOLATION`
- `RC_ALLOWED_INDEXED`

When the panel status is degraded or failing, the UI must show:

- the panel status
- the operator message
- failing reasons when present
- `actionable=false`
- `NOT_PRODUCTION_DEPLOYMENT`

## Forbidden UI actions

The panel must not render action buttons, links, forms, or controls for:

- Deploy
- Promote
- Promote to production
- Create tag
- Create release tag
- Release
- Auto resume
- Requeue
- Retry mutation
- Approve deployment
- Enable production
- Run release

## Safety invariants

The UI is read-only.

The UI:

- reads only `latest_operator_rc_evidence_panel.json`
- does not recompute readiness
- does not recompute RC state
- does not trigger deployment
- does not create release tags
- does not mutate Redis
- does not mutate runtime state
- does not mutate canonical state
- does not expose operator action buttons
- does not enable automatic production enforcement
- does not assert production-ready status

## Rendering rule

If the input artifact is missing, malformed, failing, degraded, or safety-violating, the UI must render a non-actionable degraded panel instead of hiding the failure.

## Non-goals

This contract does not:

- implement production deployment
- create a release tag
- add release controls
- add promotion controls
- mutate Redis
- mutate runtime state
- mutate canonical state
- authorize runtime mutation
- enable automatic production enforcement
- assert production-ready status

## Governance rule

The Operator Dashboard RC Panel is a presentation layer only.

The panel read model remains the source of dashboard state. The dashboard UI must never become an authority source.

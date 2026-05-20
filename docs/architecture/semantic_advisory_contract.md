# Sprint 23 — Semantic Advisory Contract

Sprint 23 locks the advisory-only boundary for SemanticBridge, FeedbackWriter, semantic memory, validation, and policy surfaces.

## Goal

Semantic and feedback surfaces may enrich, advise, score, validate, remember, and report.

They must not become hidden runtime truth authorities.

## Governed surfaces

Primary governed surfaces:

- `hfa-agents/src/hfa_agents/integration/semantic_bridge.py`
- `hfa-worker/src/hfa_worker/feedback_writer.py`
- `hfa-worker/src/hfa_worker/integration/feedback_client.py`
- `hfa-worker/src/hfa_worker/scheduler_semantic_hook.py`
- `hfa-semantic/src/hfa_semantic/memory/*`
- `hfa-semantic/src/hfa_semantic/validation/*`
- `hfa-semantic/src/hfa_semantic/policy/*`

## Advisory-only invariant

SemanticBridge may produce advisory enrichment.

FeedbackWriter may persist feedback, observations, and learning signals.

Semantic memory, validation, and policy surfaces may produce memory records, validation outcomes, policy verdicts, scores, and advisory signals.

These surfaces must not directly mutate canonical runtime truth.

## Forbidden authority behavior

Advisory surfaces must not directly mutate:

- canonical task or run terminal state
- worker ownership state
- claim/fence state
- scheduler dispatch authority state
- recovery requeue state
- replay truth state
- Lua-owned runtime authority keys

## Allowed non-authoritative behavior

Allowed:

- feedback persistence
- semantic memory persistence
- advisory enrichment
- validation reports
- policy verdict records
- observability/audit artifacts
- governance-local bounded state

## Failure semantics

Advisory failure must not directly change execution truth state.

A semantic or feedback failure may produce an advisory failure record, validation warning, or policy verdict, but runtime state transition must remain in runtime/Lua/control-plane approved paths.

## Future promotion

Any promotion from advisory behavior to authoritative runtime behavior requires a future authority-reviewed sprint, explicit design review, tests, and production readiness checklist approval.

## Non-goals

- No SemanticBridge gate enforcement.
- No FeedbackWriter hard governance enforcement.
- No HITL/confidence enforcement changes.
- No runtime behavior rewrite.

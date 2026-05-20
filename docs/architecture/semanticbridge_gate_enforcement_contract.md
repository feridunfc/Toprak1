# Sprint 25 — SemanticBridge Gate Enforcement Contract

Sprint 25 hardens SemanticBridge advisory gate behavior without promoting SemanticBridge to runtime truth authority.

## Goal

SemanticBridge may enrich, score, validate, and produce advisory gate decisions.

SemanticBridge must not directly mutate canonical runtime truth.

## Required invariants

- SemanticBridge remains advisory/non-authoritative.
- SemanticBridge gate decisions must be explicit and structured.
- Gate failure must fail closed for advisory enrichment.
- Gate failure must not directly mutate task/run terminal state.
- Gate failure must not call recovery, claim, completion, scheduler dispatch, or runtime authority mutation paths.
- Canonical runtime authority remains in runtime/Lua/control-plane approved paths.

## Gate dimensions

The enforcement surface may include:

- minimum confidence
- required enrichment fields
- malformed payload rejection
- timeout/error fail-closed behavior
- explicit rejection reasons
- audit artifact generation

## Allowed behavior

Allowed:

- advisory enrichment
- semantic scores
- validation warnings
- policy verdicts
- non-authoritative gate decisions
- observability/audit artifacts

## Forbidden behavior

Forbidden:

- canonical task state mutation
- canonical run state mutation
- worker ownership mutation
- claim/fence mutation
- scheduler dispatch authority mutation
- recovery requeue mutation
- replay truth mutation

## Non-goals

- No production auto-enforcement of runtime state.
- No canonical runtime mutation.
- No HITL workflow implementation.
- No semantic ranking algorithm rewrite.

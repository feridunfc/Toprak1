# Sprint 81.1 — Canonical Transition Core

## Status

```yaml
sprint: 81.1
implementation_authorization: EXPLICIT_USER_COMMAND_START
base_branch: baseline/local-import
base_head: 75b7010f3dde2ee07aa123398eac903b5c6b0cd4
branch: sprint/81-1-canonical-transition-core
status: IMPLEMENTATION_IN_PROGRESS
product_source_mutation: true
redis_lua_mutation: false
runtime_cutover: false
production_ready_claim: false
```

Sprint 80C.1, 80C.2 and 80C.3 are accepted and merged. This slice begins the
separately authorized Sprint 81 implementation without silently converting the
legacy Redis/Lua paths.

## Goal

Implement the persistence-independent authority core required by ADR-080C.3:

- canonical task/run aggregate identity and collision-safe component hashing;
- RFC 8785 JCS command and record hashing after UTF-8 NFC input normalization;
- exact fifteen-operation registry and operation-specific state contracts;
- authority-entry authentication, capability, target-identity and fence gate;
- receipt lookup before revision comparison;
- duplicate, idempotency-conflict, stale, future and corruption outcomes;
- strict contiguous aggregate revision planning;
- immutable canonical transition record and operation receipt generation;
- one-record/one-receipt/one-revision authority commit plan;
- canonical-store collision and projection application classification.

## Exact scope

```text
.github/workflows/sprint81-canonical-transition-core.yml
docs/implementation/sprint81/SPRINT81.1-canonical-transition-core.md
hfa-core/pyproject.toml
hfa-core/src/hfa/authority/__init__.py
hfa-core/src/hfa/authority/canonical_transition.py
hfa-core/tests/authority/test_canonical_transition.py
```

## Deliberate exclusions

This slice does **not**:

- modify `task_admit.lua`, `task_complete.lua` or any other runtime Lua script;
- select the physical Redis canonical-store layout;
- deploy an aggregate authority commit script;
- migrate existing task/run keys or synthesize historical revisions;
- change scheduler, worker, process-manager or transport behavior;
- enable runtime cutover or make a production-readiness claim.

Those changes require later bounded Sprint 81 slices after this core is reviewed.

## Contract notes

`rfc8785==0.1.4` is pinned because command and record hashes are authority
identities. Caller-owned nested payloads are normalized and deeply frozen at
command construction so later caller mutation cannot change a command hash.

The pure evaluator returns mutation cardinalities but performs no persistence.
`AuthorityCommitPlan` is the handoff contract for a later Redis/Lua atomic
implementation; it binds exactly one revision increment, one canonical record
and one immutable operation receipt.

## Exit criteria

```yaml
exact_operation_contracts: 15
canonical_core_tests: PASS
compileall: PASS
package_specific_CI: PASS_REQUIRED
authority_gate: PASS_REQUIRED
independent_code_review: REQUIRED
human_merge_decision: REQUIRED
```

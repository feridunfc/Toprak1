# Sprint 81.1 — Canonical Transition Core

## Status

```yaml
implementation_slice: 81.1
base_head: 75b7010f3dde2ee07aa123398eac903b5c6b0cd4
scope: PERSISTENCE_INDEPENDENT_AUTHORITY_CORE
redis_lua_mutation: false
runtime_cutover: false
production_ready_claim: false
independent_implementation_review: REQUIRED
human_merge_decision: REQUIRED
```

## Implemented boundary

Sprint 81.1 implements the pure authority core accepted by ADR-080C.3:

- collision-safe task/run aggregate identity;
- RFC 8785 JCS command and record hashing with NFC input normalization;
- exact fifteen-operation contract registry;
- authentication, capability, target-identity and optional fence outer gate;
- receipt-first idempotency before revision comparison;
- strict contiguous aggregate revision CAS;
- immutable canonical transition record and operation receipt;
- one-revision, one-record and one-receipt commit plan;
- canonical-store and projection collision classification.

## Final review hardening

The correction closes the independent review's five P0 findings and type-hardening requirement.

```yaml
authority_provenance:
  public_commit_entrypoint: evaluate_authority_commit
  caller_constructed_ACCEPTED_decision: FORBIDDEN
  caller_constructed_commit_plan: FORBIDDEN
  authorized_writer_source: AuthorityEntryContext.authenticated_writer_id

canonical_store:
  candidate_semantic_validation_before_empty_insert: REQUIRED
  invalid_candidate: CANONICAL_RECORD_CORRUPTION_CONFLICT

receipt_duplicate_proof:
  hash_only_probe: FORBIDDEN
  actual_canonical_record: REQUIRED
  record_hash_and_semantics: VERIFIED
  receipt_record_command_identity: FULL_MATCH

canonical_record:
  direct_unvalidated_constructor: FORBIDDEN
  validator: validate_canonical_transition_record
  validator_used_by_factory: true
  validator_used_by_store: true
  validator_used_by_receipt_probe: true
  validator_used_by_projection: true

canonical_command_hash:
  aggregate_identity_binding:
    - structured_aggregate_identity
    - canonical_aggregate_identity_sha256

primitive_validation:
  revision_and_timestamp: EXACT_INT_NOT_BOOL_NONNEGATIVE_JCS_SAFE
  fence_fields: EXACT_BOOL
  aggregate_type: EXACT_ENUM
```

## Single authority entrypoint

`evaluate_authority_commit(...)` performs the complete sequence:

1. validate the outer authority gate;
2. resolve and verify a durable receipt plus its actual canonical record;
3. distinguish duplicate, idempotency conflict and corruption;
4. compare expected and current aggregate revision;
5. validate the operation-specific state and projection-intent contract;
6. create the immutable record, receipt and commit plan internally.

`AuthorityDecision` remains an informational result. It cannot be caller-constructed and is never accepted as input to create a commit plan. `AuthorityCommitPlan.create(...)` does not exist.

## Deliberate exclusions

This slice does not:

- modify any Redis/Lua script;
- select a physical canonical-store key layout;
- synthesize historical revisions;
- change scheduler, worker, process-manager or transport runtime behavior;
- authorize migration, runtime cutover or production readiness.

## Focused verification

```yaml
focused_tests: 60_PASSED
compileall: PASS
required_negative_cases:
  - forged_ACCEPTED_decision
  - empty_store_invalid_candidate
  - fake_matching_receipt_hash
  - semantically_invalid_self_hashed_record
  - delimiter_collision_command_hash
  - bool_negative_and_unsafe_revision
  - non_boolean_fence_fields
```

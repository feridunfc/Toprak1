# ADR-080C.3 — Canonical Transition and Monotonic Aggregate Revision

- Status: **CORRECTED TECHNICAL RECOMMENDATION — READY FOR FINAL INDEPENDENT REVIEW**
- Sprint: 80C.3
- Branch: `sprint/80c3-canonical-transition-revision`
- Base: `baseline/local-import@956c3247d5ceaaa0697547a31950918cce38fcd9`
- Human architecture acceptance: **PENDING**
- Merge authorized: **NO**
- Product implementation authorized: **NO**
- Product source mutation: **0**

## 1. Accepted predecessors

```yaml
80C1:
  status: ACCEPTED_MERGED_COMPLETE
  accepted_head: 9ad9503badd72afb0a935dbb8c02e828ea02d3e2
  acceptance_comment_id: 5083135387
  merge_commit: 9d7c7a6ad52a8a546a708dda048f28d4e23befc6
80C2:
  status: ACCEPTED_MERGED_COMPLETE
  accepted_head: edb86b3560250c09cfc08ea28fa22aae88490382
  acceptance_comment_id: 5083381947
  merge_commit: 956c3247d5ceaaa0697547a31950918cce38fcd9
```

80C.3 consumes the accepted runtime truth owner, task/run aggregate boundary, child-owned revision and logical-edge coordination decisions. It does not reopen them.

## 2. Frozen evidence

```yaml
source_head: 5734ac60704f8546b8ce67e766e042ff2ce4c412
artifact_sha256: 7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588
transition_cardinality_sha256: 40325ea57afab4d1c4c31e009de7f7eba4c369e025bf6ccaa73d9a52432f35cb
operation_count: 9
canonical_transition_record_count: 0
aggregate_revision_evidence_count: 0
```

The current product exposes no verified canonical transition record and no verified aggregate revision evidence. This ADR assigns the architecture; Sprint 81 owns implementation.

## 3. Canonical aggregate and transition identity

```yaml
canonical_aggregate_identity:
  task: "task:{run_id}:{task_id}"
  run: "run:{run_id}"
  component_normalization: UTF8_NFC_EXACT_CASE_PRESERVED_FOR_IDS
  tenant_identity_role: AUTHORITATIVE_METADATA_NOT_AGGREGATE_IDENTITY

transition_id:
  format: "ctr:v1:{canonical_aggregate_identity_sha256}:{aggregate_revision}:{operation_id_sha256}"
  component_encoding: SHA256_OF_LENGTH_PREFIXED_UTF8_COMPONENT
  raw_delimiter_concatenation: FORBIDDEN
  uniqueness_scope: GLOBAL
```

Task identity preserves `run_id + task_id`. Raw task IDs and unescaped delimiter concatenation are forbidden.

## 4. Authority entry preconditions

Authorization and fencing form an outer gate before operation receipt lookup. Correct `expected_revision` is not writer authority.

```yaml
authority_entry_preconditions:
  order: OUTER_GATE_BEFORE_OPERATION_RECEIPT_LOOKUP
  validate_authenticated_writer: REQUIRED
  validate_writer_operation_capability: REQUIRED
  validate_claim_or_lease_fence_when_applicable: REQUIRED
  validate_target_aggregate_identity: REQUIRED
  accepted_predecessor_binding: ADR-080C1_RUNTIME_TRUTH_AUTHORITY_AND_ADR-080C2_AGGREGATE_BOUNDARY

precondition_failure:
  result: AUTHORITY_ENTRY_REJECTED
  operation_receipt_disclosure: FORBIDDEN
  aggregate_mutation: 0
  revision_increment: 0
  canonical_record_count: 0
  durable_conflict_record: AS_REQUIRED_BY_SECURITY_AUDIT_POLICY
```

Only after this gate succeeds may the idempotency/CAS sequence run.

## 5. Canonical command hash and authoritative-effect binding

80C.3 selects **Model A**: every requested authoritative effect is part of the canonical command hash.

```yaml
canonical_command_hash:
  algorithm: SHA256
  serialization: RFC_8785_JCS
  serialization_profile: CANONICAL_JSON_RFC_8785_JCS_WITH_UTF8_NFC_INPUT
  caller_supplied_hash_trusted: false
  omitted_optional_fields: FORBIDDEN_USE_EXPLICIT_NULL
  hash_members:
    - aggregate_type
    - canonical_aggregate_identity
    - operation_type
    - operation_id
    - expected_revision
    - intended_previous_state
    - intended_next_state
    - authoritative_payload
    - authoritative_metadata_changes
    - requested_child_effects
    - requested_projection_intents
    - causation_id
  excluded_members:
    - committed_at_ms
    - writer_generated_metadata
    - committed_revision
    - transition_id
```

Exact serialization profile:

```yaml
canonical_json_standard:
  standard: RFC_8785_JCS
  unicode_input_normalization: UTF8_NFC_BEFORE_JCS
  integer_float_encoding: RFC_8785_ECMASCRIPT_NUMBER_SERIALIZATION
  negative_zero: RFC_8785_NORMALIZATION
  non_finite_numbers: FORBIDDEN
  duplicate_object_keys: FORBIDDEN
  array_order: PRESERVED
  binary_values: BASE64URL_WITH_EXPLICIT_TYPE_TAG
  timestamp_representation: INTEGER_MILLISECONDS_UTC
```

Authoritative effects are bound as follows:

```yaml
authoritative_effect_binding:
  model: MODEL_A_HASH_ALL_REQUESTED_AUTHORITATIVE_EFFECTS
  caller_or_writer_local_effect_override: FORBIDDEN
  record_effect_mapping:
    authoritative_metadata_changes: EXACT_HASH_BOUND_COMMAND_VALUE
    child_effects: EXACT_ACCEPTED_REQUESTED_CHILD_EFFECTS
    durable_projection_intents: EXACT_ACCEPTED_REQUESTED_PROJECTION_INTENTS

same_operation_id_and_same_command_hash:
  immutable_record_effects_must_match: true
  canonical_record_hash_must_match: true
```

The authority layer may reject an invalid requested effect, but it may not silently replace hash-bound metadata, child effects or projection intents with writer-local values.

## 6. Operation receipt and mandatory evaluation order

```yaml
operation_receipt_authority:
  key: "oprcpt:v1:{canonical_aggregate_identity_sha256}:{operation_id_sha256}"
  owner: AGGREGATE_AUTHORITY_COMMIT
  immutable: true
  immutable_value:
    operation_id: REQUIRED
    canonical_command_hash: REQUIRED
    canonical_record_hash: REQUIRED
    transition_id: REQUIRED
    aggregate_revision: REQUIRED
    operation_type: REQUIRED
    committed_at_ms: REQUIRED
```

The receipt is an immutable idempotency index, not a second lifecycle truth.

```yaml
authority_evaluation_order:
  1: VALIDATE_AUTHORITY_ENTRY_PRECONDITIONS
  2: RESOLVE_OPERATION_RECEIPT
  3: COMPARE_CANONICAL_COMMAND_HASH
  4: COMPARE_EXPECTED_REVISION
  5: VALIDATE_STATE_TRANSITION
  6: COMMIT_STATE_REVISION_RECORD_RECEIPT_AND_INTENTS_ATOMICALLY
```

Exact outcomes:

```yaml
same_operation_same_command_and_record:
  result: ALREADY_APPLIED
  return_existing_transition_id: REQUIRED
  mutation: 0
same_operation_different_command:
  result: IDEMPOTENCY_CONFLICT
  mutation: 0
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
receipt_missing_stale_revision:
  result: STALE_REVISION_CONFLICT
  mutation: 0
receipt_missing_future_revision:
  result: FUTURE_REVISION_CONFLICT
  mutation: 0
  reconciliation_candidate: REQUIRED
```

`ALREADY_APPLIED` is legal only when the durable receipt proves the same command hash and canonical record hash.

## 7. Revision and aggregate creation

```yaml
revision_owner: AGGREGATE
initial_revision: 0
first_committed_transition_revision: 1
next_revision_rule: to_revision == from_revision + 1
monotonicity: STRICT_CONTIGUOUS
gap: FORBIDDEN
reuse: FORBIDDEN
regression: FORBIDDEN
expected_revision_required: true
ordering_authority: AGGREGATE_REVISION
committed_at_ms_ordering_authority: false
clock_regression_effect: NONE
same_millisecond_transitions: ALLOWED
```

For a missing aggregate, logical revision is `0`. `TASK_ADMIT` and `RUN_CREATE` require expected revision `0`, commit revision `1`, and write one record plus one receipt.

## 8. CanonicalTransitionRecord and record hash

Required immutable fields include:

```yaml
required_fields:
  - schema_version
  - transition_id
  - aggregate_type
  - canonical_aggregate_identity
  - canonical_aggregate_identity_sha256
  - from_revision
  - to_revision
  - operation_type
  - operation_id
  - canonical_command_hash
  - canonical_record_hash
  - previous_state
  - next_state
  - authoritative_metadata_changes
  - child_effects
  - causation_id
  - correlation_id
  - writer_id
  - committed_at_ms
  - durable_projection_intents
```

```yaml
canonical_record_hash:
  algorithm: SHA256
  serialization: RFC_8785_JCS
  hash_scope: ALL_IMMUTABLE_RECORD_FIELDS_EXCEPT_CANONICAL_RECORD_HASH
  caller_supplied_hash_trusted: false
```

The record hash is the comparison authority for canonical store collisions and projection application receipts.

## 9. Canonical storage collision and atomicity

```yaml
canonical_transition_store:
  owner: AGGREGATE_AUTHORITY_COMMIT
  append_only: true
  immutable: true
  replay_source: true
  physical_layout: DEFERRED_TO_SPRINT81

same_transition_id_same_record:
  result: ALREADY_PRESENT
  mutation: 0

same_transition_id_different_record:
  result: CANONICAL_RECORD_CORRUPTION_CONFLICT
  mutation: 0
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
```

```yaml
atomicity:
  state_revision_record_receipt: ONE_AUTHORITY_COMMIT
  authoritative_metadata_and_projection_intents: SAME_AUTHORITY_COMMIT
  projection_delivery: OUTSIDE_AUTHORITY_COMMIT
```

State, revision, canonical record, operation receipt and durable delivery intents are one authority commit. Projection delivery is outside that commit and is replayable.

## 10. Projection application receipt and contradiction handling

Each projection maintains an application receipt:

```yaml
projection_application_receipt:
  owner: PROJECTION
  key: canonical_aggregate_identity
  value:
    applied_revision: REQUIRED
    applied_transition_id: REQUIRED
    applied_record_hash: REQUIRED
```

Exact behavior:

```yaml
incoming_revision == applied_revision:
  same_transition_id_and_record_hash:
    result: DUPLICATE_NOOP
    projection_mutation: 0
  different_transition_id_or_record_hash:
    result: PROJECTION_CORRUPTION_CONFLICT
    projection_mutation: 0
    durable_conflict_record: REQUIRED
    reconciliation_candidate: REQUIRED

incoming_revision < applied_revision:
  result: OLDER_REVISION_NOOP
  projection_mutation: 0

incoming_revision == applied_revision + 1:
  result: APPLY

incoming_revision > applied_revision + 1:
  result: GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED
  projection_mutation: 0
```

Same revision is not sufficient proof of duplicate delivery. Transition ID and record hash must both match.

## 11. Mutation classes

```yaml
accepted_authority_mutation:
  revision_increment: 1
  canonical_transition_record_count: 1
coordination_only_mutation:
  revision_increment: 0
  canonical_transition_record_count: 0
projection_only_mutation:
  revision_increment: 0
  canonical_transition_record_count: 0
transport_only_mutation:
  revision_increment: 0
  canonical_transition_record_count: 0
rejected_or_duplicate:
  revision_increment: 0
  canonical_transition_record_count: 0
durable_conflict_record:
  aggregate_revision_increment: 0
  canonical_transition_record_count: 0
  separate_conflict_record_count: 1
```

## 12. Exact operation taxonomy

The matrix contains exactly fifteen operation contracts:

```text
TASK_ADMIT
TASK_DISPATCH
TASK_CLAIM
TASK_HEARTBEAT
TASK_COMPLETE
TASK_FAIL
TASK_REQUEUE
TASK_CANCEL
TASK_DEPENDENCY_APPLY
RUN_CREATE
RUN_TERMINATE
LEGACY_RUN_COMPLETE
TERMINAL_DUPLICATE_CLEANUP
MESSAGE_APPEND
MESSAGE_ACK
```

The validator contains an independent `EXPECTED_OPERATION_CONTRACTS` register and requires exact equality for every row. Tests mutate authority owner, lifecycle state, projection intents, stale behavior, evidence binding and mutation class to prove semantic drift fails closed. `LEGACY_RUN_COMPLETE` remains blocked until migration or an explicit compatibility contract.

## 13. Findings and implementation ownership

```yaml
no_verified_canonical_transition_record:
  decision_status: ARCHITECTURE_DECISION_ASSIGNED
  resolved_in_product: false
  implementation_sprint: 81
no_aggregate_revision_evidence:
  decision_status: ARCHITECTURE_DECISION_ASSIGNED
  resolved_in_product: false
  implementation_sprint: 81
```

## 14. Executable verification and governance

The package validator must enforce:

```yaml
authoritative_effect_hash_binding: EXACT
canonical_record_collision: EXACT
projection_same_revision_collision: EXACT
operation_taxonomy_exact_rows_machine_verified: 15
authority_entry_preconditions: EXACT
canonical_hash_serialization_standard: RFC_8785_JCS
manifest_governance_register: EXACT
exact_changed_files: 8
product_source_mutation: 0
```

```yaml
architecture_contract_complete: CORRECTED_TECHNICAL_RECOMMENDATION
independent_review: REQUIRED
human_architecture_acceptance: PENDING
merge_authorized: false
product_implementation_authorized: false
sprint81_implementation_authorized: false
production_ready_claim_authorized: false
```

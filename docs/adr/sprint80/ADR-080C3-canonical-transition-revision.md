# ADR-080C.3 — Canonical Transition and Monotonic Aggregate Revision

- Status: **CORRECTED TECHNICAL RECOMMENDATION — READY FOR INDEPENDENT REVIEW**
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

80C.3 consumes the accepted task/run truth owners, independent task aggregate model, parent/child revision ownership and process-manager coordination boundary. It does not reopen them.

## 2. Immutable evidence

```yaml
source_head: 5734ac60704f8546b8ce67e766e042ff2ce4c412
artifact_sha256: 7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588
transition_cardinality_sha256: 40325ea57afab4d1c4c31e009de7f7eba4c369e025bf6ccaa73d9a52432f35cb
operation_count: 9
canonical_transition_record_count: 0
aggregate_revision_evidence_count: 0
```

The evidence distinguishes lifecycle mutation, coordination fences, liveness timestamps, retry counters, projections, transport and audit. None of the observed records satisfies the canonical authority schema and transaction-coupling contract.

## 3. Canonical aggregate identity and transition identity

```yaml
canonical_aggregate_identity:
  task: "task:{run_id}:{task_id}"
  run: "run:{run_id}"
  raw_task_id_as_identity: FORBIDDEN

transition_id:
  format: "ctr:v1:{canonical_aggregate_identity_sha256}:{aggregate_revision}:{operation_id_sha256}"
  uniqueness_scope: GLOBAL
  aggregate_type: NORMALIZED_LOWERCASE_ENUM
  component_encoding: SHA256_OF_LENGTH_PREFIXED_UTF8_COMPONENT
  raw_delimiter_concatenation: FORBIDDEN
```

Task identity preserves the accepted 80C.2 `run_id + task_id` boundary. Tenant identity is authoritative metadata, not a replacement aggregate identity. Physical key layout is deferred to Sprint 81; logical identity is not.

## 4. Canonical command hash

Every operation is normalized by the authority layer before idempotency comparison.

```yaml
canonical_command_hash:
  algorithm: SHA256
  input_encoding: UTF_8
  serialization: CANONICAL_JSON
  object_key_order: LEXICOGRAPHIC
  insignificant_whitespace: REMOVED
  omitted_optional_fields: FORBIDDEN_USE_EXPLICIT_NULL
  caller_supplied_hash_trusted: false
  hash_members:
    - aggregate_type
    - canonical_aggregate_identity
    - operation_type
    - operation_id
    - expected_revision
    - intended_previous_state
    - intended_next_state
    - authoritative_payload
    - causation_id
  excluded_members:
    - committed_at_ms
    - writer_generated_metadata
    - committed_revision
    - transition_id
```

## 5. Operation receipt authority

```yaml
operation_receipt_authority:
  key: "oprcpt:v1:{canonical_aggregate_identity_sha256}:{operation_id_sha256}"
  owner: AGGREGATE_AUTHORITY_COMMIT
  immutable: true
  immutable_value:
    operation_id: REQUIRED
    canonical_command_hash: REQUIRED
    transition_id: REQUIRED
    aggregate_revision: REQUIRED
    operation_type: REQUIRED
    committed_at_ms: REQUIRED
```

The receipt is not a second lifecycle truth. It is the immutable idempotency index written by the same authority commit as state, revision and canonical record.

## 6. Mandatory authority evaluation order

```yaml
authority_commit_order:
  1: RESOLVE_OPERATION_RECEIPT
  2: COMPARE_CANONICAL_COMMAND_HASH
  3: COMPARE_EXPECTED_REVISION
  4: VALIDATE_STATE_TRANSITION
  5: COMMIT_STATE_REVISION_RECORD_RECEIPT_AND_INTENTS_ATOMICALLY
```

Exact outcomes:

```yaml
operation_receipt_exists_same_payload:
  result: ALREADY_APPLIED
  return_existing_transition_id: REQUIRED
  mutation: 0
  revision_increment: 0
  canonical_record_count: 0
operation_receipt_exists_different_payload:
  result: IDEMPOTENCY_CONFLICT
  mutation: 0
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
operation_receipt_missing_expected_revision_stale:
  result: STALE_REVISION_CONFLICT
  mutation: 0
  canonical_record_count: 0
operation_receipt_missing_expected_revision_future:
  result: FUTURE_REVISION_CONFLICT
  mutation: 0
  reconciliation_candidate: REQUIRED
```

`ALREADY_APPLIED` may be returned only when the durable operation receipt proves the same operation and canonical payload.

## 7. Revision contract and aggregate creation

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

For a missing aggregate, logical current revision is `0`. `TASK_ADMIT` and `RUN_CREATE` require expected revision `0`, commit revision `1`, write one record and one receipt. Task initial state is `ready` only when its immutable expected-parent set is empty; otherwise it is `pending`. Run initial state is `pending`. An existing aggregate with a different create operation is `AGGREGATE_ALREADY_EXISTS_CONFLICT`.

## 8. CanonicalTransitionRecord schema

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
  - previous_state
  - next_state
  - authoritative_metadata_changes
  - child_effects
  - causation_id
  - correlation_id
  - writer_id
  - committed_at_ms
  - durable_projection_intents
revision_rule: to_revision == from_revision + 1
aggregate_revision_alias: to_revision
```

Create/admit records require `previous_state: null` and a non-null next state. Normal lifecycle records require both states. Coordination, projection and transport-only actions must not create a canonical transition record unless they also perform an accepted authority mutation.

## 9. Canonical storage authority and atomic commit

```yaml
canonical_transition_store:
  owner: AGGREGATE_AUTHORITY_COMMIT
  append_only: true
  immutable: true
  replay_source: true
  physical_layout: DEFERRED_TO_SPRINT81
transition_uniqueness_index:
  owner: AGGREGATE_AUTHORITY_COMMIT
  key: transition_id
operation_receipt_index:
  owner: AGGREGATE_AUTHORITY_COMMIT
  key: canonical_aggregate_identity + operation_id
atomicity:
  state_revision_record_receipt: ONE_AUTHORITY_COMMIT
  authoritative_metadata_and_projection_intents: SAME_AUTHORITY_COMMIT
  projection_delivery: OUTSIDE_AUTHORITY_COMMIT
```

A merely schema-shaped event is not canonical authority. Uniqueness, receipt, state, revision and record coupling must all be enforced by the aggregate authority commit.

## 10. Mutation classification

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

Claim epochs, heartbeat times, lease TTLs and retry counters do not become aggregate revisions merely because they are monotonic or fenced.

## 11. Exact operation taxonomy

The machine-readable matrix decides fifteen operations:

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

Each row declares authority owner, before/after state, mutation class, revision consumption, record count, receipt requirement, duplicate/stale behavior, projection intents and evidence binding. `LEGACY_RUN_COMPLETE` is blocked until migration or an explicit compatibility contract; it is not silently promoted into the new authority model.

## 12. Projection application

```yaml
incoming_revision == applied_revision + 1: APPLY
incoming_revision <= applied_revision: DUPLICATE_NOOP
incoming_revision > applied_revision + 1: FAIL_CLOSED_AND_REPLAY_REQUIRED
ordering_authority: AGGREGATE_REVISION
replay_source: CANONICAL_TRANSITION_STORE
```

Projection success is outside the authority commit. Lost delivery is replayed from canonical records.

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

## 14. Governance

```yaml
architecture_contract_complete: TECHNICAL_RECOMMENDATION
independent_review: REQUIRED
human_architecture_acceptance: PENDING
merge_authorized: false
product_implementation_authorized: false
sprint81_implementation_authorized: false
production_ready_claim_authorized: false
```

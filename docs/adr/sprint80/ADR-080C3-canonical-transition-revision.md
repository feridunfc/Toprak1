# ADR-080C.3 — Canonical Transition and Monotonic Aggregate Revision

- Status: **TECHNICAL RECOMMENDATION — INDEPENDENT REVIEW REQUIRED**
- Sprint: 80C.3
- Branch: `sprint/80c3-canonical-transition-revision`
- Base: `baseline/local-import@956c3247d5ceaaa0697547a31950918cce38fcd9`
- Human architecture acceptance: **PENDING**
- Product implementation authorized: **NO**
- Product source mutation: **0**

## Accepted predecessors

```yaml
80C1:
  status: ACCEPTED_MERGED_COMPLETE
  acceptance_comment_id: 5083135387
  merge_commit: 9d7c7a6ad52a8a546a708dda048f28d4e23befc6
80C2:
  status: ACCEPTED_MERGED_COMPLETE
  acceptance_comment_id: 5083381947
  merge_commit: 956c3247d5ceaaa0697547a31950918cce38fcd9
```

80C.3 consumes the accepted task aggregate, revision-owner and cross-task coordination boundaries. It does not reopen them.

## Frozen evidence

```yaml
source_head: 5734ac60704f8546b8ce67e766e042ff2ce4c412
artifact_sha256: 7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588
transition_cardinality_sha256: 40325ea57afab4d1c4c31e009de7f7eba4c369e025bf6ccaa73d9a52432f35cb
operation_count: 9
canonical_transition_record_count: 0
aggregate_revision_evidence_count: 0
```

The current lifecycle paths do not expose a verified canonical transition record or a monotonic aggregate revision contract.

## Canonical transition identity

```yaml
transition_id:
  format: "{aggregate_type}:{aggregate_id}:{next_revision}:{operation_id}"
  authority: CANONICAL_TRANSITION_ID
  uniqueness_scope: GLOBAL
```

`operation_id` is the stable idempotency identity supplied by the authorized writer. Duplicate delivery with the same operation identity must resolve to the already committed transition and must not create a new revision or record.

## Aggregate revision contract

```yaml
revision_owner: AGGREGATE
initial_revision: 0
first_committed_transition_revision: 1
next_revision_rule: previous_revision + 1
revision_monotonicity: STRICT_CONTIGUOUS
revision_gap: FORBIDDEN
revision_reuse: FORBIDDEN
revision_regression: FORBIDDEN
expected_revision_required: true
```

The authority commit compares `expected_revision` atomically with the current aggregate revision.

```yaml
expected_revision_matches:
  mutation: APPLY
  next_revision: current_revision + 1
expected_revision_stale:
  mutation: 0
  result: ALREADY_ADVANCED_OR_STALE
expected_revision_future:
  mutation: 0
  result: REVISION_CONFLICT
  reconciliation_candidate: REQUIRED
```

## CanonicalTransitionRecord schema

Minimum immutable fields:

```yaml
schema_version: REQUIRED
transition_id: REQUIRED
aggregate_type: REQUIRED
aggregate_id: REQUIRED
aggregate_revision: REQUIRED
operation_type: REQUIRED
operation_id: REQUIRED
previous_state: REQUIRED_OR_NULL
next_state: REQUIRED_OR_NULL
child_effects: REQUIRED_ARRAY
causation_id: REQUIRED
correlation_id: REQUIRED
writer_id: REQUIRED
committed_at_ms: REQUIRED
metadata: REQUIRED_OBJECT
```

`committed_at_ms` is assigned by the authority commit point and is not trusted from the caller.

## Operation taxonomy

```yaml
lifecycle_operations:
  - TASK_ADMIT
  - TASK_DISPATCH
  - TASK_CLAIM
  - TASK_COMPLETE
  - TASK_FAIL
  - TASK_CANCEL
  - TASK_DEPENDENCY_APPLY
  - RUN_CREATE
  - RUN_TERMINATE
coordination_operations:
  - TASK_HEARTBEAT
  - LEASE_RENEW
transport_operations:
  - MESSAGE_APPEND
  - MESSAGE_ACK
```

Coordination and transport actions must not consume a lifecycle aggregate revision unless they change authoritative lifecycle state.

## Exactly-one commit invariant

For every successful authoritative lifecycle mutation:

```yaml
aggregate_revision_increment: 1
canonical_transition_record_count: 1
state_and_record_atomicity: REQUIRED
```

For every rejected, duplicate or stale operation:

```yaml
aggregate_revision_increment: 0
canonical_transition_record_count: 0
```

A child dependency application accepted under ADR-080C.2 records child effects and logical-edge resolution in the same child transition record and revision.

## Duplicate and retry behavior

```yaml
same_operation_id_same_payload:
  result: ALREADY_APPLIED
  return_existing_transition_id: REQUIRED
  mutation: 0
same_operation_id_different_payload:
  result: IDEMPOTENCY_CONFLICT
  mutation: 0
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
same_transition_id_different_record:
  result: CORRUPTION_CONFLICT
  mutation: 0
```

Canonical transition records are immutable and append-only.

## Projection application

```yaml
incoming_revision == applied_revision + 1: APPLY
incoming_revision <= applied_revision: DUPLICATE_NOOP
incoming_revision > applied_revision + 1: GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED
```

Projection success is not the authority commit point. Lost projection delivery is replayed from canonical transition records.

## Findings and implementation ownership

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

## Governance

```yaml
architecture_contract_complete: TECHNICAL_RECOMMENDATION
independent_review: REQUIRED
human_architecture_acceptance: PENDING
merge_authorized: false
product_implementation_authorized: false
sprint81_implementation_authorized: false
production_ready_claim_authorized: false
```

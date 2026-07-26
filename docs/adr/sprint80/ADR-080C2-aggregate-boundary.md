# ADR-080C.2 — Aggregate Boundary and Parent-Child Coordination

- Status: **CORRECTED TECHNICAL RECOMMENDATION — PREDECESSOR ACCEPTED; FINAL RE-REVIEW REQUIRED**
- Sprint: 80C.2
- Branch: `sprint/80c2-aggregate-boundary`
- Base: `baseline/local-import@9d7c7a6ad52a8a546a708dda048f28d4e23befc6`
- Human architecture acceptance: **NOT READY**
- Product implementation authorized: **NO**
- Product source mutation in this tranche: **0**

## 1. Immutable evidence

```yaml
source_head: 5734ac60704f8546b8ce67e766e042ff2ce4c412
artifact_sha256: 7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588
aggregate_boundary_sha256: ac5ab01455a3e8acb6bc0043167849e398df393819f5b4cdf14e9770dea9f478
observation_count: 8
global_result: CROSS_TASK_MUTATION_WITHOUT_CHILD_AUTHORITY_RECORD
blocking_findings:
  - missing_remaining_counter_unlocks_fail_open
  - ready_marker_strands_pending_child
```

The evidence remains unchanged. Current `task_complete.lua` commits parent state and directly mutates child state, counter, marker and queue in one Lua call without a child revision or child CanonicalTransitionRecord.

## 2. Predecessor governance gate

ADR-080C.1 is merged at `9d7c7a6ad52a8a546a708dda048f28d4e23befc6` and its exact post-merge human architecture acceptance is bound by PR #98 comment `5083135387`.

```yaml
predecessor_decision: ADR-080C1
accepted_head_candidate: 9ad9503badd72afb0a935dbb8c02e828ea02d3e2
accepted_head: 9ad9503badd72afb0a935dbb8c02e828ea02d3e2
merge_commit: 9d7c7a6ad52a8a546a708dda048f28d4e23befc6
acceptance_type: POST_MERGE_HUMAN_ARCHITECTURE_ACCEPTANCE
acceptance_comment_id: 5083135387
human_architecture_acceptance: ACCEPT
status: VERIFIED
```

`MERGED` was not treated as `HUMAN_ACCEPTED`; the separate exact post-merge acceptance record is now verified. Sprint 80C.2 itself still requires final independent re-review and a separate human acceptance before merge.

## 3. Selected aggregate model

```yaml
aggregate_model: INDEPENDENT_TASK_AGGREGATES_WITH_DURABLE_DEPENDENCY_PROCESS_MANAGER
logical_task_aggregate_identity: task:{run_id}:{task_id}
parent_revision_owner: PARENT_TASK_AGGREGATE
child_revision_owner: CHILD_TASK_AGGREGATE
cross_task_direct_authority_mutation: FORBIDDEN
distributed_transaction_across_tasks: false
```

Each task is one lifecycle transaction aggregate. The process manager coordinates durable fanout but is neither task truth authority nor a direct writer of child authority keys.

## 4. Logical identity and physical Redis keys

```yaml
logical_identity: run_id + task_id
physical_key_layout: EXISTING_TASK_ID_KEYS_TEMPORARILY_RETAINED
required_invariant: TASK_ID_GLOBALLY_UNIQUE_ACROSS_ALL_RUNS
collision_rule: EXISTING_TASK_ID_WITH_DIFFERENT_RUN_ID_REJECT_ADMISSION
physical_rekey_in_80C2: NOT_SELECTED
```

A run-scoped rekey is not silently implied. Any future rekey requires a separate accepted migration protocol.

## 5. Child admission and topology authority

The child aggregate owns its immutable expected parent-edge set. Child admission is the topology commit point.

```yaml
authority_scope: ONE_CHILD_TASK_AGGREGATE
atomic_commit_members:
  - child_identity
  - child_initial_state
  - expected_parent_edge_set
  - dependency_policy
  - graph_identity
  - graph_revision_or_topology_hash
  - child_initial_revision
  - one_canonical_admission_record

expected_parent_edge_set_owner: CHILD_TASK_AGGREGATE
expected_parent_edge_set_mutability: IMMUTABLE_AFTER_ADMISSION
dependency_count_authority: false
dependency_count_rule: MUST_EQUAL_CARDINALITY_OF_EXPECTED_PARENT_EDGE_SET
missing_expected_parent_set: FAIL_CLOSED
count_set_mismatch: REJECT_ADMISSION_OR_RECONCILIATION_REQUIRED
graph_identity_missing: FAIL_CLOSED
topology_revision_or_hash_missing: FAIL_CLOSED
```

## 6. Parent completion and fanout intent

One parent completion commit may write only parent authority, one parent revision, exactly one parent CanonicalTransitionRecord and one durable dependency-fanout intent. It mutates zero child and sibling authority keys.

Every fanout intent contains:

```yaml
parent_transition_id: REQUIRED
graph_identity: REQUIRED
graph_revision_or_topology_hash: REQUIRED
child_edge_set_digest: REQUIRED
child_edge_set_digest_algorithm: CANONICAL_SORTED_CHILD_EDGE_IDENTITIES_SHA256
```

## 7. Edge and command identities

```yaml
logical_edge_id:
  format: "{run_id}:{parent_task_id}:{child_task_id}:{graph_revision_or_topology_hash}"
  role: TOPOLOGY_EDGE_IDENTITY

edge_command_id:
  format: "{parent_transition_id}:{child_task_id}:{edge_outcome}:{graph_revision_or_topology_hash}"
  role: SOLE_COMMAND_AND_RECEIPT_IDEMPOTENCY_AUTHORITY

receipt_key_authority: edge_command_id
process_manager_identity: dependency-fanout:{parent_transition_id}
```

## 8. Logical-edge resolution authority

Command-key idempotency alone cannot prevent opposite outcomes for one logical edge from both applying. Therefore each child aggregate owns a logical-edge resolution index.

```yaml
logical_edge_resolution_authority:
  key: logical_edge_id
  owner: CHILD_TASK_AGGREGATE
  value:
    accepted_edge_command_id: REQUIRED
    accepted_parent_transition_id: REQUIRED
    accepted_outcome: REQUIRED
    graph_identity: REQUIRED
    graph_revision_or_topology_hash: REQUIRED
    applied_child_revision: REQUIRED
```

Application contract:

```yaml
when_logical_edge_resolution_absent:
  validate_topology: REQUIRED
  atomic_commit:
    - logical_edge_resolution
    - outcome_aware_edge_receipt
    - child_state_effect
    - child_revision
    - one_canonical_transition_record

when_same_edge_command_id_exists:
  result: ALREADY_APPLIED
  child_mutation: 0
  revision_increment: 0
  canonical_record_count: 0
  retry: STOP

when_logical_edge_resolved_by_different_command:
  result: CONTRADICTION
  child_mutation: 0
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
  retry: STOP
```

Resolution, receipt, child effect, revision and canonical record are one child-aggregate atomic commit. Different-outcome double application is impossible under this contract.

## 9. Outcome-aware receipts and policy

```yaml
applied_dependency_authority: IDEMPOTENT_APPLIED_EDGE_RECEIPTS_WITH_OUTCOME
allowed_outcomes:
  - DEPENDENCY_SATISFIED
  - DEPENDENCY_FAILED
remaining_deps: DERIVED_PROJECTION_NOT_AUTHORITY
ready_emitted: NON_AUTHORITATIVE_PROJECTION_RECEIPT
```

Under `ALL_REQUIRED_PARENTS_MUST_SUCCEED`, a satisfied edge increments the child revision once and produces one canonical record. A failed edge commits:

```yaml
child_disposition: blocked_by_failure
child_revision_increment: 1
canonical_transition_record: EXACTLY_ONE
ready_projection_intent: 0
process_manager_retry: STOP_AFTER_RECEIPT
```

Unknown policy fails closed with zero child mutation, one durable conflict record and one reconciliation candidate.

## 10. Terminal and late command matrix

Terminal disposition records belong to durable process-manager coordination state whenever child mutation is zero.

```yaml
done:
  unsatisfied_edge_command: CONTRADICTION
  child_mutation: 0
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
  retry: STOP

failed:
  unsatisfied_edge_command: CONTRADICTION
  child_mutation: 0
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
  retry: STOP

blocked_by_failure:
  same_command: ALREADY_APPLIED
  different_edge_command: TERMINAL_CHILD_ALREADY_BLOCKED
  different_edge_child_mutation: 0
  different_edge_child_revision_increment: 0
  different_edge_canonical_transition_record_count: 0
  durable_disposition_record: REQUIRED
  disposition_owner: DEPENDENCY_PROCESS_MANAGER_COORDINATION_STATE
  disposition_identity: edge_command_id
  retry: STOP

dead_lettered:
  unsatisfied_edge_command: TERMINAL_CHILD_NOOP
  child_mutation: 0
  durable_disposition_record: REQUIRED
  required_authority_evidence:
    - terminal_transition_id
    - child_state_authority_revision
  missing_authority_evidence: MISSING_AUTHORITY_DURABLE_CONFLICT_AND_RECONCILIATION
  retry: STOP

skipped:
  unsatisfied_edge_command: TERMINAL_CHILD_NOOP
  child_mutation: 0
  durable_disposition_record: REQUIRED
  required_authority_evidence:
    - terminal_transition_id
    - child_state_authority_revision
  missing_authority_evidence: MISSING_AUTHORITY_DURABLE_CONFLICT_AND_RECONCILIATION
  retry: STOP
```

After the first failed edge blocks a child, a later command for a different logical edge does not create a child receipt, revision or canonical record. It creates a durable per-command `TERMINAL_CHILD_ALREADY_BLOCKED` disposition keyed by `edge_command_id`, and retry stops.

## 11. Ready projection and canonical records

`ready_emitted` cannot authorize or suppress state. A normal `pending -> ready` commit contains state, child revision, one canonical record and ready projection intent. Missing queue projection is replayed from the canonical ready transition.

Every newly applied valid satisfied or failed receipt increments child revision exactly once and produces exactly one child CanonicalTransitionRecord. Duplicate commands and terminal no-op dispositions do not.

## 12. Finding disposition

```yaml
missing_remaining_counter_unlocks_fail_open:
  decision_status: ARCHITECTURE_DECISION_ASSIGNED
  resolved_in_product: false
  implementation_sprint: 84

ready_marker_strands_pending_child:
  decision_status: ARCHITECTURE_DECISION_ASSIGNED
  resolved_in_product: false
  implementation_sprint: 84
```

## 13. Migration and dependencies

Migration remains shadow-first, single-writer and reconciliation-backed. No dual authority is allowed.

Dependencies:

- ADR-080C.1 post-merge human architecture acceptance record — SATISFIED by comment `5083135387`;
- ADR-080C.3 canonical transition and monotonic revision contract;
- ADR-080C.4 durable authority and reconstruction contract;
- ADR-080C.5 event/state atomicity and outbox contract;
- ADR-080C.6 stable writer identity and operation allowlist.

## 14. Verification and acceptance state

```yaml
architecture_contracts:
  selected_model: DECIDED
  logical_identity: DECIDED
  physical_key_strategy: DECIDED
  topology_owner: DECIDED
  child_admission_commit_boundary: DECIDED
  dependency_count_authority: 0
  canonical_command_identity_count: 1
  logical_edge_resolution_authority: DECIDED
  same_logical_edge_conflict_check: ATOMIC
  different_outcome_double_application: IMPOSSIBLE
  failed_outcome: DECIDED
  blocked_child_late_edge_disposition: DECIDED
  terminal_state_vocabulary_mapping: COMPLETE
  technical_unresolved_choice: 0

governance:
  predecessor_human_acceptance_verified: true
  predecessor_acceptance_record_present: true
  predecessor_acceptance_comment_id: 5083135387
  human_architecture_acceptance: NOT_READY
  merge_authorized: false
  product_implementation_authorized: false
```

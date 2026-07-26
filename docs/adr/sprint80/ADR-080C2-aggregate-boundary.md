# ADR-080C.2 — Aggregate Boundary and Parent-Child Coordination

- Status: **CORRECTED TECHNICAL RECOMMENDATION — PREDECESSOR ACCEPTANCE PENDING**
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

ADR-080C.1 is merged at `9d7c7a6ad52a8a546a708dda048f28d4e23befc6`, but no exact post-merge human architecture acceptance record is currently bound to this package.

```yaml
predecessor_decision: ADR-080C1
accepted_head_candidate: 9ad9503badd72afb0a935dbb8c02e828ea02d3e2
merge_commit: 9d7c7a6ad52a8a546a708dda048f28d4e23befc6
acceptance_type: POST_MERGE_HUMAN_ARCHITECTURE_ACCEPTANCE_REQUIRED
acceptance_comment_id: null
human_architecture_acceptance: PENDING
status: BLOCKING_GOVERNANCE_PREREQUISITE
```

`MERGED` is not treated as `HUMAN_ACCEPTED`. This ADR may be technically corrected and tested, but it is not ready for human acceptance or merge until the predecessor record is present and machine-readably bound.

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

The technical rejection of a run-wide/DAG-wide aggregate is conditional on completion of the ADR-080C.1 governance chain. The other rejected alternatives are independent of that predecessor acceptance.

## 4. Logical identity versus physical Redis keys

The logical identity and physical storage identity are deliberately separated.

```yaml
logical_identity: run_id + task_id
physical_key_layout: EXISTING_TASK_ID_KEYS_TEMPORARILY_RETAINED
required_invariant: TASK_ID_GLOBALLY_UNIQUE_ACROSS_ALL_RUNS
collision_rule: EXISTING_TASK_ID_WITH_DIFFERENT_RUN_ID_REJECT_ADMISSION
physical_rekey_in_80C2: NOT_SELECTED
```

Existing keys such as `hfa:dag:task:<task_id>:state` remain a temporary physical layout. A run-scoped rekey is not silently implied. Any future rekey requires a separate accepted migration protocol with compatibility reads, writer cutover, old-key reconciliation, collision detection and rollback boundaries.

## 5. Child admission and topology authority

The child aggregate owns its immutable expected parent-edge set. Child admission is the topology commit point for that child.

```yaml
child_admission_boundary:
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
graph_identity_format: run:{run_id}:graph
graph_revision_or_topology_hash: REQUIRED_IMMUTABLE_SNAPSHOT_IDENTIFIER
```

Normative invariants:

```yaml
dependency_count:
  authority: false
  rule: MUST_EQUAL_CARDINALITY_OF_EXPECTED_PARENT_EDGE_SET

missing_expected_parent_set: FAIL_CLOSED
count_set_mismatch: REJECT_ADMISSION_OR_RECONCILIATION_REQUIRED
graph_identity_missing: FAIL_CLOSED
topology_revision_or_hash_missing: FAIL_CLOSED
active_run_topology_mutation: FORBIDDEN_UNLESS_SEPARATE_ACCEPTED_GRAPH_REVISION_PROTOCOL
```

The child reconstructs dependency truth from the immutable expected set plus outcome-aware applied-edge receipts. It never reconstructs authority from `remaining_deps`.

## 6. Parent completion boundary and fanout intent

One parent completion commit may atomically write only:

- parent state, metadata and output;
- parent monotonic revision;
- exactly one parent CanonicalTransitionRecord;
- one durable dependency-fanout intent.

It mutates zero child and zero sibling authority keys.

Every fanout intent must contain:

```yaml
parent_transition_id: REQUIRED
graph_identity: REQUIRED
graph_revision_or_topology_hash: REQUIRED
child_edge_set_digest: REQUIRED
child_edge_set_digest_algorithm: CANONICAL_SORTED_CHILD_EDGE_IDENTITIES_SHA256
```

## 7. One canonical edge-command identity

The topology edge identity and command identity have different roles, but only one command identity controls idempotency.

```yaml
logical_edge_id:
  format: "{run_id}:{parent_task_id}:{child_task_id}:{graph_revision_or_topology_hash}"
  role: TOPOLOGY_EDGE_IDENTITY

edge_command_id:
  format: "{parent_transition_id}:{child_task_id}:{edge_outcome}:{graph_revision_or_topology_hash}"
  role: SOLE_COMMAND_AND_RECEIPT_IDEMPOTENCY_AUTHORITY

receipt_key_authority: edge_command_id
process_manager_identity: dependency-fanout:{parent_transition_id}
process_manager_identity_role: COORDINATION_INSTANCE_ONLY
```

The same `edge_command_id` delivered again returns `ALREADY_APPLIED`, increments no child revision and creates no record. A different parent transition or different outcome for the same logical edge is a contradiction, not a duplicate; it produces zero child mutation, a durable conflict record and a reconciliation candidate.

## 8. Outcome-aware edge receipts

Applied dependency authority is:

```yaml
applied_dependency_authority: IDEMPOTENT_APPLIED_EDGE_RECEIPTS_WITH_OUTCOME
allowed_outcomes:
  - DEPENDENCY_SATISFIED
  - DEPENDENCY_FAILED
```

Every receipt contains:

```yaml
edge_command_id: REQUIRED
parent_transition_id: REQUIRED
parent_task_id: REQUIRED
child_task_id: REQUIRED
outcome: REQUIRED
graph_identity: REQUIRED
graph_revision_or_topology_hash: REQUIRED
applied_child_revision: REQUIRED
```

`remaining_deps` is a derived projection only. `ready_emitted` is a non-authoritative projection receipt only.

## 9. Deterministic dependency policy

The default policy is `ALL_REQUIRED_PARENTS_MUST_SUCCEED`.

### Satisfied edge

A new valid `DEPENDENCY_SATISFIED` receipt:

```yaml
child_revision_increment: 1
canonical_transition_record: EXACTLY_ONE
ready_when: ALL_EXPECTED_EDGES_HAVE_DEPENDENCY_SATISFIED_RECEIPTS_AND_CHILD_IS_PENDING
ready_projection_intent: ONE_IF_CHILD_BECOMES_READY
```

### Failed edge

A new valid `DEPENDENCY_FAILED` receipt commits inside the child aggregate:

```yaml
child_disposition: blocked_by_failure
child_revision_increment: 1
canonical_transition_record: EXACTLY_ONE
ready_projection_intent: 0
process_manager_retry: STOP_AFTER_RECEIPT
```

`blocked_by_failure` is an existing terminal task-state vocabulary value. It is not a placeholder.

Unknown dependency policy fails closed with zero child mutation, a durable conflict record and a reconciliation candidate.

## 10. Terminal and late command dispositions

A terminal/no-authority disposition record belongs to durable process-manager coordination state. It is not a child revision when child mutation is zero.

```yaml
child_already_terminal_due_to_valid_external_cancellation:
  child_mutation: 0
  disposition: TERMINAL_CHILD_NOOP
  durable_disposition_record: REQUIRED
  retry: STOP

child_already_blocked_by_failure_with_exact_receipt:
  child_mutation: 0
  disposition: ALREADY_APPLIED
  retry: STOP

child_done_or_failed_before_required_edges_complete:
  child_mutation: 0
  disposition: CONTRADICTION
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
  retry: STOP

child_ready_or_running_with_unsatisfied_required_edges:
  child_mutation: 0
  disposition: CONTRADICTION
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
  retry: STOP

parent_transition_valid_but_graph_edge_missing:
  child_mutation: 0
  disposition: INVALID_TOPOLOGY_EDGE
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
  retry: STOP

child_aggregate_missing:
  child_mutation: 0
  disposition: MISSING_AUTHORITY
  durable_conflict_record: REQUIRED
  reconciliation_candidate: REQUIRED
  retry: STOP
```

The process manager retries only until an outcome-aware receipt, durable terminal disposition or durable conflict record exists.

## 11. Ready projection semantics

`ready_emitted` cannot authorize or suppress a state transition. A normal `pending -> ready` transition commits state, child revision, one canonical record and ready-projection intent in one child boundary. Missing queue projection is replayed from the canonical ready transition. Legacy marker/state conflict fails closed and enters explicit reconciliation.

## 12. Canonical record behavior

Every newly applied valid edge receipt, satisfied or failed, increments the child revision exactly once and produces exactly one child CanonicalTransitionRecord. The final satisfied edge may include both the receipt effect and `pending -> ready` in that same record. Duplicate commands produce neither revision nor record.

The exact record schema remains ADR-080C.3. The atomic record/outbox mechanism remains ADR-080C.5.

## 13. Finding disposition

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

This ADR does not claim either product finding is fixed.

## 14. Migration and rollback

1. Run a shadow process manager with zero product writes.
2. Add child admission topology authority and outcome-aware receipts behind a single-writer gate.
3. Disable direct parent-to-child writes before making the process manager authoritative.
4. Never run legacy counter authority and child-revision authority as simultaneous writers.
5. Before any new child revision commits, rollback may return to the legacy path.
6. After cutover, rollback requires reconciliation; blind counter decrement is never re-enabled.

## 15. Dependencies

- ADR-080C.1 post-merge human architecture acceptance record;
- ADR-080C.3 canonical transition and monotonic revision contract;
- ADR-080C.4 durable authority and reconstruction contract;
- ADR-080C.5 event/state atomicity and outbox contract;
- ADR-080C.6 stable writer identity and operation allowlist.

## 16. Verification and acceptance state

```yaml
architecture_contracts:
  selected_model: DECIDED
  logical_identity: DECIDED
  physical_key_strategy: DECIDED
  topology_owner: DECIDED
  child_admission_commit_boundary: DECIDED
  graph_identity_and_topology_hash: REQUIRED
  dependency_count_authority: 0
  canonical_command_identity_count: 1
  satisfied_outcome: DECIDED
  failed_outcome: DECIDED
  terminal_child_disposition: DECIDED
  process_manager_retry_termination: DECIDED
  technical_unresolved_choice: 0

governance:
  predecessor_human_acceptance_verified: false
  predecessor_acceptance_record_present: false
  human_architecture_acceptance: NOT_READY
  merge_authorized: false
  product_implementation_authorized: false
```

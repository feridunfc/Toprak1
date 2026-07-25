# ADR-080C.2 — Aggregate Boundary and Parent-Child Coordination

- Status: **TECHNICAL RECOMMENDATION — READY FOR HUMAN ARCHITECTURE REVIEW**
- Sprint: 80C.2
- Branch: `sprint/80c2-aggregate-boundary`
- Base: `baseline/local-import@9d7c7a6ad52a8a546a708dda048f28d4e23befc6`
- Product implementation authorized: **NO**
- Product source mutation in this tranche: **0**

## 1. Immutable input

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

Observed product behavior is not reinterpreted: `task_complete.lua` commits a parent terminal transition and directly changes child state, dependency counter, ready marker and ready queue inside one Redis Lua call. No child aggregate revision or CanonicalTransitionRecord was observed.

## 2. Decision

```yaml
aggregate_model: INDEPENDENT_TASK_AGGREGATES_WITH_DURABLE_DEPENDENCY_PROCESS_MANAGER
task_aggregate_identity: task:{run_id}:{task_id}
parent_revision_owner: PARENT_TASK_AGGREGATE
child_revision_owner: CHILD_TASK_AGGREGATE
cross_task_direct_authority_mutation: FORBIDDEN
distributed_transaction_across_tasks: false
```

Each task is its own lifecycle transaction aggregate. The accepted 80C.1 task truth owner remains the task state authority; 80C.2 does not replace it with a run-wide or DAG-wide transaction authority.

A durable dependency process manager coordinates parent-child effects. It may issue commands but is not task truth authority and may not mutate child Redis authority keys directly.

## 3. Atomic mutation boundaries

### Parent completion

One parent completion commit may atomically write only:

- parent task state, metadata and output;
- parent task monotonic revision;
- exactly one parent CanonicalTransitionRecord;
- one durable dependency-fanout intent/outbox entry.

It must mutate **zero child authority keys** and **zero sibling authority keys**.

### Child dependency application

One edge command commits inside exactly one child task aggregate. It may atomically write:

- the idempotent edge receipt;
- child dependency satisfaction state;
- child state when the unlock condition becomes true;
- child monotonic revision;
- exactly one child CanonicalTransitionRecord;
- ready-queue projection intent when the child becomes ready.

It must mutate zero parent or sibling task authority keys.

## 4. Dependency authority

The mutable `remaining_deps` counter is rejected as authority. Child dependency authority is:

```yaml
expected_dependencies: IMMUTABLE_EXPECTED_PARENT_EDGE_SET
applied_dependencies: IDEMPOTENT_SATISFIED_EDGE_RECEIPTS
remaining_deps: DERIVED_PROJECTION_ONLY
edge_idempotency_key: {run_id}:{parent_task_id}:{child_task_id}:{parent_revision}
```

Rules:

- missing expected-edge metadata fails closed;
- an unexpected parent edge is rejected with no child revision and no transition record;
- a duplicate edge command returns `ALREADY_APPLIED` with no new revision and no new record;
- a new valid edge receipt increments the child revision exactly once and produces exactly one canonical record;
- the child becomes ready only when the satisfied set exactly equals the expected parent set and the child is pending;
- decrement, negative clamp and inferred zero are forbidden.

A successful parent emits `DEPENDENCY_SATISFIED`. A non-success terminal parent emits `DEPENDENCY_FAILED`; the child aggregate evaluates the immutable dependency policy. The compatibility default is `ALL_REQUIRED_PARENTS_MUST_SUCCEED`.

## 5. Ready projection semantics

`ready_emitted` is not authority. It is a projection receipt bound to the canonical child ready-transition ID.

- It cannot authorize or suppress a child state transition.
- Normal `pending -> ready` writes state, revision, canonical record and ready projection intent in the child atomic boundary.
- A missing queue projection is replayed from the canonical ready transition.
- A legacy marker/state contradiction before cutover fails closed and enters explicit reconciliation.
- An existing legacy marker must never silently strand an otherwise eligible child.

## 6. Process-manager contract

```yaml
identity: dependency-fanout:{parent_transition_id}
delivery: AT_LEAST_ONCE
child_application: EXACTLY_ONCE_BY_EDGE_RECEIPT
fanout_unit: ONE_COMMAND_PER_PARENT_CHILD_EDGE
direct_child_key_mutation: false
```

The process manager retries until the child receipt exists or an explicit terminal disposition is recorded. Pending fanout work is reconstructed from durable parent transitions/outbox entries and child edge receipts.

No distributed transaction spans the parent and every child. Correctness comes from atomic single-aggregate commits, durable delivery and deterministic idempotency.

## 7. Required canonical record behavior

Every newly applied dependency edge is a child aggregate revision even when the child remains pending. Therefore it produces exactly one child CanonicalTransitionRecord.

When the final edge is applied, the same record contains both:

- the edge-receipt effect; and
- the `pending -> ready` effect plus ready-projection intent.

A duplicate edge command produces neither a revision nor a record. The parent completion record contains durable fanout intent, not falsely claimed committed child-state effects.

The exact record schema and monotonic revision mechanics remain dependencies of ADR-080C.3. The atomic outbox/commit mechanism remains a dependency of ADR-080C.5.

## 8. Repair and replay

```yaml
missing_remaining_counter: NOT_AUTHORITY_NO_DECREMENT_OR_CLAMP
missing_expected_parent_set: FAIL_CLOSED
marker_state_conflict: EXPLICIT_RECONCILIATION
lost_fanout_delivery: REPLAY_FROM_DURABLE_PARENT_TRANSITION_OR_OUTBOX
lost_ready_projection: REPLAY_FROM_CHILD_READY_TRANSITION
orphan_edge_receipt: REJECT_UNLESS_PARENT_TRANSITION_AND_GRAPH_EDGE_VALIDATE
```

Durable source classification and reconstruction ownership remain dependencies of ADR-080C.4.

## 9. Alternatives rejected

### One DAG/run aggregate

Rejected for this product path. It would replace the accepted task truth boundary with a run-wide transaction authority, serialize unrelated task transitions, create an unbounded hot aggregate, and require broad Redis key/revision migration. That is architecture theater rather than the shortest safe correction.

### Direct parent completion mutation of child keys

Rejected because the parent writer cannot own child revisions and cannot provide one authoritative child record per effect.

### Remaining counter and ready marker as authority

Rejected because frozen evidence proves both fail-open underflow and fail-closed stranding.

### Distributed transaction across parent and all children

Rejected because it adds global atomicity and coordinator complexity without solving replay or idempotency better than per-child commands.

## 10. Finding disposition

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

This ADR assigns architecture. It does not close either product finding.

## 11. Migration constraints

1. Run a shadow process manager that calculates edge commands but performs no product writes.
2. Introduce the child command/receipt boundary behind a single-writer gate.
3. Disable direct child mutation in parent completion before making the process manager authoritative.
4. Never run legacy counter mutation and child-revision authority as simultaneous writers.
5. Before cutover, rollback may return to the legacy path only while no child revision has committed.
6. After cutover, rollback requires reconciliation; blind counter decrement must never be re-enabled.

## 12. Dependencies

- accepted ADR-080C.1 runtime truth ownership;
- ADR-080C.3 canonical transition and monotonic revision;
- ADR-080C.4 durable authority and reconstruction;
- ADR-080C.5 event/state atomicity and outbox;
- ADR-080C.6 stable writer identity and allowlist.

No product implementation is authorized until the dependent contracts required by the implementation slice are accepted.

## 13. Verification contract

```yaml
aggregate_model_decided: true
aggregate_identity_decided: true
parent_revision_owner_decided: true
child_revision_owner_decided: true
atomic_boundaries_decided: true
direct_cross_task_authority_mutation: 0
remaining_counter_authority: 0
ready_marker_authority: 0
frozen_observations_mapped: 8
frozen_findings_mapped: 2
findings_resolved_in_product: 0
technical_unresolved_choice: 0
product_source_mutation: 0
implementation_claims: 0
```

## 14. Acceptance state

```yaml
technical_recommendation: COMPLETE
human_architecture_acceptance: PENDING
ADR_status_after_human_acceptance: ACCEPTED
product_implementation_authorized_by_this_PR: false
sprint84_implementation_authorized_by_this_PR: false
```

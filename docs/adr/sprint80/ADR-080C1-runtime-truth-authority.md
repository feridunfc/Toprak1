# ADR-080C.1 — Runtime Truth Ownership

- Status: **CORRECTED TECHNICAL RECOMMENDATION — READY FOR RE-REVIEW**
- Sprint: 80C.1
- Branch: `sprint/80c1-runtime-truth-authority`
- Base: `baseline/local-import@3439618ea7ad8cf8bdd0e49d660217fc455c787c`
- Human architecture acceptance: **PENDING**
- Product implementation authorized: **NO**
- Product source mutation in this tranche: **0**

## 1. Immutable input

This decision consumes the frozen Sprint 80B evidence package without changing its meaning:

```yaml
source_head: 5734ac60704f8546b8ce67e766e042ff2ce4c412
artifact_sha256: 7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588
truth_contradictions_sha256: 037fcd91c5aeec88039dd17705a05678790ab7f7969647a1cb7e624fcfda9d07
historical_frozen_zip_verification: ALREADY_FROZEN_AND_DIGEST_PINNED
observation_count: 9
caller_count: 8
global_truth_policy: INCONSISTENT_BY_CALLER
```

The immutable repository evidence copy is retained at
`docs/adr/sprint80/evidence/truth_contradictions.run93.json`. Its bytes remain
identical to `truth_contradictions.json` in the frozen run-93 ZIP. The evidence
remains an observed product gap and is not reclassified as resolved by this ADR.

## 2. Decision boundary

80C.1 decides **truth ownership** only. It does not decide transaction aggregate identity, revision ownership, or the atomic mutation boundary.

```yaml
task_lifecycle_truth_owner: TASK_STATE_AUTHORITY
run_lifecycle_truth_owner: RUN_STATE_AUTHORITY

transaction_aggregate_boundary:
  status: DEFERRED_TO_ADR_080C2
aggregate_identity:
  status: DEFERRED_TO_ADR_080C2
revision_owner:
  status: DEFERRED_TO_ADR_080C2
atomic_mutation_boundary:
  status: DEFERRED_TO_ADR_080C2
```

Therefore the terms `TASK_AGGREGATE` and `RUN_AGGREGATE` are not frozen by this ADR. ADR-080C.2 remains free to choose independent task aggregates plus a process manager or a DAG/run aggregate.

## 3. Normative truth contract

```yaml
cross_plane_conflict_mutation: FAIL_CLOSED
missing_authority_record: FAIL_CLOSED
repair_path: EXPLICIT_RECONCILIATION
silent_auto_repair: false
```

A terminal/nonterminal RUN–TASK contradiction blocks every new lifecycle mutation until reconciliation classifies the conflict. A stale projection never authorizes mutation.

Queries are scope-specific:

- a run query returns RUN state truth;
- a task query returns TASK state truth;
- cross-plane data is conflict metadata or enrichment only;
- queries remain read-only;
- contradiction is never silently hidden.

## 4. Terminal RUN contracts

### 4.1 Heartbeat

When RUN truth is terminal and TASK truth is running, a valid owner and claim epoch are not sufficient to refresh liveness.

```yaml
run_terminal_task_running_heartbeat:
  result: REJECT
  liveness_ttl_refresh: 0
  running_zset_refresh: 0
  reconciliation_candidate: REQUIRED
```

### 4.2 Normal completion and failure

When RUN truth is terminal and TASK truth is running, normal task completion and normal task failure are rejected.

```yaml
normal_completion_when_run_terminal: REJECT
normal_failure_when_run_terminal: REJECT
child_effects: 0
output_commit: 0
normal_ack_authorization: 0
next_path: EXPLICIT_RECONCILIATION_OR_CANCELLATION_COMMAND
```

Transport duplicate cleanup is separate from lifecycle completion. An already-terminal task delivery may be ACKed only when explicit task ID, explicit run ID, and terminal task evidence match. This exception does not authorize heartbeat, completion, failure, child effects, or output writes.

## 5. Operation ownership table

| Operation | Truth owner | Required cross-plane rule | Conflict result |
|---|---|---|---|
| read/query | requested state authority | expose contradiction metadata | read-only result |
| admission | operation-scoped state authority | required counterpart exists and is compatible | fail closed |
| dispatch | TASK state authority | RUN exists and is nonterminal | zero mutation |
| claim | TASK state authority | RUN exists and is nonterminal | zero ownership mutation |
| heartbeat | TASK state authority | RUN exists and is nonterminal | reject; zero TTL refresh |
| completion/failure | TASK state authority | RUN exists and is nonterminal | reject; zero child/output effect |
| requeue | TASK state authority | RUN exists and is nonterminal | zero requeue |
| recovery | operation-scoped state authority | classify both truth planes before mutation | reconciliation candidate |
| reconciliation | dedicated coordinator | preserve evidence and explicit command authority | audited explicit path |

The machine-readable normative contract is `runtime_truth_authority_matrix.json`.

## 6. Caller migration decisions

- **Control API run query:** retain RUN truth for run-scoped output and add explicit TASK conflict metadata.
- **Run recovery:** missing RUN truth or RUN/TASK contradiction yields zero cleanup/reschedule mutation before reconciliation classification.
- **Task recovery:** missing TASK truth or terminal RUN conflict yields zero requeue mutation.
- **Modern worker duplicate guard:** TASK terminal truth suppresses execution; terminal RUN with nonterminal TASK blocks claim, heartbeat, and normal completion.
- **Legacy worker guard:** may suppress legacy execution but cannot authorize modern task lifecycle mutation.
- **Scheduler dispatch:** terminal or missing RUN truth blocks `ready -> scheduled` before Lua mutation.

## 7. Frozen finding dispositions

```yaml
inconsistent_by_caller:
  decision_status: ARCHITECTURE_DECISION_ASSIGNED
  resolved_in_product: false
  implementation_sprint: 82

contradictory_mutation_not_guaranteed_blocked:
  decision_status: ARCHITECTURE_DECISION_ASSIGNED
  resolved_in_product: false
  implementation_sprint: 82
```

These dispositions map the two accepted Sprint 80B truth findings to this ADR. They do not claim product resolution.

## 8. Dependencies

80C.1 has explicit dependencies on:

1. **ADR-080C.2** — aggregate identity, revision owner, and atomic boundary;
2. **ADR-080C.3** — canonical transition/revision contract;
3. **ADR-080C.6** — stable writer identity and allowlist.

## 9. Alternatives rejected

- **Global RUN wins:** rejected because task ownership, fencing, heartbeat, completion, and dependency effects require TASK state truth.
- **Global TASK wins:** rejected because run creation, cancellation, and run-level lifecycle require RUN state truth.
- **Caller-local winner:** rejected because Sprint 80B proved inconsistent fail-open, fail-closed, suppression, and committed mutation.
- **Silent eventual consistency:** rejected because no accepted durable revision/replay contract exists yet.
- **Prejudging aggregate boundary in 80C.1:** rejected because that choice belongs to ADR-080C.2.

## 10. Verification contract

```yaml
scope:
  exact_changed_files: 7
  product_source_mutation: 0

evidence:
  historical_frozen_zip_verification: ALREADY_FROZEN_AND_DIGEST_PINNED
  truth_evidence_digest: PASS
  observations_mapped: 9
  callers_mapped: 8

truth_decision:
  task_lifecycle_truth_owner: DECIDED
  run_lifecycle_truth_owner: DECIDED
  cross_plane_conflict_mutation: FAIL_CLOSED
  heartbeat_terminal_run_behavior: DECIDED
  completion_terminal_run_behavior: DECIDED
  technical_unresolved_choice: 0

aggregate_boundary:
  prejudged_by_80C1: false
  dependency_on_80C2: EXPLICIT

finding_mapping:
  expected_truth_findings: 2
  mapped_truth_findings: 2
  resolved_in_product: 0

governance:
  human_architecture_acceptance: PENDING
  adr_document_merge_authorized: false
  product_implementation_authorized: false
```

The package-specific workflow must run the dedicated pytest file and validator on every relevant PR HEAD. Repository ruleset configuration must separately mark that check as required; this ADR does not claim that ruleset enforcement already exists.

## 11. Acceptance state

```yaml
technical_correction: COMPLETE
package_specific_CI: REQUIRED_TO_PASS
existing_authority_gate: REQUIRED_TO_PASS
human_architecture_acceptance: PENDING
merge_authorized: false
product_implementation_authorized: false
```

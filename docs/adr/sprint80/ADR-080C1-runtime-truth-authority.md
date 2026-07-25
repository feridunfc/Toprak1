# ADR-080C.1 — Runtime Truth Authority

- Status: **TECHNICAL RECOMMENDATION — READY FOR HUMAN ARCHITECTURE REVIEW**
- Sprint: 80C.1
- Branch: `sprint/80c1-runtime-truth-authority`
- Base: `baseline/local-import@3439618ea7ad8cf8bdd0e49d660217fc455c787c`
- Product implementation authorized: **NO**
- Product source mutation in this tranche: **0**

## 1. Immutable input

This decision consumes the frozen Sprint 80B evidence package without reinterpretation:

```yaml
source_head: 5734ac60704f8546b8ce67e766e042ff2ce4c412
artifact_sha256: 7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588
truth_contradictions_sha256: 037fcd91c5aeec88039dd17705a05678790ab7f7969647a1cb7e624fcfda9d07
observation_count: 9
global_truth_policy: INCONSISTENT_BY_CALLER
```

The evidence remains an observed product gap. Passing reality tests do not mean the authority problem is fixed.

## 2. Decision

### 2.1 Aggregate-scoped authority

The system must not choose one global winner between RUN and TASK records.

```yaml
task_lifecycle_authority: TASK_AGGREGATE
run_lifecycle_authority: RUN_AGGREGATE
cross_plane_conflict_mutation: FAIL_CLOSED
repair_path: EXPLICIT_RECONCILIATION
```

A RUN record is authoritative only for run-level lifecycle decisions. A TASK record is authoritative for task admission, dispatch, claim, heartbeat, completion/failure and requeue. Records in the other plane may be used as projections, precondition guards and conflict detectors, but never as a second co-authority for the same lifecycle mutation.

### 2.2 Cross-plane invariant

A terminal/nonterminal contradiction between the run and task planes blocks every new lifecycle mutation that could deepen the contradiction.

- Terminal run + ready/running task: dispatch, claim and requeue are blocked.
- Nonterminal run + terminal task: claim, heartbeat and completion are blocked by task terminal truth.
- Missing authority record: mutation is blocked.
- Stale or missing projection: projection data cannot authorize mutation.
- Contradiction: produce a deterministic reconciliation candidate with causation and evidence references.

Transport cleanup is not a lifecycle transition. An already-terminal task delivery may be ACKed only when explicit task identity, explicit run identity and terminal task evidence match. The RUN/TASK conflict must still be surfaced for reconciliation.

### 2.3 Query behavior

Queries are scoped rather than winner-take-all:

- run endpoint → RUN authority;
- task endpoint → TASK authority;
- cross-plane data → enrichment and conflict metadata only;
- query paths remain read-only;
- contradiction is never silently hidden by returning only one plane.

### 2.4 Recovery behavior

Run recovery may mutate only the run aggregate and its projections. Task recovery may mutate only the task aggregate and its projections. Before any mutation, recovery must classify cross-plane contradiction and missing authority.

When contradiction exists:

```text
business mutation: 0
projection cleanup: 0 before reconciliation classification
requeue/reschedule: 0
output: reconciliation candidate
```

### 2.5 Legacy compatibility

`IdempotencyGuard.should_execute(run_id)` is a bounded legacy compatibility reader. It cannot authorize task execution in the production task graph. The production graph must use task authority and the cross-plane terminal guard. Legacy run-based execution authority is scheduled for migration in Sprint 82 and removal/kill-switch convergence in Sprint 86.

## 3. Operation authority table

| Operation class | Authority | Other-plane use | Conflict result |
|---|---|---|---|
| read/query | requested aggregate scope | enrichment/conflict detection | return conflict metadata; no mutation |
| admission | aggregate-local | nonterminal/existence precondition | fail closed |
| dispatch | TASK | terminal RUN guard | block before Lua mutation |
| claim | TASK | terminal RUN guard | block before ownership mutation |
| heartbeat | TASK | none for authority | reject; do not refresh liveness |
| completion/failure | TASK | RUN conflict signal | reject conflicting mutation |
| requeue | TASK | terminal RUN guard | no requeue |
| recovery | aggregate-local | contradiction classification | no mutation; reconciliation candidate |
| reconciliation | dedicated coordinator | reads both authorities and evidence | explicit audited command only |

The machine-readable normative table is `runtime_truth_authority_matrix.json`.

## 4. Caller migration decisions

### Control API run query

Keep RUN authority for a run-scoped query, but add explicit task-conflict metadata. The current behavior reads run state/meta only and hides a contradictory task plane.

### Run recovery

`RecoveryService._find_stale_runs()` must not remove a run projection or reschedule a run before classifying task-plane contradiction. Missing run authority and terminal/nonterminal disagreement both produce no mutation and a reconciliation candidate.

### Task recovery

Missing task state blocks requeue even when the running ZSET suggests a stale candidate. A terminal RUN record is a blocking cross-plane conflict, not permission to infer task truth.

### Modern worker duplicate guard

TASK state remains the task-delivery authority. If RUN is terminal while TASK is nonterminal, downstream claim is blocked. If TASK is terminal, execution is suppressed; explicit-identity ACK may proceed as transport cleanup while the conflict is recorded.

### Legacy worker guard

RUN state may suppress legacy execution but cannot authorize modern task execution. Production composition must not route task lifecycle authority through this guard.

### Scheduler dispatch

The current Lua commit reads task state and identity but not run state. The production dispatch boundary must perform a terminal RUN guard in the same accepted authority boundary before `ready -> scheduled` can commit.

## 5. Alternatives rejected

### RUN always wins

Rejected because task claim, fencing, heartbeat, completion and dependency effects are task-scoped. RUN-only authority would discard the stronger ownership and revision domain.

### TASK always wins

Rejected because run creation, run cancellation and run-level lifecycle remain real aggregate decisions. A task record cannot define the whole run lifecycle by itself.

### Caller-local winner

Rejected because Sprint 80B proved caller-local selection produces fail-open, fail-closed, suppressed and committed outcomes for equivalent contradictions.

### Silent eventual consistency

Rejected because there is no durable canonical revision/replay contract that can safely converge contradictions without explicit evidence and ownership.

## 6. Consequences

- Existing callers require migration; this ADR does not implement it.
- Cross-plane reads increase at mutation boundaries until a canonical aggregate/revision contract is implemented.
- Some currently successful operations become deterministic conflicts.
- Reconciliation becomes a first-class product capability rather than ad-hoc cleanup.
- Stable writer IDs remain dependent on ADR-080C.6.
- Canonical revision and transition records remain dependent on ADR-080C.3.

## 7. Verification contract

80C.1 is technically complete only when the decision package proves:

```yaml
frozen_artifact_sha256_matches: true
truth_evidence_sha256_matches: true
observations_mapped: 9
callers_mapped: 8
operation_classes_decided: 9
unclassified_truth_reader: 0
unclassified_truth_writer: 0
technical_unresolved_choice: 0
product_source_mutation: 0
implementation_claims: 0
```

Future Sprint 82 implementation closes only when:

- the two Sprint 80B truth XFAIL contracts pass;
- all observed callers follow this deterministic matrix;
- contradictory mutation count is zero;
- missing authority mutation count is zero;
- every conflict produces reconciliation evidence.

## 8. Acceptance state

```yaml
technical_recommendation: COMPLETE
human_architecture_acceptance: PENDING
ADR_status_after_human_acceptance: ACCEPTED
product_implementation_authorized_by_this_PR: false
```

# ADR-080C — Runtime Authority and Transition Contract Decision Package

- Status: PROPOSED
- Sprint: 80C
- Scope: architecture decisions only
- Product implementation authorized: NO
- Base branch: `baseline/local-import`
- Base commit / squash merge: `e3ae0b7b7b5943504da6fa2d8d25674ef7ccf159`

## Immutable 80B input

Sprint 80C consumes the following independently verified evidence snapshot as immutable input:

```yaml
source_branch: sprint/80b-executable-reconciliation
source_head: 5734ac60704f8546b8ce67e766e042ff2ce4c412
artifact_sha256: 7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588
squash_merge_commit: e3ae0b7b7b5943504da6fa2d8d25674ef7ccf159
finding_count: 15
```

The 80B evidence establishes accepted gaps in five categories:

- event/state parity: 3
- transition cardinality/revision: 2
- durability/reconstruction: 6
- run/task truth: 2
- parent/child correctness: 2

These are observed product gaps, not failed diagnostic tests.

## Decision boundary

80C must decide the target contracts required before implementation. It must not modify runtime, scheduler, worker, Lua, transport, semantic, or persistence behavior.

No decision in this package is considered implemented merely because it is accepted.

## ADR-80C.1 — Runtime truth authority

### Observed problem

Different callers select RUN or TASK truth differently. Contradictory state can be returned, suppressed, failed open, failed closed, rejected, or committed depending on the entry point.

### Decision required

Choose one deterministic authority contract for each operation class:

1. read/query authority;
2. admission and dispatch authority;
3. worker claim and heartbeat authority;
4. terminal completion authority;
5. recovery and reconciliation authority.

The decision must define:

- authoritative record;
- projection records;
- conflict behavior;
- stale/missing record behavior;
- whether terminal truth in either plane blocks mutation;
- repair ownership and audit requirements.

### Candidate direction

TASK-scoped execution truth is authoritative for task lifecycle operations. RUN truth is authoritative only for run-level lifecycle operations. Cross-plane conflicts must fail closed for mutation and enter an explicit reconciliation path.

This candidate remains unaccepted until the caller matrix and migration consequences are reviewed.

## ADR-80C.2 — Aggregate boundary

### Observed problem

Parent completion performs cross-task child mutations. Missing dependency counters can unlock a child fail-open; an existing ready marker can strand a zero-dependency child.

### Decision required

Choose one aggregate model:

- independent task aggregates coordinated by a process manager; or
- one DAG/run aggregate containing parent-child dependency effects.

### Required invariant

Regardless of aggregate choice:

> Every committed aggregate revision containing child effects must produce exactly one CanonicalTransitionRecord that records those child effects.

The decision must define:

- aggregate identity;
- revision owner;
- atomic mutation boundary;
- child unlock idempotency;
- missing counter behavior;
- ready marker semantics;
- repair/replay behavior.

## ADR-80C.3 — Canonical transition and revision contract

### Observed problem

No verified CanonicalTransitionRecord or aggregate revision is currently observed. Existing epochs are coordination fences, not aggregate revisions.

### Decision required

Define the minimum canonical record:

```yaml
transition_id: globally unique
aggregate_type: run | task | dag
aggregate_id: stable identity
aggregate_revision: monotonic integer
operation: controlled taxonomy
previous_state: explicit or null
next_state: explicit or null
child_effects: complete list
causation_id: stable
correlation_id: stable
idempotency_key: stable
committed_at: authoritative timestamp
writer_id: stable writer identity
schema_version: integer
```

The decision must also define:

- exactly-once meaning at the authority boundary;
- duplicate record behavior;
- revision conflict behavior;
- retry behavior;
- projection application rules;
- audit/evidence relationship;
- compatibility with existing transport messages and audit events.

Full Event Sourcing and CQRS are explicitly not implied.

## ADR-80C.4 — Durability and reconstruction authority

### Observed problem

Run state/result, task state/meta/output, and operator audit history have blocking durability or reconstruction gaps.

### Decision required

For every retained state family, classify it as exactly one of:

- durable authority;
- reconstructable projection;
- ephemeral coordination;
- retained execution cache;
- audit history;
- transport retention.

For every expiring authority or projection, define:

- reconstruction source;
- reconstruction algorithm;
- maximum tolerated loss window;
- operator-visible failure mode;
- recovery ownership;
- evidence that recovery actually works.

A label such as “reconstructable” is invalid without an executable recovery path and a durable reconstruction source.

## ADR-80C.5 — Event/state atomicity

### Observed problem

The repository permits event-first orphaning, state-first missing transport, and committed state surviving failed background audit emission.

### Decision required

Choose and document one consistency model for each transition class:

- one atomic authority write with projections emitted afterward;
- transactional outbox;
- Redis-script atomic mutation plus canonical record;
- another explicitly justified model.

The decision must state:

- which write is the commit point;
- which artifacts are authoritative;
- retry and deduplication behavior;
- how missing projections are repaired;
- whether audit failure can block the business transition;
- how orphaned records are detected and reconciled.

## ADR-80C.6 — Writer identity and feature-flag disposition

### Observed problem

80B closed unknown writer and feature-flag classifications through explicit dispositions, but writer pinning remains path-based and some composition roots are deferred.

### Decision required

Define:

- stable writer IDs;
- writer-to-operation allowlist;
- composition-root registration;
- feature-flag ownership;
- production default behavior;
- removal/revisit triggers;
- enforcement location.

The accepted design should support writer ID or writer-set hash pinning without making filesystem path identity the authority.

## Explicit non-goals

80C does not:

- implement product fixes;
- introduce full Event Sourcing;
- introduce CQRS as an architectural default;
- rewrite the scheduler or worker;
- migrate Redis keys;
- change Lua scripts;
- claim recovery is proven without executable evidence;
- convert accepted 80B gaps into resolved findings.

## Required outputs

80C closes only when it produces accepted records for:

1. runtime truth authority;
2. aggregate boundary;
3. canonical transition/revision contract;
4. durability/reconstruction authority;
5. event/state atomicity;
6. writer identity and feature-flag governance;
7. implementation sequencing and rollback boundaries.

Each accepted record must include:

- decision;
- alternatives rejected;
- consequences;
- migration constraints;
- compatibility constraints;
- verification contract;
- owning implementation sprint.

## Acceptance gates

```yaml
immutable_80b_input_identified: REQUIRED
all_15_findings_mapped_to_decisions: REQUIRED
unresolved_architecture_choice: 0
product_source_mutation: 0
implementation_claims: 0
verification_contracts_defined: REQUIRED
human_architecture_acceptance: REQUIRED
merge_authorized: false
```

## Initial status

```yaml
sprint80c_started: true
phase: ADR_DRAFTING
product_implementation_started: false
merge_authorized: false
```

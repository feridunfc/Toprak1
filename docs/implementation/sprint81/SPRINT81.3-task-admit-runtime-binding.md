# Sprint 81.3 — TASK_ADMIT Trusted Runtime Binding

```yaml
selected_operation: TASK_ADMIT
writer_identity: hfa-control/task-admission-writer:v1
operation_identity: deterministic_from_task_aggregate_identity
committed_at_ms: stable_from_DagTaskSeed_admitted_at
feature_flag: HFA_CANONICAL_TASK_ADMIT_BINDING
default: disabled
commit_order:
  - canonical_authority_first
  - legacy_projection_second
known_non_atomic_boundary: explicit
projection_failure: durable_canonical_commit_plus_retryable_projection_pending
legacy_preexisting_task: blocked_without_migration
production_cutover: unauthorized
```

The production scheduler composition root resolves the feature flag once and injects the immutable decision into `DagLua`. Disabled mode preserves the existing `task_admit.lua` behavior and does not initialise canonical authority storage.

Enabled mode constructs exactly one deterministic `TASK_ADMIT` command and a fixed trusted writer context. The operation identity is `task-admit:v1:<canonical-task-identity-sha256>`. `DagTaskSeed.admitted_at` is validated as a finite, non-negative JCS-safe integral value and is the sole source of `committed_at_ms`.

Canonical authority is evaluated and persisted before the existing legacy Lua projection. Canonical conflicts fail closed without legacy mutation. A durable canonical commit followed by projection failure raises a retry-safe projection-pending error; retry resolves the same canonical operation as already applied and retries the idempotent legacy projection.

This sprint does not claim cross-key atomicity, exactly-once business execution, migration completion, broad runtime convergence, cutover, or production readiness.

## Independent review correction — expanded authority scope

The original seven-file boundary was insufficient because policy-level evaluator
conflicts intentionally return no commit plan. Sprint 81.3 therefore requires the
smallest explicit authority persistence extension:

- `RedisCanonicalAuthorityStore.record_authority_conflict()` writes the existing
  deterministic authority conflict index/stream pair atomically;
- `authority_conflict_record.lua` preserves pair cardinality and first-payload-wins
  deduplication without lifecycle mutation;
- operator audit notes remain separate and are not treated as authority evidence.

Additional fail-closed gates:

- snapshot and receipt/record presence and identity continuity are checked before
  evaluation or legacy projection;
- any task-specific legacy footprint blocks silent canonical backfill;
- `priority`, `dependency_count`, and `admitted_at` are exact safe integers and one
  normalized seed is used by both canonical policy and legacy Lua projection;
- read/initialisation persistence failures are translated to structured runtime
  authority errors;
- CI uses full history, supplies `SPRINT81_2_REDIS_URL`, rejects skipped Redis
  authority tests, and asserts the Sprint 81.1/81.2 minimum pass counts.

Production enablement and cross-key atomicity remain unauthorized.


## Final correction scope expansion

The runtime binding requires twelve changed files. `hfa-core/src/hfa/lua/authority_head_validate.lua` provides an atomic, read-only full-head gate before an `ALREADY_APPLIED` duplicate may reach the legacy projection. The gate verifies current aggregate/proof binding, transition-log tail, outbox tail, and lifecycle-store cardinality.

The disabled feature-flag path preserves the original legacy `admitted_at` float coercion. The enabled path uses a separate projection entry point receiving the exact normalized canonical integers. Redis, Lua-loader, connection, and wrong-type failures are translated through the persistence/runtime structured error boundaries.


## V3 exact duplicate gate correction

Both duplicate sources are gated identically before legacy projection:

- evaluator-classified `ALREADY_APPLIED`;
- commit-race `RedisCanonicalAuthorityStore.commit()` returning `ALREADY_APPLIED`.

The atomic head validator is bound to the exact proof classified by the evaluator or commit plan: aggregate identity, operation ID and digest, transition ID, revision, canonical command hash, and canonical record hash. A self-consistent but different authority head is rejected. Persisted semantic corruption is reported as `CANONICAL_RECORD_CORRUPTION_CONFLICT`; Redis connectivity, script loading, or execution unavailability is reported as `CONFLICT_EVIDENCE_STORE_UNAVAILABLE`.

## V5 operator-package correction

The operator patch is generated from the exact merged Sprint 81.2 baseline and
contains all twelve changed paths, including the seven newly created files. The
duplicate head gate derives expected snapshot state, projection intents, and
commit time from the validated canonical record rather than trusting the mutable
aggregate snapshot as its own expected value. Runtime receipt loading classifies
wrong-type receipt, operation-record, and transition-index keys as persisted
semantic corruption. CI reuses one Redis 7 service on port 6389 for both the
Sprint 81.2 authority regressions and the Sprint 81.3 integration suite.

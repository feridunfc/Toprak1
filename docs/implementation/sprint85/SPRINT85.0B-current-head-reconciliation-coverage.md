# Sprint 85.0B — Current-Head Read-Only Reconciliation Coverage

## Status

```text
revision: R1.1
exact_parent: f1994b57783313eea701734f3df637c42e3812e8
branch: sprint/85-0b-current-head-reconciliation-coverage
operator_gate_0: PASS_RECOVERED
worker_exact_checkout_runtime: NOT_EXECUTED
commit: false
push: false
pr: false
production_ready: false
production_cutover_authorized: false
```

Sprint 85.0B extends the accepted Sprint 85.0A reconciliation primitives. It does not add authority, lifecycle operations, repair, replay, resource settlement, migration, or Loop Plane behavior.

## Exact tracked scope

Only these five tracked paths belong to R1.1:

1. `hfa-control/src/hfa_control/reconciliation.py`
2. `tests/unit/test_reconciliation_contract.py`
3. `tests/integration/test_current_head_reconciliation_integration.py`
4. `.github/workflows/sprint85-0b-current-head-reconciliation.yml`
5. `docs/implementation/sprint85/SPRINT85.0B-current-head-reconciliation-coverage.md`

Accepted authority, Lua, worker, scheduler, resource, persistence, canonical registry, and Sprint 85.0A TASK_REQUEUE files remain frozen.

## Architecture

```text
canonical A
    ↓
runtime A
    ↓
runtime B
    ↓
canonical B
    ↓
CONSISTENT / DRIFT / BLOCKED_EVIDENCE
```

The existing `ReconciliationFinding`, status/severity/check-class taxonomy, canonical read facade, runtime read protocol, stable observation mechanics, and deterministic finding ordering are reused. `OPERATION_CONTRACTS` remains the sole canonical lifecycle registry.

The 85.0B runtime reader exposes only read operations required for current projection evidence. The reconciliation implementation contains no Redis or lifecycle mutation call.

## Operation coverage

| Operation | Canonical current head | Current mutable truth proved | Explicit exclusions |
|---|---|---|---|
| `RUN_CREATE` | `None → pending` | `run_state=admitted`; exact current projection receipt including canonical proof and deterministic event payload hash | raw `RunAdmitted` stream; resource reservation proof comparison |
| `TASK_ADMIT` | `None → ready` or `None → pending` | actual source-owned task state/meta, remaining dependencies, RUN membership, branch-specific ready membership/score/marker | invented canonical proof fields; global tenant-active absence |
| `TASK_DISPATCH` | `ready → scheduled` | task state/meta, canonical proof, dispatch attempt/owner, ready/scheduled/running memberships, exact scheduled score | control/shard transport history |
| `TASK_CLAIM` | `scheduled → running` | claim proof, exact durable dispatch predecessor, owner/fence, scheduled removal, running membership, reservation/owner consumption | heartbeat timestamp/score equality |
| `TASK_COMPLETE` | `running → done` | terminal proof, claim predecessor proof, exact output payload/hash, running removal, retained owner/fence | dependency fanout |
| `TASK_FAIL` | `running → failed` | own-task failed projection, terminal/claim proof, running removal, retained owner/fence | output-key absence invariant; dependency failure fanout |
| `RUN_TERMINATE` | `pending/running → done/failed` | run state, owned `run_meta`, owned `run_result`, exact projection receipt, `cp_running` absence | raw results stream; resource settlement invariant |

`TASK_REQUEUE` remains owned by the accepted Sprint 85.0A reconciler and is run as regression coverage.

## Source-derived critical contracts

### RUN_CREATE pending → admitted mapping

Canonical RUN state `pending` maps to mutable `run_state=admitted`. This is consistent. The current projection receipt is verified without requiring historical stream presence. `event_payload_hash` is deterministically reconstructed from the canonical RUN_CREATE metadata using the accepted `RunAdmittedEvent` serialization contract.

Resource reservation proof is deliberately not interpreted as a RUN/resource invariant in 85.0B.

### TASK_ADMIT does not invent canonical projection metadata

The accepted TASK_ADMIT Lua postimage does not contain the later generic fields:

```text
canonical_transition_id
canonical_record_hash
canonical_command_hash
canonical_revision
canonical_operation_id
```

The reconciler therefore never requires them for TASK_ADMIT. READY and PENDING postimages are separate explicit contracts.

### TASK_CLAIM heartbeat-safe fingerprint

`TASK_HEARTBEAT` is coordination-only. A valid heartbeat may move heartbeat timestamps and the running ZSET score without changing lifecycle authority. The TASK_CLAIM runtime fingerprint therefore proves immutable claim-owned fields and running membership but intentionally excludes live heartbeat value/score equality.

### TASK_COMPLETE retained owner/fence

The accepted terminal projection retains `worker_instance_id` and `scheduler_epoch` as terminal proof/fence evidence. 85.0B expects those values to remain exact; it does not require them to be cleared.

### TASK_FAIL remains operation-specific

TASK_FAIL is not implemented as COMPLETE-minus-output. The failure projection is checked using its own terminal metadata and proof. No invariant is invented that requires the task output key to be absent.

### RUN_TERMINATE resource boundary

85.0B checks only current RUN projection truth. Resource reservation/settlement belongs to Sprint 85.0C cross-aggregate reconciliation and is not read by the 85.0B runtime fingerprint.

## Reason taxonomy

Accepted reason codes are reused wherever they already express the mismatch. The R1.1 extension uses the R1 semantic families plus one precise evidence-boundary reason:

```text
RUN_STATE_MISMATCH
PROJECTION_VALUE_MISMATCH
PROJECTION_MEMBERSHIP_MISMATCH
PROJECTION_SCORE_MISMATCH
TASK_OUTPUT_MISMATCH
RUN_META_MISMATCH
RUN_RESULT_MISMATCH
OWNERSHIP_PROJECTION_MISMATCH
DEPENDENCY_FANOUT_EVIDENCE_REQUIRED
```

Wrong Redis type remains:

```text
DRIFT / CRITICAL / PROJECTION_SCHEMA_MISMATCH
```

Unreadable or corrupt canonical evidence remains `BLOCKED_EVIDENCE`.


## R1.1 supervisor blocker resolution

R1 incorrectly treated a pending `TASK_ADMIT` head as if it permanently fixed the child's dependency-owned mutable projection. Accepted parent terminal projection proves that assumption false. A canonical parent `TASK_COMPLETE` decrements a pending child's `remaining_deps` and unlocks it at zero; canonical parent `TASK_FAIL` can move a pending/ready child to `blocked_by_failure` without advancing the child TASK canonical revision.

R1.2 validates immutable TASK_ADMIT identity/meta, RUN membership, and Redis schema/type evidence **before** considering dependency-owned ambiguity. Only if those independent invariants are clean may exact source-reachable dependency-owned evolution be classified as:

```text
BLOCKED_EVIDENCE
WARNING
DEPENDENCY_FANOUT_EVIDENCE_REQUIRED
```

This is not a claim that the evolved projection is consistent. It means the child TASK_ADMIT head alone is insufficient to distinguish accepted cross-TASK fanout from projection drift. Sprint 85.0B does not traverse parent TASK history. Sprint 85.0C owns that cross-TASK invariant proof.

The exact untouched pending postimage remains `CONSISTENT`. Wrong Redis types and combinations not proven reachable by the accepted fanout writer do not use this evidence escape hatch and remain subject to the normal DRIFT rules.

### Source-bound fanout acceptance scenarios

R1.1 adds accepted-path scenarios in which the child is admitted through canonical `DagLua`, the parent is admitted/dispatched/claimed through accepted lifecycle components, and the parent terminal mutation is applied through `TaskTerminalAuthorityBinding` and its accepted Lua projector:

1. untouched pending child -> `CONSISTENT`;
2. parent COMPLETE causes partial dependency progress -> `BLOCKED_EVIDENCE / DEPENDENCY_FANOUT_EVIDENCE_REQUIRED`;
3. parent COMPLETE performs final unlock -> same evidence-required classification;
4. parent FAIL blocks the child -> same evidence-required classification.

The reconciliation call is bracketed by deterministic semantic Redis keyspace snapshots in the A-D acceptance cases and remains zero-mutation. The snapshot compares logical type-specific values rather than opaque Redis `DUMP` serialization.

## RUN_CREATE targeted discovery delta

Result:

```text
NOT_PRODUCTION_REACHABLE_IN_SELECTED_PROFILE
```

The accepted canonical RUN_CREATE command explicitly owns `pending` canonical truth plus `legacy_projection_state=admitted`, and `RunCreateProjectionManager` accepts only `legacy_state=admitted`. The selected single-task submission composition performs RUN admission and then immediately admits the DAG task; it does not route through a legacy RUN enqueue transition. The production scheduler is constructed around `DagReadyQueue`/`DagLua` with `tenant_queue=None`, not a legacy admitted-RUN queue. A repository search found no selected-profile production call site for `enqueue_admitted`; the Lua file's existence therefore does not establish reachability.

Accordingly R1.1 does **not** broaden RUN_CREATE semantics: with current canonical head `RUN_CREATE(pending)`, the current mutable projection contract remains `run_state=admitted` until another canonical RUN lifecycle operation is committed.

## Golden source-bound postimage hardening

For each materially distinct 85.0B projection family, at least one valid reconciled postimage is now generated by the accepted projector/Lua/binding rather than by writing the reconciler's own expected table back into Redis:

- RUN_CREATE -> `RunCreateProjectionManager`;
- TASK_ADMIT -> canonical `DagLua.task_admit`;
- TASK_DISPATCH -> canonical `DagLua.task_dispatch_commit`;
- TASK_CLAIM -> `TaskClaimAuthorityBinding.prepare_claim` + accepted `DagLua.task_claim_canonical_projection` with a real `WorkerReservationManager` reservation;
- TASK_COMPLETE -> `TaskTerminalAuthorityBinding.complete`;
- TASK_FAIL -> `TaskTerminalAuthorityBinding.fail`;
- RUN_TERMINATE -> `RunTerminateAuthorityBinding.terminate`.

Manual Redis mutation of **reconciled projection fields** is used only for deliberate drift/wrong-type/corruption/race injection after a valid postimage. Golden lifecycle postimages are produced by accepted bindings/projectors. The RUN_TERMINATE golden fixture still writes the accepted terminal-event migration-readiness contract used by the existing exact-parent integration suite; that fixture is prerequisite control evidence, not a synthesized RUN_TERMINATE postimage.

## Read-only proof

The implementation statically contains zero calls to:

```text
SET / HSET / DELETE / ZADD / ZREM / XADD / EXPIRE / EVAL / EVALSHA
commit / initialise / record_authority_conflict / validate_authority_head
project / deliver / requeue / terminate / settle_once
```

The runtime reader uses only the bounded read surface needed by the explicit operation contracts (`TYPE`, `GET`, `HMGET`, `ZSCORE`, `SISMEMBER`).

Every reconciliation finding reports `mutation_attempted=false`. The test-only zero-mutation oracle uses `SCAN`/`TYPE` plus type-specific read commands and canonicalizes unordered HASH/SET contents; unsupported Redis types fail closed. This test observation surface does not expand the production reconciliation reader.

## Test matrix

The R1.1 candidate contains:

- existing 10 Sprint 85.0A unit tests unchanged plus 12 new 85.0B unit contract tests;
- 46 integration test functions producing 60 real-Redis cases after parameterization;
- source-bound golden current-projection cases for RUN_CREATE, TASK_ADMIT, TASK_DISPATCH, TASK_CLAIM, TASK_COMPLETE, TASK_FAIL, and RUN_TERMINATE;
- explicit RUN_CREATE, TASK_ADMIT, TASK_DISPATCH, TASK_CLAIM, TASK_COMPLETE, TASK_FAIL, and RUN_TERMINATE cases;
- legitimate heartbeat between TASK_CLAIM runtime observations;
- retained terminal owner/fence acceptance;
- TASK_FAIL with a pre-existing output value accepted because output absence is not an 85.0B invariant;
- canonical corruption;
- canonical A/B race;
- immutable runtime A/B race;
- wrong Redis type for all seven target operations;
- zero-mutation before/after proof for all seven target operations using deterministic semantic keyspace equality, with persistent-key (`PTTL == -1`) enforcement for that seven-operation fixture and a post-proof test-only mutation-sensitivity control.

The workflow also reruns the accepted Sprint 85.0A TASK_REQUEUE 13-case suite and the exact accepted historical green regression matrix carried forward from 85.0A.

## Workflow governance

The 85.0B workflow fails closed on:

- exact PR base SHA;
- exact feature branch;
- exact five-file tracked diff;
- frozen lifecycle/source parity;
- Redis 7 availability;
- static read-only audit;
- exact authored test inventory;
- focused unit and real-Redis current-head tests;
- Sprint 85.0A reconciliation regression;
- historical accepted green regressions;
- authority audit `banned=0`;
- write-free Python compilation;
- YAML parse;
- `git diff --check`;
- final tracked checkout cleanliness.

It carries forward the narrow editable-install `.egg-info` metadata normalization proven necessary in the final green 85.0A workflow. The final clean checkout gate is not weakened.

## R1.1 worker validation evidence

This worker environment did not contain the operator Toprak1 checkout and could not clone GitHub because outbound DNS failed. Redis server/client and the repository runtime are unavailable here. Accordingly, mandatory runtime results are not converted to PASS.

Actually executed in the worker scratch environment:

```text
85.0B reconciliation extension write-free Python compile: PASS
new unit append write-free Python compile: PASS
new integration test file write-free Python compile: PASS
workflow YAML parse: PASS
workflow bash syntax blocks: PASS (14/14)
static forbidden mutation calls in reconciliation extension: PASS (0)
new integration test function inventory: PASS (46)
new 85.0B unit test function inventory: PASS (12)
skip/xfail/deselect gaming tokens in authored source: PASS (0)
remote absence of new integration/workflow/doc paths at exact parent: PASS
standalone patch synthetic git apply --check/apply: PASS
standalone patch synthetic git diff --check: PASS
synthetic postimage Python compile: PASS (3/3)
synthetic postimage YAML parse: PASS
synthetic postimage exact candidate scope: PASS (5/5)
```

Not executed in this worker environment:

```text
exact operator worktree patch application: NOT_EXECUTED
focused combined unit suite against real repository: NOT_EXECUTED
real Redis 85.0B integration: NOT_EXECUTED
85.0A reconciliation regression runtime: NOT_EXECUTED
historical regression matrix runtime: NOT_EXECUTED
authority audit runtime: NOT_EXECUTED
real-repository final tracked scope/worktree proof: NOT_EXECUTED
```

Those gates are delegated to the provided exact-parent PowerShell validator and subsequent CI, and remain blocking until observed.

## Non-claims

```text
85.0 complete=false
85.0C implemented=false
85.1 started=false
repair=false
production_ready=false
production_cutover_authorized=false
```


### R1.2 anti-masking classification order

For a canonical `TASK_ADMIT` head with `None -> pending` and `dependency_count > 0`:

1. establish stable canonical/runtime A/B evidence;
2. evaluate independently provable child invariants first;
3. any identity, immutable metadata, RUN membership, or Redis schema/type mismatch remains normal `DRIFT`;
4. only then classify the dependency-owned values `task_state`, `remaining_deps`, `ready_member`, and `ready_emitted`;
5. exact partial progress, final unlock, failure-while-pending, and failure-after-ready shapes remain `BLOCKED_EVIDENCE / DEPENDENCY_FANOUT_EVIDENCE_REQUIRED` pending 85.0C provenance;
6. source-impossible combinations remain `DRIFT`.

The ambiguity surface is intentionally local. `DEPENDENCY_FANOUT_EVIDENCE_REQUIRED` is not a generic escape hatch and cannot suppress independently provable corruption.

R1.2 adds explicit real-Redis scenarios for identity drift during partial fanout, RUN membership drift during unlock, an impossible failure marker combination, and pending `ready_emitted=1` corruption. Positive fanout fixtures remain source-bound through accepted authority/projector paths; deliberate Redis mutation is used only after a valid accepted postimage exists.

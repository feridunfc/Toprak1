# Sprint 85.0A — Reconciliation Truth Inventory + First Read-Only Vertical Slice

## Status

```yaml
accepted_parent: a5d4b767d8a065164cc45bb861c3b95b6aec43f7
branch: sprint/85-0a-reconciliation-truth-inventory-first-vertical-slice
architecture: READ_ONLY_RECONCILIATION
first_vertical_slice: TASK_REQUEUE_CURRENT_HEAD
new_authority: 0
new_lifecycle_operation: 0
repair_path: 0
loop_plane: ABSENT
production_ready: false
production_cutover_authorized: false
```

85.0A does not complete Sprint 85.0. It establishes the truth inventory and the
first executable read-only reconciliation slice only.

## A. Architecture Constitution

The frozen direction is:

```text
84.x  CANONICALIZE THE LIFECYCLE          COMPLETE
85.0  PROVE THE LIFECYCLE                 READ ONLY
85.1  RESTORE ALREADY-DECIDED TRUTH       EXACT REPLAY ONLY
85.2  MIGRATION / DISASTER DRILLS
85.3  PRODUCTION COMPOSITION FREEZE
85.4  STAGING BURN-IN
85.5  PRODUCTION CUTOVER GATE
86.x  LOOP PLANE / SEMANTIC INTELLIGENCE
```

The governing rule is:

> Reconciliation observes truth. Repair restores already-decided truth. Only
> Command / future Loop Plane may create new semantic truth.

Therefore this module is **READ ONLY**, **NOT AUTHORITY**, **NOT REPAIR**, and
**NOT LOOP PLANE**. It is not composed into ControlPlaneService startup,
RecoveryService, scheduler, worker, API, or a periodic loop.

The reconciliation engine receives only two capability interfaces:

```text
TaskRequeueCanonicalReader
TaskRequeueRuntimeReader
```

The canonical Redis adapter may internally use the accepted
`RedisCanonicalAuthorityStore` parser, but exposes only
`read_task_requeue_head()`. It does not call `initialise()`, `commit()`, conflict
recording, projection validation Lua, or any authority writer. The runtime
adapter exposes only a TASK_REQUEUE projection read and uses Redis
`TYPE` / `GET` / `HMGET` / `ZSCORE`.

## B. Complete Operation Inventory

### Inventory rule

`hfa-core/src/hfa/authority/canonical_transition.py::OPERATION_CONTRACTS` remains
the sole canonical operation contract registry. The table below is an
**inventory/coverage document**, not a second authority catalog. Transition,
revision, mutation-class, receipt, and intent columns are transcribed from that
accepted registry. Reconciliation coverage annotations do not redefine them.

`PROVEN` production reachability means an accepted runtime/binding source is
present in the accepted parent. `NOT_PROVEN` means 85.0A did not find an
operation-specific production authority binding and refuses to infer one from
the enum alone.

| operation_type | aggregate | mutation_class | allowed previous → next | consumes revision | receipt | required intents | conditional intents | production reachable | current-head reconciliation | source / evidence | planned tranche |
|---|---|---|---|---:|---:|---|---|---|---|---|---|
| `TASK_ADMIT` | TASK | ACCEPTED_AUTHORITY_MUTATION | `None → pending, ready` | yes | yes | — | `READY_QUEUE_IF_READY` | PROVEN | YES | `task_admit_authority.py`; `ControlPlaneService`/submission composition; admitted/ready projection contract requires 85.0B source binding | 85.0B |
| `TASK_DISPATCH` | TASK | ACCEPTED_AUTHORITY_MUTATION | `ready → scheduled` | yes | yes | `CONTROL_NOTIFICATION`, `TASK_REQUEST_MESSAGE` | — | PROVEN | YES | `task_dispatch_authority.py`; production scheduler composition | 85.0B |
| `TASK_CLAIM` | TASK | ACCEPTED_AUTHORITY_MUTATION | `scheduled → running` | yes | yes | `RUNNING_SET` | — | PROVEN | YES | `task_claim_authority.py`; `TaskConsumer -> TaskClaimManager` production path | 85.0B |
| `TASK_HEARTBEAT` | TASK | COORDINATION_ONLY_MUTATION | `running → running` | no | no | `LIVENESS_TTL` | — | PROVEN | NO | `TaskConsumer` creates `HeartbeatLoop` over `TaskHeartbeatManager`; heartbeat is coordination, not canonical-head authority | NOT_APPLICABLE |
| `TASK_COMPLETE` | TASK | ACCEPTED_AUTHORITY_MUTATION | `running → done` | yes | yes | `OUTPUT_PROJECTION`, `DEPENDENCY_FANOUT_INTENT` | — | PROVEN | YES | `task_terminal_authority.py`; proof-bound terminal projection | 85.0B |
| `TASK_FAIL` | TASK | ACCEPTED_AUTHORITY_MUTATION | `running → failed` | yes | yes | `DEPENDENCY_FAILURE_FANOUT_INTENT` | — | PROVEN | YES | `task_terminal_authority.py`; proof-bound terminal projection | 85.0B |
| `TASK_REQUEUE` | TASK | ACCEPTED_AUTHORITY_MUTATION | `running → ready` | yes | yes | `READY_QUEUE`, `REQUEUE_NOTIFICATION` | — | PROVEN | YES | `task_requeue_authority.py`; `task_requeue.lua`; real `RecoveryService` production composition | **85.0A** |
| `TASK_CANCEL` | TASK | ACCEPTED_AUTHORITY_MUTATION | `pending, ready, scheduled, running → skipped` | yes | yes | `TERMINAL_PROJECTION` | — | NOT_PROVEN | UNKNOWN | canonical enum/contract exists; no operation-specific `task_cancel_authority.py` found at accepted parent | REQUIRES_DISCOVERY |
| `TASK_DEPENDENCY_APPLY` | TASK | ACCEPTED_AUTHORITY_MUTATION | `pending → pending, ready, blocked_by_failure` | yes | yes | — | `READY_QUEUE_IF_READY` | NOT_PROVEN | UNKNOWN | canonical enum/contract exists; no operation-specific dependency authority binding found at accepted parent | REQUIRES_DISCOVERY |
| `RUN_CREATE` | RUN | ACCEPTED_AUTHORITY_MUTATION | `None → pending` | yes | yes | `RUN_STATUS_PROJECTION` | — | PROVEN | YES | `run_create_authority.py`; canonical record before admitted/status projection and reservation lifecycle | 85.0B |
| `RUN_TERMINATE` | RUN | ACCEPTED_AUTHORITY_MUTATION | `pending, running → done, failed` | yes | yes | `RUN_RESULT_PROJECTION` | — | PROVEN | YES | `run_terminate_authority.py`; canonical terminal proof + settlement + projection | 85.0B / 85.0C |
| `LEGACY_RUN_COMPLETE` | RUN | UNSUPPORTED_LEGACY_OPERATION | `running → done` | no | no | — | — | NOT_APPLICABLE | NO | canonical core explicitly classifies unsupported legacy operation | NOT_APPLICABLE |
| `TERMINAL_DUPLICATE_CLEANUP` | TASK | TRANSPORT_ONLY_MUTATION | `terminal → terminal` | no | no | `AUDIT_INTENT`, `AUDIT_OUTCOME` | — | PROVEN | NO | worker terminal-duplicate/ACK compatibility path; transport only, not lifecycle head authority | NOT_APPLICABLE |
| `MESSAGE_APPEND` | — | TRANSPORT_ONLY_MUTATION | `NOT_APPLICABLE → NOT_APPLICABLE` | no | no | `STREAM_APPEND` | — | PROVEN | NO | transport stream append contract; no lifecycle aggregate revision | NOT_APPLICABLE |
| `MESSAGE_ACK` | — | TRANSPORT_ONLY_MUTATION | `NOT_APPLICABLE → NOT_APPLICABLE` | no | no | `STREAM_ACK` | — | PROVEN | NO | worker stream ACK contract; no lifecycle aggregate revision | NOT_APPLICABLE |

### Resource lifecycle inventory

Resource reservation/settlement is not a separate `OperationType`; it is bound
to canonical RUN truth:

```text
RUN_CREATE durable truth
  -> AdmissionResourceReservationManager reservation receipt

RUN_TERMINATE durable truth
  -> exact settlement of the RUN_CREATE reservation
```

Its cross-aggregate reconciliation is deferred to 85.0C. 85.0A does not create
a synthetic `RESOURCE_SETTLE` authority operation.

## C. TASK_REQUEUE Current Projection Contract

### Canonical truth source

Sources:

- `hfa-control/src/hfa_control/task_requeue_authority.py`
  - `build_task_requeue_command()`
  - `TaskRequeueProjectionManager._validate_durable_authority()`
  - `TaskRequeueProjectionManager._run_lua()`
- `hfa-core/src/hfa/lua/task_requeue.lua`
- `hfa-core/src/hfa/authority/redis_persistence.py`
  - `load_receipt_probe()`
  - `get_aggregate_snapshot()`

For an inspected TASK identity, 85.0A first proves:

```text
current head operation       = TASK_REQUEUE
previous_state               = running
next_state                   = ready
record.verify_hash()         = true
receipt.operation_id         = record.operation_id
receipt.transition_id        = record.transition_id
receipt.command_hash         = record.command_hash
receipt.record_hash          = record.record_hash
receipt.aggregate_revision   = record.to_revision
snapshot.revision            = record.to_revision
snapshot.state               = ready
snapshot transition/hash/op  = exact record head
snapshot intents             = exact durable record intents
snapshot updated_at_ms       = record.committed_at_ms
required intents             = READY_QUEUE + REQUEUE_NOTIFICATION
```

The accepted operation identity is source-derived:

```text
task-requeue:v1:<TASK identity sha256>:claim:<claim_epoch>
```

### TASK_CLAIM predecessor/causation proof

The reconciler independently reads the durable predecessor receipt/record and
requires:

```text
claim operation type         = TASK_CLAIM
claim operation id           = metadata.claim_operation_id
claim transition id          = TASK_REQUEUE causation id
claim record hash            = metadata.claim_record_hash
claim command hash           = metadata.claim_command_hash
claim to_revision            = TASK_REQUEUE from_revision
claim next_state             = running
claim dispatch_attempt       = TASK_REQUEUE dispatch_attempt
claim claim_epoch            = TASK_REQUEUE claim_epoch
```

Accepted claim operation identity:

```text
task-claim:v1:<TASK identity sha256>:attempt:<dispatch_attempt>
```

Missing/corrupt/contradictory predecessor evidence is
`BLOCKED_EVIDENCE / CLAIM_PREDECESSOR_PROOF_MISMATCH`; it is never downgraded to
ordinary projection drift.

### Expected mutable current projection

`task_requeue.lua` canonical-project mode proves the following current
expectations:

```text
TASK state                                ready
meta.task_id                              canonical task_id
meta.run_id                               canonical run_id
meta.tenant_id                            canonical tenant_id
meta.requeue_count                        canonical retry_attempt
meta.last_requeue_reason                  canonical reason_code
meta.last_requeue_at_ms                   canonical record.committed_at_ms
meta.worker_instance_id                   ""
meta.scheduler_epoch                      ""
meta.last_heartbeat_at_ms                 0

meta.canonical_transition_id              current TASK_REQUEUE transition
meta.canonical_record_hash                current TASK_REQUEUE record hash
meta.canonical_command_hash               current TASK_REQUEUE command hash
meta.canonical_revision                   current TASK_REQUEUE revision
meta.canonical_operation_id               current TASK_REQUEUE operation id

meta.requeue_canonical_transition_id      same TASK_REQUEUE transition
meta.requeue_canonical_record_hash        same record hash
meta.requeue_canonical_command_hash       same command hash
meta.requeue_canonical_revision           same revision
meta.requeue_canonical_operation_id       same operation id

task_running_zset membership              ABSENT
tenant_ready_queue membership             PRESENT
tenant_ready_queue score                  record.committed_at_ms
```

The ready score and `last_requeue_at_ms` are not wall-clock values invented by
reconciliation. The accepted authority binding explicitly derives both from the
durable `record.committed_at_ms` before invoking canonical projection.

A Redis key whose type is provably wrong is positive mismatch evidence:

```text
expected zset / observed hash
=> DRIFT / CRITICAL / PROJECTION_SCHEMA_MISMATCH
```

A missing ready queue/member is separately classified
`READY_QUEUE_MEMBERSHIP_MISSING`.

## D. TASK_REQUEUE Historical Durable Effect Contract

Current-head projection and historical effect integrity are separate check
classes:

```text
CURRENT_PROJECTION
HISTORICAL_DURABLE_EFFECT
CROSS_AGGREGATE_INVARIANT   # schema only in 85.0A; substantial checks deferred
```

`task_requeue.lua` canonical-deliver mode first validates the requeue proof,
then appends `TaskRequeued`, and finally stores:

```text
meta.requeue_notification_operation_id = canonical TASK_REQUEUE operation_id
```

That field is the accepted durable delivery proof used by 85.0A.

The raw `TaskRequeued` stream entry is deliberately **not** a reconciliation
requirement. The accepted delivery uses approximate stream retention; therefore
raw stream trimming does not erase the durable proof and is not by itself
DRIFT.

Acceptance requirement:

```text
durable requeue_notification_operation_id valid
raw historical TaskRequeued entry trimmed/absent
=> HISTORICAL_DURABLE_EFFECT / CONSISTENT
```

## E. Observation Stability Contract

85.0A never assumes live Redis is static.

Outer observation:

```text
canonical evidence A
    -> runtime evidence fingerprint A
    -> runtime evidence fingerprint B
canonical evidence B
```

Canonical stability fingerprint includes:

```text
revision
state
transition_id
canonical_record_hash
canonical_command_hash
operation_id
```

The canonical reader also brackets its own record/receipt/predecessor reads with
an aggregate snapshot re-read. Any head change prevents a stable
CONSISTENT/DRIFT result.

Runtime fingerprint includes the exact state/meta values and ready/running
membership scores used by the comparison. Two different runtime fingerprints
produce:

```text
BLOCKED_EVIDENCE / PROJECTION_OBSERVATION_CHANGED
```

Canonical head change produces:

```text
BLOCKED_EVIDENCE / CANONICAL_CHANGED_DURING_OBSERVATION
```

No lock or write is used to stabilize observation.

## F. Status / Severity / Reason Vocabulary

Top-level status is frozen to:

```text
CONSISTENT
DRIFT
BLOCKED_EVIDENCE
```

Severity is independent:

```text
INFO
WARNING
CRITICAL
```

85.0A reason codes are deterministic machine-readable constants:

```text
CONSISTENT
CANONICAL_EVIDENCE_UNAVAILABLE
CANONICAL_RECORD_CORRUPTION
CANONICAL_CHANGED_DURING_OBSERVATION
PROJECTION_EVIDENCE_UNAVAILABLE
PROJECTION_OBSERVATION_CHANGED
PROJECTION_SCHEMA_MISMATCH
TASK_STATE_MISMATCH
TASK_META_IDENTITY_MISMATCH
CANONICAL_PROOF_MISMATCH
REQUEUE_PROOF_MISMATCH
CLAIM_PREDECESSOR_PROOF_MISMATCH
REQUEUE_COUNT_MISMATCH
REQUEUE_REASON_MISMATCH
REQUEUE_TIMESTAMP_MISMATCH
WORKER_OWNERSHIP_NOT_CLEARED
HEARTBEAT_NOT_CLEARED
RUNNING_INDEX_MEMBERSHIP_PRESENT
READY_QUEUE_MEMBERSHIP_MISSING
READY_QUEUE_SCORE_MISMATCH
REQUEUE_DELIVERY_PROOF_MISSING
REQUEUE_DELIVERY_PROOF_MISMATCH
```

Unknown/unreadable evidence never becomes DRIFT. Proven schema mismatch is not
unknown.

## G. Finding Contract and Determinism

`ReconciliationFinding` is an immutable dataclass containing:

```text
aggregate_type / run_id / task_id
check_class / contract_id / contract_version
status / severity / reason_code
canonical revision/operation/state/transition/hash/op/committed_at_ms
expected
observed
evidence:
  canonical_proven
  canonical_stable
  projection_read_complete
  projection_observation_stable
  refs
observation started/completed timestamps
mutation_attempted=false
```

There is no fake mutable `observed_version`. `semantic_key()` deliberately
excludes observation timestamps so wall-clock sampling cannot change semantic
finding equality.

Multiple findings are sorted deterministically by:

```text
aggregate_type
run_id
task_id
check_class
reason_code
```

No Redis iteration order participates in report semantics.

## H. Zero Mutation Evidence

85.0A safety is proven at three levels.

### Structural capability boundary

The engine has no raw Redis dependency and no authority/projection/resource
manager. The runtime reader public surface is read-only. The canonical facade
exposes only the read method used by this slice.

A workflow AST gate rejects calls to mutation/repair methods including:

```text
set / hset / delete / zadd / zrem / xadd / expire / eval / evalsha
commit / initialise / record_authority_conflict / validate_authority_head
project / deliver / requeue / terminate / settle_once
```

### Real-Redis exact before/after proof

Scenario M captures all isolated Redis keys and their actual values by Redis
type (excluding naturally changing TTLs), runs reconciliation, and requires:

```text
before == after
```

The reconciler writes no diagnostic key or audit stream. The report is returned
to the caller only.

### Zero-authority claims

During reconciliation the intended counts are:

```text
canonical revisions created  0
canonical receipts created   0
TASK/RUN lifecycle writes    0
queue/index mutations        0
resource mutations           0
events emitted               0
projection replay            0
delivery replay              0
repair paths                 0
```

`mutation_attempted=false` in the result is informational, not the safety proof.

## I. 85.0A Acceptance Inventory

Authored test inventory after implementation:

```text
focused unit contracts                         10
real Redis TASK_REQUEUE scenarios A-M          13
```

A-M cover:

```text
A  fully consistent current + durable effect
B  TASK state mismatch
C  ready queue missing
D  stale running membership
E  generic canonical projection proof mismatch
F  requeue-specific proof mismatch
G  proven wrong Redis type
H  canonical record corruption
I  canonical change during observation
J  projection change during observation
K  durable delivery proof missing
L  raw stream trimmed, durable proof valid
M  exact zero-mutation keyspace proof
```

I/J use deterministic `asyncio.Event` barriers; no sleep synchronization is
used.

## J. Historical Regression Boundary

85.0A workflow retains accepted-green gates for:

- Sprint 84.9 focused / authority Redis / recovery / production composition
- Sprint 84.8 resource settlement
- Sprint 84.7E crash/redelivery
- Sprint 84.7D RUN_TERMINATE production
- Sprint 84.7C2/C1 terminal production/authority
- legacy terminal green matrix
- Sprint 84.7B TASK_CLAIM production
- full TASK_CLAIM authority
- canonical RUN termination
- RUN finalization
- production worker
- Sprint 83.7
- Sprint 83.8
- authority audit `banned=0`

The previously proven baseline-red three-file legacy recovery suite is not
reintroduced as a fake green requirement.

## K. Deferred Scope

85.0A implements only stable current-head TASK_REQUEUE plus its durable delivery
proof.

Deferred:

```text
85.0B
  RUN_CREATE
  TASK_ADMIT
  TASK_DISPATCH
  TASK_CLAIM
  TASK_COMPLETE
  TASK_FAIL
  RUN_TERMINATE
  complete current-head production coverage

85.0C
  historical durable effects beyond TASK_REQUEUE
  TASK <-> RUN invariants
  RUN <-> resource reservation/settlement invariants
  operation reachability discovery for unresolved canonical enum rows

85.0D
  operator CLI/report composition
  read-only production acceptance surface
```

`TASK_CANCEL` and `TASK_DEPENDENCY_APPLY` remain `REQUIRES_DISCOVERY`; no generic
fake rule is supplied.

## Final Non-Claims

```text
85.0 complete                         false
new lifecycle authority              false
new semantic decision                false
automatic reconciliation loop        false
repair                               false
Loop Plane                           false
production_ready                     false
production_cutover_authorized        false
```

85.0A proves one bounded statement only:

> For a stable canonical TASK_REQUEUE head, the system can independently derive
> the expected current projection and durable effect evidence, compare it with
> live Redis, distinguish proven drift from unavailable evidence, survive
> concurrent observation safely, and return deterministic findings without
> changing lifecycle truth.

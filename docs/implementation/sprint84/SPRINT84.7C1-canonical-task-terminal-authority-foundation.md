# Sprint 84.7C1 — Canonical TASK Terminal Authority Foundation

## Status

Implementation candidate against accepted parent:

```text
623df852c72c676b183407baf41de66ad87e9c5d
```

Target branch:

```text
sprint/84-7c1-canonical-task-terminal-authority-foundation
```

This is an authority-foundation sprint. It deliberately does **not** bind the
new authority into WorkerService or TaskConsumer.


## R2.1 accepted-baseline regression-test contract repair

The R2 terminal authority architecture is unchanged. R2.1 is test-contract
maintenance discovered only after the R2 canonical real-Redis acceptance passed
on the operator machine.

```text
Original R2 product implementation scope: 6 files
Final R2.1 PR scope:                10 files
```

The additional four files are accepted-baseline regression-test maintenance only:

```text
tests/core/test_task_consumer_fenced_completion_contract.py
tests/integration/test_task_complete_owner_integration.py
tests/integration/test_task_complete_unlock_integration.py
tests/integration/test_task_output_persistence_integration.py
```

Two stale accepted-baseline contracts were repaired without changing product
behavior:

```text
1. FakeClaimManager.claim_start() lacked the already-required run_id argument
   used by the accepted TaskConsumer production call.

2. Legacy task-completion integration fixtures predated the accepted
   task_complete.lua identity/RUN-truth guards and did not seed task_id/run_id
   metadata plus a valid nonterminal RUN state.
```

The test assertions remain about the same behaviors. R2.1 does not weaken TASK
identity validation, RUN-truth guards, owner/fence checks, terminal semantics, or
the R2 durable terminal-authority proof boundary.

R2 product byte identity is locked for this maintenance revision:

```text
hfa-control/src/hfa_control/task_terminal_authority.py
SHA-256 a5abd71bbb64410dc837fac625a3d0b0aa824d3fee0e3f68092066d1f2134778

hfa-core/src/hfa/lua/task_complete.lua
SHA-256 4bfcab69627f1b5fcc79331c13854f66f90cf8f437beff84c74b2f24f685ab48
```

## Pre-implementation truth

The accepted baseline had:

```text
TASK_COMPLETE taxonomy             true
TASK_COMPLETE canonical primitive  false
TASK_COMPLETE canonical authority  false

TASK_FAIL taxonomy                 true
TASK_FAIL canonical primitive      false
TASK_FAIL canonical authority      false
```

The runtime terminal owner was:

```text
TaskConsumer
→ completion_manager.task_complete(...)
→ DagLua.task_complete(...)
→ task_complete.lua
```

`task_complete.lua` therefore combined de-facto lifecycle authority with
projection/effect writes. Failure propagation was additionally fragmented:

```text
task_complete.lua → parent running → failed
FailureSweeper    → child pending/ready → blocked_by_failure
```

## Pattern audit

The implementation follows the existing canonical pattern already proven by
TASK_ADMIT, TASK_DISPATCH, TASK_CLAIM and RUN_TERMINATE:

```text
operation-specific normalized input
→ AuthorityCommand
→ AuthorityEntryContext
→ evaluate_authority_commit(...)
→ RedisCanonicalAuthorityStore
→ immutable record + operation receipt + aggregate head
→ proof-bound projection
```

No new canonical store, receipt subsystem, aggregate type or lifecycle state
machine is introduced.

## Aggregate identity and revision contract

Both terminal operations continue the existing canonical TASK aggregate:

```text
CanonicalAggregateIdentity(
    aggregate_type=TASK,
    run_id=<run>,
    task_id=<task>,
)
```

The expected revision is derived from the exact canonical TASK_CLAIM head.
Conceptually for a first-attempt single-task lifecycle:

```text
TASK_ADMIT     rev 0 → 1
TASK_DISPATCH  rev 1 → 2
TASK_CLAIM     rev 2 → 3
TASK terminal  rev 3 → 4
```

The implementation does not hard-code revision 3. It loads and validates the
actual TASK_CLAIM record/receipt and uses its `to_revision` as terminal
`expected_revision`.

## One claim generation — one terminal operation slot

TASK_COMPLETE and TASK_FAIL deliberately share one deterministic operation
identity for a claim generation:

```text
task-terminal:v1:<task-aggregate-sha256>:claim:<claim_epoch>
```

The operation type, output and failure reason remain part of the canonical
command hash.

Therefore:

```text
COMPLETE(payload A) → COMPLETE(payload A)  = exact receipt replay
COMPLETE(payload A) → COMPLETE(payload B)  = idempotency conflict
FAIL(reason A)      → FAIL(reason B)        = idempotency conflict
COMPLETE             → FAIL                 = same-slot conflict
FAIL                 → COMPLETE             = same-slot conflict
```

No last-write-wins terminal policy is introduced.

## Terminal ownership proof

Before a first terminal authority commit the binding proves that the canonical
TASK head is the exact TASK_CLAIM transition and validates:

```text
task_id
run_id
tenant_id
worker_instance_id
scheduler_epoch
claim_epoch
TASK_CLAIM transition_id
TASK_CLAIM canonical_record_hash
TASK_CLAIM canonical_command_hash
TASK_CLAIM operation_id
TASK_CLAIM revision
```

It also validates the existing runtime claim projection against those values and
requires nonterminal RUN truth. A stale worker, stale scheduler fence or stale
claim generation cannot authorize a terminal canonical transition.

## TASK_COMPLETE command

Canonical transition:

```text
running → done
```

Required projection intents remain the existing taxonomy contract:

```text
OUTPUT_PROJECTION
DEPENDENCY_FANOUT_INTENT
```

Successful output JSON is parsed and serialized with the repository's existing
canonical JSON implementation before it enters the authority command. The
canonical output payload is stored in the durable record, not only its hash, so
projection can be reconstructed without task re-execution.

## TASK_FAIL command

Canonical transition:

```text
running → failed
```

Required projection intent remains:

```text
DEPENDENCY_FAILURE_FANOUT_INTENT
```

The command carries deterministic failure reason plus the exact claim/fence
proof. Existing failure output behavior is preserved: executor output is not a
TASK_FAIL output projection.

## Authority-before-projection

The central invariant is:

```text
RedisCanonicalAuthorityStore commit
        ↓
immutable terminal record + receipt durable
        ↓
proof-bound task_complete.lua projection
```

Never:

```text
task_complete.lua terminal mutation
→ canonical receipt afterwards
```

## Canonical projection role of task_complete.lua

Legacy/default callers still pass the historical argument set and keep existing
behavior unchanged.

C1 adds an optional canonical projection mode. R2 closes the projector proof
boundary: caller-supplied hashes are not accepted as authority merely because
they are well formed. `TaskTerminalProjectionManager` must first prove the exact
terminal operation and its predecessor claim from `RedisCanonicalAuthorityStore`.

Before the Lua projector is invoked, the manager validates:

```text
exact TASK aggregate identity
exact durable terminal operation record + receipt
exact current terminal aggregate head
terminal operation type / operation id / transition id
canonical command hash / canonical record hash
from revision / to revision
previous state running / next state done|failed
durable projection intents
exact TASK_CLAIM predecessor record + receipt
claim operation id / transition id / hashes / revision / metadata
```

Only after that durable-store proof succeeds does `task_complete.lua` perform
its existing canonical projection checks:

```text
canonical operation type/state agreement
canonical revision = claim revision + 1
claim proof in task_meta
worker/scheduler/claim fences
RUN truth
projection target Redis types
child dependency counter safety
```

Thus the required gate is additive:

```text
exact durable TASK_CLAIM proof
AND
exact durable TASK_COMPLETE/TASK_FAIL record + receipt + current head
→ terminal Lua projection allowed
```

After successful proof validation it applies only projection/effect duties:

```text
parent TASK state/meta projection
running-zset removal
COMPLETE output projection
COMPLETE dependency fanout
FAIL dependency-failure fanout
```

It writes the terminal canonical proof into task metadata. Reapplying that exact
proof returns:

```text
canonical_terminal_already_projected
```

before dependency counters or child states are changed again.

## COMPLETE dependency fanout

Canonical COMPLETE preserves the accepted success behavior:

```text
parent done
→ pending child remaining deps - 1
→ zero remaining
→ child ready
→ ready-emitted marker
→ ready queue ZADD NX
```

Projection preflight rejects malformed dependency counters before the first
terminal projection mutation so a predictable Redis WRONGTYPE/DECR error cannot
leave the parent half-projected.

## FAIL dependency fanout

Canonical TASK_FAIL is the authority. Its durable
`DEPENDENCY_FAILURE_FANOUT_INTENT` is projected in canonical projection mode:

```text
pending child → blocked_by_failure
ready child   → blocked_by_failure + remove stale ready-queue membership
terminal child → unchanged
```

`FailureSweeper` remains untouched for legacy/default compatibility. It is not
called as a second canonical authority in C1.

## Projection interruption and replay

C1 exposes manager-level projection replay only for an already-durable terminal
record:

```text
authority COMMITTED
→ projection interrupted
→ replay_terminal_projection(...)
→ load exact terminal record/receipt
→ validate original TASK_CLAIM proof
→ retry same projection
```

No task executor, stream redelivery, WorkerService retry loop, TASK_REQUEUE or
scheduler recovery is introduced. Full worker crash/restart acceptance remains
84.7E.

## Exact changed-file scope

```text
.github/workflows/sprint84-7c1-canonical-task-terminal-authority-foundation.yml
docs/implementation/sprint84/SPRINT84.7C1-canonical-task-terminal-authority-foundation.md
hfa-control/src/hfa_control/task_terminal_authority.py
hfa-core/src/hfa/lua/task_complete.lua
tests/unit/test_task_terminal_authority_contract.py
tests/integration/test_task_terminal_authority_integration.py
```

Exactly 6 files.

## Explicitly unchanged production surfaces

```text
hfa-worker/src/hfa_worker/main.py
hfa-worker/src/hfa_worker/process_root.py
hfa-worker/src/hfa_worker/consumer.py
hfa-worker/src/hfa_worker/task_consumer.py
hfa-control/src/hfa_control/run_termination.py
hfa-control/src/hfa_control/failure_sweeper.py
hfa-control/src/hfa_control/dag_lua.py
```

Therefore:

```text
TASK_COMPLETE production composition = false
TASK_FAIL production composition     = false
RUN_TERMINATE production injection   = false
```

## New C1 tests

Unit contract inventory:

```text
7 tests
```

Real-Redis integration inventory after R2:

```text
23 tests
```

The integration suite covers:

- new canonical COMPLETE;
- new canonical FAIL;
- COMPLETE exact replay;
- FAIL exact replay;
- changed COMPLETE output conflict;
- changed FAIL reason conflict;
- COMPLETE-vs-FAIL one-terminal race;
- worker identity fencing;
- scheduler epoch fencing;
- claim epoch fencing;
- COMPLETE authority-committed/projection-retry;
- FAIL authority-committed/projection-retry;
- canonical projection WRONGTYPE preflight;
- forged terminal projector input with no canonical terminal record rejected;
- tampered terminal transition/hash/revision/operation proof rejected;
- exact durable terminal proof projects and exact projection replay is idempotent;
- legacy failed-completion compatibility.

## Historical regression gates retained in CI/validator

```text
legacy TASK terminal behavior             18
Sprint 84.7B focused                      76
full TASK_CLAIM                           83
production RUN finalization               41
production worker                         43
Sprint 83.7                               174
Sprint 83.8                               199
```

The new C1 tests live in new files, so existing exact-count historical suites do
not gain accidental tests.

## Local sandbox evidence

Observed in this sandbox:

```text
C1 unit authority contracts          7 passed
TASK_COMPLETE key contract           1 passed
compileall                            PASS
authority audit                      PASS_WITH_RISKS
  allowed                            33
  banned                              0
  scanned_files                     283
  suspicious                        278
```

A test-only external shim was required to emulate the unavailable `rfc8785`
package for the local unit run; no repository dependency or source file was
changed for that shim.

The sandbox does not provide the real Redis/Python Redis environment required by
C1 integration acceptance, and does not provide PowerShell 7 to execute the
validator artifact itself. Those are mandatory operator gates.

## Post-C1 authority matrix target

```text
TASK_ADMIT:
  taxonomy: true
  canonical primitive: true
  authority: true
  production bound: true

TASK_DISPATCH:
  taxonomy: true
  canonical primitive: true
  authority: true
  production bound: true

TASK_CLAIM:
  taxonomy: true
  canonical primitive: true
  authority: true
  production bound: true

TASK_COMPLETE:
  taxonomy: true
  canonical primitive: true
  authority: true
  canonical projection capable: true
  production bound: false

TASK_FAIL:
  taxonomy: true
  canonical primitive: true
  authority: true
  canonical projection capable: true
  production bound: false

RUN_TERMINATE:
  canonical authority: true
  production bound: false

TASK_REQUEUE:
  canonical authority: false
```

## Explicit non-claims

Even if operator Redis validation passes:

```text
TASK_COMPLETE canonical authority foundation: true
TASK_FAIL canonical authority foundation: true

TASK_COMPLETE production composition: false
TASK_FAIL production composition: false

RUN_TERMINATE production injection: false
TASK_REQUEUE canonical authority: false
resource settlement: false

production_ready: false
production_cutover_authorized: false
```

The next roadmap step after independent acceptance and merge is:

```text
84.7C2 TASK Terminal Production Composition
```

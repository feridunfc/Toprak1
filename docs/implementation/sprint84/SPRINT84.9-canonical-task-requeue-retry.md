# Sprint 84.9 — Canonical TASK_REQUEUE / Retry R3

## Baseline and status

Accepted parent: `541c6a1f1d1753d917cfacce855d917faa64bc6a` (Sprint 84.8 / PR #141).

`production_ready=false`

`production_cutover_authorized=false`

R3 preserves the R2 production composition and canonical idempotency correction, then closes retry-exhaustion through the already accepted canonical RUN termination/resource-settlement lifecycle.

## Production recovery truth

The production composition root remains `ControlPlaneService`. It owns one leader-started `RecoveryService`. No second recovery service/coordinator is introduced. Worker production owns TASK heartbeat/execution; scheduler production owns dispatch, not stale-TASK recovery.

The existing DAG indexes remain the discovery surface:

- `DagRedisKey.tenant_active_set()` for stale TASK tenant discovery;
- `DagRedisKey.task_running_zset(tenant_id)` for stale running TASK discovery;
- `RedisKey.cp_running()` for stale RUN rediscovery;
- `DagRedisKey.run_tasks(run_id)` for exact RUN→TASK membership.

## Canonical rollout dependencies

`HFA_CANONICAL_TASK_REQUEUE_BINDING` remains default false.

When enabled, `RecoveryService` now requires all of:

- `HFA_CANONICAL_TASK_ADMIT_BINDING=true`
- `HFA_CANONICAL_TASK_DISPATCH_BINDING=true`
- `HFA_CANONICAL_TASK_CLAIM_BINDING=true`
- `HFA_CANONICAL_TASK_TERMINAL_BINDING=true`
- `HFA_CANONICAL_RUN_CREATE_BINDING=true`

Configuration flags are necessary but not sufficient. Before canonical TASK retry mutates a specific RUN, recovery also proves that RUN's durable canonical `RUN_CREATE` record/receipt and exact admission resource reservation exist. The reservation must be `FINALIZED` for nonterminal TASK recovery. A RUN without this proof fails closed and never falls back to legacy mutable RUN recovery.

`RecoveryService` composes the existing `AdmissionResourceReservationManager` and `RunTerminateAuthorityBinding(resource_manager=...)`; neither implementation is duplicated or modified.

## Canonical TASK_REQUEUE and idempotency

R2 semantics are preserved:

- existing `OperationType.TASK_REQUEUE`;
- existing TASK aggregate/revision chain;
- legal transition `running -> ready`;
- operation slot per `claim_epoch`;
- retry-attempt authority is durable `TASK_CLAIM.dispatch_attempt`;
- exact TASK_CLAIM predecessor proof;
- durable authority commit before mutable projection/delivery;
- proof-bound `canonical_project` and `canonical_deliver`;
- normal TASK_DISPATCH -> TASK_CLAIM continuation.

Caller-local `requeued_at_ms` / `ready_score` are not canonical command semantics. Projection score and delivery time are reconstructed from the first durable TASK_REQUEUE record's `committed_at_ms`. Divergent semantic duplicates remain `IDEMPOTENCY_CONFLICT`.

R3 additionally makes production stale recovery receipt-first. If TASK_REQUEUE is durable but projection/delivery failed, `TaskRecoveryManager` recognizes the durable `ready` TASK_REQUEUE head and re-enters the existing TASK_REQUEUE replay surface instead of demanding a new running TASK_CLAIM. No second revision, requeue-count increment, or TaskRequeued delivery is created.

## Retry exhaustion lifecycle

For an exact durable TASK_CLAIM whose `dispatch_attempt` exceeds `HeartbeatPolicy.max_requeue_count`, no TASK_REQUEUE operation is created.

Required sequence:

`stale TASK -> exact TASK_CLAIM -> TASK_FAIL -> TASK terminal projection -> RUN_TERMINATE -> resource settlement -> RUN projection`

`TaskTerminalAuthorityBinding.fail()` remains the only TASK_FAIL authority. `RunTerminateAuthorityBinding` remains the only RUN terminal authority. `AdmissionResourceReservationManager` remains the only settlement implementation.

If TASK_FAIL is durable but its mutable projection failed, R3 reads the durable terminal TASK head and uses `TaskTerminalAuthorityBinding.replay_terminal_projection()`. A second TASK_FAIL revision is not created.

## Stale RUN role under canonical retry

When canonical TASK retry is enabled, stale RUNs never enter legacy `_handle_stale()` / `run_recovery_commit.lua` retry mutation. They are classified from canonical RUN/TASK/resource evidence:

### Case 1 — canonical TASK lifecycle nonterminal

Canonical TASK head is `pending`, `ready`, `scheduled`, or `running`.

Result: suppress legacy RUN retry. TASK lifecycle remains authoritative.

### Case 2 — every TASK canonical terminal

Every canonical TASK head is `done` or `failed`, every terminal mutable TASK projection agrees, canonical RUN_CREATE/resource proof exists, and the RUN is still in the stale RUN projection.

Result: attempt/replay the existing `RunTerminateAuthorityBinding`. This naturally performs:

`RUN_TERMINATE durable -> existing resource settlement -> RUN projection`

The trigger/finalization inputs are reconstructed deterministically from durable terminal TASK records. Caller wall clock is not used to create a new terminal operation identity.

### Case 3 — missing/corrupt/contradictory evidence

Examples include missing canonical TASK aggregate/receipt, wrong RUN→TASK index type, missing RUN_CREATE/resource proof, TASK identity/tenant mismatch, terminal canonical TASK with unproven mutable terminal projection, or unsupported canonical state.

Result: fail closed. No legacy RUN reschedule fallback and no silent repair.

## Crash/replay exhaustion boundaries

R3 production acceptance covers:

- K1 — TASK_FAIL commit fails before durability: TASK/RUN/resources unchanged; no RUN_TERMINATE.
- K2 — TASK_FAIL durable / TASK projection fails: replay terminal projection from durable TASK head; no second TASK_FAIL revision.
- K3 — TASK terminal projection complete / RUN_TERMINATE commit fails: TASK remains failed, RUN remains canonical revision 1, resources FINALIZED; the next stale-RUN sweep retries RUN_TERMINATE.
- K4 — RUN_TERMINATE durable / settlement fails: RUN terminal authority remains revision 2, resources FINALIZED, mutable RUN projection remains nonterminal; stale-RUN replay reuses the same RUN operation/revision and settles once.
- K5 — settlement succeeds / RUN projection fails: reservation is SETTLED and counters decrement exactly once; stale-RUN replay reuses RUN revision 2 and only recovers projection.
- K6 — full exhaustion: TASK failed, RUN failed, reservation SETTLED, admission counters zero exactly once, no legacy RUN reschedule, no duplicate TaskRequeued or RUN terminal delivery.

TASK terminal projection removes the task from its running ZSET. Therefore K3/K4/K5 recovery is intentionally rediscovered through the existing stale RUN (`RedisKey.cp_running`) path; no new pending-recovery registry exists.

## Heartbeat / stale race contract

The stale detector's `observed_at_ms` is the eligibility boundary for one recovery invocation.

A same-claim heartbeat arriving after stale detection but before the already-in-flight canonical TASK_REQUEUE commit is intentionally too late to revoke that invocation. If the canonical commit never becomes durable, the next sweep re-runs stale detection and the newer heartbeat can prevent a new retry decision.

Once TASK_REQUEUE commits, the canonical TASK head is `ready`; a late completion from the old claim cannot create TASK terminal truth because the old running head has already been consumed. The next legitimate claim increments the existing fencing generation.

The race acceptance uses `asyncio.Event` around the real authority commit boundary, not sleeps.

## Production acceptance inventory

Sprint 84.9 R3 source contains:

- 13 focused authority unit tests;
- 8 real-Redis TASK_REQUEUE authority tests;
- 11 A–J + direct exhaustion real-Redis tests;
- 16 real `ControlPlaneService -> RecoveryService` production-composition tests, including K1–K6 and heartbeat/stale race.

Total Sprint 84.9 source inventory: **48 tests**.

The valid default-off production compatibility proof remains `test_flag_false_control_plane_preserves_historical_run_recovery` inside the 16-test real `ControlPlaneService -> RecoveryService` production-composition suite. All baseline-green prior sprint regression matrices remain mandatory operator gates. FakeRedis is not critical acceptance evidence.

## Known Pre-Existing Baseline-Red Recovery Tests

The following three legacy recovery test files are known pre-existing baseline debt and are **not** accepted as passing tests:

- `tests/core/test_task_recovery_core.py`
- `tests/integration/test_task_recovery_integration.py`
- `tests/integration/test_task_recovery_chaos_integration.py`

Exact accepted parent:

`541c6a1f1d1753d917cfacce855d917faa64bc6a`

Observed independently on that exact accepted parent: **2 passed / 8 failed**.

Observed on the Sprint 84.9 R4 candidate: **2 passed / 8 failed**.

Classification: **PRE_EXISTING_BASELINE_DEBT / NOT_SPRINT_84_9_REGRESSION**. The failures are legacy identity-fixture debt. R5 does not mark them passing, does not xfail/skip/deselect them, and does not change product behavior to satisfy them. The invalid mandatory `10 passed` expectation is removed from the Sprint 84.9 green acceptance matrix.

Sprint 84.9's valid default-off compatibility proof remains the real production-composition test `test_flag_false_control_plane_preserves_historical_run_recovery`, exercised through `ControlPlaneService -> RecoveryService`. Startup fail-closed coverage also remains, including `test_flag_true_control_plane_rejects_incomplete_canonical_retry_chain` and `test_flag_true_requires_canonical_run_create_dependency`. Cleanup of the three old baseline-red tests is separate historical debt and is outside canonical TASK_REQUEUE authority closure.

## Validator portability

The Windows operator validator uses the installed Python `redis` client for Redis preflight: connect, `PING`, `INFO server`, require major version >= 7, close. It does not require `redis-cli.exe` on PATH. GitHub Actions may continue using the Redis container's internal `redis-cli` health command.

## Frozen implementation areas

R3 does not modify:

- `canonical_transition.py`;
- TASK_CLAIM authority;
- TASK terminal authority;
- RUN_CREATE authority;
- RUN_TERMINATE authority;
- admission resource settlement implementation;
- `ControlPlaneService` composition root;
- scheduler source;
- worker source.

The lifecycle closure is composition/recovery logic only.

# Sprint 84.7E — Worker Crash / Redelivery Safety Acceptance

## Status

- Accepted parent: `9a0ab239a8733e7ad5a3675217a4c34bad72e308`
- Target branch: `sprint/84-7e-worker-crash-retry-acceptance`
- Production readiness: `false`
- Production cutover authorized: `false`
- New lifecycle authority: `false`
- Canonical TASK_REQUEUE authority: `false`

## Purpose

Sprint 84.7E closes worker crash and Redis PEL redelivery composition around the
canonical production lifecycle already selected through Sprint 84.7D. It does
not introduce execution retry authority or a new claim generation.

The invariant is that a Redis stream delivery may be retried while already
committed canonical work is never blindly executed a second time.

## Production composition

The accepted Profile D lifecycle remains:

```text
TASK_ADMIT
→ TASK_DISPATCH
→ TASK_CLAIM
→ executor
→ TASK_COMPLETE / TASK_FAIL
→ TASK projection
→ RUN_TERMINATE
→ RUN projection
→ XACK
```

Sprint 84.7E adds only a worker-side recovery branch before the normal claim
path:

```text
redelivered stream message
→ rebuild TaskContext
→ existing runtime-terminal duplicate classification

runtime TASK terminal
→ existing Sprint 84.7D RUN-only recovery
→ XACK only if RUN policy allows it

runtime TASK running
→ read task_meta only as claim-generation lookup input
→ existing TaskTerminalAuthorityBinding.replay_terminal_projection()
→ durable TASK terminal record/receipt/head/predecessor claim are validated
→ replay TASK projection only
→ existing RUN_TERMINATE finalization/replay
→ XACK only after required projections complete

no durable TASK terminal
→ existing TaskConsumer / TASK_CLAIM path
```

The mutable task metadata is not promoted to authority. It supplies only
`task_id`, `run_id`, `tenant_id`, original `worker_instance_id`,
`scheduler_epoch`, and `claim_epoch` to the existing proof-bound replay call.
Any mismatch with durable authority fails closed.

## Cross-worker recovery

A replacement worker may repair an already durable terminal projection because
projection replay does not create lifecycle authority. The adapter therefore
uses the original claim-generation identity stored in task metadata and lets
`TaskTerminalAuthorityBinding` prove it against the durable TASK terminal and
predecessor TASK_CLAIM records.

The replacement worker does not manufacture a worker identity, scheduler epoch,
claim epoch, TASK revision, or terminal operation id.

## Reclaim configuration

`WorkerService` now accepts `reclaim_idle_ms` as an internal configuration
value and passes it to the existing `WorkerConsumer` constructor. The historical
default remains `60000`. No environment variable is added. Acceptance tests use
`0` to make restart/XCLAIM behavior deterministic without waiting one minute.

## Acceptance scenarios

The real-Redis acceptance file contains seven scenarios:

1. **Claim commit / claim projection pending** — first worker commits
   `TASK_CLAIM` but projection fails; same worker id restarts; exact claim replay
   completes projection; executor runs exactly once; TASK terminal and RUN
   terminal complete; PEL becomes empty.
2. **Claim projected / execution ambiguous** — executor raises after claim
   projection; restart sees exact claim already projected; no second execution,
   no TASK terminal, no RUN terminal, no ACK; PEL remains pending.
3. **Executor result / terminal authority missing** — executor returned but the
   terminal gateway failed before canonical terminal commit; restart does not
   reconstruct the lost in-memory result and does not execute again.
4. **TASK terminal commit / TASK projection pending** — first worker commits the
   terminal TASK revision but projection fails; a different worker reclaims the
   PEL message and replays only the existing TASK projection; executor remains
   suppressed; RUN finalization completes; TASK revision and operation id stay
   unchanged.
5. **RUN commit / RUN projection pending** — TASK terminal is fully projected,
   RUN authority is durable, RUN projection fails; replacement worker uses the
   existing Sprint 84.7D RUN-only replay; no executor or TASK mutation; RUN
   revision and operation id stay unchanged.
6. **ACK loss** — TASK and RUN authority/projections complete but XACK is forced
   to fail; replacement worker reclaims and eventually ACKs without allocating
   any lifecycle revision.
7. **Different-worker running claim** — replacement worker XCLAIMs a delivery
   whose canonical claim is already projected but has no terminal authority;
   execution is suppressed, no ACK occurs, no requeue occurs, and canonical
   TASK remains running for Sprint 84.9.

The integration suite explicitly inspects Redis PEL ownership, delivery count,
and PEL cardinality. Cross-worker cases use the existing XPENDING/XCLAIM path
rather than invoking authority bindings directly.

## Negative authority sentinels

The acceptance suite fails immediately if selected canonical recovery reaches:

- legacy `BaseExecutor.execute`;
- legacy `DagLua.task_complete`;
- historical `run_terminate_from_tasks.lua`;
- `TaskRecoveryManager.requeue_stale_task()`.

The production files contain no `TASK_REQUEUE`, `task_requeue.lua`,
`RecoveryService`, or new retry/recovery operation type.

## Frozen boundaries

Sprint 84.7E does not modify the existing authority implementations or queue
mechanics:

- `hfa-control/src/hfa_control/task_terminal_authority.py`
- `hfa-control/src/hfa_control/task_claim_authority.py`
- `hfa-control/src/hfa_control/task_claim.py`
- `hfa-control/src/hfa_control/run_terminate_authority.py`
- `hfa-control/src/hfa_control/run_termination.py`
- `hfa-worker/src/hfa_worker/consumer.py`
- `hfa-worker/src/hfa_worker/task_consumer.py`
- `hfa-control/src/hfa_control/task_recovery.py`
- `hfa-control/src/hfa_control/recovery.py`
- `hfa-core/src/hfa/lua/task_requeue.lua`
- `hfa-core/src/hfa/lua/task_complete.lua`

CI and the Windows validator compare these files directly with the exact
accepted parent.

## Exact repository scope

Exactly seven files are expected after historical CI contract maintenance:

1. `hfa-worker/src/hfa_worker/main.py`
2. `hfa-worker/src/hfa_worker/run_finalizing_runtime.py`
3. `tests/core/test_production_worker_crash_retry_contract.py`
4. `tests/integration/test_worker_crash_retry_canonical_composition_integration.py`
5. `.github/workflows/sprint84-7e-worker-crash-retry-acceptance.yml`
6. `docs/implementation/sprint84/SPRINT84.7E-worker-crash-retry-acceptance.md`
7. `.github/workflows/sprint84-7d-run-terminate-production-injection.yml`

## Required gates

The Sprint 84.7E workflow requires real Redis 7 and gates:

- 84.7E focused crash/retry contracts: 8;
- 84.7E real Redis restart/reclaim scenarios: 7;
- 84.7D focused / Redis / canonical RUN authority: 7 / 6 / 23;
- C2 focused / Redis: 22 / 3;
- C1 unit / Redis / legacy terminal: 7 / 23 / 18;
- 84.7B focused: 76;
- TASK_CLAIM: 83;
- RUN finalization: 41;
- production worker: 43;
- Sprint 83.7: 174;
- Sprint 83.8: 199;
- zero skip/deselect/xfail/xpass exclusions;
- authority audit `banned = 0`;
- workflow YAML parse;
- product compileall and focused test py_compile;
- `git diff --check`.

## Post-E lifecycle boundary

If the real-Redis and regression gates pass, the proven matrix is intended to
be:

```text
TASK_ADMIT       production_bound=true
TASK_DISPATCH    production_bound=true
TASK_CLAIM       production_bound=true
TASK_COMPLETE    production_bound=true
TASK_FAIL        production_bound=true
RUN_TERMINATE    production_bound=true

worker crash/redelivery:
  durable authority replay=true
  missing TASK projection replay=true
  missing RUN projection replay=true
  ACK-loss replay=true
  blind duplicate executor retry=forbidden

TASK_REQUEUE canonical_authority=false
new execution generation after abandoned running claim=not implemented
production_ready=false
production_cutover_authorized=false
```

The intentionally stranded ambiguous-running windows are not a Sprint 84.7E
liveness defect. A new execution generation belongs to Sprint 84.9 and must not
be synthesized by redelivery handling.
The seventh file is historical regression-contract maintenance only. Sprint 84.7D
originally froze `run_finalizing_runtime.py` because that adapter was outside D.
Sprint 84.7E intentionally evolves it for restart/redelivery recovery. The D
workflow therefore retains the old byte lock on the original D feature branch,
while later regression-trigger branches prove D behavior through the D tests
instead of requiring obsolete adapter bytes.

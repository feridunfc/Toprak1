# Sprint 83.1 — Runtime Alpha Product Acceptance

## Purpose

Sprint 83.1 adds a one-command, real-Redis acceptance harness for the current canonical TASK execution path.

The harness answers a narrower and more useful question than production readiness:

> Can an engineer submit a deterministic test task into the current TASK lifecycle, dispatch it through the guarded Lua boundary, execute it through `WorkerConsumer -> TaskConsumer`, read the output, verify ACK ownership, and receive an explicit explanation when RUN truth blocks dispatch?

The answer is recorded as `runtime_alpha_testable`.

This sprint does not claim that the user-facing product, autonomous agent layer, external provider path, or general self-healing loop is complete.

## Exact runtime path

The healthy acceptance scenario executes:

```text
Disposable Redis 7 database
→ test-only RUN authority seed
→ DagLua.task_admit
→ WorkerReservationManager.reserve
→ DagLua.task_dispatch_commit
→ TaskRequested stream delivery
→ WorkerConsumer
→ TaskConsumer.consume_once
→ TaskClaimManager.claim_start
→ TaskHeartbeatManager
→ deterministic AlphaEchoExecutor
→ DagLua.task_complete
→ task output read
→ stream ACK verification
→ ControlPlaneService.get_run_state
```

The harness does not call the executor or message processor directly.

## Truth-conflict scenario

The second scenario creates:

```text
RUN = done
TASK = ready
```

It then calls the production `DagLua.task_dispatch_commit` boundary and requires:

- `run_truth_terminal_conflict`;
- `detail_code=run_state_terminal`;
- zero TASK lifecycle mutation;
- zero ready/scheduled projection mutation;
- zero shard-stream append;
- durable operation-bound conflict evidence for `TASK_DISPATCH`.

## Honest readiness levels

A successful harness run returns:

```yaml
status: PASS_WITH_LIMITATIONS
runtime_alpha_testable: true
product_alpha_ready: false
production_ready: false
```

`runtime_alpha_testable=true` means only that the bounded technical acceptance path passed.

`product_alpha_ready=false` remains mandatory in Sprint 83.1 because the acceptance report deliberately exposes these open product gaps:

1. terminal TASK completion is not yet coordinated with RUN finalization;
2. no user-facing `TASK_CANCEL` command is included;
3. no user-facing retry/requeue command is included;
4. the executor is deterministic and local, not an enabled external provider;
5. the harness requires a disposable Redis test database.

## RUN finalization evidence

The healthy TASK reaches `done` and its output is readable. The RUN remains `running` because coordinated RUN finalization is not part of the accepted Sprint 83.1 scope.

The read model must therefore return:

```yaml
run_state: running
run_truth_status: conflict
run_finalization_gap: true
detail_code: task_terminal_run_nonterminal
```

This is an expected limitation, not hidden success.

## Safety boundaries

The command defaults to Redis database 15 and does not call `FLUSHDB` unless `--reset-test-db` is supplied.

`--reset-test-db` must be used only with a disposable local or CI Redis database.

Sprint 83.1 does not:

- enable `HFA_CANONICAL_TASK_ADMIT_BINDING`;
- enable an external executor;
- make an LLM or network call;
- add automatic reconciliation;
- add silent repair;
- add production deployment or release actions;
- mutate historical Sprint 80–82 evidence;
- modify production scheduler, worker, authority, Lua, API, or lifecycle source files.

## One-command acceptance

```powershell
python scripts/runtime_alpha_acceptance.py `
  --redis-url redis://localhost:6389/15 `
  --acceptance-id local-s83-1 `
  --reset-test-db `
  --out local_out/sprint83/runtime_alpha_acceptance.json `
  --json
```

Expected command exit code:

```text
0 when status=PASS_WITH_LIMITATIONS
1 when status=FAIL
```

## Deterministic report contract

For a fixed `acceptance_id` and a reset disposable database, the JSON report must be byte-semantically stable after JSON parsing.

Required top-level fields:

```yaml
schema_version: 1
sprint: "83.1"
source: runtime_alpha_product_acceptance
status: PASS_WITH_LIMITATIONS
runtime_alpha_testable: true
product_alpha_ready: false
production_ready: false
global_truth_policy: OPERATION_SCOPED_FAIL_CLOSED
one_command_acceptance_available: true
real_redis_used: true
canonical_task_admit_used: true
runtime_truth_guarded_dispatch_used: true
worker_consumer_used: true
task_consumer_used: true
task_output_readable: true
conflict_explanation_available: true
run_finalization_supported: false
cancel_command_supported: false
retry_command_supported: false
external_executor_enabled: false
automatic_repair_authorized: false
production_cutover_authorized: false
```

Required scenarios:

```text
canonical_task_success
terminal_run_blocks_dispatch
```

## Exit criteria

Sprint 83.1 is technically complete only when:

- the dedicated workflow checks out the exact PR head;
- bootstrap scope is exact;
- Redis 7 runs the three acceptance tests with zero skips;
- the one-command harness produces the required JSON artifact;
- Sprint 82.1–82.4 regressions remain green;
- existing thin CLI and minimal HTTP API tests remain green;
- compile/whitespace checks pass;
- Authority Gate passes;
- no production runtime file is changed.

## Out of scope / next slices

Likely follow-on slices are:

```text
83.2 coordinated single-task RUN finalization
83.3 user-visible status/result surface over durable Redis truth
83.4 explicit cancellation contract
83.5 bounded retry/requeue product command
```

Those names are planning labels, not implementation authorization.

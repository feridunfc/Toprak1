# IRONCLAD V23.1 Hardening Roadmap

## What was added in this patch set

### 1. Dispatch commit assertion matrix
A new real-Redis integration suite validates the full `dispatch_commit_detailed(...)` outcome matrix for:
- `admitted -> scheduled`
- `queued -> scheduled`
- `scheduled -> already_running`
- `running -> already_running`
- terminal states -> `illegal_transition`
- missing state -> `state_conflict`

This closes the gap between unit-level confidence and real-Redis behavioral proof.

### 2. Exact stream payload validation
The integration suite now verifies that the control stream and shard stream contain the exact expected payload fields after atomic dispatch, including:
- `event_type`
- `run_id`
- `tenant_id`
- `agent_type`
- `worker_group`
- `shard`
- `region`
- `policy`
- `priority`
- `payload`
- `trace_parent`
- `trace_state`

This proves that the God Script is not only atomic, but also serializes the correct event contract.

### 3. SchedulerLoop end-to-end integration
A new integration test exercises `SchedulerLoop.run_cycle(max_dispatches=1)` against real Redis while using deterministic in-test fakes for:
- tenant queue
- fairness picker
- snapshot builder
- worker scorer
- shard resolver
- dispatch controller

This proves the control-plane orchestration path can drive a real atomic dispatch and produce the expected stream side effects.

### 4. Chaos coverage under strict mode
A new strict-mode chaos suite validates:
- Redis client reconnect after a committed dispatch
- duplicate dispatch attempt returns `already_running`
- `SCRIPT FLUSH` mid-run followed by strict-mode recovery reschedule

This moves the system from nominal-path confidence to degraded-path confidence.

## Files included
- `tests/integration/test_strict_cas_smoke.py`
- `tests/integration/test_dispatch_commit_matrix_integration.py`
- `tests/integration/test_scheduler_loop_e2e_integration.py`
- `tests/integration/test_strict_mode_chaos_integration.py`
- `IRONCLAD_V23_1_ROADMAP.md`

## What this patch set does not change
No production implementation was modified in this bundle.
This is an integration-hardening pack that raises confidence in the current production code.

## Release meaning
With these tests in place, the system has evidence for:
- atomic dispatch on real Redis
- exact stream payload correctness
- strict-mode state-machine enforcement
- duplicate-dispatch rejection
- NOSCRIPT reload resilience
- stale recovery in strict mode

## Recommended next sprint: V23.2

### Sprint goal
Move from “strong integration confidence” to “release-gate certainty”.

### Tasks
1. Add deterministic assertions for every `DispatchCommitResult.status` metadata payload.
2. Add integration tests for control/shard stream trimming semantics (`MAXLEN ~`).
3. Add a scheduler-loop fairness drift test over multiple tenants and multiple cycles.
4. Add stale-recovery end-to-end tests for DLQ threshold crossing.
5. Add worker-pool degradation scenarios:
   - no schedulable workers
   - capacity zero
   - draining workers only
6. Add replay/idempotency tests for duplicate shard delivery on worker side.
7. Add CI profile that runs the strict-mode integration suite separately from the core suite.

## Recommended next sprint: V23.3

### Sprint goal
Operational hardening and production rollout guardrails.

### Tasks
1. Add metrics assertions in integration tests:
   - Lua fallback counters must remain zero in strict mode.
   - dispatch success/failure counters must match stream-side outcomes.
2. Add Redis failover / restart scenario test if infra permits.
3. Add long-run soak test for loader stability and SHA cache correctness.
4. Add alerting playbook for:
   - repeated `NOSCRIPT`
   - repeated `state_conflict`
   - repeated `already_running`
5. Add release checklist requiring:
   - core suite green
   - integration suite green
   - strict-mode suite green
   - no direct state-write regression

## Brutal status
The system is no longer “apparently correct”.
It is now backed by real-Redis atomicity evidence.

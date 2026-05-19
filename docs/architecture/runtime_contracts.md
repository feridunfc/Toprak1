# Runtime Contracts

## Purpose

This document records the current runtime contract between worker-side execution code and core runtime state storage.

Sprint 14 / Phase 1.5 focuses on stabilizing this contract before cognitive hardening, archive/replay hardening, Redis chaos, or staging soak work.

## Current worker-side StateStore contract

`hfa-worker/src/hfa_worker/consumer.py` and `hfa-worker/src/hfa_worker/idempotency.py` currently depend on the compatibility `StateStore` API from `hfa.runtime.state_store`.

Required worker-facing methods:

- `StateStore(redis_client, ...)`
- `is_terminal(run_id) -> bool`
- `mark_running(run_id, worker_id, worker_group, shard) -> bool`
- `renew_claim(run_id) -> bool`
- `release_claim(run_id) -> bool`
- `store_result(run_id, tenant_id, status, payload, cost_cents, tokens_used, error=...)`
- `transition_state(run_id, state)`
- `mark_completed(run_id)`

## Current core runtime StateStore layers

`hfa-core/src/hfa/runtime/state_store.py` contains multiple layers:

- `ControlStateStore`
- `RedisControlStateStore`
- `StateStore`

The newer core/control-plane surface includes:

- `reserve_worker(...)`
- `update_vruntime(...)`
- `get_owner(...)`
- `set_owner(...)`
- `get_task_state(...)`
- `set_task_state(...)`
- `set_task_output(...)`
- `store_task_output(...)`
- `complete_once(...)`

## Contract risk

There are two overlapping StateStore contracts:

1. Worker compatibility lifecycle contract.
2. Newer control-plane/runtime contract.

The worker path still relies on the compatibility lifecycle methods. Those methods must remain stable until the worker is explicitly migrated to the newer runtime contract.

## Stabilization rule

Do not remove or silently change worker-facing compatibility methods until:

1. Equivalent new runtime methods exist.
2. WorkerConsumer and IdempotencyGuard are migrated.
3. Unit tests prove terminal, stale-claim, release, renewal, and completion behavior.
4. Dashboard/replay evidence confirms no regression.

## Sprint 14 target

14A documents the current contract.

14B should add explicit compatibility tests for the worker-facing StateStore methods before any implementation changes.

14C should address scheduler Lua fallback duplication separately.

14D should address deployment smoke separately.

## Additional import contract finding

Some legacy/core tests import `FakeExecutor` from `hfa_worker.executor`, while the current executor factory imports `FakeExecutor` from `hfa_worker.fake_executor`.

Current observed layout:

- `hfa_worker.executor` exports `BaseExecutor`.
- `hfa_worker.fake_executor` defines `FakeExecutor`.
- `hfa_worker.executor_factory` imports `FakeExecutor` from `hfa_worker.fake_executor`.

This indicates an executor public import compatibility contract that should be stabilized before broader runtime/cognitive hardening.

Required follow-up:

- Either re-export `FakeExecutor` from `hfa_worker.executor` for backward compatibility.
- Or migrate all imports to `hfa_worker.fake_executor`.
- Add a regression test for the chosen public import contract.

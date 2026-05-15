# Sprint 2 Acceptance Report — CQRS Pilot: Task Completion Vertical Slice

## Scope

Modified or added only Sprint 2 contract files plus this required acceptance report:

- `hfa-core/src/hfa/runtime/state_store.py`
- `hfa-core/src/hfa/events/apply_event.py`
- `hfa-core/src/hfa_core/events/event_types.py`
- `tests/core/test_task_complete_cqrs_slice.py`
- `acceptance_report.md`

No worker, agent, scheduler, or broad projection rewrite was performed.

## Event flow summary

With `IRON_V3_COMPLETION_SLICE=1`, `StateStore.complete_once` now follows this authoritative completion slice flow:

1. Duplicate completion is suppressed before event append or projection.
2. Stale owner completion is fenced before event append or projection.
3. `TASK_COMPLETION_REQUESTED` is durably appended before any terminal projection attempt.
4. `TASK_COMPLETED` or `TASK_FAILED` is durably appended before terminal Redis projection.
5. Runtime terminal state is projected only after the final completion event append succeeds.
6. If either requested or final event append fails, the method returns a non-ok result and does not write terminal state or owner projection.

When `IRON_V3_COMPLETION_SLICE` is disabled, legacy behavior remains governed by the existing Sprint 1 `IRON_V3_EVENT_GATE` path and background event emission compatibility.

## Acceptance criteria

- `TASK_COMPLETION_REQUESTED` exists: PASS
- Final terminal state requires `TASK_COMPLETED` or `TASK_FAILED` under `IRON_V3_COMPLETION_SLICE`: PASS
- Stale owner is fenced before terminal projection: PASS
- Duplicate completion is suppressed before terminal projection: PASS
- Completion slice is replay-rebuildable through `hfa.events.apply_event.replay_completion_slice`: PASS
- Rollback flag exists: PASS (`IRON_V3_COMPLETION_SLICE`)

## Verification run

Commands run locally in the patch workspace:

```bash
PYTHONPATH=hfa-core/src:hfa-control/src:hfa-worker/src:hfa-agents/src:hfa-semantic/src \
  python -m compileall \
  hfa-core/src/hfa/runtime/state_store.py \
  hfa-control/src/hfa_control/idempotent_completion.py \
  hfa-control/src/hfa_control/task_ownership.py \
  hfa-control/src/hfa_control/task_claim.py \
  hfa-core/src/hfa/events/apply_event.py \
  hfa-core/src/hfa_core/events/event_types.py

PYTHONPATH=hfa-core/src:hfa-control/src:hfa-worker/src:hfa-agents/src:hfa-semantic/src \
  python -m pytest tests/core/test_task_complete_*.py -q --tb=short

PYTHONPATH=hfa-core/src:hfa-control/src:hfa-worker/src:hfa-agents/src:hfa-semantic/src \
  python -m pytest tests/core/test_task_complete_cqrs_slice.py \
  tests/core/test_event_store_core.py \
  tests/core/test_replay_engine_core.py -q --tb=short
```

Results:

- `tests/core/test_task_complete_*.py`: 8 passed
- completion slice + event/replay smoke: 12 passed

## Remaining gaps

- Existing broader `tests/core` failures remain outside Sprint 2 scope and were not modified.
- Existing worker direct-finalization risks remain excluded by the Sprint 2 contract.
- Existing scheduler sealing remains deferred to Sprint 4.

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

---

# Sprint 3 Acceptance Report — LLM Sealing + Claim Check

## Scope

Modified or added only Sprint 3 contract files plus this required acceptance report:

- `hfa-core/src/hfa/events/completion_capture.py`
- `hfa-core/src/hfa/events/execution_artifacts.py`
- `hfa-agents/src/hfa_agents/base/agent_base.py`
- `tests/core/test_completion_capture_required.py`
- `tests/core/test_replay_without_llm.py`
- `acceptance_report.md`

No control-plane, worker, scheduler, or docs files were changed.

## Artifact schema summary

Sprint 3 adds a shared completion capture helper for agent/LLM-like outputs.
Captured events use `LLM_COMPLETION_CAPTURED` and include:

- `provider`
- `model`
- `prompt_hash`
- `system_prompt_hash`
- `output_hash`
- `tokens`
- `cost_cents`
- `fallback_used`
- `output`
- `capture_helper`

The `output` field is event-safe:

- Small outputs use `mode=inline` with deterministic content hash.
- Large outputs use `mode=claim_check`, `artifact_ref`, `content_hash`, and `size_bytes`.
- Large payload content is not embedded in the event.

Replay uses `replay_captured_completion()` and `resolve_execution_artifact()` to reconstruct output from event payloads and artifact references only.  No live LLM call is required or available in the replay helper.

## Feature flag behavior

- `IRON_V3_LLM_SEALING=0` or unset: legacy agent execution behavior is preserved.
- `IRON_V3_LLM_SEALING=1`: `AgentBase.execute()` seals output through `hfa.events.completion_capture.capture_agent_completion`.
- If strict capture append fails while the flag is enabled, the agent returns a failed result with HITL required.

## Acceptance criteria

- Replay never calls live LLM: PASS
- Large outputs use claim-check reference: PASS
- Small outputs inline correctly: PASS
- Strict mode append failure returns failed result: PASS
- Listed roles use shared helper through `AgentBase.execute()`: PASS
- Rollback flag exists: PASS (`IRON_V3_LLM_SEALING`)

## Verification run

Commands run locally in the patch workspace:

```bash
PYTHONPATH=hfa-core/src:hfa-agents/src:hfa-semantic/src \
  python -m compileall \
  hfa-core/src/hfa/events/completion_capture.py \
  hfa-core/src/hfa/events/execution_artifacts.py \
  hfa-agents/src/hfa_agents/base/agent_base.py \
  hfa-agents/src/hfa_agents/roles/coder.py \
  hfa-agents/src/hfa_agents/roles/tester.py \
  hfa-agents/src/hfa_agents/roles/architect.py \
  hfa-agents/src/hfa_agents/roles/researcher.py

PYTHONPATH=hfa-core/src:hfa-agents/src:hfa-semantic/src \
  python -m pytest tests/core/test_completion_capture_required.py \
  tests/core/test_replay_without_llm.py -q --tb=short
```

Results:

- Sprint 3 focused tests: 7 passed

## Remaining gaps

- Existing broader repository drift remains outside Sprint 3 scope.
- This sprint does not modify worker execution, scheduler commit, or control-plane replay internals.
- Large artifact storage is provided by a minimal local/file protocol or injected artifact store; production object storage can be introduced later through the same `ArtifactStore` protocol without changing event schema.

# Sprint 7 Acceptance Report — Semantic Constitution + Release

## Scope

Changed only Sprint 7 files:

- `hfa-semantic/src/hfa_semantic/runtime/semantic_hook.py`
- `hfa-semantic/src/hfa_semantic/validation/safety_verdict.py`
- `hfa-semantic/src/hfa_semantic/runtime/engine.py`
- `hfa-agents/src/hfa_agents/integration/semantic_bridge.py`
- `hfa-worker/src/hfa_worker/scheduler_semantic_hook.py`
- `docs/ops/config_matrix.md`
- `docs/ops/runbook.md`
- `docs/ops/release_checklist.md`
- `tests/integration/test_phase5b_strict_mode.py`
- `tests/integration/test_phase6a_runtime_safety.py`

## Acceptance

- Advisory path may degrade/fail open.
- Gate path fails closed.
- Safety verdicts carry replay/audit visibility flags.
- Agent semantic bridge keeps advisory enrichment separate from gate evaluation.
- Scheduler/worker semantic prewarm remains non-authoritative.
- Ops docs define release mode and rollback.

## Feature flag / rollback

Rollback:

```text
IRON_SEMANTIC_GATE_MODE=legacy
```

## Test command

```powershell
$env:IRON_SEMANTIC_GATE_MODE="gate"
python -m pytest tests/integration/test_phase5b_strict_mode.py tests/integration/test_phase6a_runtime_safety.py -q --tb=short
```

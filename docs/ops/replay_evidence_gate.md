# Sprint 12 — Replay Evidence Gate Plan

## Current replay state

Current replay support exists but is not yet a production release gate.

Known replay components:

- `scripts/replay_compare.py`
- `scripts/full_auto_v3_2_smoke.py`
- `tests/core/test_replay_engine_core.py`
- `tests/core/test_replay_without_llm.py`
- `tests/integration/test_replay_compare_enforcement.py`
- `hfa-dashboard/backend/read_models/replay.py`
- `hfa-dashboard/backend/routes/replay.py`

## Current dashboard behavior

The dashboard replay panel is read-only and reports readiness only:

- whether `scripts/replay_compare.py` exists
- whether `scripts/full_auto_v3_2_smoke.py` exists
- `last_result: not_executed_by_dashboard`

The dashboard does not execute replay and does not currently read replay artifact bodies.

## Sprint 12 target

Sprint 12 turns replay readiness into replay evidence:

1. `scripts/replay_compare.py` can emit JSON artifact output.
2. CI can generate and upload a replay evidence artifact.
3. Dashboard replay read model can prefer a generated replay artifact when present.
4. Replay remains read-only from dashboard.
5. Release gating starts conservative: tool failure fails CI; mismatch enforcement can remain explicit/configured until replay fixtures are production-grade.

## Non-goals

- Do not make dashboard execute replay.
- Do not add replay write/mutation endpoints.
- Do not claim production replay completeness until cold restart, archive/replay semantics, and staging replay fixtures are proven.

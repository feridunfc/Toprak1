# Sprint 6 Acceptance Report — Proof Enforcement + Quarantine

## Scope
Implemented only Sprint 6 proof/quarantine enforcement surfaces:

- `hfa-control/src/hfa_control/reconciliation_manager.py`
- `hfa-control/src/hfa_control/task_recovery.py`
- `hfa-control/src/hfa_control/scheduler_loop.py`
- `scripts/replay_compare.py`
- `tests/integration/test_reconciliation.py`
- `tests/integration/test_recovery_modes.py`
- `tests/integration/test_quarantine.py`
- `tests/integration/test_runtime_recovery_guard.py`
- `tests/integration/test_replay_compare_enforcement.py`

`hfa-control/src/hfa_control/recovery.py` and `hfa-worker/src/hfa_worker/runtime/worker_runtime.py` were not changed because existing Sprint 5/Sprint 4 surfaces already expose the needed quarantine/proof hooks for this slice.

## Feature flag
- `IRON_V3_PROOF_ENFORCEMENT`

Rollback: disable `IRON_V3_PROOF_ENFORCEMENT`.

## Acceptance mapping

- gaps_or_duplicates_imply_ambiguous: covered by `recovery_proof_decision` and `evaluate_replay_compare`.
- ambiguous_blocks_auto_resume: covered by `TaskRecoveryManager.proof_allows_auto_resume`.
- quarantined_runs_blocked_by_scheduler_and_worker: scheduler quarantine projection plus existing worker quarantine read path.
- replay_integrity_failure_nonzero_cli: covered by `scripts/replay_compare.py`.
- critical_drift_requires_manual_path: `ReconciliationManager` blocks auto-correction under the flag.

## Suggested verification

```powershell
$env:IRON_V3_PROOF_ENFORCEMENT="1"
python -m pytest tests/integration/test_reconciliation.py tests/integration/test_recovery_modes.py tests/integration/test_quarantine.py tests/integration/test_runtime_recovery_guard.py tests/integration/test_replay_compare_enforcement.py -q --tb=short
```

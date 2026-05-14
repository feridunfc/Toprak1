# Sprint 1 Acceptance Report

## Scope

PASS WITH NOTE. Code changes are limited to Sprint 1 allowed files plus the
required output artifacts `changed_files.txt` and `acceptance_report.md`.

## Acceptance checks

- critical write paths go through event gate or are explicitly blocked/logged:
  PASS. `StateStore.complete_once` uses `AuthoritativeEventGate` before terminal
  Redis state/owner mutation when `IRON_V3_EVENT_GATE` is enabled. Missing or
  failed event append returns `event_gate_blocked` and does not mutate projection.
- strict-mode safety leakage is closed:
  PASS. Semantic gate mode fails closed; tester strict mode marks failure as
  HITL-required; `BudgetGuard` forces fail-closed when strict mode is enabled.
- budget authority is read from one source:
  PASS. `BudgetGuard.authority_source` exposes the Redis namespace as the single
  budget authority for that guard instance. Strict mode disables fail-open.
- correction loop feedback path is single and non-conflicting:
  PASS. `hfa_control.feedback.handle_result` normalizes execution results into
  one correction-loop decision; workflow engine consumes it once per step.
- constitution docs are consistent:
  PASS. Added constitution, authority matrix, and command/event/effect taxonomy.

## Rollback

Disable `IRON_V3_EVENT_GATE` to preserve legacy background event emission and
projection mutation order in `StateStore.complete_once`.

## Known residual risks

- Scheduler background event emission remains non-authoritative and is documented
  as such; scheduler modules are excluded from Sprint 1 scope.
- Worker direct-finalization risk remains outside this patch because
  `hfa-worker/**` is explicitly excluded.
- Existing replay coverage remains narrow; Sprint 1 only gates the terminal
  completion vertical slice.

# Sprint 9B — Event/Proof Path for Remaining Authority Risks

## Scope

Sprint 9B resolves the remaining Sprint 9A banned findings without using
comment-only greenwashing.

## Changes

- `SchedulerLoop._increment_epoch` is classified as lease-counter authority.
- `SchedulerLoop._quarantine_run` appends `RUN_QUARANTINED` before Redis
  quarantine projections when an event store is available.
- `FailureSweeper.sweep_failed_task` appends `TASK_BLOCKED_BY_FAILURE` before
  writing the child task block marker when an event store is available.
- Legacy/backward-compatible constructors remain valid.

## Expected Audit Result

Target:

- `banned` should drop from 4 to 0.
- Suspicious findings may remain and are intentionally not part of this phase.

## Notes

The fallback behavior preserves existing tests and degraded local usage. When an
event store is present, append failure blocks the corresponding projection write.

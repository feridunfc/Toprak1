
# V24.1C — Owner-aware Completion

## Goal
Prevent ghost completion after stale recovery or ownership transfer.

## What this pack adds
- `task_complete.lua` ownership fencing
- `TASK_OWNER_MISMATCH` reason
- DagLua wrapper now requires `worker_instance_id`
- core + integration tests for owner-aware completion

## Validated
- correct owner can complete
- wrong owner rejected
- late completion after requeue rejected

## Why it matters
A stale or partitioned worker must never be able to finalize a task it no longer owns.

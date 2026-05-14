
# V23.6A — FailureSweeper (Lazy Failure Propagation)

## What it does
- When a task fails, direct children are marked as `blocked_by_failure`
- No Lua change required
- Idempotent

## Why
Prevents orphan DAG nodes accumulating in pending/ready

## Next
- Recursive propagation (optional)
- Failure reason propagation

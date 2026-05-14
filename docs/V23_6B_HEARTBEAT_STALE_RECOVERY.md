
# V23.6B — Heartbeat + Stale Recovery

## Scope
- Worker heartbeat ownership
- Stale task detection
- Atomic stale requeue via Lua
- Retry exhaustion to failed

## Included
- `task_requeue.lua`
- `TaskHeartbeatManager`
- `TaskRecoveryManager`
- core + integration tests

## Invariants
- only running tasks accept heartbeat
- owner mismatch rejected
- stale running task requeues atomically
- retry exhaustion terminates deterministically


# V24.1B — Worker Heartbeat Loop Wiring

## Goal
Wire worker-side heartbeats into the real execution lifecycle.

## What this pack adds
- `HeartbeatLoop` async lifecycle helper
- `TaskConsumer` heartbeat integration
- core tests for clean start/stop
- integration tests for running heartbeat behavior

## Invariants
- claim happens before execute
- heartbeat runs only while execute is active
- heartbeat stops cleanly in `finally`
- active worker is not marked stale while heartbeating

## Next
- V24.1C owner-aware completion
- ghost completion rejection
- task started/completed event emission

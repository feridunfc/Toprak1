# Sprint 16 — Cold Restart + In-flight Recovery Contract

## Current runtime recovery primitives

The repository already contains recovery primitives in `hfa-control/src/hfa_control/task_recovery.py`:

- `TaskHeartbeatManager.record_heartbeat(...)`
- `TaskRecoveryManager.find_stale_tasks(...)`
- `TaskRecoveryManager.requeue_stale_task(...)`
- `TaskRecoveryManager.proof_allows_auto_resume(...)`

## Recovery model

Cold restart recovery must not blindly resume ambiguous in-flight work.

A task can be considered recovery-candidate only when:

1. It is present in the tenant running ZSET.
2. Its task metadata heartbeat is missing or older than the heartbeat stale threshold.
3. Replay/runtime proof allows auto-resume.
4. Requeue is performed through the canonical Lua requeue path.
5. Requeue result is recorded as evidence.

## Non-goals for Sprint 16A

- No production auto-resume loop yet.
- No Redis Sentinel/cluster failover yet.
- No mutation outside existing Lua recovery path.
- No bypass of replay proof gate.

## Sprint 16 gates

- Read-only stale running audit artifact.
- Unit tests for stale detection.
- Unit tests for proof gate blocking ambiguous resume.
- Optional mutation test through existing `requeue_stale_task` path.

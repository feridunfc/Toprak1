"""
hfa_worker/task_context.py
---------------------------
IRONCLAD Sprint 2 — Task execution context with fence tuple.

Sprint 2 change: TaskContext now carries scheduler_epoch and claim_epoch
received from the claim result.  Both values must be passed to every
task_complete and heartbeat call so the Lua fence can reject stale writes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TaskContext:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str
    worker_group: str
    worker_instance_id: str
    payload: dict[str, Any]
    shard: int = 0
    trace_parent: str = ""
    trace_state: str = ""
    required_capabilities: list[str] | None = None
    # Sprint 2: fence tuple — populated from TaskClaimResult after claim_start
    scheduler_epoch: str = ""
    claim_epoch: str = ""

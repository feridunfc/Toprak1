
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class TaskClaimedEvent:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str
    worker_group: str
    worker_instance_id: str
    claimed_at_ms: int


@dataclass
class TaskHeartbeatEvent:
    task_id: str
    worker_instance_id: str
    last_heartbeat_at_ms: int


@dataclass
class TaskStartedEvent:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str
    worker_group: str
    worker_instance_id: str
    started_at_ms: int
    trace_parent: Optional[str] = None
    trace_state: Optional[str] = None

"""
hfa-core/src/hfa/events/schema.py
IRONCLAD Sprint 10/11 — Complete event schema

Sprint 9 events preserved unchanged.
Sprint 10/11 events are additive.

IRONCLAD rules
--------------
* cost_cents: int — no float USD, anywhere.
* trace_parent / trace_state: W3C TraceContext across process boundaries.
* from_redis() never raises — safe defaults on missing keys.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from hfa.events.codec import decode_field


@dataclass
class HFAEvent:
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: float = field(default_factory=time.time)
    trace_parent: Optional[str] = None
    trace_state: Optional[str] = None

    @classmethod
    def from_redis(cls, data: dict) -> "HFAEvent":
        kw: Dict[str, Any] = {}
        for fname, fobj in cls.__dataclass_fields__.items():
            raw = data.get(fname.encode()) or data.get(fname)
            if raw is not None:
                kw[fname] = decode_field(fname, raw, str(fobj.type))
        return cls(**kw)


# ---------------------------------------------------------------------------
# Sprint 9 — unchanged
# ---------------------------------------------------------------------------


@dataclass
class RunRequestedEvent(HFAEvent):
    event_type: str = "RunRequested"
    run_id: str = ""
    tenant_id: str = ""
    agent_type: str = ""
    priority: int = 5
    payload: Dict[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""


@dataclass
class RunCompletedEvent(HFAEvent):
    event_type: str = "RunCompleted"
    run_id: str = ""
    tenant_id: str = ""
    worker_id: str = ""
    cost_cents: int = 0
    tokens_used: int = 0
    payload: Dict[str, Any] = field(default_factory=dict)
    completed_at: float = field(default_factory=time.time)


@dataclass
class RunFailedEvent(HFAEvent):
    event_type: str = "RunFailed"
    run_id: str = ""
    tenant_id: str = ""
    worker_id: str = ""
    error: str = ""
    cost_cents: int = 0
    tokens_used: int = 0
    payload: Dict[str, Any] = field(default_factory=dict)
    completed_at: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Sprint 10 — additive
# ---------------------------------------------------------------------------


@dataclass
class RunAdmittedEvent(HFAEvent):
    event_type: str = "RunAdmitted"
    run_id: str = ""
    tenant_id: str = ""
    agent_type: str = ""
    priority: int = 5
    preferred_region: str = ""
    preferred_placement: str = "LEAST_LOADED"
    payload: Dict[str, Any] = field(default_factory=dict)
    estimated_cost_cents: int = 0
    admitted_at: float = field(default_factory=time.time)


@dataclass
class RunScheduledEvent(HFAEvent):
    event_type: str = "RunScheduled"
    run_id: str = ""
    tenant_id: str = ""
    agent_type: str = ""
    worker_group: str = ""
    shard: int = 0
    region: str = ""
    policy: str = "LEAST_LOADED"
    scheduled_at: float = field(default_factory=time.time)


@dataclass
class RunRescheduledEvent(HFAEvent):
    event_type: str = "RunRescheduled"
    run_id: str = ""
    tenant_id: str = ""
    previous_worker: str = ""
    reschedule_count: int = 0
    reason: str = ""
    rescheduled_at: float = field(default_factory=time.time)


@dataclass
class RunDeadLetteredEvent(HFAEvent):
    event_type: str = "RunDeadLettered"
    run_id: str = ""
    tenant_id: str = ""
    reason: str = ""
    reschedule_count: int = 0
    original_error: str = ""
    cost_cents: int = 0
    dead_lettered_at: float = field(default_factory=time.time)


@dataclass
class WorkerHeartbeatEvent(HFAEvent):
    event_type: str = "WorkerHeartbeat"
    worker_id: str = ""
    worker_group: str = ""
    region: str = ""
    shards: List[int] = field(default_factory=list)
    capacity: int = 0
    inflight: int = 0
    version: str = ""
    capabilities: List[str] = field(default_factory=list)


@dataclass
class WorkerDrainingEvent(HFAEvent):
    event_type: str = "WorkerDraining"
    worker_id: str = ""
    worker_group: str = ""
    region: str = ""
    drain_deadline_utc: str = ""
    reason: str = ""


@dataclass
class GraphPatchedEvent(HFAEvent):
    event_type: str = "GraphPatched"
    run_id: str = ""
    tenant_id: str = ""
    op: str = ""
    node_id: str = ""
    seq: int = 0
    data: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Sprint 12 — additive only (no existing fields changed)
# ---------------------------------------------------------------------------


@dataclass
class RunStartedEvent(HFAEvent):
    """
    Emitted when a worker successfully claims a run and begins execution.
    Additive — optional for consumers that do not need it.
    """

    event_type: str = "RunStarted"
    run_id: str = ""
    tenant_id: str = ""
    worker_id: str = ""
    worker_group: str = ""
    shard: int = 0
    started_at: float = field(default_factory=time.time)

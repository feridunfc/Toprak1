"""
hfa/dag/schema.py
-----------------
Authoritative Redis key builders + canonical meta-field names for IRONCLAD.

Sprint 2 addition: TaskMetaField — a single source of truth for every hash
field written into task_meta_key (hfa:dag:task:<id>:meta).
Python and Lua must never use raw strings for these field names.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ── Prefix constants ──────────────────────────────────────────────────────────

_TASK_PREFIX   = "hfa:dag:task:"
_TENANT_PREFIX = "hfa:dag:tenant:"
_WORKER_PREFIX = "hfa:dag:worker:"
_RUN_PREFIX    = "hfa:dag:run:"


# ── Sprint 2: canonical task meta field names ─────────────────────────────────
# Any code that writes to task_meta_key MUST use these constants.
# Lua scripts embed the string literals directly (no import), but the comment
# in each script references this class so drift is visible at review time.

class TaskMetaField:
    """Canonical hash field names for hfa:dag:task:<id>:meta."""

    # Ownership / fencing tuple (Sprint 2)
    WORKER_INSTANCE_ID   = "worker_instance_id"   # set at claim
    SCHEDULER_EPOCH      = "scheduler_epoch"       # set at claim from reservation
    CLAIM_EPOCH          = "claim_epoch"           # incremented atomically at claim

    # Timing
    CLAIMED_AT_MS        = "claimed_at_ms"
    LAST_HEARTBEAT_AT_MS = "last_heartbeat_at_ms"
    COMPLETED_AT_MS      = "completed_at_ms"
    DEAD_LETTERED_AT_MS  = "dead_lettered_at_ms"
    LAST_REQUEUE_AT_MS   = "last_requeue_at_ms"

    # State bookkeeping
    TERMINAL_STATE       = "terminal_state"
    COMPLETION_REASON    = "completion_reason"
    REQUEUE_COUNT        = "requeue_count"
    LAST_REQUEUE_REASON  = "last_requeue_reason"

    # Identity
    TASK_ID              = "task_id"
    RUN_ID               = "run_id"
    TENANT_ID            = "tenant_id"
    AGENT_TYPE           = "agent_type"

    # Dispatch metadata
    PRIORITY             = "priority"
    ADMITTED_AT          = "admitted_at"
    SCHEDULED_AT         = "scheduled_at"
    PAYLOAD_JSON         = "payload_json"
    TRACE_PARENT         = "trace_parent"
    TRACE_STATE          = "trace_state"
    REGION               = "region"
    POLICY               = "policy"
    WORKER_GROUP         = "worker_group"
    SHARD                = "shard"
    DISPATCH_POLICY      = "dispatch_policy"
    DISPATCH_REGION      = "dispatch_region"


# ── DagRedisKey builders ──────────────────────────────────────────────────────

class DagRedisKey:
    # ── Task-level ────────────────────────────────────────────────────────────

    @staticmethod
    def task_state(task_id: str) -> str:
        return f"{_TASK_PREFIX}{task_id}:state"

    @staticmethod
    def task_meta(task_id: str) -> str:
        return f"{_TASK_PREFIX}{task_id}:meta"

    @staticmethod
    def task_output(task_id: str) -> str:
        return f"{_TASK_PREFIX}{task_id}:output"

    @staticmethod
    def task_lineage(task_id: str) -> str:
        return f"{_TASK_PREFIX}{task_id}:lineage"

    @staticmethod
    def task_children(task_id: str) -> str:
        return f"{_TASK_PREFIX}{task_id}:children"

    @staticmethod
    def task_remaining_deps(task_id: str) -> str:
        return f"{_TASK_PREFIX}{task_id}:remaining_deps"

    @staticmethod
    def task_ready_emitted(task_id: str) -> str:
        return f"{_TASK_PREFIX}{task_id}:ready_emitted"

    # ── Run-level ─────────────────────────────────────────────────────────────

    @staticmethod
    def run_state(run_id: str) -> str:
        return f"{_RUN_PREFIX}{run_id}:state"

    @staticmethod
    def run_tasks(run_id: str) -> str:
        return f"{_RUN_PREFIX}{run_id}:tasks"

    @staticmethod
    def run_graph(run_id: str) -> str:
        return f"{_RUN_PREFIX}{run_id}:graph"

    # ── Tenant-level ──────────────────────────────────────────────────────────

    @staticmethod
    def tenant_ready_zset(tenant_id: str) -> str:
        return f"{_TENANT_PREFIX}{tenant_id}:ready"

    @staticmethod
    def task_ready_queue(tenant_id: str) -> str:
        return DagRedisKey.tenant_ready_zset(tenant_id)

    @staticmethod
    def tenant_ready_queue(tenant_id: str) -> str:
        return DagRedisKey.tenant_ready_zset(tenant_id)

    @staticmethod
    def task_scheduled_zset(tenant_id: str) -> str:
        return f"{_TENANT_PREFIX}{tenant_id}:scheduled"

    @staticmethod
    def task_running_zset(tenant_id: str) -> str:
        return f"{_TENANT_PREFIX}{tenant_id}:running"

    @staticmethod
    def tenant_inflight(tenant_id: str) -> str:
        return f"{_TENANT_PREFIX}{tenant_id}:inflight"

    @staticmethod
    def tenant_active_set() -> str:
        return "hfa:dag:tenants:active"

    @staticmethod
    def completion_stream(tenant_id: str) -> str:
        return f"{_TENANT_PREFIX}{tenant_id}:completion_stream"

    # ── Worker-level ──────────────────────────────────────────────────────────

    @staticmethod
    def worker_reservation(worker_id: str) -> str:
        return f"{_WORKER_PREFIX}{worker_id}:reservation"

    @staticmethod
    def worker_reservation_pattern() -> str:
        return f"{_WORKER_PREFIX}*:reservation"

    @staticmethod
    def worker_load(worker_id: str) -> str:
        return f"{_WORKER_PREFIX}{worker_id}:load"

    @staticmethod
    def worker_capacity(worker_id: str) -> str:
        return f"{_WORKER_PREFIX}{worker_id}:capacity"

    @staticmethod
    def worker_heartbeat(worker_id: str) -> str:
        return f"{_WORKER_PREFIX}{worker_id}:heartbeat"

    # ── Scheduler-level ───────────────────────────────────────────────────────

    @staticmethod
    def scheduler_metric_counter(metric_name: str) -> str:
        return f"hfa:dag:scheduler:metrics:{metric_name}"

    @staticmethod
    def scheduler_decision_stream() -> str:
        return "hfa:dag:scheduler:decisions"

    # ── Prefix helpers ────────────────────────────────────────────────────────

    @staticmethod
    def task_state_prefix() -> str:
        return _TASK_PREFIX

    @staticmethod
    def task_meta_prefix() -> str:
        return _TASK_PREFIX

    @staticmethod
    def task_remaining_deps_prefix() -> str:
        return _TASK_PREFIX

    @staticmethod
    def task_ready_emitted_prefix() -> str:
        return _TASK_PREFIX


# ── Domain model dataclasses ──────────────────────────────────────────────────

@dataclass(frozen=True)
class DagTaskSeed:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str = ""
    worker_group: str = ""
    priority: int = 0
    admitted_at: float = 0.0
    dependency_count: int = 0
    child_task_ids: tuple[str, ...] = ()
    input_payload: dict[str, Any] = field(default_factory=dict)
    required_capabilities: list[str] = field(default_factory=list)
    region: str = ""
    policy: str = ""
    payload_json: str = ""
    trace_parent: str = ""
    trace_state: str = ""
    parent_task_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DagTaskDispatchInput:
    task_id: str
    run_id: str
    tenant_id: str
    worker_id: str = ""
    worker_group: str = ""
    agent_type: str = ""
    shard: int = 0
    priority: int = 0
    admitted_at: float = 0.0
    scheduled_at: float = 0.0
    scheduled_zset: str = ""
    running_zset: str = ""
    control_stream: str = ""
    shard_stream: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    required_capabilities: list[str] = field(default_factory=list)
    region: str = ""
    policy: str = ""
    payload_json: str = ""
    trace_parent: str = ""
    trace_state: str = ""

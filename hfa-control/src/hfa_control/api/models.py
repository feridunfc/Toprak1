"""Control-plane API response models.

Sprint 82.3 extends the run-state read model additively. RUN state remains the
run-query authority; TASK rows are read-only enrichment and explicit conflict
metadata only.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    from pydantic import BaseModel, Field

    class _CompatModel(BaseModel):
        def model_dump(self) -> dict:
            parent = getattr(super(), "model_dump", None)
            if callable(parent):
                return parent()
            return self.dict()

    class WorkerResponse(_CompatModel):
        worker_id: str
        worker_group: str
        region: str
        shards: List[int]
        capacity: int
        inflight: int
        load_factor: float
        status: str
        last_seen: float
        version: str
        capabilities: List[str]

    class ShardResponse(_CompatModel):
        shard: int
        worker_group: str
        stream_len: int
        owner_alive: bool

    class PlacementResponse(_CompatModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        reschedule_count: int
        admitted_at: float

    class DLQEntryResponse(_CompatModel):
        run_id: str
        tenant_id: str
        reason: str
        delivery_count: int
        dead_lettered_at: float
        original_error: str
        cost_cents: int

    class HealthResponse(_CompatModel):
        is_leader: bool
        instance_id: str
        region: str
        registry_size: int
        healthy_workers: int
        scheduler_lag: int
        dlq_depth: int

    class LiveResponse(_CompatModel):
        status: str
        service: str
        instance_id: str

    class ReadyCheckDetail(_CompatModel):
        ok: bool
        message: str = ""

    class ReadyResponse(_CompatModel):
        status: str
        instance_id: str
        is_leader: bool
        checks: Dict[str, ReadyCheckDetail]

        def model_dump(self) -> dict:
            data = super().model_dump()
            data["checks"] = {
                key: value.model_dump() if hasattr(value, "model_dump") else dict(value)
                for key, value in self.checks.items()
            }
            return data

    class WorkerSummary(_CompatModel):
        worker_id: str
        worker_group: str
        region: str
        status: str
        is_draining: bool
        inflight: int
        capacity: int
        shards: List[int]
        version: str
        last_seen: float

    class WorkerListResponse(_CompatModel):
        count: int
        workers: List[WorkerSummary]

    class RunStateResponse(_CompatModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        reschedule_count: int
        admitted_at: float
        task_count: int = 0
        task_truth: List[Dict[str, Any]] = Field(default_factory=list)
        truth_status: str = "consistent"
        truth_conflict: bool = False
        truth_conflicts: List[Dict[str, Any]] = Field(default_factory=list)

    class RunClaimResponse(_CompatModel):
        run_id: str
        claimed: bool
        owner: Optional[str]
        ttl_seconds: int

    class RunResultResponse(_CompatModel):
        run_id: str
        tenant_id: str
        status: str
        cost_cents: int
        tokens_used: int
        error: Optional[str]
        payload: Dict[str, Any]
        completed_at: float

    class RunSubmissionRequest(_CompatModel):
        payload: Dict[str, Any] = Field(default_factory=dict)
        agent_type: str = "default"
        priority: int = 5
        estimated_cost_cents: int = 0
        preferred_region: str = ""
        preferred_placement: str = "LEAST_LOADED"
        required_capabilities: List[str] = Field(default_factory=list)
        trace_parent: str = ""
        trace_state: str = ""

    class RunSubmissionResponse(_CompatModel):
        status: str
        tenant_id: str
        run_id: str
        task_id: str
        run_admitted: bool
        task_admitted: bool
        task_ready: bool
        task_admit_status: str
        failure_code: Optional[str]
        failure_type: Optional[str]
        dispatch_possible: bool
        automatic_retry: bool
        automatic_rollback: bool
        automatic_repair: bool

    class RunStatusResultResponse(_CompatModel):
        schema_version: int
        run_id: str
        status: str
        terminal: bool
        outcome: Optional[str]
        result: Optional[Dict[str, Any]]
        error: Optional[Dict[str, Any]]
        submitted_at: Optional[float]
        started_at: Optional[float]
        finished_at: Optional[float]
        updated_at: Optional[float]
        freshness: str
        completeness: str
        completeness_reason: Optional[str]
        internal_state: Optional[str]
        task_counts: Dict[str, int] = Field(default_factory=dict)
        state_ttl_seconds: int
        meta_ttl_seconds: int
        result_ttl_seconds: int
        issues: List[str] = Field(default_factory=list)
        task_output_status: str = "RUN_UNKNOWN"
        task_id: Optional[str] = None
        task_state: Optional[str] = None
        task_output: Any = None
        task_output_issues: List[str] = Field(
            default_factory=list
        )

    class RunningRunSummary(_CompatModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        started_at: float
        claim_owner: Optional[str]

    class RunningRunsResponse(_CompatModel):
        count: int
        runs: List[RunningRunSummary]

    class StaleRunSummary(_CompatModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        reschedule_count: int
        running_since: float
        stale_for_seconds: float

    class StaleRunsResponse(_CompatModel):
        count: int
        runs: List[StaleRunSummary]

    class RecoverySummaryResponse(_CompatModel):
        stale_count: int
        dlq_count: int
        schedulable_workers: int
        draining_workers: int

    class DLQListResponse(_CompatModel):
        count: int
        entries: List[DLQEntryResponse]

except ImportError:
    from dataclasses import asdict, dataclass, field

    class _DataclassModel:
        def model_dump(self) -> dict:
            return asdict(self)

    @dataclass
    class WorkerResponse(_DataclassModel):
        worker_id: str
        worker_group: str
        region: str
        shards: List[int]
        capacity: int
        inflight: int
        load_factor: float
        status: str
        last_seen: float
        version: str
        capabilities: List[str]

    @dataclass
    class ShardResponse(_DataclassModel):
        shard: int
        worker_group: str
        stream_len: int
        owner_alive: bool

    @dataclass
    class PlacementResponse(_DataclassModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        reschedule_count: int
        admitted_at: float

    @dataclass
    class DLQEntryResponse(_DataclassModel):
        run_id: str
        tenant_id: str
        reason: str
        delivery_count: int
        dead_lettered_at: float
        original_error: str
        cost_cents: int

    @dataclass
    class HealthResponse(_DataclassModel):
        is_leader: bool
        instance_id: str
        region: str
        registry_size: int
        healthy_workers: int
        scheduler_lag: int
        dlq_depth: int

    @dataclass
    class LiveResponse(_DataclassModel):
        status: str
        service: str
        instance_id: str

    @dataclass
    class ReadyCheckDetail(_DataclassModel):
        ok: bool
        message: str = ""

    @dataclass
    class ReadyResponse(_DataclassModel):
        status: str
        instance_id: str
        is_leader: bool
        checks: Dict[str, ReadyCheckDetail] = field(default_factory=dict)

    @dataclass
    class WorkerSummary(_DataclassModel):
        worker_id: str
        worker_group: str
        region: str
        status: str
        is_draining: bool
        inflight: int
        capacity: int
        shards: List[int]
        version: str
        last_seen: float

    @dataclass
    class WorkerListResponse(_DataclassModel):
        count: int
        workers: List[WorkerSummary] = field(default_factory=list)

    @dataclass
    class RunStateResponse(_DataclassModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        reschedule_count: int
        admitted_at: float
        task_count: int = 0
        task_truth: List[Dict[str, Any]] = field(default_factory=list)
        truth_status: str = "consistent"
        truth_conflict: bool = False
        truth_conflicts: List[Dict[str, Any]] = field(default_factory=list)

    @dataclass
    class RunClaimResponse(_DataclassModel):
        run_id: str
        claimed: bool
        owner: Optional[str]
        ttl_seconds: int

    @dataclass
    class RunResultResponse(_DataclassModel):
        run_id: str
        tenant_id: str
        status: str
        cost_cents: int
        tokens_used: int
        error: Optional[str]
        payload: Dict[str, Any]
        completed_at: float

    @dataclass
    class RunSubmissionRequest(_DataclassModel):
        payload: Dict[str, Any] = field(default_factory=dict)
        agent_type: str = "default"
        priority: int = 5
        estimated_cost_cents: int = 0
        preferred_region: str = ""
        preferred_placement: str = "LEAST_LOADED"
        required_capabilities: List[str] = field(default_factory=list)
        trace_parent: str = ""
        trace_state: str = ""

    @dataclass
    class RunSubmissionResponse(_DataclassModel):
        status: str
        tenant_id: str
        run_id: str
        task_id: str
        run_admitted: bool
        task_admitted: bool
        task_ready: bool
        task_admit_status: str
        failure_code: Optional[str]
        failure_type: Optional[str]
        dispatch_possible: bool
        automatic_retry: bool
        automatic_rollback: bool
        automatic_repair: bool

    @dataclass
    class RunStatusResultResponse(_DataclassModel):
        schema_version: int
        run_id: str
        status: str
        terminal: bool
        outcome: Optional[str]
        result: Optional[Dict[str, Any]]
        error: Optional[Dict[str, Any]]
        submitted_at: Optional[float]
        started_at: Optional[float]
        finished_at: Optional[float]
        updated_at: Optional[float]
        freshness: str
        completeness: str
        completeness_reason: Optional[str]
        internal_state: Optional[str]
        state_ttl_seconds: int
        meta_ttl_seconds: int
        result_ttl_seconds: int
        task_counts: Dict[str, int] = field(default_factory=dict)
        issues: List[str] = field(default_factory=list)
        task_output_status: str = "RUN_UNKNOWN"
        task_id: Optional[str] = None
        task_state: Optional[str] = None
        task_output: Any = None
        task_output_issues: List[str] = field(
            default_factory=list
        )

    @dataclass
    class RunningRunSummary(_DataclassModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        started_at: float
        claim_owner: Optional[str]

    @dataclass
    class RunningRunsResponse(_DataclassModel):
        count: int
        runs: List[RunningRunSummary] = field(default_factory=list)

    @dataclass
    class StaleRunSummary(_DataclassModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        reschedule_count: int
        running_since: float
        stale_for_seconds: float

    @dataclass
    class StaleRunsResponse(_DataclassModel):
        count: int
        runs: List[StaleRunSummary] = field(default_factory=list)

    @dataclass
    class RecoverySummaryResponse(_DataclassModel):
        stale_count: int
        dlq_count: int
        schedulable_workers: int
        draining_workers: int

    @dataclass
    class DLQListResponse(_DataclassModel):
        count: int
        entries: List[DLQEntryResponse] = field(default_factory=list)

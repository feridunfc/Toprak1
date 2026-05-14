"""
hfa-control/src/hfa_control/api/models.py
IRONCLAD Sprint 10 / Sprint 13 — Control Plane API response models

Uses Pydantic when available, falls back to dataclasses for environments
where Pydantic is not installed (e.g. test containers).
Sprint 10 models preserved unchanged.
Sprint 13 adds operational response models (additive only).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    from pydantic import BaseModel as _Base

    class _ModelMixin(_Base):
        def model_dump(self) -> dict:  # pydantic v2 already has this
            try:
                return super().model_dump()
            except AttributeError:
                return self.dict()  # pydantic v1 fallback

except ImportError:
    from dataclasses import dataclass, asdict

    class _ModelMixin:  # type: ignore[no-redef]
        """dataclass-based shim when Pydantic is absent."""

        def model_dump(self) -> dict:
            return asdict(self)  # type: ignore[arg-type]

    def dataclass_model(cls):
        """Apply @dataclass and return."""
        return dataclass(cls)

    _Base = _ModelMixin  # type: ignore[assignment]


def _model(cls):
    """Decorator: apply dataclass if Pydantic absent, else return as-is."""
    try:
        # from pydantic import BaseModel

        return cls
    except ImportError:
        from dataclasses import dataclass

        # Add default_factory for mutable defaults
        return dataclass(cls)


# ---------------------------------------------------------------------------
# Sprint 10 — preserved unchanged (field for field)
# ---------------------------------------------------------------------------

try:
    from pydantic import BaseModel

    class WorkerResponse(BaseModel):
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

    class ShardResponse(BaseModel):
        shard: int
        worker_group: str
        stream_len: int
        owner_alive: bool

    class PlacementResponse(BaseModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        reschedule_count: int
        admitted_at: float

    class DLQEntryResponse(BaseModel):
        run_id: str
        tenant_id: str
        reason: str
        delivery_count: int
        dead_lettered_at: float
        original_error: str
        cost_cents: int

    class HealthResponse(BaseModel):
        is_leader: bool
        instance_id: str
        region: str
        registry_size: int
        healthy_workers: int
        scheduler_lag: int
        dlq_depth: int

    # Sprint 13 — additive
    class LiveResponse(BaseModel):
        status: str
        service: str
        instance_id: str

    class ReadyCheckDetail(BaseModel):
        ok: bool
        message: str = ""

    class ReadyResponse(BaseModel):
        status: str
        instance_id: str
        is_leader: bool
        checks: Dict[str, ReadyCheckDetail]

        def model_dump(self) -> dict:
            d = super().model_dump()
            d["checks"] = {k: v.model_dump() for k, v in self.checks.items()}
            return d

    class WorkerSummary(BaseModel):
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

    class WorkerListResponse(BaseModel):
        count: int
        workers: List[WorkerSummary]

    class RunStateResponse(BaseModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        reschedule_count: int
        admitted_at: float

    class RunClaimResponse(BaseModel):
        run_id: str
        claimed: bool
        owner: Optional[str]
        ttl_seconds: int

    class RunResultResponse(BaseModel):
        run_id: str
        tenant_id: str
        status: str
        cost_cents: int
        tokens_used: int
        error: Optional[str]
        payload: Dict[str, Any]
        completed_at: float

    class RunningRunSummary(BaseModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        started_at: float
        claim_owner: Optional[str]

    class RunningRunsResponse(BaseModel):
        count: int
        runs: List[RunningRunSummary]

    class StaleRunSummary(BaseModel):
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        reschedule_count: int
        running_since: float
        stale_for_seconds: float

    class StaleRunsResponse(BaseModel):
        count: int
        runs: List[StaleRunSummary]

    class RecoverySummaryResponse(BaseModel):
        stale_count: int
        dlq_count: int
        schedulable_workers: int
        draining_workers: int

    class DLQListResponse(BaseModel):
        count: int
        entries: List[DLQEntryResponse]

except ImportError:
    # ---------------------------------------------------------------------------
    # Pydantic not available — dataclass fallback
    # ---------------------------------------------------------------------------
    from dataclasses import dataclass, field

    @dataclass
    class WorkerResponse:
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

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class ShardResponse:
        shard: int
        worker_group: str
        stream_len: int
        owner_alive: bool

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class PlacementResponse:
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        reschedule_count: int
        admitted_at: float

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class DLQEntryResponse:
        run_id: str
        tenant_id: str
        reason: str
        delivery_count: int
        dead_lettered_at: float
        original_error: str
        cost_cents: int

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class HealthResponse:
        is_leader: bool
        instance_id: str
        region: str
        registry_size: int
        healthy_workers: int
        scheduler_lag: int
        dlq_depth: int

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class LiveResponse:
        status: str
        service: str
        instance_id: str

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class ReadyCheckDetail:
        ok: bool
        message: str = ""

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class ReadyResponse:
        status: str
        instance_id: str
        is_leader: bool
        checks: Dict[str, ReadyCheckDetail] = field(default_factory=dict)

        def model_dump(self):
            return {
                "status": self.status,
                "instance_id": self.instance_id,
                "is_leader": self.is_leader,
                "checks": {k: v.model_dump() for k, v in self.checks.items()},
            }

    @dataclass
    class WorkerSummary:
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

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class WorkerListResponse:
        count: int
        workers: List[WorkerSummary]

        def model_dump(self):
            return {
                "count": self.count,
                "workers": [w.model_dump() for w in self.workers],
            }

    @dataclass
    class RunStateResponse:
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        reschedule_count: int
        admitted_at: float

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class RunClaimResponse:
        run_id: str
        claimed: bool
        owner: Optional[str]
        ttl_seconds: int

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class RunResultResponse:
        run_id: str
        tenant_id: str
        status: str
        cost_cents: int
        tokens_used: int
        error: Optional[str]
        payload: Dict[str, Any]
        completed_at: float

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class RunningRunSummary:
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        shard: int
        started_at: float
        claim_owner: Optional[str]

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class RunningRunsResponse:
        count: int
        runs: List[RunningRunSummary]

        def model_dump(self):
            return {"count": self.count, "runs": [r.model_dump() for r in self.runs]}

    @dataclass
    class StaleRunSummary:
        run_id: str
        tenant_id: str
        state: str
        worker_group: str
        reschedule_count: int
        running_since: float
        stale_for_seconds: float

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class StaleRunsResponse:
        count: int
        runs: List[StaleRunSummary]

        def model_dump(self):
            return {"count": self.count, "runs": [r.model_dump() for r in self.runs]}

    @dataclass
    class RecoverySummaryResponse:
        stale_count: int
        dlq_count: int
        schedulable_workers: int
        draining_workers: int

        def model_dump(self):
            from dataclasses import asdict

            return asdict(self)

    @dataclass
    class DLQListResponse:
        count: int
        entries: List[DLQEntryResponse]

        def model_dump(self):
            return {
                "count": self.count,
                "entries": [e.model_dump() for e in self.entries],
            }

"""
hfa-control/src/hfa_control/models.py
IRONCLAD Sprint 10 — Shared data models
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import List

from hfa.config.keys import RedisKey


class WorkerStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"  # 1–2 missed heartbeats
    DRAINING = "draining"  # graceful shutdown
    DEAD = "dead"  # TTL expired — no new placements


class RunState(str, Enum):
    ADMITTED = "admitted"
    QUEUED = "queued"        # Sprint 18: in per-tenant fair queue, awaiting dispatch
    SCHEDULED = "scheduled"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    RESCHEDULED = "rescheduled"
    DEAD_LETTERED = "dead_lettered"


@dataclass
class WorkerProfile:
    worker_id: str
    worker_group: str
    region: str
    capacity: int
    inflight: int
    status: WorkerStatus = WorkerStatus.HEALTHY
    last_seen: float = 0.0
    shards: List[int] = field(default_factory=list)
    version: str = ""
    capabilities: List[str] = field(default_factory=list)

    @property
    def load_factor(self) -> float:
        if self.capacity <= 0:
            return 1.0
        return self.inflight / self.capacity

    @property
    def available_slots(self) -> int:
        return max(0, self.capacity - self.inflight)

    @property
    def is_draining(self) -> bool:
        return self.status == WorkerStatus.DRAINING

    @classmethod
    def from_redis_hash(cls, raw: dict) -> "WorkerProfile":
        def _s(k: bytes | str) -> str:
            v = raw.get(k) or raw.get(k.encode() if isinstance(k, str) else k.decode())
            return (v.decode() if isinstance(v, bytes) else v) or ""

        def _i(k: str) -> int:
            try:
                return int(_s(k))
            except (ValueError, TypeError):
                return 0

        def _f(k: str) -> float:
            try:
                return float(_s(k))
            except (ValueError, TypeError):
                return 0.0

        def _list(k: str):
            s = _s(k)
            if not s:
                return []
            try:
                return json.loads(s)
            except json.JSONDecodeError:
                return []

        status_raw = _s("status")
        try:
            status = WorkerStatus(status_raw)
        except ValueError:
            status = WorkerStatus.HEALTHY

        return cls(
            worker_id=_s("worker_id"),
            worker_group=_s("worker_group"),
            region=_s("region"),
            capacity=_i("capacity"),
            inflight=_i("inflight"),
            status=status,
            last_seen=_f("last_seen"),
            shards=_list("shards"),
            version=_s("version"),
            capabilities=_list("capabilities"),
        )


@dataclass
class ControlPlaneConfig:
    region: str = "us-east-1"
    instance_id: str = ""
    stream_shards: int = 32
    worker_heartbeat_ttl: float = 30.0  # declare DEAD after this many seconds
    stale_run_timeout: float = 600.0  # seconds before running→rescheduled
    recovery_sweep_interval: float = 30.0
    registry_ttl: int = 60  # Redis TTL for worker hash key
    max_reschedule_attempts: int = 3
    dlq_stream: str = field(default_factory=lambda: RedisKey.stream_dlq())
    heartbeat_stream: str = field(default_factory=lambda: RedisKey.stream_heartbeat())
    results_stream: str = field(default_factory=lambda: RedisKey.stream_results())
    control_stream: str = field(default_factory=lambda: RedisKey.stream_control())
    leader_key: str = field(default_factory=lambda: RedisKey.cp_leader())
    leader_ttl: int = 15
    leader_renew_interval: float = 5.0
    running_zset: str = field(default_factory=lambda: RedisKey.cp_running())
    # XAUTOCLAIM settings
    autoclaim_idle_ms: int = 30_000  # reclaim after 30s idle in PEL
    autoclaim_count: int = 10
    # Sprint 16: enable tenant-aware fair scheduling queue
    fair_scheduling: bool = False
    # Sprint 19 scheduler loop
    scheduler_loop_max_dispatches: int = 32
    scheduler_loop_max_duration_ms: int = 100
    scheduler_loop_max_failures: int = 8
    scheduler_loop_idle_sleep_ms: int = 250
    scheduler_loop_error_sleep_ms: int = 1000

    # Sprint 23: CAS-only enforcement (production should set True)
    strict_cas_mode: bool = False

    # Sprint 19 dispatch pacing
    dispatch_tokens_capacity: int = 128
    dispatch_tokens_refill_per_sec: float = 32.0
    dispatch_degraded_refill_per_sec: float = 4.0
    dispatch_aimd_enabled: bool = True

    # Sprint 19 worker scoring
    worker_score_latency_weight: float = 1.0
    worker_score_failure_weight: float = 2.0
    worker_score_saturation_weight: float = 1.5
    worker_score_affinity_bonus: float = 0.5
    worker_score_capability_bonus: float = 0.75

    # Sprint 21: starvation prevention (logarithmic capped aging)
    scheduler_age_weight: float = 0.0         # 0 = disabled; e.g. 1.0 = mild aging
    scheduler_age_max_boost: int = 5           # max priority-bands a job can jump via aging

    # Sprint 21: backpressure (all 0 = disabled, opt-in)
    backpressure_hard_inflight_cap: int = 0   # absolute hard stop
    backpressure_soft_inflight_cap: int = 0   # soft throttle entry threshold
    backpressure_hysteresis_ratio: float = 0.8 # soft_low = soft_high * ratio
    backpressure_max_queue_ratio: float = 10.0 # queue_depth > ratio * capacity → soft
    backpressure_worker_saturation: float = 0.95  # weighted load threshold for hard stop
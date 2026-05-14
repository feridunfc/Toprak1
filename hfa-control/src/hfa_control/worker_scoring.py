from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

from hfa_control.models import WorkerStatus


@dataclass(frozen=True)
class WorkerScore:
    worker_group: str
    worker_id: str
    total: float
    capacity_term: float
    latency_term: float
    failure_term: float
    saturation_term: float
    affinity_term: float
    capability_term: float


@dataclass(frozen=True)
class WorkerSelection:
    chosen: Optional[str]
    scores: Tuple[WorkerScore, ...]
    reason: str


class WorkerScorer:
    def __init__(self, config) -> None:
        self._config = config
        self._dispatch_failures: dict[str, int] = {}
        self._successes: dict[str, int] = {}

    def filter_candidates(
        self,
        workers: Iterable,
        *,
        agent_type: str,
        preferred_region: Optional[str],
        policy: str,
    ) -> list:
        candidates = []
        for w in workers:
            if getattr(w, "drained", False):
                continue
            if not getattr(w, "schedulable", True):
                continue
            if getattr(w, "available_slots", 0) <= 0:
                continue

            last_seen = float(getattr(w, "last_seen", 0.0) or 0.0)
            if last_seen and (time.time() - last_seen) > 60.0:
                continue

            status_enum = getattr(w, "status_enum", None)
            if status_enum and status_enum not in (WorkerStatus.HEALTHY, WorkerStatus.DEGRADED):
                continue

            caps = set(getattr(w, "capabilities", ()) or ())
            if policy == "CAPABILITY_MATCH" and agent_type and caps and agent_type not in caps:
                continue

            candidates.append(w)
        return candidates

    def select_worker_group(
        self,
        workers: Iterable,
        *,
        agent_type: str,
        preferred_region: Optional[str],
        policy: str,
    ) -> WorkerSelection:
        candidates = self.filter_candidates(
            workers,
            agent_type=agent_type,
            preferred_region=preferred_region,
            policy=policy,
        )
        if not candidates:
            return WorkerSelection(None, (), "worker_pool_empty")

        scores = tuple(
            self._score_worker(
                w,
                agent_type=agent_type,
                preferred_region=preferred_region,
                policy=policy,
            )
            for w in candidates
        )
        best = max(scores, key=lambda s: (s.total, s.worker_group))
        return WorkerSelection(best.worker_group, scores, "selected")

    def observe_dispatch_success(self, worker_group: str) -> None:
        self._successes[worker_group] = self._successes.get(worker_group, 0) + 1
        self._dispatch_failures[worker_group] = max(0, self._dispatch_failures.get(worker_group, 0) - 1)

    def observe_dispatch_failure(self, worker_group: str, reason: str) -> None:
        self._dispatch_failures[worker_group] = self._dispatch_failures.get(worker_group, 0) + 1

    def observe_run_result(
        self,
        worker_group: str,
        *,
        latency_ms: Optional[float],
        success: bool,
    ) -> None:
        if success:
            self._successes[worker_group] = self._successes.get(worker_group, 0) + 1
        else:
            self._dispatch_failures[worker_group] = self._dispatch_failures.get(worker_group, 0) + 1

    def _score_worker(
        self,
        worker,
        *,
        agent_type: str,
        preferred_region: Optional[str],
        policy: str,
    ) -> WorkerScore:
        capacity = max(float(getattr(worker, "capacity", 1) or 1), 1.0)
        inflight = float(getattr(worker, "inflight", 0) or 0)
        available_slots = float(getattr(worker, "available_slots", 0) or 0)
        load_factor = float(getattr(worker, "load_factor", 0.0) or 0.0)
        latency_ms = float(getattr(worker, "latency_ewma_ms", 0.0) or 0.0)

        fail_penalty_raw = float(getattr(worker, "failure_penalty", 0.0) or 0.0)
        fail_penalty_raw += float(self._dispatch_failures.get(worker.worker_group, 0))

        capacity_term = available_slots / capacity
        saturation_term = -max(load_factor, inflight / capacity) * self._config.worker_score_saturation_weight
        latency_term = -(latency_ms / 1000.0) * self._config.worker_score_latency_weight
        failure_term = -fail_penalty_raw * self._config.worker_score_failure_weight

        affinity_term = 0.0
        region = getattr(worker, "region", None)
        if preferred_region and region == preferred_region:
            affinity_term = self._config.worker_score_affinity_bonus

        capability_term = 0.0
        caps = set(getattr(worker, "capabilities", ()) or ())
        if agent_type and agent_type in caps:
            capability_term = self._config.worker_score_capability_bonus

        if policy == "LEAST_LOADED":
            capacity_term *= 1.5
        elif policy == "REGION_AFFINITY":
            affinity_term *= 1.5
        elif policy == "CAPABILITY_MATCH":
            capability_term *= 1.5

        total = capacity_term + saturation_term + latency_term + failure_term + affinity_term + capability_term
        return WorkerScore(
            worker_group=worker.worker_group,
            worker_id=getattr(worker, "worker_id", worker.worker_group),
            total=total,
            capacity_term=capacity_term,
            latency_term=latency_term,
            failure_term=failure_term,
            saturation_term=saturation_term,
            affinity_term=affinity_term,
            capability_term=capability_term,
        )

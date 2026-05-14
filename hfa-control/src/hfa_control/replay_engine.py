
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ReplaySummary:
    run_id: str
    final_state: str = "unknown"
    admitted_at_ms: Optional[int] = None
    completed_at_ms: Optional[int] = None
    failed_at_ms: Optional[int] = None
    attempts: int = 0
    scheduled_count: int = 0
    claim_count: int = 0
    requeue_count: int = 0
    failure_count: int = 0
    current_worker_id: Optional[str] = None
    last_scheduled_worker: Optional[str] = None
    claimed_workers: list[str] = field(default_factory=list)
    completed_by: Optional[str] = None
    history_length: int = 0
    unknown_events: list[str] = field(default_factory=list)
    timeline: list[dict[str, Any]] = field(default_factory=list)


class ReplayEngine:
    @staticmethod
    def _timeline_entry(event: dict[str, Any]) -> dict[str, Any]:
        return {
            "event_type": event.get("event_type"),
            "timestamp_ms": int(event.get("timestamp_ms", 0) or 0),
            "worker_id": event.get("worker_id"),
            "details": event.get("details") or {},
        }

    @classmethod
    def rebuild_state(cls, run_id: str, events: list[dict[str, Any]]) -> ReplaySummary:
        summary = ReplaySummary(run_id=run_id)
        if not events:
            return summary

        for event in events:
            summary.history_length += 1
            summary.timeline.append(cls._timeline_entry(event))

            event_type = str(event.get("event_type", "") or "")
            ts = int(event.get("timestamp_ms", 0) or 0)
            worker = event.get("worker_id")

            if event_type == "TASK_ADMITTED":
                summary.final_state = "admitted"
                if summary.admitted_at_ms is None:
                    summary.admitted_at_ms = ts

            elif event_type == "TASK_SCHEDULED":
                summary.final_state = "scheduled"
                summary.scheduled_count += 1
                if worker:
                    summary.last_scheduled_worker = str(worker)

            elif event_type == "TASK_CLAIMED":
                summary.final_state = "running"
                summary.claim_count += 1
                if worker:
                    worker = str(worker)
                    summary.current_worker_id = worker
                    if worker not in summary.claimed_workers:
                        summary.claimed_workers.append(worker)
                        summary.attempts += 1

            elif event_type == "TASK_REQUEUED":
                summary.final_state = "ready"
                summary.requeue_count += 1
                summary.current_worker_id = None

            elif event_type == "TASK_COMPLETED":
                summary.final_state = "completed"
                summary.completed_at_ms = ts
                if worker:
                    summary.completed_by = str(worker)
                    summary.current_worker_id = str(worker)

            elif event_type == "TASK_FAILED":
                summary.final_state = "failed"
                summary.failed_at_ms = ts
                summary.failure_count += 1
                if worker:
                    summary.current_worker_id = str(worker)

            else:
                summary.unknown_events.append(event_type)

        return summary

    @staticmethod
    def detect_mismatch(replay_summary: ReplaySummary, actual_redis_state: str | None) -> bool:
        actual = (actual_redis_state or "unknown").strip().lower()
        expected = replay_summary.final_state.strip().lower()
        return expected == actual

    @classmethod
    async def replay_from_store(cls, event_store, run_id: str) -> ReplaySummary:
        history = await event_store.get_run_history(run_id)
        return cls.rebuild_state(run_id, history)

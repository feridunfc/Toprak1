"""
Replay-safe projection helpers for authoritative events.

Sprint 2 introduces a narrow CQRS pilot for task completion.  The helper below
keeps that slice explicit: TASK_COMPLETION_REQUESTED records intent but never
creates terminal projection state; only TASK_COMPLETED or TASK_FAILED may project
runtime terminal state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from hfa_core.events.event_types import (
    TASK_COMPLETION_REQUESTED,
    TASK_COMPLETED,
    TASK_FAILED,
    runtime_state_for_final_event,
)


@dataclass(frozen=True)
class AppliedEventResult:
    ok: bool
    event_type: str
    runtime_state: str | None = None
    terminal: bool = False
    reason: str = ""


@dataclass
class CompletionSliceProjection:
    task_id: str
    run_id: str = ""
    requested_count: int = 0
    final_event_count: int = 0
    duplicate_final_events: int = 0
    final_state: str | None = None
    final_event_type: str | None = None
    worker_id: str | None = None
    unknown_events: list[str] = field(default_factory=list)

    @property
    def terminal(self) -> bool:
        return self.final_state in {"done", "failed"}


def _details(event: dict[str, Any]) -> dict[str, Any]:
    raw = event.get("details") or {}
    return raw if isinstance(raw, dict) else {}


def apply_completion_event(
    event: dict[str, Any],
    projection: CompletionSliceProjection,
) -> AppliedEventResult:
    """
    Apply one event to a completion-slice projection.

    The invariant is deliberately strict: requested events are observable but
    non-terminal.  Terminal projection requires TASK_COMPLETED or TASK_FAILED.
    Duplicate final events are counted and ignored so first final event wins.
    """
    event_type = str(event.get("event_type") or "")
    details = _details(event)

    if event_type == TASK_COMPLETION_REQUESTED:
        projection.requested_count += 1
        if not projection.run_id:
            projection.run_id = str(event.get("run_id") or details.get("run_id") or "")
        return AppliedEventResult(
            ok=True,
            event_type=event_type,
            terminal=False,
            reason="completion_requested_is_not_terminal",
        )

    runtime_state = runtime_state_for_final_event(event_type)
    if runtime_state is not None:
        if projection.terminal:
            projection.duplicate_final_events += 1
            return AppliedEventResult(
                ok=True,
                event_type=event_type,
                runtime_state=projection.final_state,
                terminal=True,
                reason="duplicate_final_event_ignored",
            )
        projection.final_event_count += 1
        projection.final_state = runtime_state
        projection.final_event_type = event_type
        projection.worker_id = event.get("worker_id") or details.get("worker_id")
        if not projection.run_id:
            projection.run_id = str(event.get("run_id") or details.get("run_id") or "")
        return AppliedEventResult(
            ok=True,
            event_type=event_type,
            runtime_state=runtime_state,
            terminal=True,
            reason="terminal_projection_applied",
        )

    projection.unknown_events.append(event_type)
    return AppliedEventResult(
        ok=False,
        event_type=event_type,
        terminal=False,
        reason="unknown_event_for_completion_slice",
    )


def replay_completion_slice(
    *,
    task_id: str,
    events: Iterable[dict[str, Any]],
) -> CompletionSliceProjection:
    projection = CompletionSliceProjection(task_id=task_id)
    for event in events:
        apply_completion_event(event, projection)
    return projection


__all__ = [
    "AppliedEventResult",
    "CompletionSliceProjection",
    "apply_completion_event",
    "replay_completion_slice",
]

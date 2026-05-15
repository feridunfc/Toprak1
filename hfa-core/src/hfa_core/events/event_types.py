"""
Canonical event type constants for the Sprint 2 completion CQRS slice.

This module is intentionally small and additive.  It does not replace the
existing hfa_control.event_store.EventStore constants; it gives completion-slice
code and tests a stable import surface while the repository converges toward a
single event taxonomy.
"""

from __future__ import annotations

TASK_COMPLETION_REQUESTED = "TASK_COMPLETION_REQUESTED"
TASK_COMPLETED = "TASK_COMPLETED"
TASK_FAILED = "TASK_FAILED"

TASK_COMPLETION_FINAL_EVENTS = frozenset({TASK_COMPLETED, TASK_FAILED})
TASK_COMPLETION_EVENTS = frozenset({
    TASK_COMPLETION_REQUESTED,
    TASK_COMPLETED,
    TASK_FAILED,
})

EVENT_TO_RUNTIME_STATE = {
    TASK_COMPLETED: "done",
    TASK_FAILED: "failed",
}


def is_completion_event(event_type: str) -> bool:
    return event_type in TASK_COMPLETION_EVENTS


def is_final_completion_event(event_type: str) -> bool:
    return event_type in TASK_COMPLETION_FINAL_EVENTS


def runtime_state_for_final_event(event_type: str) -> str | None:
    return EVENT_TO_RUNTIME_STATE.get(event_type)

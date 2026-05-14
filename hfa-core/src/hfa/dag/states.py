"""
hfa/dag/states.py
-----------------
Single authoritative DAG task state vocabulary for IRONCLAD.

Sprint 2 addition: CLAIMABLE_STATES, COMPLETABLE_STATES, HEARTBEAT_ALLOWED_STATES —
centralized legality sets consumed by Lua scripts and Python fencing helpers.
"""
from __future__ import annotations

from enum import StrEnum
from typing import FrozenSet


class DagTaskState(StrEnum):
    PENDING   = "pending"
    READY     = "ready"
    SCHEDULED = "scheduled"
    RUNNING   = "running"

    # Terminal
    DONE               = "done"
    FAILED             = "failed"
    BLOCKED_BY_FAILURE = "blocked_by_failure"
    DEAD_LETTERED      = "dead_lettered"
    SKIPPED            = "skipped"


# ── State set helpers ─────────────────────────────────────────────────────────

TERMINAL_STATES: FrozenSet[DagTaskState] = frozenset({
    DagTaskState.DONE,
    DagTaskState.FAILED,
    DagTaskState.BLOCKED_BY_FAILURE,
    DagTaskState.DEAD_LETTERED,
    DagTaskState.SKIPPED,
})

ACTIVE_STATES: FrozenSet[DagTaskState] = frozenset({
    DagTaskState.PENDING,
    DagTaskState.READY,
    DagTaskState.SCHEDULED,
    DagTaskState.RUNNING,
})

READY_LIKE: FrozenSet[DagTaskState] = frozenset({DagTaskState.READY})

RUNNING_LIKE: FrozenSet[DagTaskState] = frozenset({
    DagTaskState.SCHEDULED,
    DagTaskState.RUNNING,
})

# ── Sprint 2: operation legality sets ─────────────────────────────────────────
# These are the single authoritative definitions for what state a task must be
# in for each operation to be legal.  Lua scripts enforce this atomically;
# Python uses these sets for pre-flight validation and error classification.

# claim_start is only legal when the task is scheduled (dispatcher-committed).
CLAIMABLE_STATES: FrozenSet[DagTaskState] = frozenset({DagTaskState.SCHEDULED})

# task_complete is only legal when the task is running (claimed by a worker).
COMPLETABLE_STATES: FrozenSet[DagTaskState] = frozenset({DagTaskState.RUNNING})

# Heartbeat writes are only accepted when the task is running.
HEARTBEAT_ALLOWED_STATES: FrozenSet[DagTaskState] = frozenset({DagTaskState.RUNNING})


# ── Transition table ──────────────────────────────────────────────────────────

ALLOWED_TRANSITIONS: dict[DagTaskState, FrozenSet[DagTaskState]] = {
    DagTaskState.PENDING:   frozenset({DagTaskState.READY}),
    DagTaskState.READY:     frozenset({DagTaskState.SCHEDULED}),
    DagTaskState.SCHEDULED: frozenset({DagTaskState.RUNNING, DagTaskState.READY}),
    DagTaskState.RUNNING:   frozenset({
        DagTaskState.DONE,
        DagTaskState.FAILED,
        DagTaskState.READY,
        DagTaskState.DEAD_LETTERED,
    }),
    DagTaskState.DONE:               frozenset(),
    DagTaskState.FAILED:             frozenset(),
    DagTaskState.BLOCKED_BY_FAILURE: frozenset(),
    DagTaskState.DEAD_LETTERED:      frozenset(),
    DagTaskState.SKIPPED:            frozenset(),
}


def is_terminal(state: str) -> bool:
    return state in TERMINAL_STATES


def is_transition_allowed(from_state: str, to_state: str) -> bool:
    try:
        frm = DagTaskState(from_state)
    except ValueError:
        return False
    allowed = ALLOWED_TRANSITIONS.get(frm, frozenset())
    try:
        return DagTaskState(to_state) in allowed
    except ValueError:
        return False

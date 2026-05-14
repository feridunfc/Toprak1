"""Compatibility shim for the authoritative state machine.

This module MUST NOT implement state transition logic.
It exists only so legacy imports from ``hfa_control.state_machine`` continue to
work while all state authority lives in ``hfa.state``.
"""

from hfa.state import (
    VALID_TRANSITIONS,
    TERMINAL_STATES,
    InvalidStateTransition,
    TransitionResult,
    get_run_state,
    is_terminal,
    transition_state,
    validate_transition,
)

__all__ = [
    "VALID_TRANSITIONS",
    "TERMINAL_STATES",
    "InvalidStateTransition",
    "TransitionResult",
    "get_run_state",
    "is_terminal",
    "transition_state",
    "validate_transition",
]

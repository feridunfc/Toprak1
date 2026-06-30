"""
hfa-worker/src/hfa_worker/models.py
IRONCLAD Sprint 1 — Canonical Execution Models

CANONICAL SOURCE OF TRUTH:
  ExecutionResult  — the ONE result type all executors must return
  ExecutionError   — base exception hierarchy for all executor errors

All other result types (execution_types.ExecutionResult) are deprecated.
Executors MUST return hfa_worker.models.ExecutionResult.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional


# ── Canonical Result ──────────────────────────────────────────────────────────

@dataclass
class ExecutionResult:
    """
    Canonical execution result. All BaseExecutor implementations MUST return this.

    Fields
    ------
    status      : "done" | "failed"  — terminal state of the execution
    payload     : dict               — output data (free-form, executor-defined)
    error       : str | None         — human-readable error message when status="failed"
    cost_cents  : int                — estimated cost in USD cents
    tokens_used : int                — total LLM tokens consumed
    """
    status: Literal["done", "failed"]
    payload: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    cost_cents: int = 0
    tokens_used: int = 0
    provider: Optional[str] = None

    def __post_init__(self) -> None:
        assert self.status in ("done", "failed"), (
            f"ExecutionResult.status must be 'done' or 'failed', got: {self.status!r}"
        )
        assert isinstance(self.payload, dict), (
            f"ExecutionResult.payload must be a dict, got: {type(self.payload)}"
        )

    @property
    def is_success(self) -> bool:
        return self.status == "done"

    @property
    def is_terminal_failure(self) -> bool:
        return self.status == "failed"

    @property
    def output_text(self) -> str:
        """Backward-compatible convenience accessor for legacy executor callers."""
        value = self.payload.get("output_text", "")
        if isinstance(value, str):
            return value
        return str(value)


# ── Exception Hierarchy ───────────────────────────────────────────────────────

class ExecutionError(Exception):
    """Base class for all executor errors."""
    pass


class TerminalExecutionError(ExecutionError):
    """Non-retryable execution failure. Worker should mark run as 'failed'."""
    def __init__(self, message: str, cost_cents: int = 0, tokens_used: int = 0):
        super().__init__(message)
        self.cost_cents = cost_cents
        self.tokens_used = tokens_used


class InfrastructureError(ExecutionError):
    """Transient infrastructure failure. Worker may retry."""
    pass


# ── Aliases for execution_types exception compat ──────────────────────────────
# These mirror the names in execution_types.py so that code importing either
# module gets semantically equivalent exception types.

class ExecutionTransientError(InfrastructureError):
    """Retryable transient error (network, timeout, rate-limit)."""
    pass


class ExecutionPermanentError(TerminalExecutionError):
    """Non-retryable permanent error (bad request, auth, provider rejection)."""
    pass


class ExecutionTimeoutError(ExecutionTransientError):
    """Request timed out. Retryable."""
    pass


class ExecutionRateLimitError(ExecutionTransientError):
    """Provider rate limit hit. Retryable with backoff."""
    pass


class ExecutionProviderError(ExecutionPermanentError):
    """Provider rejected the request permanently."""
    pass

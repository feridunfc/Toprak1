"""
hfa-worker/src/hfa_worker/executor.py
IRONCLAD Sprint 1 — Canonical Executor Interface

CANONICAL INTERFACE — all executors must subclass BaseExecutor.

    class MyExecutor(BaseExecutor):
        async def execute(self, run_event: RunRequestedEvent) -> ExecutionResult:
            ...

Contract
--------
- Input:  hfa.events.schema.RunRequestedEvent
- Output: hfa_worker.models.ExecutionResult
- Never raises — all exceptions must be caught and returned as
  ExecutionResult(status="failed", error=...)
"""

from __future__ import annotations

import abc

from hfa.events.schema import RunRequestedEvent
from hfa_worker.models import ExecutionResult

__all__ = ["BaseExecutor", "FakeExecutor"]


class BaseExecutor(abc.ABC):
    """
    Abstract base class for all executors.

    Implementations: FakeExecutor, OpenAIExecutor, CognitiveExecutor.

    Every executor MUST:
      1. Accept a RunRequestedEvent as its only argument.
      2. Return hfa_worker.models.ExecutionResult (status="done"|"failed").
      3. Never raise — wrap exceptions into ExecutionResult(status="failed").
    """

    @abc.abstractmethod
    async def execute(self, run_event: RunRequestedEvent) -> ExecutionResult:
        """Execute the run and return a canonical ExecutionResult."""
        raise NotImplementedError


# Backward-compatible public import contract.
# Some tests and older integrations import FakeExecutor from hfa_worker.executor,
# even though the implementation lives in hfa_worker.fake_executor.
# Keep this re-export stable until all callers are explicitly migrated.
from hfa_worker.fake_executor import FakeExecutor  # noqa: E402

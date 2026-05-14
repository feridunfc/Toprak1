"""
hfa-worker/src/hfa_worker/fake_executor.py
IRONCLAD Sprint 1 — Fake Executor (Canonical)

Implements BaseExecutor. Returns deterministic fake results.
Used in tests, local dev, and CI.

Input compatibility
-------------------
Accepts both RunRequestedEvent (canonical) and ExecutionRequest (legacy
consumer.py adapter). Both expose the same .payload attribute, so the
execute() implementation works identically for either type.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from hfa_worker.executor import BaseExecutor
from hfa_worker.models import ExecutionResult

logger = logging.getLogger(__name__)


class FakeExecutor(BaseExecutor):
    """
    Deterministic fake executor for testing and local development.

    Parameters
    ----------
    should_succeed  : if True, returns status="done"; else status="failed"
    fail_with       : if set, raises this exception (tests exceptional paths)
    cost_cents      : fixed cost to report
    tokens_used     : fixed token count to report
    """

    def __init__(
        self,
        should_succeed: bool = True,
        fail_with: Optional[Exception] = None,
        cost_cents: int = 0,
        tokens_used: int = 0,
    ) -> None:
        self.should_succeed = should_succeed
        self.fail_with = fail_with
        self.cost_cents = cost_cents
        self.tokens_used = tokens_used

    async def execute(self, run_event: Any) -> ExecutionResult:
        """
        Execute fake run. Accepts RunRequestedEvent or ExecutionRequest.

        Returns canonical hfa_worker.models.ExecutionResult.
        """
        if self.fail_with:
            raise self.fail_with

        payload = getattr(run_event, "payload", {}) or {}
        prompt = str(payload.get("prompt", ""))
        run_id = getattr(run_event, "run_id", "unknown")

        if self.should_succeed:
            fake_output = (
                f"FAKE_RESPONSE: {prompt[:50]}..." if prompt else "FAKE_RESPONSE: no_prompt"
            )
            return ExecutionResult(
                status="done",
                payload={
                    "run_id": run_id,
                    "result": "success",
                    "output_text": fake_output,
                    "input": payload,
                },
                cost_cents=self.cost_cents,
                tokens_used=self.tokens_used,
            )

        return ExecutionResult(
            status="failed",
            payload={"run_id": run_id},
            error="Business logic failure",
            cost_cents=self.cost_cents,
            tokens_used=self.tokens_used,
        )

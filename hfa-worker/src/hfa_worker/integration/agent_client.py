"""Agent integration client for Sprint 5 worker/effect closure."""
from __future__ import annotations

from typing import Any


class AgentClient:
    """Thin async adapter for invoking a stateless agent workflow.

    The adapter deliberately does not persist state and does not know about
    scheduler authority.  It only invokes a supplied workflow object.
    """

    def __init__(self, workflow: Any | None = None) -> None:
        self._workflow = workflow

    async def invoke(self, payload: dict[str, Any]) -> Any:
        if self._workflow is None:
            return {"status": "skipped", "reason": "agent_workflow_unconfigured"}
        run = getattr(self._workflow, "run", None) or getattr(self._workflow, "execute", None)
        if not callable(run):
            return {"status": "skipped", "reason": "agent_workflow_not_callable"}
        result = run(payload)
        if hasattr(result, "__await__"):
            result = await result
        return result

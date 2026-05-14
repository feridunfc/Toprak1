
from __future__ import annotations

from dataclasses import dataclass

from hfa_worker.task_context import TaskContext


@dataclass(frozen=True)
class TaskExecutionResult:
    ok: bool
    output: dict
    error: str = ""


class TaskExecutor:
    async def execute(self, ctx: TaskContext) -> TaskExecutionResult:
        return TaskExecutionResult(ok=True, output={"task_id": ctx.task_id, "agent_type": ctx.agent_type})

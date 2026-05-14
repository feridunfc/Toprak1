from __future__ import annotations

from hfa_agents.base.agent_base import AgentBase
from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.base.registry import AgentRegistry


@AgentRegistry.register("tester")
class TesterAgent(AgentBase):
    role = "tester"

    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        code = event.context.get("generated_code", "")
        if not code:
            return ExecutionResult(status="failed", reasoning_trace=["missing_generated_code"], confidence=0.0)
        test_passed = "def main" in code
        return ExecutionResult(
            status="success" if test_passed else "failed",
            output_data={"test_passed": test_passed},
            reasoning_trace=["tester_invariants_checked"],
            confidence=0.78 if test_passed else 0.20,
        )

from __future__ import annotations

import os

from hfa_agents.base.agent_base import AgentBase
from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.base.registry import AgentRegistry

_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}


def _strict_mode_enabled() -> bool:
    return os.getenv("IRON_STRICT_MODE", os.getenv("IRONCLAD_STRICT_MODE", "0")) not in _FALSE_VALUES


@AgentRegistry.register("tester")
class TesterAgent(AgentBase):
    role = "tester"

    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        code = event.context.get("generated_code", "")
        strict_mode = _strict_mode_enabled()
        if not code:
            return ExecutionResult(
                status="failed",
                reasoning_trace=["missing_generated_code", "tester_fail_closed" if strict_mode else "tester_advisory_failure"],
                suggested_feedback="Generated code is missing.",
                requires_hitl=strict_mode,
                confidence=0.0,
            )
        test_passed = "def main" in code
        return ExecutionResult(
            status="success" if test_passed else "failed",
            output_data={"test_passed": test_passed},
            reasoning_trace=["tester_invariants_checked"],
            suggested_feedback=None if test_passed else "Tester invariant failed: expected a def main entry point.",
            requires_hitl=strict_mode and not test_passed,
            confidence=0.78 if test_passed else 0.20,
        )

from __future__ import annotations

from hfa_agents.base.agent_base import AgentBase
from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.base.registry import AgentRegistry


@AgentRegistry.register("compliance")
class ComplianceAgent(AgentBase):
    role = "compliance"

    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        code = event.context.get("generated_code", "")
        violations = []
        lowered = code.lower()
        if "password" in lowered or "secret" in lowered:
            violations.append("hardcoded_credential_detected")
        if violations:
            return ExecutionResult(
                status="failed",
                reasoning_trace=violations,
                requires_hitl=True,
                suggested_feedback="compliance_failure",
                confidence=0.95,
            )
        return ExecutionResult(
            status="success",
            reasoning_trace=["compliance_passed"],
            confidence=0.90,
        )

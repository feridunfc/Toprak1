from __future__ import annotations

from hfa_agents.base.agent_base import AgentBase
from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.base.registry import AgentRegistry


@AgentRegistry.register("architect")
class ArchitectAgent(AgentBase):
    role = "architect"

    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        plan = event.context.get("execution_plan", [])
        findings = event.context.get("research_findings", [])
        summary = {
            "system_style": "service-oriented",
            "recommended_stack": ["Python", "FastAPI", "Redis", "PostgreSQL"],
            "components": ["api", "worker", "semantic", "agent-orchestrator"],
            "plan_steps": len(plan),
            "research_findings": len(findings),
        }
        return ExecutionResult(
            status="success",
            output_data={"architecture_summary": summary},
            reasoning_trace=["architecture_synthesized"],
            confidence=0.80,
        )

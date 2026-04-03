from __future__ import annotations

from hfa_agents.base.agent_base import AgentBase
from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.base.registry import AgentRegistry
from hfa_semantic.api.models import SemanticQueryRequest


@AgentRegistry.register("researcher")
class ResearcherAgent(AgentBase):
    role = "researcher"

    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        req = SemanticQueryRequest(
            intent="technical context and similar solutions",
            context_event_id=event.event_id,
            vector_query=event.goal,
            min_confidence=0.82,
            graph_depth=2,
        )
        results = await self.semantic_engine.merge_engine.execute_query(req)
        findings = [r.model_dump() for r in results]
        return ExecutionResult(
            status="success",
            output_data={"research_findings": findings},
            reasoning_trace=[f"semantic_intersections={len(findings)}"],
            confidence=0.84,
        )

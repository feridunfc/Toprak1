from __future__ import annotations

from hfa_agents.base.agent_base import AgentBase
from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.base.registry import AgentRegistry
from hfa_semantic.api.models import SemanticQueryRequest


@AgentRegistry.register("supervisor")
class SupervisorAgent(AgentBase):
    role = "supervisor"

    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        existing = event.context.get("execution_plan")
        if existing:
            return ExecutionResult(
                status="success",
                output_data={"execution_plan": existing},
                reasoning_trace=["reused_existing_plan"],
                confidence=0.95,
            )

        req = SemanticQueryRequest(
            intent="similar project decomposition",
            context_event_id=event.event_id,
            vector_query=event.goal,
            min_confidence=0.82,
        )
        similar = await self.semantic_engine.merge_engine.execute_query(req)
        reused_count = len(similar)

        plan = [
            {"step": "researcher", "depends_on": []},
            {"step": "architect", "depends_on": ["researcher"]},
            {"step": "coder", "depends_on": ["architect"]},
            {"step": "tester", "depends_on": ["coder"]},
            {"step": "compliance", "depends_on": ["tester"]},
        ]

        return ExecutionResult(
            status="success",
            output_data={"execution_plan": plan},
            artifacts={"plan_source": "semantic_supervisor", "similar_plans_found": reused_count},
            reasoning_trace=[f"generated_dag_plan:similar={reused_count}"],
            confidence=0.88,
        )

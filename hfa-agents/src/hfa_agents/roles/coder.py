from __future__ import annotations

from hfa_agents.base.agent_base import AgentBase
from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa_agents.base.registry import AgentRegistry
from hfa_agents.mcp.tools.sandbox import SandboxTool


@AgentRegistry.register("coder")
class CoderAgent(AgentBase):
    role = "coder"

    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        arch = event.context.get("architecture_summary", {})
        findings = event.context.get("research_findings", [])
        generated_code = (
            "# generated baseline\n"
            "def main():\n"
            "    return {'status': 'ok'}\n"
        )
        sandbox_result = await SandboxTool().execute(generated_code, language="python")
        status = "success" if sandbox_result.get("success") else "failed"
        return ExecutionResult(
            status=status,
            output_data={"generated_code": generated_code, "sandbox_result": sandbox_result},
            reasoning_trace=[f"code_generated:findings={len(findings)}:components={len(arch.get('components', []))}"],
            confidence=0.76,
        )

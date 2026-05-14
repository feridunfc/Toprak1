from __future__ import annotations

from typing import Any, Dict

from hfa_agents.mcp.tools.semantic_query import SemanticQueryTool
from hfa_agents.mcp.tools.sandbox import SandboxTool


class MCPHost:
    def __init__(self, semantic_engine):
        self._tools: Dict[str, Any] = {
            "semantic_query": SemanticQueryTool(semantic_engine),
            "sandbox_execute": SandboxTool(),
        }

    async def execute_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if tool_name not in self._tools:
            return {"success": False, "error": f"unknown_tool:{tool_name}"}
        tool = self._tools[tool_name]
        return await tool.execute(**params)

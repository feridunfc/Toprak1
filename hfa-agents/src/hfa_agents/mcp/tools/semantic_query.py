from __future__ import annotations

from hfa_semantic.api.models import SemanticQueryRequest


class SemanticQueryTool:
    def __init__(self, semantic_engine):
        self.semantic_engine = semantic_engine

    async def execute(self, intent: str, query: str, context_event_id: str = "current", min_confidence: float = 0.82):
        req = SemanticQueryRequest(
            intent=intent,
            context_event_id=context_event_id,
            vector_query=query,
            min_confidence=min_confidence,
        )
        results = await self.semantic_engine.merge_engine.execute_query(req)
        return {"success": True, "results": [r.model_dump() for r in results]}

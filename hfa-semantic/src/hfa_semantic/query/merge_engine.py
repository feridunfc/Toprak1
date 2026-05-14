from __future__ import annotations

from hfa_semantic.api.models import QueryResult, SemanticQueryRequest
from hfa_semantic.query.graph_executor import GraphExecutor
from hfa_semantic.query.vector_executor import VectorExecutor


class MergeEngine:
    def __init__(self, vector_exec: VectorExecutor, graph_exec: GraphExecutor):
        self._vector = vector_exec
        self._graph = graph_exec

    async def execute_query(self, request: SemanticQueryRequest) -> list[QueryResult]:
        candidates = await self._vector.search_similar_events(request.vector_query, limit=5)
        final_results: list[QueryResult] = []
        for candidate_id, score in candidates.items():
            if score < request.min_confidence:
                continue
            insights = await self._graph.get_relations(
                source_event_id=request.context_event_id,
                target_event_id=candidate_id,
                max_depth=request.graph_depth,
            )
            if not insights:
                continue
            final_results.append(
                QueryResult(
                    historical_event_id=candidate_id,
                    similarity_score=score,
                    graph_insights=insights,
                    validated_truth=True,
                )
            )
        final_results.sort(key=lambda x: x.similarity_score, reverse=True)
        return final_results

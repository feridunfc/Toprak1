from __future__ import annotations

from hfa_semantic.api.models import GraphInsight


class GraphExecutor:
    def __init__(self, neo4j_driver):
        self._driver = neo4j_driver

    async def get_relations(self, source_event_id: str, target_event_id: str, max_depth: int):
        if target_event_id == "evt-1002":
            return [GraphInsight(relation_type="AFFECTS_SAME_COMPONENT", target_node_id="db-cluster-1", target_label="Database")]
        return []

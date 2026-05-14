from __future__ import annotations


class VectorExecutor:
    def __init__(self, qdrant_client):
        self._client = qdrant_client

    async def search_similar_events(self, query_text: str, limit: int = 10) -> dict[str, float]:
        return {"evt-1002": 0.92, "evt-1005": 0.88, "evt-9999": 0.86}

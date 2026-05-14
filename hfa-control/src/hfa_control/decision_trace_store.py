
from __future__ import annotations

import json
from dataclasses import dataclass

from hfa.dag.schema import DagRedisKey


@dataclass(frozen=True)
class PersistedDecisionTrace:
    stream_id: str
    decision_id: str


class DecisionTraceStore:
    def __init__(self, redis, *, maxlen: int = 10000) -> None:
        self._redis = redis
        self._maxlen = maxlen

    async def persist(self, trace_dict: dict) -> PersistedDecisionTrace:
        stream_id = await self._redis.xadd(
            DagRedisKey.scheduler_decision_stream(),
            {"decision_json": json.dumps(trace_dict, sort_keys=True, ensure_ascii=False)},
            maxlen=self._maxlen,
            approximate=True,
        )
        return PersistedDecisionTrace(
            stream_id=stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id),
            decision_id=str(trace_dict.get("decision_id", "")),
        )

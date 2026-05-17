"""Read-only event stream read model."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from read_models.redis_reader import RedisReadClient, RedisReadUnavailable, decode


@dataclass(frozen=True)
class EventStreamReadModel:
    redis: RedisReadClient

    @classmethod
    def from_env(cls) -> "EventStreamReadModel":
        return cls(redis=RedisReadClient.from_env())

    def snapshot(self, *, limit: int = 25) -> dict[str, Any]:
        try:
            client = self.redis.client()
        except RedisReadUnavailable as exc:
            return {"mode": "read-only", "available": False, "items": [], "count": 0, "source": "redis", "note": str(exc)}

        items: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for pattern in ("hfa:events:*", "hfa:event:*"):
            for raw_key in client.scan_iter(match=pattern, count=100):
                key = str(decode(raw_key))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                key_type = decode(client.type(raw_key))
                if key_type == "list":
                    for raw_event in client.lrange(raw_key, max(0, -limit), -1):
                        items.append(self._event_from_list_item(key, raw_event))
                        if len(items) >= limit:
                            break
                elif key_type == "stream":
                    for event_id, fields in client.xrevrange(raw_key, count=limit):
                        items.append({"key": key, "id": decode(event_id), "kind": "stream", "event": decode(fields)})
                        if len(items) >= limit:
                            break
                if len(items) >= limit:
                    break
            if len(items) >= limit:
                break

        return {"mode": "read-only", "available": True, "items": items[:limit], "count": len(items[:limit]), "source": self.redis.url, "note": "Read-only Redis scan over hfa:events:* / hfa:event:*"}

    @staticmethod
    def _event_from_list_item(key: str, raw_event: Any) -> dict[str, Any]:
        decoded = decode(raw_event)
        parsed: Any = decoded
        if isinstance(decoded, str):
            try:
                parsed = json.loads(decoded)
            except json.JSONDecodeError:
                parsed = decoded
        return {"key": key, "kind": "list", "event": parsed}

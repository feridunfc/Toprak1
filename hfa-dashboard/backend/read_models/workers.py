"""Read-only worker/effect telemetry read model."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from read_models.redis_reader import RedisReadClient, RedisReadUnavailable, decode


WORKER_PATTERNS = ("hfa:worker:*", "hfa:workers:*", "hfa:cp:worker:*", "hfa:control:worker:*")
EFFECT_PATTERNS = ("hfa:effect:*", "hfa:effects:*", "hfa:effect_ledger:*", "hfa:ledger:effect:*")
DLQ_PATTERNS = ("hfa:dlq:*", "hfa:dead_letter:*", "hfa:deadletter:*")


@dataclass(frozen=True)
class WorkerTelemetryReadModel:
    redis: RedisReadClient

    @classmethod
    def from_env(cls) -> "WorkerTelemetryReadModel":
        return cls(redis=RedisReadClient.from_env())

    def snapshot(self, *, limit: int = 50) -> dict[str, Any]:
        try:
            client = self.redis.client()
        except RedisReadUnavailable as exc:
            return {"mode": "read-only", "available": False, "workers": [], "effects": [], "counts": {"workers": 0, "effects": 0, "dead_letter": 0}, "note": str(exc)}

        workers = self._collect_hash_like(client, WORKER_PATTERNS, limit)
        effects = self._collect_hash_like(client, EFFECT_PATTERNS, limit)
        return {
            "mode": "read-only",
            "available": True,
            "workers": workers,
            "effects": effects,
            "counts": {"workers": len(workers), "effects": len(effects), "dead_letter": self._count_matching(client, DLQ_PATTERNS)},
            "source": self.redis.url,
            "note": "Read-only Redis scan for worker/effect/dead-letter telemetry",
        }

    def _collect_hash_like(self, client: Any, patterns: tuple[str, ...], limit: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for pattern in patterns:
            for raw_key in client.scan_iter(match=pattern, count=100):
                key = str(decode(raw_key))
                if key in seen:
                    continue
                seen.add(key)
                key_type = decode(client.type(raw_key))
                if key_type == "hash":
                    payload = decode(client.hgetall(raw_key))
                elif key_type in {"string", "list", "zset", "set"}:
                    payload = {"type": key_type, "ttl": client.ttl(raw_key)}
                else:
                    payload = {"type": key_type}
                out.append({"key": key, "type": key_type, "payload": payload})
                if len(out) >= limit:
                    return out
        return out

    @staticmethod
    def _count_matching(client: Any, patterns: tuple[str, ...]) -> int:
        count = 0
        seen: set[str] = set()
        for pattern in patterns:
            for raw_key in client.scan_iter(match=pattern, count=100):
                key = str(decode(raw_key))
                if key in seen:
                    continue
                seen.add(key)
                count += 1
        return count

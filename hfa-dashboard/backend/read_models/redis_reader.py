"""Small read-only Redis adapter for dashboard read models."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


class RedisReadUnavailable(RuntimeError):
    pass


def decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {decode(k): decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decode(v) for v in value]
    if isinstance(value, tuple):
        return tuple(decode(v) for v in value)
    return value


@dataclass(frozen=True)
class RedisReadClient:
    url: str

    @classmethod
    def from_env(cls) -> "RedisReadClient":
        return cls(url=os.getenv("HFA_DASHBOARD_REDIS_URL") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6389/0")

    def client(self):
        try:
            import redis
        except Exception as exc:
            raise RedisReadUnavailable("redis package is not installed") from exc

        try:
            client = redis.Redis.from_url(
                self.url,
                decode_responses=False,
                socket_connect_timeout=0.25,
                socket_timeout=0.5,
            )
            client.ping()
            return client
        except Exception as exc:
            raise RedisReadUnavailable(f"redis read connection unavailable: {exc}") from exc


from __future__ import annotations

from typing import Any

from hfa.runtime.idempotency import IdempotencyKeys, IdempotencyResult


class IdempotencyStore:
    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    async def acquire_dispatch_token(self, *, task_id: str, token_value: str, ttl_seconds: int = 3600) -> IdempotencyResult:
        key = IdempotencyKeys.dispatch_token(task_id)
        ok = await self._redis.set(key, token_value, ex=ttl_seconds, nx=True)
        if ok:
            return IdempotencyResult(accepted=True, reason="dispatch_token_acquired", token_key=key)
        existing = await self._redis.get(key)
        return IdempotencyResult(
            accepted=False,
            reason="dispatch_duplicate",
            token_key=key,
            existing_value=existing if existing is not None else "",
        )

    async def acquire_completion_token(self, *, task_id: str, token_value: str, ttl_seconds: int = 3600) -> IdempotencyResult:
        key = IdempotencyKeys.completion_token(task_id)
        ok = await self._redis.set(key, token_value, ex=ttl_seconds, nx=True)
        if ok:
            return IdempotencyResult(accepted=True, reason="completion_token_acquired", token_key=key)
        existing = await self._redis.get(key)
        return IdempotencyResult(
            accepted=False,
            reason="completion_duplicate",
            token_key=key,
            existing_value=existing if existing is not None else "",
        )

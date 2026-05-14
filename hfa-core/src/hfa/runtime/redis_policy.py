
from __future__ import annotations

import asyncio
import random
from typing import Any, Awaitable, Callable

from hfa.runtime.failure_semantics import (
    DistributedError,
    FailureCategory,
    classify_redis_error,
)


class RedisCallPolicy:
    def __init__(
        self,
        max_retries: int = 3,
        base_delay_ms: int = 100,
        jitter_ms: int = 25,
    ) -> None:
        self.max_retries = max_retries
        self.base_delay_ms = base_delay_ms
        self.jitter_ms = jitter_ms

    async def execute_with_policy(
        self,
        operation_name: str,
        coroutine_func: Callable[..., Awaitable[Any]],
        *args,
        **kwargs,
    ) -> Any:
        attempt = 0

        while True:
            try:
                return await coroutine_func(*args, **kwargs)
            except Exception as exc:
                category = classify_redis_error(exc)

                if category == FailureCategory.TERMINAL:
                    raise DistributedError(
                        f"Terminal failure in {operation_name}",
                        category,
                        exc,
                    ) from exc

                if attempt >= self.max_retries:
                    raise DistributedError(
                        f"Max retries exceeded for {operation_name}",
                        category,
                        exc,
                    ) from exc

                delay_ms = self.base_delay_ms * (2 ** attempt)
                if self.jitter_ms > 0:
                    delay_ms += random.randint(0, self.jitter_ms)

                await asyncio.sleep(delay_ms / 1000.0)
                attempt += 1


import pytest
from unittest.mock import AsyncMock

from hfa.runtime.idempotency_store import IdempotencyStore

pytestmark = pytest.mark.asyncio


async def test_dispatch_token_first_write_wins():
    redis = AsyncMock()
    redis.set.return_value = True

    store = IdempotencyStore(redis)
    result = await store.acquire_dispatch_token(task_id="task-1", token_value="run-1:worker-1")

    assert result.accepted is True
    assert result.reason == "dispatch_token_acquired"


async def test_dispatch_duplicate_returns_existing_value():
    redis = AsyncMock()
    redis.set.return_value = False
    redis.get.return_value = "run-1:worker-1"

    store = IdempotencyStore(redis)
    result = await store.acquire_dispatch_token(task_id="task-1", token_value="run-1:worker-2")

    assert result.accepted is False
    assert result.reason == "dispatch_duplicate"
    assert result.existing_value == "run-1:worker-1"


async def test_completion_duplicate_returns_existing_value():
    redis = AsyncMock()
    redis.set.return_value = False
    redis.get.return_value = "run-1:worker-1"

    store = IdempotencyStore(redis)
    result = await store.acquire_completion_token(task_id="task-1", token_value="run-1:worker-2")

    assert result.accepted is False
    assert result.reason == "completion_duplicate"
    assert result.existing_value == "run-1:worker-1"

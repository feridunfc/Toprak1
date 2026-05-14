
import pytest
from unittest.mock import AsyncMock

from hfa_worker.input_resolver import InputResolver, MissingParentOutputError

pytestmark = pytest.mark.asyncio


async def test_single_parent_output_inject():
    redis = AsyncMock()
    redis.mget.return_value = ['{"code":"print(1)"}']

    resolver = InputResolver(redis)
    template = {"prompt": "${task_123.output}"}
    result = await resolver.resolve(template)

    assert result.referenced_task_ids == ["task_123"]
    assert result.hydrated == {"prompt": {"code": "print(1)"}}


async def test_multiple_placeholder_inject():
    redis = AsyncMock()
    redis.mget.return_value = ["CODE123", "CTX999"]

    resolver = InputResolver(redis)
    template = {
        "prompt": "Test this code: ${task_a.output}",
        "context": "${task_b.output}",
    }
    result = await resolver.resolve(template)

    assert result.referenced_task_ids == ["task_a", "task_b"]
    assert result.hydrated["prompt"] == "Test this code: CODE123"
    assert result.hydrated["context"] == "CTX999"


async def test_missing_output_fails_fast():
    redis = AsyncMock()
    redis.mget.return_value = [None]

    resolver = InputResolver(redis)
    template = {"prompt": "${task_x.output}"}

    with pytest.raises(MissingParentOutputError):
        await resolver.resolve(template)


import pytest

from hfa.dag.schema import DagRedisKey
from hfa_worker.input_resolver import InputResolver, MissingParentOutputError

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_single_parent_output_inject(redis_client):
    await redis_client.set(DagRedisKey.task_output("task_123"), '{"code":"print(1)"}')

    resolver = InputResolver(redis_client)
    template = {"prompt": "${task_123.output}"}
    result = await resolver.resolve(template)

    assert result.hydrated == {"prompt": {"code": "print(1)"}}


@pytest.mark.integration
async def test_multiple_placeholder_inject(redis_client):
    await redis_client.set(DagRedisKey.task_output("parent_a"), "CODE_A")
    await redis_client.set(DagRedisKey.task_output("parent_b"), '{"ctx":"CTX_B"}')

    resolver = InputResolver(redis_client)
    template = {
        "prompt": "Execute ${parent_a.output}",
        "context": "${parent_b.output}",
        "nested": ["${parent_a.output}", {"x": "${parent_b.output}"}],
    }
    result = await resolver.resolve(template)

    assert result.hydrated["prompt"] == "Execute CODE_A"
    assert result.hydrated["context"] == {"ctx": "CTX_B"}
    assert result.hydrated["nested"][0] == "CODE_A"
    assert result.hydrated["nested"][1]["x"] == {"ctx": "CTX_B"}


@pytest.mark.integration
async def test_missing_output_fails_fast(redis_client):
    resolver = InputResolver(redis_client)
    template = {"prompt": "${missing_parent.output}"}

    with pytest.raises(MissingParentOutputError):
        await resolver.resolve(template)


@pytest.mark.integration
async def test_retry_same_input_consistency(redis_client):
    await redis_client.set(DagRedisKey.task_output("task_retry"), '{"v":42}')

    resolver = InputResolver(redis_client)
    template = {
        "payload": {
            "first": "${task_retry.output}",
            "second": "again:${task_retry.output}",
        }
    }

    first = await resolver.resolve(template)
    second = await resolver.resolve(template)

    assert first.hydrated == second.hydrated
    assert first.referenced_task_ids == second.referenced_task_ids

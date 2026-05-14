
import json
import pytest

from hfa.dag.schema import DagRedisKey
from hfa_worker.input_resolver import InputResolver

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_multi_parent_list_merge(redis_client):
    await redis_client.set(DagRedisKey.task_output("task_1"), '{"a":1}')
    await redis_client.set(DagRedisKey.task_output("task_2"), '{"b":2}')
    await redis_client.set(DagRedisKey.task_output("task_3"), '"plain"')

    resolver = InputResolver(redis_client)
    template = {
        "merged": {
            "__merge__": ["task_1", "task_2", "task_3"],
            "mode": "list",
        }
    }
    result = await resolver.resolve(template)

    assert result.hydrated["merged"] == [{"a": 1}, {"b": 2}, "plain"]


@pytest.mark.integration
async def test_multi_parent_dict_merge(redis_client):
    await redis_client.set(DagRedisKey.task_output("coder_1"), "print('a')")
    await redis_client.set(DagRedisKey.task_output("coder_2"), "print('b')")

    resolver = InputResolver(redis_client)
    template = {
        "inputs": {
            "__merge__": {"kod_1": "coder_1", "kod_2": "coder_2"},
            "mode": "dict",
        }
    }
    result = await resolver.resolve(template)

    assert result.hydrated["inputs"] == {"kod_1": "print('a')", "kod_2": "print('b')"}


@pytest.mark.integration
async def test_mixed_json_string_merge(redis_client):
    await redis_client.set(DagRedisKey.task_output("parent_json"), '{"status":"ok"}')
    await redis_client.set(DagRedisKey.task_output("parent_text"), "Hata bulundu")

    resolver = InputResolver(redis_client)
    template = {
        "report": {
            "__merge__": {"json_part": "parent_json", "text_part": "parent_text"},
            "mode": "dict",
        }
    }
    result = await resolver.resolve(template)

    assert result.hydrated["report"]["json_part"] == {"status": "ok"}
    assert result.hydrated["report"]["text_part"] == "Hata bulundu"


@pytest.mark.integration
async def test_lineage_record_persists(redis_client):
    await redis_client.set(DagRedisKey.task_output("p1"), '{"x":1}')
    await redis_client.set(DagRedisKey.task_output("p2"), "RAW-TEXT")

    resolver = InputResolver(redis_client)
    template = {
        "payload": {
            "__merge__": ["p1", "p2"],
            "mode": "list",
        }
    }
    result = await resolver.resolve(template)
    await resolver.persist_lineage(task_id="child_1", lineage_record=result.lineage_record, ttl_seconds=3600)

    raw = await redis_client.get(DagRedisKey.task_lineage("child_1"))
    assert raw is not None

    lineage = json.loads(raw)
    assert lineage["parent_count"] == 2
    assert "p1" in lineage["parents"]
    assert "p2" in lineage["parents"]
    assert lineage["parents"]["p1"]["raw"] == '{"x":1}'
    assert lineage["parents"]["p2"]["raw"] == "RAW-TEXT"
    assert lineage["parents"]["p1"]["decoded"] == {"x": 1}
    assert lineage["parents"]["p2"]["decoded"] == "RAW-TEXT"
    assert lineage["parents"]["p1"]["sha256"]
    assert lineage["combined_sha256"]

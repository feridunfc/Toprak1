"""
hfa-control/tests/test_sprint10_graph_store.py
IRONCLAD Sprint 10 — GraphStore tests
"""

from __future__ import annotations

import json
import pytest
import fakeredis.aioredis as faredis

from hfa.obs.graph_store import RedisGraphStore


class TestRedisGraphStore:
    @pytest.mark.asyncio
    async def test_save_and_load_snapshot(self):
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        graph_json = json.dumps({"run_id": "r1", "nodes": []})
        await store.save_snapshot("r1", graph_json)
        loaded = await store.load_snapshot("r1")
        assert json.loads(loaded)["run_id"] == "r1"

    @pytest.mark.asyncio
    async def test_load_snapshot_missing_returns_none(self):
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        result = await store.load_snapshot("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_append_and_load_patches(self):
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        await store.append_patch(
            "r2", {"seq": 0, "op": "node_started", "node_id": "n1", "ts": 1.0, "data": {}}
        )
        await store.append_patch(
            "r2",
            {
                "seq": 1,
                "op": "node_committed",
                "node_id": "n1",
                "ts": 2.0,
                "data": {"cost_cents": 10},
            },
        )
        patches = await store.load_patches("r2")
        assert len(patches) == 2
        # cost_cents must be int — no float USD
        assert patches[1]["data"]["cost_cents"] == 10
        assert isinstance(patches[1]["data"]["cost_cents"], int)

    @pytest.mark.asyncio
    async def test_load_patches_after_seq_filters(self):
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        for i in range(5):
            await store.append_patch(
                "r3", {"seq": i, "op": "x", "node_id": "n1", "ts": 0.0, "data": {}}
            )
        patches = await store.load_patches("r3", after_seq=3)
        assert all(p["seq"] >= 3 for p in patches)
        assert len(patches) == 2  # seq 3 and 4

    @pytest.mark.asyncio
    async def test_load_patches_empty_returns_empty_list(self):
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        result = await store.load_patches("nonexistent")
        assert result == []

    @pytest.mark.asyncio
    async def test_next_seq_increments(self):
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        assert await store.next_seq("r4") == 0
        await store.append_patch("r4", {"seq": 0, "op": "x", "node_id": "n", "ts": 0.0, "data": {}})
        assert await store.next_seq("r4") == 1

    @pytest.mark.asyncio
    async def test_delete_removes_all_keys(self):
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        await store.save_snapshot("r5", '{"run_id":"r5"}')
        await store.append_patch("r5", {"seq": 0, "op": "x", "node_id": "n", "ts": 0.0, "data": {}})
        await store.delete("r5")
        assert await store.load_snapshot("r5") is None
        assert await store.load_patches("r5") == []

    @pytest.mark.asyncio
    async def test_save_snapshot_overwrites(self):
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        await store.save_snapshot("r6", '{"v":1}')
        await store.save_snapshot("r6", '{"v":2}')
        loaded = await store.load_snapshot("r6")
        assert json.loads(loaded)["v"] == 2

    @pytest.mark.asyncio
    async def test_load_patches_handles_corrupt_json(self):
        """Corrupt patch entries are skipped with a warning, not raised."""
        redis = faredis.FakeRedis()
        store = RedisGraphStore(redis)
        # Manually push corrupt data
        await redis.rpush("hfa:graph:patch:r7", b"NOT_JSON")
        await redis.rpush(
            "hfa:graph:patch:r7",
            json.dumps({"seq": 0, "op": "x", "node_id": "n", "ts": 0.0, "data": {}}).encode(),
        )
        patches = await store.load_patches("r7")
        assert len(patches) == 1  # corrupt entry skipped

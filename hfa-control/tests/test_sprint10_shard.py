"""
hfa-control/tests/test_sprint10_shard.py
IRONCLAD Sprint 10 — ShardOwnershipManager tests
"""

from __future__ import annotations

import pytest
import fakeredis.aioredis as faredis

from hfa_control.shard import ShardOwnershipManager
from hfa_control.models import ControlPlaneConfig
from hfa_control.exceptions import ShardOwnershipError


def _cfg() -> ControlPlaneConfig:
    return ControlPlaneConfig(instance_id="test")


class TestShardOwnershipManager:
    @pytest.mark.asyncio
    async def test_claim_shard_succeeds(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        ok = await mgr.claim_shard(0, "grp-a")
        assert ok is True

    @pytest.mark.asyncio
    async def test_claim_shard_fails_if_already_owned(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        await mgr.claim_shard(1, "grp-a")
        ok2 = await mgr.claim_shard(1, "grp-b")
        assert ok2 is False

    @pytest.mark.asyncio
    async def test_claim_writes_owners_hash(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        await mgr.claim_shard(2, "grp-a")
        raw = await redis.hget("hfa:cp:shard:owners", 2)
        assert raw.decode() == "grp-a"

    @pytest.mark.asyncio
    async def test_renew_extends_ttl(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        await mgr.claim_shard(3, "grp-a")
        ok = await mgr.renew_shard(3, "grp-a")
        assert ok is True

    @pytest.mark.asyncio
    async def test_renew_fails_if_not_owner(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        await mgr.claim_shard(4, "grp-b")
        ok = await mgr.renew_shard(4, "grp-a")  # wrong group
        assert ok is False

    @pytest.mark.asyncio
    async def test_shards_for_group(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        await mgr.claim_shard(5, "grp-a")
        await mgr.claim_shard(6, "grp-a")
        await mgr.claim_shard(7, "grp-b")
        shards = await mgr.shards_for_group("grp-a")
        assert 5 in shards
        assert 6 in shards
        assert 7 not in shards

    @pytest.mark.asyncio
    async def test_shard_for_group_deterministic(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        await mgr.claim_shard(8, "grp-a")
        await mgr.claim_shard(9, "grp-a")
        s1 = await mgr.shard_for_group("grp-a", "run-abc")
        s2 = await mgr.shard_for_group("grp-a", "run-abc")
        assert s1 == s2  # same run_id → same shard

    @pytest.mark.asyncio
    async def test_shard_for_group_raises_when_no_shards(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        with pytest.raises(ShardOwnershipError):
            await mgr.shard_for_group("nonexistent-group", "run-001")

    @pytest.mark.asyncio
    async def test_all_owners(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        await mgr.claim_shard(10, "grp-a")
        await mgr.claim_shard(11, "grp-b")
        owners = await mgr.all_owners()
        assert owners[10] == "grp-a"
        assert owners[11] == "grp-b"

    @pytest.mark.asyncio
    async def test_orphan_monitor_removes_expired_shard(self):
        redis = faredis.FakeRedis()
        mgr = ShardOwnershipManager(redis, _cfg())
        # Manually write owners map without the owner key (simulating TTL expiry)
        await redis.hset("hfa:cp:shard:owners", 12, "grp-dead")
        # hfa:cp:shard:owner:12 does NOT exist (expired)
        await mgr._check_orphans()
        owners = await mgr.all_owners()
        assert 12 not in owners

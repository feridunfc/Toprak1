"""
hfa-control/tests/test_sprint10_leader.py
IRONCLAD Sprint 10 — LeaderElection tests
"""

from __future__ import annotations

import pytest
import fakeredis.aioredis as faredis

from hfa_control.leader import LeaderElection
from hfa_control.models import ControlPlaneConfig
from hfa_control.exceptions import LeadershipError


def _cfg(ttl=5, renew=1.0, **kw) -> ControlPlaneConfig:
    return ControlPlaneConfig(
        instance_id="inst-a",
        leader_ttl=ttl,
        leader_renew_interval=renew,
        **kw,
    )


class TestLeaderElection:
    @pytest.mark.asyncio
    async def test_acquire_leadership(self):
        redis = faredis.FakeRedis()
        leader = LeaderElection(redis, "inst-a", _cfg())
        await leader._try_acquire()
        assert leader.is_leader is True

    @pytest.mark.asyncio
    async def test_fencing_token_increments_on_acquire(self):
        redis = faredis.FakeRedis()
        leader = LeaderElection(redis, "inst-a", _cfg())
        await leader._try_acquire()
        assert leader.fencing_token >= 1

    @pytest.mark.asyncio
    async def test_renew_retains_leadership(self):
        redis = faredis.FakeRedis()
        leader = LeaderElection(redis, "inst-a", _cfg())
        await leader._try_acquire()
        assert leader.is_leader
        # Call again — should renew TTL, remain leader
        await leader._try_acquire()
        assert leader.is_leader

    @pytest.mark.asyncio
    async def test_standby_does_not_acquire_when_leader_exists(self):
        redis = faredis.FakeRedis()
        leader_a = LeaderElection(redis, "inst-a", _cfg())
        leader_b = LeaderElection(redis, "inst-b", _cfg())
        await leader_a._try_acquire()
        await leader_b._try_acquire()
        assert leader_a.is_leader is True
        assert leader_b.is_leader is False

    @pytest.mark.asyncio
    async def test_leadership_lost_when_key_stolen(self):
        redis = faredis.FakeRedis()
        leader = LeaderElection(redis, "inst-a", _cfg())
        await leader._try_acquire()
        assert leader.is_leader
        # Simulate key stolen by another instance
        await redis.set(leader._config.leader_key, "inst-b")
        await leader._try_acquire()
        assert leader.is_leader is False

    @pytest.mark.asyncio
    async def test_assert_leader_raises_when_standby(self):
        redis = faredis.FakeRedis()
        leader = LeaderElection(redis, "inst-b", _cfg())
        # inst-a holds leadership
        await redis.set(
            leader._config.leader_key, "inst-a", ex=leader._config.leader_ttl
        )
        await leader._try_acquire()
        with pytest.raises(LeadershipError):
            leader.assert_leader()

    @pytest.mark.asyncio
    async def test_close_releases_leadership(self):
        redis = faredis.FakeRedis()
        leader = LeaderElection(redis, "inst-a", _cfg())
        await leader._try_acquire()
        assert leader.is_leader
        await leader.close()
        # Key should be deleted
        val = await redis.get(leader._config.leader_key)
        assert val is None

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        redis = faredis.FakeRedis()
        leader = LeaderElection(redis, "inst-a", _cfg())
        await leader.close()  # should not raise even if never started
        await leader.close()

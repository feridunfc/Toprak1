from __future__ import annotations

from types import SimpleNamespace

import pytest

from hfa_control.shard import OWNER_TTL, ShardOwnershipManager
from hfa_worker.main import WorkerService
from hfa_worker.task_executor import TaskExecutionResult


class LegacyExecutorProbe:
    async def execute(self, event):
        return SimpleNamespace(
            status="done",
            payload={},
            cost_cents=0,
            tokens_used=0,
            error="",
        )


class CanonicalExecutorProbe:
    async def execute(self, ctx):
        return TaskExecutionResult(ok=True, output={})


class PartialProductionRedis:
    async def set(self, *args, **kwargs):
        return True

    async def get(self, *args, **kwargs):
        return None

    async def hset(self, *args, **kwargs):
        return 1

    # Intentionally no expire(): production must fail closed.


class CompleteProductionRedis(PartialProductionRedis):
    async def expire(self, *args, **kwargs):
        return 1


class OwnerChangesAfterReadRedis:
    def __init__(self) -> None:
        # The authoritative value at atomic-renew time is foreign.  A legacy
        # split GET/EXPIRE implementation receives a stale own-group read.
        self.owner = "foreign-group"
        self.expire_calls = 0
        self.eval_calls = 0

    async def get(self, key):
        return "group-79"

    async def expire(self, key, ttl):
        self.expire_calls += 1
        return 1

    async def eval(self, *args):
        self.eval_calls += 1
        return 1 if self.owner == "group-79" else 0

    async def script_load(self, script):
        return "renew-sha"

    async def evalsha(self, *args):
        self.eval_calls += 1
        return 1 if self.owner == "group-79" else 0


class ExpireRejectedRedis:
    async def get(self, key):
        return "group-79"

    async def expire(self, key, ttl):
        return 0

    async def eval(self, *args):
        return 0

    async def script_load(self, script):
        return "renew-sha"

    async def evalsha(self, *args):
        return 0


def _production_config(**overrides):
    config = {
        "production": True,
        "worker_id": "worker-fencing-79",
        "worker_group": "group-79",
        "shards": [1],
        "executor": LegacyExecutorProbe(),
        "task_executor": CanonicalExecutorProbe(),
    }
    config.update(overrides)
    return config


def test_production_never_silently_disables_shard_leases() -> None:
    with pytest.raises(
        (TypeError, ValueError, RuntimeError),
        match="(?i)redis|shard|lease|expire",
    ):
        WorkerService(
            PartialProductionRedis(),
            _production_config(),
        )


def test_shard_renew_interval_must_be_safely_below_ttl() -> None:
    with pytest.raises(
        ValueError,
        match="(?i)shard.*renew|interval|ttl",
    ):
        WorkerService(
            CompleteProductionRedis(),
            _production_config(shard_renew_interval=float(OWNER_TTL)),
        )


@pytest.mark.asyncio
async def test_atomic_shard_renew_rejects_owner_change() -> None:
    redis = OwnerChangesAfterReadRedis()
    manager = ShardOwnershipManager(redis, SimpleNamespace())

    renewed = await manager.renew_shard(1, "group-79")

    assert renewed is False
    assert redis.owner == "foreign-group"
    assert redis.expire_calls == 0, (
        "Renewal must not issue a non-atomic EXPIRE after a separate GET"
    )


@pytest.mark.asyncio
async def test_failed_expire_is_not_reported_as_renewed() -> None:
    manager = ShardOwnershipManager(
        ExpireRejectedRedis(),
        SimpleNamespace(),
    )

    renewed = await manager.renew_shard(1, "group-79")

    assert renewed is False

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from hfa.config.keys import RedisKey
from hfa_worker.drain import DrainManager
from hfa_worker.main import WorkerService


class RedisShardProbe:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.set_calls: list[tuple[str, str, bool, int | None]] = []
        self.expire_calls: list[tuple[str, int]] = []
        self.delete_calls: list[str] = []
        self.xadd_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, *, nx=False, ex=None):
        self.set_calls.append((key, value, bool(nx), ex))
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def hset(self, *args, **kwargs):
        return 1

    async def expire(self, key, ttl):
        self.expire_calls.append((key, int(ttl)))
        return key in self.values

    async def eval(self, script, numkeys, *args):
        if numkeys == 1:
            key, worker_group, _ttl = args
        elif numkeys == 2:
            key, _owners_key, worker_group, _ttl, _shard = args
        else:
            raise AssertionError(
                f"Unexpected shard Lua key count: {numkeys}"
            )

        return 1 if self.values.get(key) == worker_group else 0

    async def delete(self, key):
        self.delete_calls.append(key)
        return 1 if self.values.pop(key, None) is not None else 0

    async def xgroup_create(self, *args, **kwargs):
        return True

    async def xadd(self, *args, **kwargs):
        self.xadd_calls.append((args, kwargs))
        return "1-0"


class LegacyExecutorProbe:
    async def execute(self, event):
        return SimpleNamespace(
            status="done",
            payload={},
            cost_cents=0,
            tokens_used=0,
            error="",
        )


class CanonicalTaskExecutorProbe:
    async def execute(self, ctx):
        return SimpleNamespace(ok=True, output={}, error="")


class DagLuaProbe:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def initialise(self) -> None:
        self.events.append("dag_lua.initialise")


class ConsumerLifecycleProbe:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.prepare_calls = 0
        self.start_calls = 0
        self.close_calls = 0
        self._pulling = True

    @property
    def inflight_count(self) -> int:
        return 0

    @property
    def is_draining(self) -> bool:
        return not self._pulling

    async def prepare_consumer_groups(self) -> None:
        self.prepare_calls += 1
        self.events.append("consumer.prepare_groups")

    async def start(self) -> None:
        self.start_calls += 1
        self._pulling = True
        self.events.append("consumer.start")

    async def close(self) -> None:
        self.close_calls += 1
        self._pulling = False
        self.events.append("consumer.close")

    def stop_pulling(self) -> None:
        self._pulling = False


class HeartbeatProbe:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.start_calls = 0
        self.close_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        self.events.append("heartbeat.start")

    async def close(self) -> None:
        self.close_calls += 1
        self.events.append("heartbeat.close")


class DrainNoopProbe:
    async def start_drain(self, reason: str = "", timeout: float = 0.0) -> None:
        return None

    def reset(self) -> None:
        return None


class ShardManagerProbe:
    def __init__(
        self,
        events: list[str],
        *,
        claim_results: dict[int, bool] | None = None,
        renew_results: dict[int, list[bool]] | None = None,
    ) -> None:
        self.events = events
        self.claim_results = dict(claim_results or {})
        self.renew_results = {
            shard: list(results)
            for shard, results in (renew_results or {}).items()
        }
        self.claim_calls: list[tuple[int, str]] = []
        self.renew_calls: list[tuple[int, str]] = []

    async def claim_shard(self, shard: int, worker_group: str) -> bool:
        self.claim_calls.append((shard, worker_group))
        self.events.append(f"shard.claim:{shard}")
        return self.claim_results.get(shard, True)

    async def renew_shard(self, shard: int, worker_group: str) -> bool:
        self.renew_calls.append((shard, worker_group))
        self.events.append(f"shard.renew:{shard}")
        results = self.renew_results.get(shard)
        if results:
            return results.pop(0)
        return True


def _service(
    redis: RedisShardProbe,
    *,
    shards: list[int] | None = None,
    shard_renew_interval: float = 0.01,
) -> WorkerService:
    return WorkerService(
        redis,
        {
            "production": True,
            "worker_id": "worker-shard-79",
            "worker_group": "group-79",
            "region": "eu-west-1",
            "shards": list(shards or [1]),
            "shard_renew_interval": shard_renew_interval,
            "executor": LegacyExecutorProbe(),
            "task_executor": CanonicalTaskExecutorProbe(),
        },
    )


def _install_lifecycle(
    service: WorkerService,
    events: list[str],
    shard_manager: ShardManagerProbe,
) -> tuple[ConsumerLifecycleProbe, HeartbeatProbe]:
    consumer = ConsumerLifecycleProbe(events)
    heartbeat = HeartbeatProbe(events)

    service._dag_lua = DagLuaProbe(events)
    service._consumer = consumer
    service._heartbeat = heartbeat
    service._drain_manager = DrainNoopProbe()
    service._shard_manager = shard_manager

    return consumer, heartbeat


async def _wait_until(
    predicate,
    *,
    timeout: float = 0.5,
    interval: float = 0.01,
) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return bool(predicate())


@pytest.mark.asyncio
async def test_consumer_groups_are_prepared_before_first_heartbeat() -> None:
    redis = RedisShardProbe()
    events: list[str] = []
    service = _service(redis)
    _install_lifecycle(service, events, ShardManagerProbe(events))

    await service.start()

    try:
        assert events.index("consumer.prepare_groups") < events.index(
            "heartbeat.start"
        )
        assert events.index("heartbeat.start") < events.index("consumer.start")
    finally:
        await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_production_start_confirms_all_shard_group_leases() -> None:
    redis = RedisShardProbe()
    events: list[str] = []
    service = _service(redis, shards=[1, 2])
    manager = ShardManagerProbe(events)
    _install_lifecycle(service, events, manager)

    await service.start()

    try:
        assert manager.claim_calls == [
            (1, "group-79"),
            (2, "group-79"),
        ]
        assert events == [
            "dag_lua.initialise",
            "consumer.prepare_groups",
            "shard.claim:1",
            "shard.claim:2",
            "heartbeat.start",
            "consumer.start",
        ]
    finally:
        await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_existing_same_group_shard_lease_is_accepted() -> None:
    redis = RedisShardProbe()
    owner_key = RedisKey.cp_shard_owner(1)
    redis.values[owner_key] = "group-79"

    events: list[str] = []
    service = _service(redis)
    manager = ShardManagerProbe(
        events,
        claim_results={1: False},
        renew_results={1: [True]},
    )
    _install_lifecycle(service, events, manager)

    await service.start()

    try:
        assert service.is_ready is True
        assert manager.claim_calls == [(1, "group-79")]
        assert manager.renew_calls
    finally:
        await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_foreign_group_shard_lease_blocks_startup() -> None:
    redis = RedisShardProbe()
    redis.values[RedisKey.cp_shard_owner(1)] = "foreign-group"

    events: list[str] = []
    service = _service(redis)
    manager = ShardManagerProbe(
        events,
        claim_results={1: False},
        renew_results={1: [False]},
    )
    consumer, heartbeat = _install_lifecycle(service, events, manager)

    with pytest.raises(RuntimeError, match="(?i)shard|lease|owner"):
        await service.start()

    assert service.is_ready is False
    assert consumer.start_calls == 0
    assert heartbeat.start_calls == 0


@pytest.mark.asyncio
async def test_shard_lease_loss_clears_readiness() -> None:
    redis = RedisShardProbe()
    events: list[str] = []
    service = _service(redis, shard_renew_interval=0.01)
    manager = ShardManagerProbe(
        events,
        renew_results={1: [False]},
    )
    _install_lifecycle(service, events, manager)

    await service.start()

    try:
        cleared = await _wait_until(lambda: service.is_ready is False)
        assert cleared is True
        assert service.last_failure is not None
        assert manager.renew_calls
    finally:
        await service.close(drain_timeout=0)


@pytest.mark.asyncio
async def test_stopping_one_worker_does_not_delete_group_lease() -> None:
    redis = RedisShardProbe()
    owner_key = RedisKey.cp_shard_owner(1)
    redis.values[owner_key] = "group-79"

    consumer = ConsumerLifecycleProbe([])
    manager = DrainManager(
        redis=redis,
        worker_id="worker-a-79",
        worker_group="group-79",
        shards=[1],
        consumer=consumer,
    )

    await manager.start_drain(reason="shutdown", timeout=0)

    assert redis.values.get(owner_key) == "group-79"
    assert owner_key not in redis.delete_calls

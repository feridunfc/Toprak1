from __future__ import annotations

from hfa_control.models import ControlPlaneConfig, WorkerStatus
from hfa_control.registry import WorkerRegistry
from hfa_worker.heartbeat import WorkerHeartbeatPublisher


def _bytes(value) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


class RedisProjectionProbe:
    def __init__(self) -> None:
        self.stream_messages: list[tuple[str, dict]] = []
        self.hashes: dict[str, dict[bytes, bytes]] = {}
        self.sets: dict[str, set[bytes]] = {}

    async def xadd(self, stream, fields, **kwargs):
        self.stream_messages.append((str(stream), dict(fields)))
        return b"1-0"

    async def hget(self, key, field):
        return self.hashes.get(str(key), {}).get(_bytes(field))

    async def hgetall(self, key):
        return dict(self.hashes.get(str(key), {}))

    async def hset(self, key, field=None, value=None, mapping=None):
        target = self.hashes.setdefault(str(key), {})
        if mapping is not None:
            for map_key, map_value in mapping.items():
                target[_bytes(map_key)] = _bytes(map_value)
        elif field is not None:
            target[_bytes(field)] = _bytes(value)
        return 1

    async def expire(self, *args, **kwargs):
        return 1

    async def sadd(self, key, *values):
        target = self.sets.setdefault(str(key), set())
        target.update(_bytes(value) for value in values)
        return len(values)

    async def smembers(self, key):
        return set(self.sets.get(str(key), set()))


async def _redis_round_trip_message(redis: RedisProjectionProbe) -> dict:
    assert redis.stream_messages
    _stream, fields = redis.stream_messages[-1]
    return {
        _bytes(key): _bytes(value)
        for key, value in fields.items()
    }


async def _publish_and_project_draining_worker(
    redis: RedisProjectionProbe,
    registry: WorkerRegistry,
) -> None:
    publisher = WorkerHeartbeatPublisher(
        redis=redis,
        worker_id="worker-drain-red-79-8",
        worker_group="group-drain-red-79-8",
        region="region-drain-red-79-8",
        shards=[0],
        capacity=1,
        inflight_fn=lambda: 0,
        is_draining_fn=lambda: True,
        version="79.8-red",
        capabilities=["fake"],
    )

    await publisher._publish()
    message = await _redis_round_trip_message(redis)
    assert message[b"is_draining"] == b"1"
    await registry._handle(message)


import pytest


@pytest.mark.asyncio
async def test_draining_heartbeat_projects_worker_as_unschedulable() -> None:
    redis = RedisProjectionProbe()
    registry = WorkerRegistry(
        redis,
        ControlPlaneConfig(
            instance_id="cp-drain-red-79-8",
            worker_heartbeat_ttl=30.0,
            registry_ttl=60,
        ),
    )

    await _publish_and_project_draining_worker(redis, registry)

    profile = await registry.get_worker("worker-drain-red-79-8")
    assert profile.status is WorkerStatus.DRAINING, (
        "Worker heartbeat already carries is_draining=1, but WorkerRegistry "
        "currently drops that field and republishes the worker as HEALTHY."
    )

    schedulable = await registry.list_schedulable_workers(
        region="region-drain-red-79-8"
    )
    assert not schedulable, (
        "A worker that has stopped pulling must disappear from scheduler "
        "eligibility before shutdown waits for inflight work."
    )

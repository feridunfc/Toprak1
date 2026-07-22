from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from urllib.parse import urlparse

import pytest
import redis.asyncio as redis_async

from hfa.config.keys import RedisKey
from hfa.dag.schema import DagRedisKey, DagTaskSeed
from hfa_control.models import ControlPlaneConfig
from hfa_control.registry import WorkerRegistry
from hfa_control.scheduler import build_production_scheduler
from hfa_control.shard import ShardOwnershipManager
from hfa_worker.fake_executor import FakeExecutor
from hfa_worker.heartbeat import WorkerHeartbeatPublisher
from hfa_worker.main import WorkerService


REAL_REDIS_FLAG = "HFA_REAL_REDIS_E2E"
REDIS_URL = os.environ.get(
    "HFA_REAL_REDIS_E2E_URL",
    "redis://localhost:6380/15",
)
CONSUMER_GROUP = "worker_consumers"


class RecordingFakeExecutor(FakeExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.invocations: list[str] = []

    async def execute(self, event):
        self.invocations.append(
            str(getattr(event, "task_id", "") or "")
        )
        return await super().execute(event)


def _real_redis_enabled() -> bool:
    return os.environ.get(REAL_REDIS_FLAG, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _assert_dedicated_redis_url(redis_url: str) -> None:
    parsed = urlparse(redis_url)
    path = (parsed.path or "").strip("/")
    try:
        database = int(path or "0")
    except ValueError as exc:
        raise AssertionError(
            f"Real Redis RED requires a numeric database: {redis_url}"
        ) from exc

    allow_db_zero = os.environ.get(
        "HFA_REAL_REDIS_E2E_ALLOW_DB_ZERO",
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    assert database != 0 or allow_db_zero, (
        "Refusing to FLUSHDB Redis database 0. Use a dedicated database."
    )


async def _wait_until(
    predicate,
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_error: BaseException | None = None
    while loop.time() < deadline:
        try:
            result = await predicate()
            if result:
                return result
        except BaseException as exc:
            last_error = exc
        await asyncio.sleep(interval)

    if last_error is not None:
        raise AssertionError(
            f"Timed out waiting for RED condition; last error: {last_error}"
        ) from last_error
    raise AssertionError("Timed out waiting for RED condition")


async def _worker_visible(
    registry: WorkerRegistry,
    worker_id: str,
):
    try:
        profile = await registry.get_worker(worker_id)
    except Exception:
        return None
    return profile if profile.worker_id == worker_id else None


async def _task_state(redis, task_id: str) -> str:
    raw = await redis.get(DagRedisKey.task_state(task_id))
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw or "")


async def _pending_count(redis, stream: str) -> int:
    try:
        pending = await redis.xpending(stream, CONSUMER_GROUP)
    except Exception:
        return 0
    if isinstance(pending, dict):
        return int(pending.get("pending", 0))
    if isinstance(pending, (tuple, list)) and pending:
        return int(pending[0])
    return int(pending or 0)


async def _task_requested_exists(redis, stream: str, task_id: str):
    entries = await redis.xrevrange(stream, count=100)
    for message_id, raw in entries:
        data = {
            (
                key.decode("utf-8", errors="replace")
                if isinstance(key, bytes)
                else str(key)
            ): (
                value.decode("utf-8", errors="replace")
                if isinstance(value, bytes)
                else str(value)
            )
            for key, value in dict(raw).items()
        }
        if (
            data.get("event_type") == "TaskRequested"
            and data.get("task_id") == task_id
        ):
            return message_id, data
    return None


@pytest.mark.asyncio
async def test_same_group_wrong_consumer_delivery_cannot_strand_task() -> None:
    if not _real_redis_enabled():
        pytest.skip(
            f"Set {REAL_REDIS_FLAG}=1 to run the two-worker routing RED."
        )

    _assert_dedicated_redis_url(REDIS_URL)
    redis = redis_async.from_url(REDIS_URL, decode_responses=False)
    try:
        await redis.ping()
    except Exception as exc:
        await redis.aclose()
        pytest.fail(f"Dedicated real Redis unavailable at {REDIS_URL}: {exc}")

    suffix = uuid.uuid4().hex[:10]
    worker_a_id = f"worker-a-routing-red-{suffix}"
    worker_b_id = f"worker-b-routing-red-{suffix}"
    worker_group = f"group-routing-red-{suffix}"
    tenant_id = f"tenant-routing-red-{suffix}"
    run_id = f"run-routing-red-{suffix}"
    task_id = f"task-routing-red-{suffix}"
    scheduler_epoch = f"epoch-routing-red-{suffix}"
    region = f"region-routing-red-{suffix}"
    shard = 0
    shard_stream = RedisKey.stream_shard(shard)

    config = ControlPlaneConfig(
        instance_id=f"cp-routing-red-{suffix}",
        region=region,
        stream_shards=1,
        scheduler_loop_max_dispatches=1,
        scheduler_loop_idle_sleep_ms=10_000,
        scheduler_loop_error_sleep_ms=10,
        scheduler_loop_max_failures=1,
        scheduler_reservation_ttl_seconds=30,
        worker_heartbeat_ttl=30.0,
        registry_ttl=60,
    )

    registry = WorkerRegistry(redis, config)
    shards = ShardOwnershipManager(redis, config)
    scheduler = build_production_scheduler(
        redis=redis,
        config=config,
        registry=registry,
        shards=shards,
        event_store=None,
    )

    executor_a = RecordingFakeExecutor()
    executor_b = RecordingFakeExecutor()
    worker_a = WorkerService(
        redis,
        {
            "production": True,
            "worker_id": worker_a_id,
            "worker_group": worker_group,
            "region": region,
            "shards": [shard],
            "capacity": 1,
            "version": "79.8-routing-red",
            "capabilities": ["fake"],
            "executor": executor_a,
            "shard_manager": shards,
            "shard_renew_interval": 1.0,
        },
    )
    worker_b = WorkerService(
        redis,
        {
            "production": True,
            "worker_id": worker_b_id,
            "worker_group": worker_group,
            "region": region,
            "shards": [shard],
            "capacity": 1,
            "version": "79.8-routing-red",
            "capabilities": ["fake"],
            "executor": executor_b,
            "shard_manager": shards,
            "shard_renew_interval": 1.0,
        },
    )

    worker_a_started = False
    worker_b_started = False

    await redis.flushdb()
    try:
        await registry.start()

        # Register only worker A before scheduling, without starting its
        # consumer. This makes scheduler reservation ownership deterministic.
        publisher_a = WorkerHeartbeatPublisher(
            redis=redis,
            worker_id=worker_a_id,
            worker_group=worker_group,
            region=region,
            shards=[shard],
            capacity=1,
            inflight_fn=lambda: 0,
            is_draining_fn=lambda: False,
            version="79.8-routing-red",
            capabilities=["fake"],
        )
        await publisher_a._publish()
        await _wait_until(
            lambda: _worker_visible(registry, worker_a_id)
        )

        assert await shards.claim_shard(shard, worker_group) is True

        await scheduler.start(scheduler_epoch=scheduler_epoch)

        seed = DagTaskSeed(
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            agent_type="fake",
            worker_group=worker_group,
            priority=5,
            admitted_at=time.time(),
            dependency_count=0,
            input_payload={
                "task_id": task_id,
                "run_id": run_id,
                "prompt": "same-group routing red",
            },
            required_capabilities=[],
            region=region,
            policy="LEAST_LOADED",
            payload_json=json.dumps(
                {
                    "task_id": task_id,
                    "run_id": run_id,
                    "tenant_id": tenant_id,
                    "prompt": "same-group routing red",
                },
                sort_keys=True,
            ),
        )
        admitted = await scheduler.composition.dag_lua.task_admit(seed)
        assert admitted.admitted is True
        assert admitted.ready is True

        await scheduler.tick(max_dispatches=1)
        _message_id, message = await _wait_until(
            lambda: _task_requested_exists(
                redis,
                shard_stream,
                task_id,
            )
        )
        assert message["worker_group"] == worker_group

        # Start B first. With the current shared consumer group, B receives
        # the message reserved for A and leaves it in B's PEL.
        await worker_b.start()
        worker_b_started = True

        loop = asyncio.get_running_loop()
        wrong_delivery_deadline = loop.time() + 1.0
        while loop.time() < wrong_delivery_deadline:
            if (
                await _pending_count(redis, shard_stream) > 0
                or await _task_state(redis, task_id) == "done"
            ):
                break
            await asyncio.sleep(0.05)

        # Start the scheduler-selected worker after B had the first chance.
        # Correct designs may transfer ownership, reroute, or use a per-worker
        # delivery path, but the task must not remain stranded.
        await worker_a.start()
        worker_a_started = True

        completed = False
        completion_deadline = loop.time() + 3.0
        while loop.time() < completion_deadline:
            if await _task_state(redis, task_id) == "done":
                completed = True
                break
            await asyncio.sleep(0.05)

        pending = await _pending_count(redis, shard_stream)
        state = await _task_state(redis, task_id)

        assert completed, (
            "A TaskRequested message reserved for worker A was delivered to "
            "same-group worker B and became stranded. "
            f"state={state!r} pending={pending} "
            f"worker_a_invocations={executor_a.invocations!r} "
            f"worker_b_invocations={executor_b.invocations!r}"
        )
        assert pending == 0
    finally:
        await scheduler.close()
        if worker_a_started:
            await worker_a.close(drain_timeout=0)
        if worker_b_started:
            await worker_b.close(drain_timeout=0)
        await registry.close()
        await redis.flushdb()
        await redis.aclose()

from __future__ import annotations

import asyncio
import inspect
import json
import os
import time
import uuid
from pathlib import Path
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
from hfa_worker.main import WorkerService

REAL_REDIS_FLAG = "HFA_REAL_REDIS_E2E"
REDIS_URL = os.environ.get("HFA_REAL_REDIS_E2E_URL", "redis://localhost:6380/15")
CONSUMER_GROUP = "worker_consumers"


class RecordingFakeExecutor(FakeExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.invocations: list[dict[str, object]] = []

    async def execute(self, event):
        self.invocations.append(
            {
                "task_id": str(getattr(event, "task_id", "") or ""),
                "run_id": str(getattr(event, "run_id", "") or ""),
                "tenant_id": str(getattr(event, "tenant_id", "") or ""),
            }
        )
        return await super().execute(event)


def _real_redis_enabled() -> bool:
    return os.environ.get(REAL_REDIS_FLAG, "").strip().lower() in {
        "1", "true", "yes", "on"
    }


def _assert_dedicated_redis_url(redis_url: str) -> None:
    parsed = urlparse(redis_url)
    path = (parsed.path or "").strip("/")
    try:
        database = int(path or "0")
    except ValueError as exc:
        raise AssertionError(
            f"Real Redis E2E requires an explicit numeric database: {redis_url}"
        ) from exc
    allow_db_zero = os.environ.get(
        "HFA_REAL_REDIS_E2E_ALLOW_DB_ZERO", ""
    ).strip().lower() in {"1", "true", "yes", "on"}
    assert database != 0 or allow_db_zero, (
        "Refusing to FLUSHDB Redis database 0. Use a dedicated database such as "
        "redis://localhost:6380/15, or explicitly set "
        "HFA_REAL_REDIS_E2E_ALLOW_DB_ZERO=1."
    )


async def _wait_until(predicate, *, timeout: float = 8.0, interval: float = 0.05):
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
            f"Timed out waiting for canonical E2E condition; last error: {last_error}"
        ) from last_error
    raise AssertionError("Timed out waiting for canonical E2E condition")


async def _matching_task_requested(redis, stream: str, task_id: str):
    entries = await redis.xrevrange(stream, count=100)
    for message_id, raw in entries:
        data = {
            (key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)):
            (value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value))
            for key, value in dict(raw).items()
        }
        if data.get("event_type") == "TaskRequested" and data.get("task_id") == task_id:
            return message_id, data
    return None


async def _worker_projection_visible(registry: WorkerRegistry, worker_id: str):
    try:
        profile = await registry.get_worker(worker_id)
    except Exception:
        return None
    return profile if profile.worker_id == worker_id else None


async def _task_is_done(redis, task_id: str):
    value = await redis.get(DagRedisKey.task_state(task_id))
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value == "done"


def test_real_redis_acceptance_harness_forbids_shortcuts() -> None:
    source = inspect.getsource(
        test_real_redis_production_scheduler_to_worker_canonical_e2e
    )

    required = (
        "build_production_scheduler(",
        "WorkerRegistry(",
        "ShardOwnershipManager(",
        "WorkerService(",
        ".task_admit(",
        ".tick(",
        "TaskRequested",
        "task_claim_start.lua",
        "task_complete.lua",
        "xpending",
    )
    for token in required:
        assert token in source, f"required production step absent: {token}"

    forbidden = (
        "WorkerConsumer(",
        "TaskConsumer(",
        "._process_message(",
        ".xadd(",
        ".hset(",
        "worker_reservation(",
        "task_reservation_owner(",
    )
    for token in forbidden:
        assert token not in source, (
            f"forbidden acceptance shortcut present: {token}"
        )



@pytest.mark.asyncio
async def test_real_redis_production_scheduler_to_worker_canonical_e2e() -> None:
    if not _real_redis_enabled():
        pytest.skip(
            f"Set {REAL_REDIS_FLAG}=1 to run the dedicated real-Redis canonical production E2E acceptance."
        )

    _assert_dedicated_redis_url(REDIS_URL)
    redis = redis_async.from_url(REDIS_URL, decode_responses=False)
    try:
        await redis.ping()
    except Exception as exc:
        await redis.aclose()
        pytest.fail(f"Dedicated real Redis unavailable at {REDIS_URL}: {exc}")

    suffix = uuid.uuid4().hex[:10]
    worker_id = f"worker-e2e-79-7-{suffix}"
    worker_group = f"group-e2e-79-7-{suffix}"
    tenant_id = f"tenant-e2e-79-7-{suffix}"
    run_id = f"run-e2e-79-7-{suffix}"
    task_id = f"task-e2e-79-7-{suffix}"
    scheduler_epoch = f"epoch-e2e-79-7-{suffix}"
    shard = 0
    region = "e2e-79-7"
    shard_stream = RedisKey.stream_shard(shard)

    cp_config = ControlPlaneConfig(
        instance_id=f"cp-e2e-79-7-{suffix}",
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

    registry = WorkerRegistry(redis, cp_config)
    shards = ShardOwnershipManager(redis, cp_config)
    executor = RecordingFakeExecutor()
    worker = WorkerService(
        redis,
        {
            "production": True,
            "worker_id": worker_id,
            "worker_group": worker_group,
            "region": region,
            "shards": [shard],
            "capacity": 1,
            "version": "79.7-e2e",
            "capabilities": ["fake"],
            "executor": executor,
            "shard_manager": shards,
            "shard_renew_interval": 1.0,
        },
    )
    scheduler = build_production_scheduler(
        redis=redis,
        config=cp_config,
        registry=registry,
        shards=shards,
        event_store=None,
    )

    await redis.flushdb()
    try:
        await registry.start()
        await worker.start()

        profile = await _wait_until(
            lambda: _worker_projection_visible(registry, worker_id)
        )
        assert profile.worker_group == worker_group
        assert shard in profile.shards
        assert profile.capacity == 1

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
                "prompt": "canonical production e2e",
            },
            required_capabilities=[],
            region=region,
            policy="LEAST_LOADED",
            payload_json=json.dumps(
                {
                    "task_id": task_id,
                    "run_id": run_id,
                    "tenant_id": tenant_id,
                    "prompt": "canonical production e2e",
                },
                sort_keys=True,
            ),
        )

        admit_result = await scheduler.composition.dag_lua.task_admit(seed)
        assert admit_result.admitted is True
        assert admit_result.ready is True

        await scheduler.tick(max_dispatches=1)

        message_id, message = await _wait_until(
            lambda: _matching_task_requested(redis, shard_stream, task_id)
        )
        assert message_id
        assert message["event_type"] == "TaskRequested"
        assert message["task_id"] == task_id
        assert message["run_id"] == run_id
        assert message["scheduler_epoch"] == scheduler_epoch
        assert message["worker_group"] == worker_group

        await _wait_until(lambda: _task_is_done(redis, task_id))

        meta_raw = await redis.hgetall(DagRedisKey.task_meta(task_id))
        meta = {
            (key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)):
            (value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value))
            for key, value in meta_raw.items()
        }
        assert meta["terminal_state"] == "done"
        assert meta["worker_instance_id"] == worker_id
        assert meta["scheduler_epoch"] == scheduler_epoch
        assert int(meta["claim_epoch"]) >= 1
        assert meta["completed_at_ms"]

        output = await redis.get(DagRedisKey.task_output(task_id))
        assert output not in (None, b"", "")
        assert executor.invocations
        assert executor.invocations[0]["run_id"] == run_id

        pending = await redis.xpending(shard_stream, CONSUMER_GROUP)
        if isinstance(pending, dict):
            pending_count = int(pending.get("pending", 0))
        elif isinstance(pending, (tuple, list)) and pending:
            pending_count = int(pending[0])
        else:
            pending_count = int(pending or 0)
        assert pending_count == 0

        state = await redis.get(DagRedisKey.task_state(task_id))
        if isinstance(state, bytes):
            state = state.decode("utf-8", errors="replace")
        assert state == "done"

        claim_source = (
            Path(__file__).parents[2] / "hfa-core" / "src" / "hfa" / "lua" / "task_claim_start.lua"
        )
        complete_source = claim_source.with_name("task_complete.lua")
        assert claim_source.exists()
        assert complete_source.exists()
    finally:
        await scheduler.close()
        await worker.close(drain_timeout=0)
        await registry.close()
        await redis.flushdb()
        await redis.aclose()

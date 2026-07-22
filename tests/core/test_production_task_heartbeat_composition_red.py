from __future__ import annotations

from types import SimpleNamespace

from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_worker.main import WorkerService
from hfa_worker.task_executor import TaskExecutionResult


class RedisProbe:
    async def set(self, *args, **kwargs):
        return True

    async def get(self, *args, **kwargs):
        return None

    async def expire(self, *args, **kwargs):
        return 1

    async def hset(self, *args, **kwargs):
        return 1

    async def eval(self, *args, **kwargs):
        return 1


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
        return TaskExecutionResult(ok=True, output={"task_id": ctx.task_id})


def test_production_worker_composes_fenced_task_heartbeat_manager() -> None:
    redis = RedisProbe()
    service = WorkerService(
        redis,
        {
            "production": True,
            "worker_id": "worker-heartbeat-red-79-8",
            "worker_group": "group-heartbeat-red-79-8",
            "region": "region-heartbeat-red-79-8",
            "shards": [0],
            "capacity": 1,
            "capabilities": ["fake"],
            "executor": LegacyExecutorProbe(),
            "task_executor": CanonicalExecutorProbe(),
        },
    )

    task_consumer = getattr(service, "_task_consumer", None)
    assert task_consumer is not None

    heartbeat_manager = getattr(task_consumer, "_heartbeat_manager", None)
    assert isinstance(heartbeat_manager, TaskHeartbeatManager), (
        "Production WorkerService must inject TaskHeartbeatManager into the "
        "canonical TaskConsumer. Without it, long-running tasks become stale "
        "while still executing."
    )
    assert getattr(heartbeat_manager, "_redis", None) is redis

    interval_ms = int(
        getattr(task_consumer, "_heartbeat_interval_ms", 0) or 0
    )
    stale_after_ms = int(
        getattr(
            getattr(heartbeat_manager, "_policy", None),
            "stale_after_ms",
            0,
        )
        or 0
    )
    assert 0 < interval_ms < stale_after_ms, (
        "Canonical heartbeat interval must be positive and strictly below "
        "the stale-task threshold."
    )

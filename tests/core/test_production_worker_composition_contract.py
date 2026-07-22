from __future__ import annotations

from dataclasses import dataclass

import pytest

from hfa_control.dag_lua import DagLua
from hfa_control.task_claim import TaskClaimManager
from hfa_worker.main import WorkerService
from hfa_worker.task_consumer import TaskConsumer
from hfa_worker.task_executor import TaskExecutionResult


class RedisProbe:
    """Identity-only Redis probe for shallow production composition contracts."""

    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True

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


@dataclass
class LegacyExecutionResult:
    status: str = "done"
    payload: dict | None = None
    cost_cents: int = 0
    tokens_used: int = 0
    error: str = ""


class LegacyExecutorProbe:
    async def execute(self, event):
        return LegacyExecutionResult(payload={"run_id": getattr(event, "run_id", "")})


class CanonicalTaskExecutorProbe:
    async def execute(self, ctx):
        return TaskExecutionResult(ok=True, output={"task_id": ctx.task_id})


def _production_config(
    *,
    worker_id: str | None = "worker-prod-79",
    task_executor: object | None = None,
) -> dict:
    config = {
        "production": True,
        "worker_group": "group-prod-79",
        "region": "eu-west-1",
        "shards": [3],
        "capacity": 4,
        "capabilities": ["base", "python"],
        "executor": LegacyExecutorProbe(),
        "task_executor": task_executor or CanonicalTaskExecutorProbe(),
    }
    if worker_id is not None:
        config["worker_id"] = worker_id
    return config


def _build_service(
    *,
    redis: RedisProbe | None = None,
    worker_id: str | None = "worker-prod-79",
    task_executor: object | None = None,
) -> tuple[WorkerService, RedisProbe, object]:
    redis = redis or RedisProbe()
    task_executor = task_executor or CanonicalTaskExecutorProbe()
    service = WorkerService(
        redis,
        _production_config(worker_id=worker_id, task_executor=task_executor),
    )
    return service, redis, task_executor


def test_production_worker_service_constructs_canonical_task_graph() -> None:
    service, _, _ = _build_service()

    assert isinstance(getattr(service, "_dag_lua", None), DagLua)
    assert isinstance(getattr(service, "_task_claim_manager", None), TaskClaimManager)
    assert isinstance(getattr(service, "_task_consumer", None), TaskConsumer)


def test_production_worker_service_injects_task_consumer_into_worker_consumer() -> None:
    service, _, _ = _build_service()

    task_consumer = getattr(service, "_task_consumer", None)
    worker_consumer = getattr(service, "_consumer", None)

    assert task_consumer is not None
    assert worker_consumer is not None
    assert getattr(worker_consumer, "_task_consumer", None) is task_consumer


def test_production_worker_uses_one_dag_lua_for_claim_and_completion() -> None:
    service, _, _ = _build_service()

    dag_lua = getattr(service, "_dag_lua", None)
    claim_manager = getattr(service, "_task_claim_manager", None)
    task_consumer = getattr(service, "_task_consumer", None)

    assert dag_lua is not None
    assert claim_manager is not None
    assert task_consumer is not None
    assert getattr(claim_manager, "_dag_lua", None) is dag_lua
    assert getattr(task_consumer, "_completion_manager", None) is dag_lua


def test_production_worker_graph_preserves_shared_redis_identity() -> None:
    service, redis, _ = _build_service()

    components = {
        "service": service,
        "dag_lua": getattr(service, "_dag_lua", None),
        "worker_consumer": getattr(service, "_consumer", None),
        "heartbeat": getattr(service, "_heartbeat", None),
        "drain_manager": getattr(service, "_drain_manager", None),
    }

    missing = [name for name, component in components.items() if component is None]
    assert not missing, f"Missing production components: {missing}"

    mismatched = [
        name
        for name, component in components.items()
        if getattr(component, "_redis", None) is not redis
    ]
    assert not mismatched, f"Components not using injected shared Redis: {mismatched}"


@pytest.mark.parametrize("worker_id", ["", None])
def test_production_worker_identity_must_be_explicit_and_nonempty(
    worker_id: str | None,
) -> None:
    with pytest.raises((RuntimeError, ValueError), match="worker"):
        _build_service(worker_id=worker_id)


def test_production_task_consumer_uses_canonical_task_executor() -> None:
    task_executor = CanonicalTaskExecutorProbe()
    service, _, _ = _build_service(task_executor=task_executor)

    task_consumer = getattr(service, "_task_consumer", None)
    assert task_consumer is not None
    assert getattr(task_consumer, "_executor", None) is task_executor


def test_production_worker_service_is_borrower_of_shared_redis() -> None:
    service, redis, _ = _build_service()

    assert redis.closed is False
    assert getattr(service, "_redis", None) is redis

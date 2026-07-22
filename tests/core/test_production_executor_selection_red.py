from __future__ import annotations

from dataclasses import dataclass

import pytest

import hfa_worker.main as worker_main
from hfa_worker.main import WorkerService
from hfa_worker.process_root import config_from_env


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


@dataclass
class ExecutorProbe:
    async def execute(self, event):
        raise AssertionError("executor must not run during composition")


def test_env_process_root_requires_explicit_executor_mode() -> None:
    with pytest.raises(
        RuntimeError,
        match="WORKER_EXECUTOR_MODE",
    ):
        config_from_env(
            {
                "WORKER_ID": "worker-explicit-executor-79-13",
                "WORKER_GROUP": "group-explicit-executor-79-13",
                "WORKER_SHARDS": "0",
            }
        )


def test_production_service_cannot_silently_default_to_fake_executor(
    monkeypatch,
) -> None:
    factory_calls: list[dict] = []

    def build_executor_probe(config):
        factory_calls.append(dict(config))
        return ExecutorProbe()

    monkeypatch.setattr(
        worker_main,
        "build_executor",
        build_executor_probe,
    )

    with pytest.raises(
        ValueError,
        match="executor_mode",
    ):
        WorkerService(
            RedisProbe(),
            {
                "production": True,
                "worker_id": "worker-explicit-executor-79-13",
                "worker_group": "group-explicit-executor-79-13",
                "region": "region-explicit-executor-79-13",
                "shards": [0],
                "capacity": 1,
            },
        )

    assert factory_calls == [], (
        "Production composition must reject a missing executor selection "
        "before build_executor() can apply its development fake default."
    )


def test_explicit_executor_injection_remains_supported() -> None:
    service = WorkerService(
        RedisProbe(),
        {
            "production": True,
            "worker_id": "worker-injected-executor-79-13",
            "worker_group": "group-injected-executor-79-13",
            "region": "region-injected-executor-79-13",
            "shards": [0],
            "capacity": 1,
            "executor": ExecutorProbe(),
        },
    )

    assert getattr(service, "_executor", None) is None or (
        getattr(getattr(service, "_consumer", None), "_executor", None)
        is not None
    )

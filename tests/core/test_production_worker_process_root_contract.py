from __future__ import annotations

import importlib
import importlib.util
from typing import Any

import pytest


def _load_process_root():
    spec = importlib.util.find_spec("hfa_worker.process_root")
    assert spec is not None, (
        "Production worker process root is missing: "
        "expected hfa_worker.process_root"
    )
    return importlib.import_module("hfa_worker.process_root")


def test_production_worker_process_root_is_reachable() -> None:
    main_spec = importlib.util.find_spec("hfa_worker.__main__")
    assert main_spec is not None, (
        "python -m hfa_worker is not reachable: "
        "expected hfa_worker.__main__"
    )

    module = _load_process_root()
    run_worker_process = getattr(module, "run_worker_process", None)
    assert callable(run_worker_process), (
        "hfa_worker.process_root.run_worker_process() is required"
    )


@pytest.mark.asyncio
async def test_process_root_owns_redis_close(monkeypatch) -> None:
    module = _load_process_root()
    events: list[str] = []

    class RedisProbe:
        async def aclose(self) -> None:
            events.append("redis.close")

    redis = RedisProbe()

    def redis_factory(url: str):
        events.append(f"redis.create:{url}")
        return redis

    class ServiceProbe:
        def __init__(self, redis_arg: Any, config: dict[str, Any]) -> None:
            assert redis_arg is redis
            assert config["production"] is True
            events.append("service.init")

        async def start(self) -> None:
            events.append("service.start")

        async def close(self) -> None:
            events.append("service.close")

    async def wait_for_shutdown() -> None:
        events.append("shutdown.wait")

    monkeypatch.setattr(module, "WorkerService", ServiceProbe)

    await module.run_worker_process(
        redis_factory=redis_factory,
        config={
            "redis_url": "redis://worker-root-79:6379/0",
            "production": False,
            "worker_id": "worker-root-79",
            "worker_group": "group-root-79",
            "shards": [1],
        },
        wait_for_shutdown=wait_for_shutdown,
    )

    assert events == [
        "redis.create:redis://worker-root-79:6379/0",
        "service.init",
        "service.start",
        "shutdown.wait",
        "service.close",
        "redis.close",
    ]

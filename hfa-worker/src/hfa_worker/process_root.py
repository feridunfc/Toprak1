from __future__ import annotations

import asyncio
import inspect
import os
import signal
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import redis.asyncio as redis_async

from hfa_control.product_profile import (
    ProductMode,
    parse_product_mode,
)
from hfa_worker.main import WorkerService


RedisFactory = Callable[[str], Any]
ShutdownWaiter = Callable[[], Awaitable[None]]

_TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_ENV_VALUES = frozenset({"0", "false", "no", "off"})


def _parse_strict_bool_env(
    name: str,
    raw: str | None,
    *,
    default: bool,
) -> bool:
    if raw is None:
        return default

    normalized = str(raw).strip().lower()
    if normalized in _TRUE_ENV_VALUES:
        return True
    if normalized in _FALSE_ENV_VALUES:
        return False

    raise RuntimeError(
        f"{name} must be one of "
        "1,true,yes,on,0,false,no,off"
    )


def _parse_shards(raw: str) -> list[int]:
    values = [part.strip() for part in raw.split(",") if part.strip()]
    return [int(value) for value in values] if values else [0]


def config_from_env(
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    source = os.environ if env is None else env
    product_mode = parse_product_mode(
        source.get("HFA_PRODUCT_MODE")
    )
    configured_worker_group = str(
        source.get("WORKER_GROUP") or ""
    ).strip()
    if (
        product_mode is ProductMode.SINGLE_TASK_ALPHA
        and not configured_worker_group
    ):
        raise RuntimeError(
            "WORKER_GROUP is required for SINGLE_TASK_ALPHA"
        )

    executor_mode = str(
        source.get("WORKER_EXECUTOR_MODE") or ""
    ).strip()
    if not executor_mode:
        raise RuntimeError(
            "WORKER_EXECUTOR_MODE is required for the production worker "
            "process root"
        )

    return {
        "redis_url": source.get("REDIS_URL", "redis://localhost:6379/0"),
        "production": True,
        "product_mode": product_mode.value,
        "worker_id": source.get("WORKER_ID", ""),
        "worker_group": (
            configured_worker_group or "default"
        ),
        "region": source.get("WORKER_REGION", "us-east-1"),
        "shards": _parse_shards(source.get("WORKER_SHARDS", "0")),
        "capacity": int(source.get("WORKER_CAPACITY", "10")),
        "version": source.get("WORKER_VERSION", "0.0.0"),
        "executor_mode": executor_mode,
        "run_termination_binding_enabled": _parse_strict_bool_env(
            "WORKER_RUN_TERMINATION_BINDING",
            source.get("WORKER_RUN_TERMINATION_BINDING"),
            default=False,
        ),
        "canonical_task_admit_binding": _parse_strict_bool_env(
            "HFA_CANONICAL_TASK_ADMIT_BINDING",
            source.get("HFA_CANONICAL_TASK_ADMIT_BINDING"),
            default=False,
        ),
        "canonical_task_dispatch_binding": _parse_strict_bool_env(
            "HFA_CANONICAL_TASK_DISPATCH_BINDING",
            source.get("HFA_CANONICAL_TASK_DISPATCH_BINDING"),
            default=False,
        ),
        "canonical_task_claim_binding": _parse_strict_bool_env(
            "HFA_CANONICAL_TASK_CLAIM_BINDING",
            source.get("HFA_CANONICAL_TASK_CLAIM_BINDING"),
            default=False,
        ),
        "canonical_task_terminal_binding": _parse_strict_bool_env(
            "HFA_CANONICAL_TASK_TERMINAL_BINDING",
            source.get("HFA_CANONICAL_TASK_TERMINAL_BINDING"),
            default=False,
        ),
        "shard_renew_interval": float(
            source.get("WORKER_SHARD_RENEW_INTERVAL", "30")
        ),
    }


def _default_redis_factory(redis_url: str):
    return redis_async.from_url(redis_url)


async def _wait_for_shutdown_signal() -> None:
    event = asyncio.Event()
    loop = asyncio.get_running_loop()

    installed: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, event.set)
            installed.append(sig)
        except (NotImplementedError, RuntimeError):
            continue

    try:
        await event.wait()
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


async def run_worker_process(
    *,
    redis_factory: RedisFactory | None = None,
    config: Mapping[str, Any] | None = None,
    wait_for_shutdown: ShutdownWaiter | None = None,
) -> None:
    resolved = dict(config_from_env() if config is None else config)
    resolved["production"] = True

    redis_url = str(
        resolved.get("redis_url")
        or os.environ.get("REDIS_URL")
        or "redis://localhost:6379/0"
    )
    factory = redis_factory or _default_redis_factory
    redis = await _maybe_await(factory(redis_url))
    service: WorkerService | None = None

    try:
        service = WorkerService(redis, resolved)
        await service.start()
        waiter = wait_for_shutdown or _wait_for_shutdown_signal

        wait_for_failure = getattr(service, "wait_for_failure", None)
        if not callable(wait_for_failure):
            await waiter()
        else:
            shutdown_task = asyncio.create_task(
                waiter(),
                name="worker-process.shutdown-wait",
            )
            failure_task = asyncio.create_task(
                wait_for_failure(),
                name="worker-process.failure-wait",
            )
            try:
                done, _pending = await asyncio.wait(
                    {shutdown_task, failure_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if failure_task in done:
                    failure = await failure_task
                    if isinstance(failure, BaseException):
                        raise failure
                    raise RuntimeError(
                        "WorkerService reported a fatal failure without "
                        "an exception"
                    )
                await shutdown_task
            finally:
                for task in (shutdown_task, failure_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(
                    shutdown_task,
                    failure_task,
                    return_exceptions=True,
                )
    finally:
        try:
            if service is not None:
                await service.close()
        finally:
            close = getattr(redis, "aclose", None)
            if callable(close):
                await close()


def main() -> int:
    asyncio.run(run_worker_process())
    return 0

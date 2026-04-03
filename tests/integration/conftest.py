from __future__ import annotations

import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Iterator

import pytest
import pytest_asyncio
import redis.asyncio as redis

COMPOSE_FILE = Path(__file__).resolve().parents[2] / "docker-compose.integration.yml"
REDIS_URL = "redis://127.0.0.1:6389/0"


def _run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _wait_for_redis(url: str, timeout_s: float = 30.0) -> None:
    import asyncio

    async def _ping_loop() -> None:
        deadline = time.monotonic() + timeout_s
        last_error = None
        while time.monotonic() < deadline:
            client = redis.from_url(url, decode_responses=True)
            try:
                pong = await client.ping()
                if pong is True:
                    await client.aclose()
                    return
            except Exception as exc:
                last_error = exc
            finally:
                try:
                    await client.aclose()
                except Exception:
                    pass
            await asyncio.sleep(0.25)
        raise RuntimeError(f"Redis did not become healthy in {timeout_s}s. last_error={last_error!r}")

    asyncio.run(_ping_loop())


@pytest.fixture(scope="session", autouse=True)
def integration_redis_stack() -> Iterator[None]:
    use_existing = os.getenv("USE_EXISTING_REDIS") == "1"
    managed = False

    if not use_existing:
        if _docker_available():
            _run(["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--remove-orphans"])
            _wait_for_redis(REDIS_URL, timeout_s=30.0)
            managed = True
        else:
            import warnings
            warnings.warn(
                "Docker not available. Set USE_EXISTING_REDIS=1 to use a local Redis.",
                stacklevel=1,
            )

    try:
        yield
    finally:
        if managed and _docker_available():
            try:
                _run(["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"])
            except Exception:
                pass


@pytest.fixture(scope="session", autouse=True)
def strict_mode_env(integration_redis_stack: None) -> Iterator[None]:
    old_redis_url = os.environ.get("REDIS_URL")
    old_strict = os.environ.get("HFA_STRICT_CAS_MODE")
    old_env = os.environ.get("APP_ENV")

    os.environ["REDIS_URL"] = REDIS_URL
    os.environ["HFA_STRICT_CAS_MODE"] = "1"
    os.environ["APP_ENV"] = "test-integration"

    try:
        yield
    finally:
        if old_redis_url is None:
            os.environ.pop("REDIS_URL", None)
        else:
            os.environ["REDIS_URL"] = old_redis_url
        if old_strict is None:
            os.environ.pop("HFA_STRICT_CAS_MODE", None)
        else:
            os.environ["HFA_STRICT_CAS_MODE"] = old_strict
        if old_env is None:
            os.environ.pop("APP_ENV", None)
        else:
            os.environ["APP_ENV"] = old_env


@pytest_asyncio.fixture
async def redis_client(integration_redis_stack: None, strict_mode_env: None):
    client = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        yield client
    finally:
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest_asyncio.fixture
async def real_redis(redis_client):
    return redis_client


@pytest_asyncio.fixture(autouse=True)
async def flush_redis(redis_client):
    await redis_client.flushdb()
    yield
    await redis_client.flushdb()


@pytest.fixture
def unique_ns() -> str:
    return f"it:{uuid.uuid4().hex[:12]}"

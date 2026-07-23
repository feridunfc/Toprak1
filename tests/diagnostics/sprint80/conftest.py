from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Callable

import pytest
import pytest_asyncio
import redis.asyncio as redis_asyncio

REPO_ROOT = Path(__file__).resolve().parents[3]


def load_sprint80_module(name: str) -> ModuleType:
    path = REPO_ROOT / "scripts" / "sprint80" / f"{name}.py"
    module_name = f"sprint80_{name}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load Sprint 80 module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "sprint80_reality: proves currently observed repository behavior",
    )
    config.addinivalue_line(
        "markers",
        "sprint80_contract: frozen target contract expected to xfail",
    )
    config.addinivalue_line(
        "markers",
        "sprint80_real_redis: requires an isolated Redis 7 instance",
    )


def pytest_collection_modifyitems(items):
    """Keep only the cardinality report fixture on one module-scoped event loop.

    Existing Redis-backed diagnostics remain function-scoped. A global loop-scope
    override causes cross-loop Redis futures, so the diagnostic-only scope is
    attached locally. Replace this private pytest-asyncio hook with the public
    loop_scope decorator when the repository pins a compatible pytest-asyncio
    version.
    """
    seen: set[int] = set()
    for item in items:
        module = getattr(item, "module", None)
        fixture = getattr(module, "cardinality_report", None) if module else None
        function = getattr(fixture, "_fixture_function", None)
        if function is None or id(function) in seen:
            continue
        function._loop_scope = "module"
        seen.add(id(function))


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def sprint80_module_loader() -> Callable[[str], ModuleType]:
    return load_sprint80_module


@pytest_asyncio.fixture
async def sprint80_redis():
    url = os.getenv("SPRINT80_REDIS_URL", "")
    if not url:
        pytest.skip("SPRINT80_REDIS_URL is required for Redis-backed diagnostics")

    client = redis_asyncio.Redis.from_url(url, decode_responses=False)
    await client.ping()
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()

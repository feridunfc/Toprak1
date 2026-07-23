from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Callable

import pytest

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


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def sprint80_module_loader() -> Callable[[str], ModuleType]:
    return load_sprint80_module

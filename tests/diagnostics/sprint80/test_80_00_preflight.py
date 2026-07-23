from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Callable

import pytest


@pytest.fixture(scope="module")
def preflight(sprint80_module_loader: Callable[[str], ModuleType]) -> ModuleType:
    return sprint80_module_loader("preflight")


@pytest.mark.sprint80_reality
def test_audit_base_is_frozen(preflight: ModuleType):
    assert preflight.AUDIT_BASE_BRANCH == "baseline/local-import"
    assert preflight.AUDIT_BASE_COMMIT == "2eca85b2d9b115ad4588641b020e98efdd570a2d"


@pytest.mark.sprint80_reality
def test_current_checkout_is_supported_audit_mode(repo_root: Path, preflight: ModuleType):
    result = preflight.run_preflight(repo_root)
    assert result.status == "PASS", result
    assert result.mode in {"exact_baseline_overlay", "diagnostic_branch"}
    assert result.merge_base == preflight.AUDIT_BASE_COMMIT


@pytest.mark.sprint80_reality
def test_product_source_tree_is_not_modified_by_diagnostics(
    repo_root: Path,
    preflight: ModuleType,
):
    result = preflight.run_preflight(repo_root)
    assert result.unexpected_changed_paths == ()


@pytest.mark.sprint80_reality
def test_diagnostic_redis_requires_local_nondefault_port(monkeypatch, preflight: ModuleType):
    monkeypatch.delenv("SPRINT80_ALLOW_REDIS_6379", raising=False)
    assert preflight.redis_url_is_isolated("redis://127.0.0.1:6389/0") is True
    assert preflight.redis_url_is_isolated("redis://localhost:6379/0") is False
    assert preflight.redis_url_is_isolated("redis://redis.internal:6389/0") is False


@pytest.mark.sprint80_reality
def test_required_runtime_files_exist(repo_root: Path, preflight: ModuleType):
    missing = [
        path
        for path in preflight.REQUIRED_RUNTIME_FILES
        if not (repo_root / path).is_file()
    ]
    assert missing == []

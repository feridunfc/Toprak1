from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Callable

import pytest

REQUIRED_FIELDS = {
    "writer_id",
    "path",
    "line",
    "function",
    "state_family",
    "key_pattern",
    "operation",
    "write_semantics",
    "transaction_boundary",
    "guard",
    "feature_flags",
    "production_reachability",
    "authority_class",
    "redis_capability_source",
    "composition_root",
}

CRITICAL_WRITER_PATHS = {
    "hfa-core/src/hfa/state/__init__.py",
    "hfa-core/src/hfa/runtime/state_store.py",
    "hfa-control/src/hfa_control/admission.py",
    "hfa-control/src/hfa_control/dag_lua.py",
    "hfa-core/src/hfa/lua/task_complete.lua",
    "hfa-worker/src/hfa_worker/consumer.py",
}


@pytest.fixture(scope="module")
def inventory(sprint80_module_loader: Callable[[str], ModuleType]) -> ModuleType:
    return sprint80_module_loader("writer_inventory")


@pytest.fixture(scope="module")
def writer_payload(repo_root: Path, inventory: ModuleType) -> dict:
    return inventory.render_inventory(inventory.scan_writer_inventory(repo_root))


@pytest.mark.sprint80_reality
def test_writer_inventory_is_nonempty(writer_payload: dict):
    assert writer_payload["writer_count"] > 0


@pytest.mark.sprint80_reality
def test_every_writer_matches_inventory_schema(writer_payload: dict, inventory: ModuleType):
    for writer in writer_payload["writers"]:
        assert REQUIRED_FIELDS <= writer.keys(), writer
        assert writer["authority_class"] in inventory.ALLOWED_AUTHORITY_CLASSES
        assert writer["production_reachability"] in inventory.ALLOWED_REACHABILITY


@pytest.mark.sprint80_reality
def test_critical_state_writer_surfaces_are_observed(writer_payload: dict):
    observed_paths = {writer["path"] for writer in writer_payload["writers"]}
    missing = sorted(CRITICAL_WRITER_PATHS - observed_paths)
    assert missing == []


@pytest.mark.sprint80_reality
def test_raw_redis_capability_distribution_is_reported(writer_payload: dict):
    capabilities = {
        writer["redis_capability_source"] for writer in writer_payload["writers"]
    }
    assert "lua_gateway" in capabilities
    assert "compatibility_facade" in capabilities
    assert "injected_raw_client" in capabilities


@pytest.mark.sprint80_reality
def test_every_writer_has_production_reachability_classification(
    writer_payload: dict,
    inventory: ModuleType,
):
    invalid = [
        writer["writer_id"]
        for writer in writer_payload["writers"]
        if writer["production_reachability"] not in inventory.ALLOWED_REACHABILITY
    ]
    assert invalid == []


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Sprint 80 inventory has not yet reduced unknown writer classification to zero",
)
def test_no_unknown_writer_remains(writer_payload: dict):
    assert writer_payload["unknown_writer_count"] == 0

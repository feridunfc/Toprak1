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
def test_every_writer_has_allowed_reachability_label(
    writer_payload: dict,
    inventory: ModuleType,
):
    invalid = [
        writer["writer_id"]
        for writer in writer_payload["writers"]
        if writer["production_reachability"] not in inventory.ALLOWED_REACHABILITY
    ]
    assert invalid == []


@pytest.mark.sprint80_reality
def test_unknown_labels_are_reported_as_unresolved_not_as_completion(writer_payload: dict):
    assert writer_payload["unknown_writer_count"] > 0
    assert writer_payload["production_reachability_counts"]["unknown"] > 0


@pytest.mark.sprint80_contract
def test_no_unknown_production_default_writer_remains(writer_payload: dict):
    offenders = [
        writer["writer_id"]
        for writer in writer_payload["writers"]
        if writer["production_reachability"] == "production_default"
        and writer["authority_class"] == "unknown"
    ]
    assert offenders == []


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Canonical-candidate writers still exist without proven production reachability; "
        "repository composition evidence has not converged"
    ),
)
def test_no_unknown_canonical_candidate_reachability_remains(writer_payload: dict):
    offenders = [
        writer["writer_id"]
        for writer in writer_payload["writers"]
        if writer["authority_class"] == "canonical_candidate"
        and writer["production_reachability"] == "unknown"
    ]
    assert offenders == []


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Some production-default or production-flagged writer candidates still lack a "
        "resolved composition root"
    ),
)
def test_every_production_writer_has_composition_root_evidence(writer_payload: dict):
    offenders = [
        writer["writer_id"]
        for writer in writer_payload["writers"]
        if writer["production_reachability"]
        in {"production_default", "production_flagged"}
        and writer["composition_root"] == "unknown"
    ]
    assert offenders == []


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason=(
        "The current inventory uses filename/path heuristics and does not yet attach "
        "per-writer classification evidence"
    ),
)
def test_no_writer_is_classified_only_from_filename_without_evidence(writer_payload: dict):
    assert all(
        writer.get("authority_evidence")
        and writer.get("reachability_evidence")
        and writer.get("composition_evidence")
        for writer in writer_payload["writers"]
    )


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Sprint 80 inventory has not yet reduced unknown writer classification to zero",
)
def test_no_unknown_writer_remains(writer_payload: dict):
    assert writer_payload["unknown_writer_count"] == 0

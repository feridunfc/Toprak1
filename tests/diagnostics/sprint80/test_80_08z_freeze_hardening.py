from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

import pytest


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _first_existing(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError(", ".join(str(path) for path in paths))


@pytest.fixture(scope="module")
def freeze_hardening_model(
    sprint80_module_loader: Callable[[str], ModuleType],
) -> ModuleType:
    return sprint80_module_loader("freeze_hardening")


@pytest.fixture(scope="module")
def hardened_summary(
    repo_root: Path,
    sprint80_module_loader: Callable[[str], ModuleType],
    freeze_hardening_model: ModuleType,
) -> dict[str, Any]:
    directory = repo_root / "local_out/sprint80"
    final_summary_model = sprint80_module_loader("final_summary")
    transition_cardinality = _read_json(directory / "transition_cardinality.json")
    ttl_durability = _read_json(directory / "ttl_durability.json")
    truth_contradictions = _read_json(directory / "truth_contradictions.json")
    aggregate_boundary = _read_json(directory / "aggregate_boundary.json")
    base = final_summary_model.render_final_summary(
        writer_inventory=_read_json(directory / "writer_inventory.json"),
        truth_contradictions=truth_contradictions,
        aggregate_boundary=aggregate_boundary,
        transition_cardinality=transition_cardinality,
        environment=_read_json(
            _first_existing(
                directory / "environment_after.json",
                directory / "environment.json",
            )
        ),
        preflight_after=_read_json(
            _first_existing(
                directory / "preflight_after.json",
                directory / "preflight.json",
            )
        ),
        reality_counts={"failed": 0, "errors": 0},
        contract_counts={"failed": 0, "errors": 0, "xpassed": 0},
        manifest_validated=True,
    )
    return freeze_hardening_model.harden_summary_payload(
        base,
        transition_cardinality=transition_cardinality,
        ttl_durability=ttl_durability,
        truth_contradictions=truth_contradictions,
        aggregate_boundary=aggregate_boundary,
    )


@pytest.mark.sprint80_reality
def test_final_summary_has_complete_accepted_finding_register(
    hardened_summary: dict[str, Any],
    freeze_hardening_model: ModuleType,
):
    register = hardened_summary["accepted_findings"]
    assert hardened_summary["schema_version"] == 2
    assert register["complete"] is True
    assert register["finding_count"] == 15
    assert register["missing_expected_finding_ids"] == []
    assert register["malformed_finding_ids"] == []
    assert set(register["categories"]) == set(
        freeze_hardening_model.EXPECTED_FINDING_IDS_BY_CATEGORY
    )
    for category, expected in (
        freeze_hardening_model.EXPECTED_FINDING_IDS_BY_CATEGORY.items()
    ):
        observed = {
            row["finding_id"] for row in register["categories"][category]
        }
        assert observed == set(expected)


@pytest.mark.sprint80_reality
def test_each_accepted_finding_is_machine_readable(
    hardened_summary: dict[str, Any],
    freeze_hardening_model: ModuleType,
):
    for rows in hardened_summary["accepted_findings"]["categories"].values():
        for row in rows:
            assert freeze_hardening_model.REQUIRED_FINDING_FIELDS <= row.keys()
            assert all(
                row[field]
                for field in freeze_hardening_model.REQUIRED_FINDING_FIELDS
            )
            assert row["status"] == "ACCEPTED_GAP"
            assert row["product_fix_deferred_to"] == (
                "SPRINT_80C_POST_ADR_IMPLEMENTATION"
            )


@pytest.mark.sprint80_reality
def test_durability_register_is_derived_from_ttl_artifact(
    repo_root: Path,
    hardened_summary: dict[str, Any],
):
    ttl = _read_json(repo_root / "local_out/sprint80/ttl_durability.json")
    assert set(ttl["blocking_findings"]) == {
        "run_state",
        "run_result",
        "dag_task_state",
        "dag_task_meta",
        "task_output",
        "operator_audit_stream",
    }
    durability = hardened_summary["accepted_findings"]["categories"][
        "durability"
    ]
    assert len(durability) == 6
    assert {row["source_artifact"] for row in durability} == {
        "ttl_durability.json"
    }


@pytest.mark.sprint80_reality
def test_finding_register_is_a_closure_gate(hardened_summary: dict[str, Any]):
    gate = hardened_summary["closure_gates"]["accepted_finding_register"]
    assert gate == {
        "status": "PASS",
        "expected_count": 15,
        "observed_count": 15,
        "unresolved_count": 0,
        "missing_finding_ids": [],
        "malformed_finding_ids": [],
    }
    assert hardened_summary["closure_status"] == (
        "PASS_WITH_EXPLICIT_DISPOSITIONS"
    )
    assert hardened_summary["merge_authorized"] is False
    assert hardened_summary["sprint80c_started"] is False


@pytest.mark.sprint80_reality
def test_manifest_requires_exact_mandatory_evidence_set(
    tmp_path: Path,
    freeze_hardening_model: ModuleType,
):
    artifacts = []
    for name in sorted(freeze_hardening_model.REQUIRED_EVIDENCE):
        path = tmp_path / name
        path.write_text(f"evidence:{name}\n", encoding="utf-8")
        artifacts.append(
            {
                "path": name,
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    manifest = {
        "branch": "sprint/80b-executable-reconciliation",
        "head": "test-head",
        "artifacts": artifacts,
    }
    result = freeze_hardening_model.validate_manifest(
        tmp_path,
        manifest,
        expected_branch="sprint/80b-executable-reconciliation",
        expected_head="test-head",
    )
    assert result["status"] == "VALID"
    assert result["artifact_count"] == 12
    assert result["required_evidence_count"] == 12

    incomplete = {
        **manifest,
        "artifacts": [
            row for row in artifacts if row["path"] != "ttl_durability.json"
        ],
    }
    with pytest.raises(ValueError, match="mandatory evidence mismatch"):
        freeze_hardening_model.validate_manifest(
            tmp_path,
            incomplete,
            expected_branch="sprint/80b-executable-reconciliation",
            expected_head="test-head",
        )


@pytest.mark.sprint80_reality
def test_p2_debts_remain_explicitly_deferred(hardened_summary: dict[str, Any]):
    hardening = hardened_summary["freeze_hardening"]
    assert hardening["mandatory_evidence_count"] == 12
    assert hardening["finding_register_complete"] is True
    assert hardening["writer_id_pinning"] == "P2_DEFERRED"
    assert hardening["standalone_manifest_validation"] == "P2_DEFERRED"

from __future__ import annotations

import copy
import importlib.util
import json
import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
VALIDATOR_PATH = REPO_ROOT / "scripts/sprint80c/validate_runtime_truth_authority.py"
spec = importlib.util.spec_from_file_location("sprint80c1_validator", VALIDATOR_PATH)
assert spec and spec.loader
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)

EVIDENCE_PATH = REPO_ROOT / validator.EVIDENCE_RELATIVE_PATH
MATRIX_PATH = REPO_ROOT / "docs/adr/sprint80/runtime_truth_authority_matrix.json"
MANIFEST_PATH = REPO_ROOT / "docs/adr/sprint80/runtime_truth_authority_manifest.json"
CHANGED_PATHS = sorted(validator.ALLOWED_CHANGED_PATHS)


def _evidence() -> dict:
    evidence, errors = validator.validate_evidence_bytes(EVIDENCE_PATH.read_bytes())
    assert errors == []
    return evidence


def _matrix() -> dict:
    return validator.load_json(MATRIX_PATH)


def _operation(matrix: dict, operation: str) -> dict:
    return next(
        row
        for row in matrix["operation_classes"]
        if row["operation_class"] == operation
    )


def test_committed_frozen_truth_evidence_is_exact() -> None:
    data = EVIDENCE_PATH.read_bytes()
    evidence, errors = validator.validate_evidence_bytes(data)
    assert validator.sha256_bytes(data) == validator.TRUTH_SHA
    assert errors == []
    assert evidence["schema_version"] == 1
    assert evidence["observation_count"] == 9
    assert evidence["global_truth_policy"] == "INCONSISTENT_BY_CALLER"
    assert {
        row["caller"] for row in evidence["observations"]
    } == validator.EXPECTED_CALLERS
    assert {
        row["scenario"] for row in evidence["observations"]
    } == validator.EXPECTED_SCENARIOS


def test_corrected_matrix_is_complete_and_review_pending() -> None:
    matrix = _matrix()
    assert validator.validate_matrix(matrix, _evidence()) == []
    assert matrix["human_architecture_acceptance"] == "PENDING"
    assert matrix["product_implementation_authorized"] is False


def test_terminal_run_rejects_task_heartbeat() -> None:
    heartbeat = _operation(_matrix(), "heartbeat")
    assert heartbeat["terminal_run_behavior"] == "REJECT"
    assert heartbeat["liveness_ttl_refresh"] == 0
    assert heartbeat["running_zset_refresh"] == 0
    assert heartbeat["reconciliation_candidate"] == "REQUIRED"
    assert (
        "run truth exists and is not terminal"
        in heartbeat["cross_plane_preconditions"]
    )


def test_terminal_run_rejects_normal_completion_and_failure() -> None:
    completion = _operation(_matrix(), "completion_failure")
    assert completion["terminal_run_contract"] == {
        "child_effects": 0,
        "next_path": "EXPLICIT_RECONCILIATION_OR_CANCELLATION_COMMAND",
        "normal_ack_authorization": 0,
        "normal_completion": "REJECT",
        "normal_failure": "REJECT",
        "output_commit": 0,
    }
    cleanup = completion["transport_duplicate_cleanup"]
    assert cleanup["lifecycle_transition"] is False
    assert cleanup["ack_exception"] == (
        "ALLOW_ONLY_WITH_EXPLICIT_TASK_ID_RUN_ID_AND_TERMINAL_TASK_EVIDENCE"
    )


def test_aggregate_boundary_is_explicitly_deferred_to_80c2() -> None:
    matrix = _matrix()
    deferral = matrix["aggregate_boundary_deferral"]
    assert deferral["prejudged_by_80C1"] is False
    for key in (
        "transaction_aggregate_boundary",
        "aggregate_identity",
        "revision_owner",
        "atomic_mutation_boundary",
    ):
        assert deferral[key]["status"] == "DEFERRED_TO_ADR_080C2"
    assert (
        "ADR-080C2 aggregate identity, revision owner and atomic boundary"
        in matrix["dependencies"]
    )


def test_two_frozen_findings_are_exactly_mapped_and_unresolved() -> None:
    rows = _matrix()["finding_dispositions"]
    assert {row["finding_id"] for row in rows} == validator.EXPECTED_FINDINGS
    assert len(rows) == 2
    assert all(
        row["decision_status"] == "ARCHITECTURE_DECISION_ASSIGNED"
        for row in rows
    )
    assert all(row["resolved_in_product"] is False for row in rows)
    assert all(row["implementation_sprint"] == 82 for row in rows)


def test_source_binding_set_and_git_verification_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    matrix = _matrix()

    def fake_git_blob_sha(repo_root: Path, base_head: str, path: str) -> str:
        assert base_head == validator.BASE
        return validator.EXPECTED_SOURCE_BINDINGS[path]

    monkeypatch.setattr(validator, "git_blob_sha", fake_git_blob_sha)
    assert validator.validate_matrix(
        matrix,
        _evidence(),
        repo_root=REPO_ROOT,
        verify_source_bindings=True,
    ) == []


def test_manifest_hash_size_bundle_index_and_exact_scope() -> None:
    manifest = validator.load_json(MANIFEST_PATH)
    assert validator.validate_manifest(REPO_ROOT, manifest) == []
    assert validator.validate_scope(CHANGED_PATHS) == []
    assert len(manifest["artifacts"]) == 6
    assert manifest["bundle_index_algorithm"] == validator.BUNDLE_INDEX_ALGORITHM


def test_full_local_bundle_validation() -> None:
    zip_value = os.getenv("SPRINT80_RUN93_ZIP", "")
    zip_path = Path(zip_value) if zip_value else None
    report = validator.validate_bundle(
        REPO_ROOT,
        evidence_zip=zip_path,
        changed_paths=CHANGED_PATHS,
    )
    assert report["status"] == "PASS", report
    assert report["observation_count"] == 9
    assert report["caller_count"] == 8
    assert report["operation_class_count"] == 9
    assert report["finding_count"] == 2
    assert report["changed_path_count"] == 7
    assert report["product_source_mutation"] == 0
    assert report["implementation_authorized"] is False
    if zip_path is None:
        assert report["historical_frozen_zip_verification"] == (
            "ALREADY_FROZEN_AND_DIGEST_PINNED"
        )


def test_negative_scope_guard_rejects_product_source() -> None:
    errors = validator.validate_scope(
        [*CHANGED_PATHS, "hfa-core/src/hfa/lua/task_dispatch_commit.lua"]
    )
    assert errors
    assert "unexpected" in errors[0]


def test_negative_matrix_guard_rejects_aggregate_prejudgment() -> None:
    matrix = copy.deepcopy(_matrix())
    matrix["global_policy"]["task_lifecycle_authority"] = "TASK_AGGREGATE"
    errors = validator.validate_matrix(matrix, _evidence())
    assert any("must not freeze" in error for error in errors)


def test_negative_heartbeat_and_completion_guards() -> None:
    matrix = copy.deepcopy(_matrix())
    _operation(matrix, "heartbeat")["terminal_run_behavior"] = "ALLOW"
    _operation(matrix, "completion_failure")[
        "terminal_run_contract"
    ]["child_effects"] = 1
    errors = validator.validate_matrix(matrix, _evidence())
    assert "terminal RUN heartbeat behavior must be REJECT" in errors
    assert (
        "terminal RUN completion contract mismatch for child_effects"
        in errors
    )


def test_negative_committed_evidence_guard_rejects_changed_scenario() -> None:
    changed = copy.deepcopy(_evidence())
    changed["observations"][0]["scenario"] = "invented_scenario"
    data = (json.dumps(changed, indent=2, sort_keys=True) + "\n").encode()
    _, errors = validator.validate_evidence_bytes(data)
    assert errors
    assert any("scenario set mismatch" in error for error in errors)

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
VALIDATOR_PATH = REPO_ROOT / "scripts/sprint80c/validate_runtime_truth_authority.py"
spec = importlib.util.spec_from_file_location("sprint80c1_validator", VALIDATOR_PATH)
assert spec and spec.loader
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)

CHANGED_PATHS = sorted(validator.ALLOWED_CHANGED_PATHS)


def test_frozen_truth_evidence_and_matrix_are_complete() -> None:
    evidence_path = REPO_ROOT / "docs/adr/sprint80/evidence/truth_contradictions.run93.json"
    matrix_path = REPO_ROOT / "docs/adr/sprint80/runtime_truth_authority_matrix.json"
    evidence = validator.load_json(evidence_path)
    matrix = validator.load_json(matrix_path)
    assert validator.sha256_file(evidence_path) == validator.EXPECTED_TRUTH_SHA256
    assert validator.validate_evidence(evidence) == []
    assert validator.validate_matrix(matrix, evidence) == []


def test_manifest_hash_size_and_exact_scope() -> None:
    manifest = validator.load_json(
        REPO_ROOT / "docs/adr/sprint80/runtime_truth_authority_manifest.json"
    )
    assert validator.validate_manifest(REPO_ROOT, manifest) == []
    assert validator.validate_scope(CHANGED_PATHS) == []


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
    assert report["product_source_mutation"] == 0
    assert report["implementation_authorized"] is False


def test_negative_scope_guard_rejects_product_source() -> None:
    errors = validator.validate_scope(
        [*CHANGED_PATHS, "hfa-core/src/hfa/lua/task_dispatch_commit.lua"]
    )
    assert errors
    assert "unexpected" in errors[0]


def test_negative_matrix_guard_rejects_caller_local_policy() -> None:
    evidence = validator.load_json(
        REPO_ROOT / "docs/adr/sprint80/evidence/truth_contradictions.run93.json"
    )
    matrix = validator.load_json(
        REPO_ROOT / "docs/adr/sprint80/runtime_truth_authority_matrix.json"
    )
    matrix["global_policy"]["cross_plane_conflict_mutation"] = "INCONSISTENT_BY_CALLER"
    errors = validator.validate_matrix(matrix, evidence)
    assert "global policy mismatch for cross_plane_conflict_mutation" in errors

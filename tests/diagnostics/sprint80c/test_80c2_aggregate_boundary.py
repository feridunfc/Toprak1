from __future__ import annotations

import copy
import importlib.util
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
VALIDATOR_PATH = REPO_ROOT / "scripts/sprint80c/validate_aggregate_boundary.py"
spec = importlib.util.spec_from_file_location("sprint80c2_validator", VALIDATOR_PATH)
assert spec and spec.loader
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)

EVIDENCE_PATH = REPO_ROOT / "docs/adr/sprint80/evidence/aggregate_boundary.run93.json"
MATRIX_PATH = REPO_ROOT / "docs/adr/sprint80/aggregate_boundary_matrix.json"
MANIFEST_PATH = REPO_ROOT / "docs/adr/sprint80/aggregate_boundary_manifest.json"
CHANGED_PATHS = sorted(validator.ALLOWED_CHANGED_PATHS)


def evidence():
    return validator.load_json(EVIDENCE_PATH)


def matrix():
    return validator.load_json(MATRIX_PATH)


def test_committed_frozen_aggregate_evidence_is_exact() -> None:
    assert validator.sha256_file(EVIDENCE_PATH) == validator.EXPECTED_EVIDENCE_SHA256
    assert validator.validate_evidence(evidence()) == []


def test_selected_model_is_independent_task_aggregates_with_process_manager() -> None:
    value = matrix()["selected_model"]
    assert value["name"] == "INDEPENDENT_TASK_AGGREGATES_WITH_DURABLE_DEPENDENCY_PROCESS_MANAGER"
    assert value["task_aggregate_identity"] == "task:{run_id}:{task_id}"
    assert value["process_manager_role"] == "DURABLE_COORDINATION_ONLY_NOT_TASK_TRUTH_AUTHORITY"


def test_parent_completion_cannot_mutate_child_authority() -> None:
    value = matrix()
    assert value["selected_model"]["direct_parent_to_child_authority_mutation"] == "FORBIDDEN"
    assert value["transaction_boundaries"]["parent_completion"]["child_authority_keys_mutated"] == 0


def test_parent_and_child_have_separate_revision_owners() -> None:
    value = matrix()["selected_model"]
    assert value["parent_revision_owner"] == "PARENT_TASK_AGGREGATE"
    assert value["child_revision_owner"] == "CHILD_TASK_AGGREGATE"


def test_edge_delivery_is_at_least_once_and_child_application_is_exactly_once() -> None:
    value = matrix()
    assert value["selected_model"]["delivery_semantics"] == "AT_LEAST_ONCE_EDGE_COMMAND_DELIVERY_EXACTLY_ONCE_CHILD_AGGREGATE_APPLICATION"
    assert value["dependency_contract"]["duplicate_edge_command"] == "ALREADY_APPLIED_NO_REVISION_NO_RECORD"


def test_missing_dependency_topology_fails_closed_without_counter_authority() -> None:
    value = matrix()["dependency_contract"]
    assert value["expected_dependency_authority"] == "IMMUTABLE_EXPECTED_PARENT_EDGE_SET"
    assert value["remaining_deps"] == "DERIVED_PROJECTION_NOT_AUTHORITY"
    assert value["missing_expected_edge_metadata"] == "FAIL_CLOSED_AND_EMIT_RECONCILIATION_CANDIDATE"


def test_ready_marker_is_non_authoritative_and_projection_is_replayable() -> None:
    value = matrix()["ready_projection_contract"]
    assert value["marker_can_authorize_or_suppress_state_transition"] is False
    assert value["missing_queue_projection"] == "REPLAY_FROM_CANONICAL_READY_TRANSITION"


def test_each_new_child_edge_effect_has_one_revision_and_record() -> None:
    value = matrix()["canonical_child_effect_contract"]
    assert value["every_new_edge_receipt_increments_child_revision"] is True
    assert value["every_new_edge_receipt_produces_exactly_one_canonical_transition_record"] is True
    assert value["duplicate_edge_receipt_produces_record"] is False


def test_two_frozen_findings_are_mapped_and_unresolved() -> None:
    rows = matrix()["finding_dispositions"]
    assert {row["finding_id"] for row in rows} == validator.EXPECTED_FINDINGS
    assert all(row["resolved_in_product"] is False and row["implementation_sprint"] == 84 for row in rows)


def test_dependent_adrs_are_explicit() -> None:
    deps = set(matrix()["dependencies"])
    assert "ADR-080C3 canonical transition and monotonic revision contract" in deps
    assert "ADR-080C4 durable authority and reconstruction contract" in deps
    assert "ADR-080C5 event-state atomicity and outbox contract" in deps


def test_source_binding_set_is_exact() -> None:
    rows = matrix()["source_bindings"]
    assert {row["path"]: row["blob_sha"] for row in rows} == validator.EXPECTED_SOURCE_BINDINGS


def test_manifest_hash_size_bundle_index_and_exact_scope() -> None:
    manifest = validator.load_json(MANIFEST_PATH)
    assert validator.validate_manifest(REPO_ROOT, manifest) == []
    assert validator.validate_scope(CHANGED_PATHS) == []


def test_full_local_bundle_validation() -> None:
    zip_value = os.getenv("SPRINT80_RUN93_ZIP", "")
    report = validator.validate_bundle(REPO_ROOT, evidence_zip=Path(zip_value) if zip_value else None, changed_paths=CHANGED_PATHS)
    assert report["status"] == "PASS", report
    assert report["observation_count"] == 8
    assert report["finding_count"] == 2
    assert report["product_source_mutation"] == 0
    assert report["implementation_authorized"] is False
    assert report["historical_frozen_zip_verification"] == ("PASS" if zip_value else "NOT_RUN")


def test_negative_scope_guard_rejects_product_source() -> None:
    assert validator.validate_scope([*CHANGED_PATHS, "hfa-core/src/hfa/lua/task_complete.lua"])


def test_negative_matrix_guard_rejects_dag_aggregate_prejudgment() -> None:
    value = copy.deepcopy(matrix())
    value["selected_model"]["name"] = "ONE_DAG_OR_RUN_AGGREGATE"
    assert "selected aggregate model mismatch" in validator.validate_matrix(value, evidence())


def test_negative_matrix_guard_rejects_counter_or_marker_authority() -> None:
    value = copy.deepcopy(matrix())
    value["dependency_contract"]["remaining_deps"] = "AUTHORITATIVE_COUNTER"
    value["ready_projection_contract"]["marker_can_authorize_or_suppress_state_transition"] = True
    errors = validator.validate_matrix(value, evidence())
    assert "remaining counter cannot be authority" in errors
    assert "ready marker cannot be authority" in errors


def test_negative_evidence_mutation_is_detected() -> None:
    value = copy.deepcopy(evidence())
    value["cross_task_mutation_observed"] = False
    assert "cross-task mutation observation must remain true" in validator.validate_evidence(value)

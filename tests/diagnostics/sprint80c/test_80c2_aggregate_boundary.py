from __future__ import annotations

import copy
import importlib.util
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("validator", ROOT / "scripts/sprint80c/validate_aggregate_boundary.py")
assert spec and spec.loader
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)

EVIDENCE = ROOT / "docs/adr/sprint80/evidence/aggregate_boundary.run93.json"
MATRIX = ROOT / "docs/adr/sprint80/aggregate_boundary_matrix.json"
MANIFEST = ROOT / "docs/adr/sprint80/aggregate_boundary_manifest.json"


def evidence():
    return validator.readj(EVIDENCE)


def matrix():
    return validator.readj(MATRIX)


def test_frozen_evidence_is_exact():
    assert validator.sha(EVIDENCE.read_bytes()) == validator.EVIDENCE_SHA
    assert validator.validate_evidence(evidence()) == []


def test_selected_model_and_revision_owners():
    value = matrix()["selected_model"]
    assert value["name"] == "INDEPENDENT_TASK_AGGREGATES_WITH_DURABLE_DEPENDENCY_PROCESS_MANAGER"
    assert value["parent_revision_owner"] == "PARENT_TASK_AGGREGATE"
    assert value["child_revision_owner"] == "CHILD_TASK_AGGREGATE"


def test_predecessor_acceptance_is_exactly_bound():
    value = matrix()["predecessor_governance"]
    assert value["accepted_head_candidate"] == "9ad9503badd72afb0a935dbb8c02e828ea02d3e2"
    assert value["accepted_head"] == "9ad9503badd72afb0a935dbb8c02e828ea02d3e2"
    assert value["acceptance_comment_id"] == 5083135387
    assert value["acceptance_type"] == "POST_MERGE_HUMAN_ARCHITECTURE_ACCEPTANCE"
    assert value["human_architecture_acceptance"] == "ACCEPT"
    assert value["status"] == "VERIFIED"


def test_child_admission_commits_topology_authority():
    value = matrix()["transaction_boundaries"]["child_admission"]
    assert value["expected_parent_edge_set_owner"] == "CHILD_TASK_AGGREGATE"
    assert value["expected_parent_edge_set_mutability"] == "IMMUTABLE_AFTER_ADMISSION"
    assert value["dependency_count_authority"] is False


def test_logical_and_physical_identity_are_separate():
    value = matrix()["physical_identity_strategy"]
    assert value["logical_identity"] == "run_id_plus_task_id"
    assert value["physical_key_layout"] == "EXISTING_TASK_ID_KEYS_TEMPORARILY_RETAINED"
    assert value["physical_rekey_in_80C2"] == "NOT_SELECTED"


def test_logical_edge_resolution_authority_is_child_owned():
    value = matrix()["logical_edge_resolution_authority"]
    assert value["key"] == "logical_edge_id"
    assert value["owner"] == "CHILD_TASK_AGGREGATE"
    assert value["atomicity"] == "RESOLUTION_RECEIPT_CHILD_EFFECT_REVISION_AND_RECORD_ONE_CHILD_AGGREGATE_COMMIT"


def test_logical_edge_resolution_value_is_complete():
    value = matrix()["logical_edge_resolution_authority"]["value"]
    assert set(value) == {
        "accepted_edge_command_id",
        "accepted_parent_transition_id",
        "accepted_outcome",
        "graph_identity",
        "graph_revision_or_topology_hash",
        "applied_child_revision",
    }
    assert all(field_value == "REQUIRED" for field_value in value.values())


def test_first_logical_edge_application_is_atomic():
    value = matrix()["logical_edge_resolution_authority"]["when_logical_edge_resolution_absent"]
    assert value["validate_topology"] == "REQUIRED"
    assert set(value["atomic_commit"]) == {
        "logical_edge_resolution",
        "outcome_aware_edge_receipt",
        "child_state_effect",
        "child_revision",
        "one_canonical_transition_record",
    }


def test_same_command_is_exact_noop():
    value = matrix()["logical_edge_resolution_authority"]["when_same_edge_command_id_exists"]
    assert value == {
        "result": "ALREADY_APPLIED",
        "child_mutation": 0,
        "revision_increment": 0,
        "canonical_record_count": 0,
        "retry": "STOP",
    }


def test_different_command_for_resolved_edge_is_contradiction():
    value = matrix()["logical_edge_resolution_authority"]["when_logical_edge_resolved_by_different_command"]
    assert value["result"] == "CONTRADICTION"
    assert value["child_mutation"] == 0
    assert value["durable_conflict_record"] == "REQUIRED"
    assert value["reconciliation_candidate"] == "REQUIRED"
    assert value["retry"] == "STOP"


def test_child_application_boundary_contains_resolution_and_receipt():
    members = matrix()["transaction_boundaries"]["child_dependency_application"]["atomic_commit_members"]
    assert "logical_edge_resolution" in members
    assert "outcome_aware_edge_receipt" in members


def test_receipts_are_outcome_aware_and_logical_edge_bound():
    value = matrix()["edge_receipt_contract"]
    assert value["authority"] == "IDEMPOTENT_APPLIED_EDGE_RECEIPTS_WITH_OUTCOME"
    assert set(value["allowed_outcomes"]) == {"DEPENDENCY_SATISFIED", "DEPENDENCY_FAILED"}
    assert "logical_edge_id" in value["required_fields"]


def test_failed_edge_has_deterministic_child_disposition():
    value = matrix()["dependency_policy_contract"]["failed_edge"]
    assert value["child_disposition"] == "blocked_by_failure"
    assert value["child_revision_increment"] == 1
    assert value["canonical_transition_record"] == "EXACTLY_ONE"
    assert value["ready_projection_intent"] == 0
    assert value["process_manager_retry"] == "STOP_AFTER_RECEIPT"


def test_blocked_child_same_command_is_duplicate():
    value = matrix()["terminal_state_command_disposition"]["blocked_by_failure"]
    assert value["same_command"] == "ALREADY_APPLIED"


def test_blocked_child_different_edge_is_terminal_disposition():
    value = matrix()["terminal_state_command_disposition"]["blocked_by_failure"]
    assert value["different_edge_command"] == "TERMINAL_CHILD_ALREADY_BLOCKED"
    assert value["different_edge_child_mutation"] == 0
    assert value["different_edge_child_revision_increment"] == 0
    assert value["different_edge_canonical_transition_record_count"] == 0
    assert value["durable_disposition_record"] == "REQUIRED"
    assert value["disposition_owner"] == "DEPENDENCY_PROCESS_MANAGER_COORDINATION_STATE"
    assert value["disposition_identity"] == "edge_command_id"
    assert value["retry"] == "STOP"


def test_terminal_vocabulary_is_exact_and_complete():
    value = matrix()["terminal_state_command_disposition"]
    assert set(value) == {"done", "failed", "blocked_by_failure", "dead_lettered", "skipped"}


def test_done_and_failed_late_edges_are_exact_contradictions():
    value = matrix()["terminal_state_command_disposition"]
    for state in ("done", "failed"):
        assert value[state] == {
  "unsatisfied_edge_command": "CONTRADICTION",
  "child_mutation": 0,
  "durable_conflict_record": "REQUIRED",
  "reconciliation_candidate": "REQUIRED",
  "retry": "STOP",
        }


def test_dead_lettered_is_authority_evidenced_terminal_noop():
    value = matrix()["terminal_state_command_disposition"]["dead_lettered"]
    assert value["unsatisfied_edge_command"] == "TERMINAL_CHILD_NOOP"
    assert set(value["required_authority_evidence"]) == {"terminal_transition_id", "child_state_authority_revision"}
    assert value["missing_authority_evidence"] == "MISSING_AUTHORITY_DURABLE_CONFLICT_AND_RECONCILIATION"
    assert value["retry"] == "STOP"


def test_skipped_is_authority_evidenced_terminal_noop():
    value = matrix()["terminal_state_command_disposition"]["skipped"]
    assert value["unsatisfied_edge_command"] == "TERMINAL_CHILD_NOOP"
    assert set(value["required_authority_evidence"]) == {"terminal_transition_id", "child_state_authority_revision"}
    assert value["missing_authority_evidence"] == "MISSING_AUTHORITY_DURABLE_CONFLICT_AND_RECONCILIATION"
    assert value["retry"] == "STOP"


def test_two_findings_are_mapped_and_unresolved():
    rows = matrix()["finding_dispositions"]
    assert {row["finding_id"] for row in rows} == validator.FINDINGS
    assert all(row["resolved_in_product"] is False and row["implementation_sprint"] == 84 for row in rows)


def test_manifest_hashes_and_exact_scope():
    assert validator.validate_manifest(ROOT, validator.readj(MANIFEST)) == []
    assert validator.validate_scope(validator.PATHS) == []


def test_full_bundle_is_technical_pass_with_governance_blocker():
    zip_value = os.getenv("SPRINT80_RUN93_ZIP")
    report = validator.validate_bundle(
        ROOT,
        evidence_zip=Path(zip_value) if zip_value else None,
        changed_paths=validator.PATHS,
    )
    assert report["technical_status"] == "PASS"
    assert report["status"] == "PASS"
    assert report["governance_status"] == "PASS"
    assert report["predecessor_human_acceptance"] == "ACCEPT"
    assert report["implementation_authorized"] is False


def test_negative_missing_logical_edge_authority():
    value = copy.deepcopy(matrix())
    del value["logical_edge_resolution_authority"]
    assert "logical-edge resolution key" in validator.validate_matrix(value, evidence())


def test_negative_logical_edge_authority_wrong_owner():
    value = copy.deepcopy(matrix())
    value["logical_edge_resolution_authority"]["owner"] = "PROCESS_MANAGER"
    assert "logical-edge resolution owner" in validator.validate_matrix(value, evidence())


def test_negative_split_logical_edge_commit():
    value = copy.deepcopy(matrix())
    value["logical_edge_resolution_authority"]["when_logical_edge_resolution_absent"]["atomic_commit"].remove("logical_edge_resolution")
    assert "logical-edge atomic commit" in validator.validate_matrix(value, evidence())


def test_negative_opposite_outcome_not_contradiction():
    value = copy.deepcopy(matrix())
    value["logical_edge_resolution_authority"]["when_logical_edge_resolved_by_different_command"]["result"] = "APPLY"
    assert "logical-edge contradiction" in validator.validate_matrix(value, evidence())


def test_negative_blocked_child_different_edge_applies_receipt():
    value = copy.deepcopy(matrix())
    value["terminal_state_command_disposition"]["blocked_by_failure"]["different_edge_child_revision_increment"] = 1
    assert "blocked child late-edge disposition" in validator.validate_matrix(value, evidence())


def test_negative_terminal_vocabulary_incomplete():
    value = copy.deepcopy(matrix())
    del value["terminal_state_command_disposition"]["skipped"]
    assert "terminal vocabulary coverage" in validator.validate_matrix(value, evidence())


def test_negative_dead_lettered_without_authority_evidence():
    value = copy.deepcopy(matrix())
    value["terminal_state_command_disposition"]["dead_lettered"]["required_authority_evidence"] = []
    assert "dead_lettered disposition" in validator.validate_matrix(value, evidence())


def test_negative_skipped_without_authority_evidence():
    value = copy.deepcopy(matrix())
    value["terminal_state_command_disposition"]["skipped"]["required_authority_evidence"] = []
    assert "skipped disposition" in validator.validate_matrix(value, evidence())


def test_negative_optional_logical_edge_resolution_field():
    value = copy.deepcopy(matrix())
    value["logical_edge_resolution_authority"]["value"]["accepted_outcome"] = "OPTIONAL"
    assert "logical-edge resolution value" in validator.validate_matrix(value, evidence())


def test_negative_done_retry_continues():
    value = copy.deepcopy(matrix())
    value["terminal_state_command_disposition"]["done"]["retry"] = "CONTINUE"
    assert "done disposition" in validator.validate_matrix(value, evidence())


def test_negative_failed_conflict_record_removed():
    value = copy.deepcopy(matrix())
    value["terminal_state_command_disposition"]["failed"]["durable_conflict_record"] = "NONE"
    assert "failed disposition" in validator.validate_matrix(value, evidence())


def test_negative_missing_terminal_evidence_silent_noop_dead_lettered():
    value = copy.deepcopy(matrix())
    value["terminal_state_command_disposition"]["dead_lettered"]["missing_authority_evidence"] = "SILENT_NOOP"
    assert "dead_lettered disposition" in validator.validate_matrix(value, evidence())


def test_negative_missing_terminal_evidence_silent_noop_skipped():
    value = copy.deepcopy(matrix())
    value["terminal_state_command_disposition"]["skipped"]["missing_authority_evidence"] = "SILENT_NOOP"
    assert "skipped disposition" in validator.validate_matrix(value, evidence())


def test_negative_manifest_immutable_base_head():
    value = copy.deepcopy(validator.readj(MANIFEST))
    value["base_head"] = "wrong"
    assert "manifest immutable register" in validator.validate_manifest(ROOT, value)


def test_negative_manifest_predecessor_comment_id():
    value = copy.deepcopy(validator.readj(MANIFEST))
    value["predecessor_acceptance_comment_id"] = 0
    assert "manifest predecessor acceptance" in validator.validate_manifest(ROOT, value)


def test_negative_product_source_scope():
    assert validator.validate_scope([*validator.PATHS, "hfa-core/src/hfa/lua/task_complete.lua"])


def test_negative_evidence_mutation():
    value = copy.deepcopy(evidence())
    value["cross_task_mutation_observed"] = False
    assert "cross-task observation" in validator.validate_evidence(value)

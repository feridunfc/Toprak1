from __future__ import annotations

import copy
import importlib.util
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location(
    "validator", ROOT / "scripts/sprint80c/validate_canonical_transition_revision.py"
)
assert spec and spec.loader
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)

EVIDENCE = ROOT / "docs/adr/sprint80/evidence/transition_cardinality.run93.json"
MATRIX = ROOT / "docs/adr/sprint80/canonical_transition_revision_matrix.json"
MANIFEST = ROOT / "docs/adr/sprint80/canonical_transition_revision_manifest.json"


def evidence():
    return validator.read_json(EVIDENCE)


def matrix():
    return validator.read_json(MATRIX)


def operation(value, name):
    return next(row for row in value["operation_matrix"] if row["operation"] == name)


def matrix_errors(value):
    return validator.validate_matrix(value, evidence())


def test_frozen_evidence_exact():
    assert validator.sha(EVIDENCE.read_bytes()) == validator.EVIDENCE_SHA
    assert validator.validate_evidence(evidence()) == []


def test_predecessor_acceptance_bindings_exact():
    value = matrix()["predecessors"]
    assert value["ADR-080C1"]["acceptance_comment_id"] == 5083135387
    assert value["ADR-080C2"]["acceptance_comment_id"] == 5083381947
    assert value["ADR-080C2"]["merge_commit"] == validator.BASE


def test_canonical_task_identity_preserves_run_id():
    value = matrix()["canonical_aggregate_identity"]
    assert value["task"] == "task:{run_id}:{task_id}"
    assert value["raw_task_id_as_canonical_identity"] == "FORBIDDEN"


def test_transition_identity_uses_encoded_components():
    value = matrix()["transition_identity"]
    assert value["format"] == "ctr:v1:{canonical_aggregate_identity_sha256}:{aggregate_revision}:{operation_id_sha256}"
    assert value["component_encoding"] == "SHA256_OF_LENGTH_PREFIXED_UTF8_COMPONENT"
    assert value["raw_delimiter_concatenation"] == "FORBIDDEN"


def test_authority_entry_preconditions_are_outer_gate():
    value = matrix()["authority_entry_preconditions"]
    assert value["order"] == "OUTER_GATE_BEFORE_OPERATION_RECEIPT_LOOKUP"
    assert value["validate_authenticated_writer"] == "REQUIRED"
    assert value["validate_writer_operation_capability"] == "REQUIRED"
    assert value["validate_claim_or_lease_fence_when_applicable"] == "REQUIRED"
    assert value["precondition_failure"]["operation_receipt_disclosure"] == "FORBIDDEN"
    assert matrix()["authority_evaluation_order"][0] == "VALIDATE_AUTHORITY_ENTRY_PRECONDITIONS"


def test_canonical_command_hash_binds_all_authoritative_effects():
    value = matrix()["canonical_command_hash"]
    assert value["serialization"] == "RFC_8785_JCS"
    assert value["caller_supplied_hash_trusted"] is False
    for field in ["authoritative_metadata_changes", "requested_child_effects", "requested_projection_intents"]:
        assert field in value["hash_members"]
    binding = matrix()["authoritative_effect_binding"]
    assert binding["model"] == "MODEL_A_HASH_ALL_REQUESTED_AUTHORITATIVE_EFFECTS"
    assert binding["same_operation_id_and_same_command_hash"]["immutable_record_effects_must_match"] is True


def test_rfc8785_profile_is_exact():
    value = matrix()["canonical_command_hash"]["canonical_json_standard"]
    assert value["standard"] == "RFC_8785_JCS"
    assert value["unicode_input_normalization"] == "UTF8_NFC_BEFORE_JCS"
    assert value["non_finite_numbers"] == "FORBIDDEN"
    assert value["duplicate_object_keys"] == "FORBIDDEN"
    assert value["array_order"] == "PRESERVED"
    assert value["binary_values"] == "BASE64URL_WITH_EXPLICIT_TYPE_TAG"


def test_operation_receipt_is_aggregate_owned_immutable_index():
    value = matrix()["operation_receipt_authority"]
    assert value["owner"] == "AGGREGATE_AUTHORITY_COMMIT"
    assert value["immutable"] is True
    assert set(value["immutable_value"]) == validator.RECEIPT_FIELDS
    assert value["immutable_value"]["canonical_record_hash"] == "REQUIRED"
    assert value["second_lifecycle_truth"] is False


def test_duplicate_and_stale_are_distinct():
    value = matrix()["idempotency_and_concurrency"]
    assert value["operation_receipt_exists_same_payload"]["result"] == "ALREADY_APPLIED"
    assert value["operation_receipt_exists_same_payload"]["canonical_record_hash_match"] == "REQUIRED"
    assert value["operation_receipt_missing_expected_revision_stale"]["result"] == "STALE_REVISION_CONFLICT"
    assert value["operation_receipt_missing_expected_revision_future"]["result"] == "FUTURE_REVISION_CONFLICT"


def test_strict_contiguous_revision_and_timestamp_non_authority():
    value = matrix()["revision_contract"]
    assert value["initial_revision"] == 0
    assert value["first_committed_transition_revision"] == 1
    assert value["gap"] == value["reuse"] == value["regression"] == "FORBIDDEN"
    assert value["ordering_authority"] == "AGGREGATE_REVISION"
    assert value["committed_at_ms_ordering_authority"] is False


def test_record_schema_includes_canonical_record_hash():
    value = matrix()["canonical_transition_record"]
    assert set(value["required_fields"]) == validator.RECORD_FIELDS
    assert value["canonical_record_hash"] == {
        "algorithm": "SHA256",
        "serialization": "RFC_8785_JCS",
        "hash_scope": "ALL_IMMUTABLE_RECORD_FIELDS_EXCEPT_CANONICAL_RECORD_HASH",
        "caller_supplied_hash_trusted": False,
    }


def test_canonical_store_collision_is_fail_closed():
    value = matrix()["canonical_storage_authority"]["canonical_record_collision"]
    assert value["same_transition_id_same_record"]["result"] == "ALREADY_PRESENT"
    conflict = value["same_transition_id_different_record"]
    assert conflict["result"] == "CANONICAL_RECORD_CORRUPTION_CONFLICT"
    assert conflict["mutation"] == 0
    assert conflict["durable_conflict_record"] == "REQUIRED"


def test_projection_same_revision_distinguishes_duplicate_and_corruption():
    value = matrix()["projection_contract"]
    receipt = value["projection_application_receipt"]
    assert receipt["value"] == {
        "applied_revision": "REQUIRED",
        "applied_transition_id": "REQUIRED",
        "applied_record_hash": "REQUIRED",
    }
    same = value["incoming_revision_equals_applied_revision"]
    assert same["same_transition_id_and_record_hash"]["result"] == "DUPLICATE_NOOP"
    assert same["different_transition_id_or_record_hash"]["result"] == "PROJECTION_CORRUPTION_CONFLICT"
    assert value["incoming_revision_older_than_applied_revision"]["result"] == "OLDER_REVISION_NOOP"
    assert value["incoming_revision_is_applied_revision_plus_one"]["result"] == "APPLY"
    assert value["incoming_revision_greater_than_applied_revision_plus_one"]["result"] == "GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED"


def test_exact_operation_contract_register():
    observed = {row["operation"]: row for row in matrix()["operation_matrix"]}
    assert observed == validator.EXPECTED_OPERATION_CONTRACTS
    assert len(observed) == 15


def test_all_frozen_operations_are_mapped():
    mapping = matrix()["frozen_operation_mapping"]
    assert set(mapping) == validator.FROZEN_OPERATIONS
    assert {row["operation"] for row in evidence()["operations"]} == set(mapping)


def test_findings_mapped_but_not_resolved():
    rows = matrix()["finding_dispositions"]
    assert {row["finding_id"] for row in rows} == validator.FINDINGS
    assert all(row["resolved_in_product"] is False and row["implementation_sprint"] == 81 for row in rows)


def test_manifest_hashes_scope_and_governance():
    assert validator.validate_manifest(ROOT, validator.read_json(MANIFEST)) == []
    assert validator.validate_scope(validator.PATHS) == []


def test_full_local_bundle():
    zip_path = os.getenv("SPRINT80_RUN93_ZIP")
    report = validator.validate_bundle(
        ROOT,
        evidence_zip=Path(zip_path) if zip_path else None,
        changed_paths=validator.PATHS,
    )
    assert report["technical_status"] == "PASS"
    assert report["product_implementation_authorized"] is False


def test_negative_effect_metadata_removed_from_hash():
    value = copy.deepcopy(matrix())
    value["canonical_command_hash"]["hash_members"].remove("authoritative_metadata_changes")
    assert "canonical hash members" in matrix_errors(value)


def test_negative_effect_child_removed_from_hash():
    value = copy.deepcopy(matrix())
    value["canonical_command_hash"]["hash_members"].remove("requested_child_effects")
    assert "canonical hash members" in matrix_errors(value)


def test_negative_effect_projection_removed_from_hash():
    value = copy.deepcopy(matrix())
    value["canonical_command_hash"]["hash_members"].remove("requested_projection_intents")
    assert "canonical hash members" in matrix_errors(value)


def test_negative_same_hash_effect_match_disabled():
    value = copy.deepcopy(matrix())
    value["authoritative_effect_binding"]["same_operation_id_and_same_command_hash"]["immutable_record_effects_must_match"] = False
    assert "authoritative effect binding" in matrix_errors(value)


def test_negative_projection_same_revision_corruption_noop():
    value = copy.deepcopy(matrix())
    value["projection_contract"]["incoming_revision_equals_applied_revision"]["different_transition_id_or_record_hash"]["result"] = "DUPLICATE_NOOP"
    assert "projection contract" in matrix_errors(value)


def test_negative_projection_receipt_record_hash_removed():
    value = copy.deepcopy(matrix())
    del value["projection_contract"]["projection_application_receipt"]["value"]["applied_record_hash"]
    assert "projection contract" in matrix_errors(value)


def test_negative_canonical_collision_different_record_noop():
    value = copy.deepcopy(matrix())
    value["canonical_storage_authority"]["canonical_record_collision"]["same_transition_id_different_record"]["result"] = "ALREADY_PRESENT"
    assert "canonical record collision" in matrix_errors(value)


def test_negative_authority_entry_fence_optional():
    value = copy.deepcopy(matrix())
    value["authority_entry_preconditions"]["validate_claim_or_lease_fence_when_applicable"] = "OPTIONAL"
    assert "authority entry preconditions" in matrix_errors(value)


def test_negative_precondition_discloses_receipt():
    value = copy.deepcopy(matrix())
    value["authority_entry_preconditions"]["precondition_failure"]["operation_receipt_disclosure"] = "ALLOWED"
    assert "authority entry preconditions" in matrix_errors(value)


def test_negative_nonfinite_numbers_allowed():
    value = copy.deepcopy(matrix())
    value["canonical_command_hash"]["canonical_json_standard"]["non_finite_numbers"] = "ALLOWED"
    assert "canonical JSON exact standard" in matrix_errors(value)


def test_negative_duplicate_json_keys_allowed():
    value = copy.deepcopy(matrix())
    value["canonical_command_hash"]["canonical_json_standard"]["duplicate_object_keys"] = "ALLOWED"
    assert "canonical JSON exact standard" in matrix_errors(value)


def test_negative_task_complete_authority_owner_changed():
    value = copy.deepcopy(matrix())
    operation(value, "TASK_COMPLETE")["authority_owner"] = "RUN_AGGREGATE"
    assert "exact operation contract register" in matrix_errors(value)


def test_negative_task_dispatch_next_state_changed():
    value = copy.deepcopy(matrix())
    operation(value, "TASK_DISPATCH")["lifecycle_state_after"] = "running"
    assert "exact operation contract register" in matrix_errors(value)


def test_negative_dependency_projection_intent_removed():
    value = copy.deepcopy(matrix())
    operation(value, "TASK_DEPENDENCY_APPLY")["projection_intents"] = []
    assert "exact operation contract register" in matrix_errors(value)


def test_negative_task_cancel_terminal_state_changed():
    value = copy.deepcopy(matrix())
    operation(value, "TASK_CANCEL")["lifecycle_state_after"] = "done"
    assert "exact operation contract register" in matrix_errors(value)


def test_negative_run_terminate_stale_behavior_changed():
    value = copy.deepcopy(matrix())
    operation(value, "RUN_TERMINATE")["stale_revision_behavior"] = "NOT_APPLICABLE"
    assert "exact operation contract register" in matrix_errors(value)


def test_negative_message_ack_mutation_class_changed():
    value = copy.deepcopy(matrix())
    operation(value, "MESSAGE_ACK")["mutation_class"] = "ACCEPTED_AUTHORITY_MUTATION"
    assert "exact operation contract register" in matrix_errors(value)


def test_negative_run_terminate_evidence_binding_changed():
    value = copy.deepcopy(matrix())
    operation(value, "RUN_TERMINATE")["evidence_binding"] = "unrelated_source"
    assert "exact operation contract register" in matrix_errors(value)


def test_negative_manifest_branch_changed():
    value = copy.deepcopy(validator.read_json(MANIFEST))
    value["branch"] = "sprint/wrong"
    assert "manifest exact governance register" in validator.validate_manifest(ROOT, value)


def test_negative_manifest_merge_authorized():
    value = copy.deepcopy(validator.read_json(MANIFEST))
    value["merge_authorized"] = True
    assert "manifest exact governance register" in validator.validate_manifest(ROOT, value)


def test_negative_manifest_exact_changed_files():
    value = copy.deepcopy(validator.read_json(MANIFEST))
    value["exact_changed_files"] = 7
    assert "manifest exact governance register" in validator.validate_manifest(ROOT, value)


def test_negative_product_source_scope():
    assert validator.validate_scope([*validator.PATHS, "hfa-core/src/hfa/lua/task_complete.lua"])


def test_negative_evidence_mutation():
    value = copy.deepcopy(evidence())
    value["canonical_transition_record_count"] = 1
    assert "evidence canonical record count" in validator.validate_evidence(value)

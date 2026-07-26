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


def test_canonical_command_hash_contract():
    value = matrix()["canonical_command_hash"]
    assert value["required"] is True
    assert value["algorithm"] == "SHA256"
    assert value["serialization"] == "CANONICAL_JSON"
    assert value["caller_supplied_hash_trusted"] is False
    assert "expected_revision" in value["hash_members"]
    assert "committed_at_ms" in value["excluded_members"]


def test_operation_receipt_is_aggregate_owned_immutable_index():
    value = matrix()["operation_receipt_authority"]
    assert value["owner"] == "AGGREGATE_AUTHORITY_COMMIT"
    assert value["immutable"] is True
    assert set(value["immutable_value"]) == validator.RECEIPT_FIELDS
    assert all(item == "REQUIRED" for item in value["immutable_value"].values())
    assert value["second_lifecycle_truth"] is False


def test_authority_commit_order_receipt_before_revision():
    assert matrix()["authority_commit_order"] == [
        "RESOLVE_OPERATION_RECEIPT",
        "COMPARE_CANONICAL_COMMAND_HASH",
        "COMPARE_EXPECTED_REVISION",
        "VALIDATE_STATE_TRANSITION",
        "COMMIT_STATE_REVISION_RECORD_RECEIPT_AND_INTENTS_ATOMICALLY",
    ]


def test_duplicate_and_stale_are_distinct():
    value = matrix()["idempotency_and_concurrency"]
    assert value["operation_receipt_exists_same_payload"]["result"] == "ALREADY_APPLIED"
    assert value["operation_receipt_missing_expected_revision_stale"]["result"] == "STALE_REVISION_CONFLICT"
    assert value["operation_receipt_missing_expected_revision_future"]["result"] == "FUTURE_REVISION_CONFLICT"
    assert value["already_applied_proof"] == "DURABLE_OPERATION_RECEIPT_ONLY"


def test_same_operation_different_payload_fails_closed():
    value = matrix()["idempotency_and_concurrency"]["operation_receipt_exists_different_payload"]
    assert value["result"] == "IDEMPOTENCY_CONFLICT"
    assert value["mutation"] == 0
    assert value["durable_conflict_record"] == "REQUIRED"
    assert value["reconciliation_candidate"] == "REQUIRED"


def test_strict_contiguous_revision_and_timestamp_non_authority():
    value = matrix()["revision_contract"]
    assert value["initial_revision"] == 0
    assert value["first_committed_transition_revision"] == 1
    assert value["next_revision_rule"] == "to_revision == from_revision + 1"
    assert value["gap"] == value["reuse"] == value["regression"] == "FORBIDDEN"
    assert value["ordering_authority"] == "AGGREGATE_REVISION"
    assert value["committed_at_ms_ordering_authority"] is False


def test_create_and_admit_contract():
    value = matrix()["aggregate_creation_contract"]
    assert value["aggregate_not_exists_logical_current_revision"] == 0
    assert value["required_expected_revision"] == 0
    assert value["committed_revision"] == 1
    assert value["previous_state"] is None
    assert value["task_next_state_rule"] == "READY_ONLY_IF_IMMUTABLE_EXPECTED_PARENT_SET_IS_EMPTY_OTHERWISE_PENDING"
    assert value["run_next_state"] == "pending"
    assert value["canonical_record_count"] == value["operation_receipt_count"] == 1


def test_record_schema_and_revision_fields_exact():
    value = matrix()["canonical_transition_record"]
    assert set(value["required_fields"]) == validator.RECORD_FIELDS
    assert value["revision_rule"] == "to_revision == from_revision + 1"
    assert value["aggregate_revision_alias"] == "to_revision"
    assert value["create_or_admit_state_rule"] == {"previous_state": None, "next_state": "REQUIRED"}
    assert value["normal_lifecycle_state_rule"] == {"previous_state": "REQUIRED", "next_state": "REQUIRED"}


def test_canonical_store_and_atomic_commit_owner():
    value = matrix()["canonical_storage_authority"]
    assert value["canonical_transition_store"]["owner"] == "AGGREGATE_AUTHORITY_COMMIT"
    assert value["canonical_transition_store"]["replay_source"] is True
    assert value["transition_uniqueness_index"] == {"owner": "AGGREGATE_AUTHORITY_COMMIT", "key": "transition_id"}
    assert value["operation_receipt_index"] == {"owner": "AGGREGATE_AUTHORITY_COMMIT", "key": "canonical_aggregate_identity + operation_id"}
    assert value["atomicity"]["state_revision_record_receipt"] == "ONE_AUTHORITY_COMMIT"
    assert value["atomicity"]["projection_delivery"] == "OUTSIDE_AUTHORITY_COMMIT"


def test_mutation_classification_separates_coordination_and_transport():
    value = matrix()["mutation_classification"]
    assert value["ACCEPTED_AUTHORITY_MUTATION"] == {"revision_increment": 1, "canonical_transition_record_count": 1}
    assert value["COORDINATION_ONLY_MUTATION"] == {"revision_increment": 0, "canonical_transition_record_count": 0}
    assert value["TRANSPORT_ONLY_MUTATION"] == {"revision_increment": 0, "canonical_transition_record_count": 0}
    assert value["DURABLE_CONFLICT_RECORD"] == {"aggregate_revision_increment": 0, "canonical_transition_record_count": 0, "separate_conflict_record_count": 1}


def test_exact_operation_taxonomy_and_row_schema():
    rows = matrix()["operation_matrix"]
    assert {row["operation"] for row in rows} == validator.TAXONOMY
    assert len(rows) == 15
    assert all(set(row) == validator.OP_FIELDS for row in rows)


def test_authority_operations_consume_exactly_one_revision_and_record():
    for row in matrix()["operation_matrix"]:
        if row["consumes_revision"]:
            assert row["mutation_class"] == "ACCEPTED_AUTHORITY_MUTATION"
            assert row["canonical_record_count_on_success"] == 1
            assert row["operation_receipt_required"] is True


def test_heartbeat_is_coordination_only():
    row = next(row for row in matrix()["operation_matrix"] if row["operation"] == "TASK_HEARTBEAT")
    assert row["mutation_class"] == "COORDINATION_ONLY_MUTATION"
    assert row["consumes_revision"] is False
    assert row["canonical_record_count_on_success"] == 0


def test_legacy_run_complete_is_blocked():
    row = next(row for row in matrix()["operation_matrix"] if row["operation"] == "LEGACY_RUN_COMPLETE")
    assert row["decision_status"] == "BLOCKED"
    assert row["mutation_class"] == "UNSUPPORTED_LEGACY_OPERATION"
    assert row["duplicate_behavior"] == "BLOCKED_UNTIL_MIGRATED_OR_EXPLICIT_COMPATIBILITY_CONTRACT"


def test_all_frozen_operations_are_mapped():
    mapping = matrix()["frozen_operation_mapping"]
    assert set(mapping) == validator.FROZEN_OPERATIONS
    assert {row["operation"] for row in evidence()["operations"]} == set(mapping)


def test_projection_revision_contract():
    assert matrix()["projection_contract"] == {
        "next_contiguous_revision": "APPLY",
        "duplicate_or_older_revision": "DUPLICATE_NOOP",
        "revision_gap": "FAIL_CLOSED_AND_REPLAY_REQUIRED",
        "ordering_authority": "AGGREGATE_REVISION",
        "replay_source": "CANONICAL_TRANSITION_STORE",
    }


def test_findings_mapped_but_not_resolved():
    rows = matrix()["finding_dispositions"]
    assert {row["finding_id"] for row in rows} == validator.FINDINGS
    assert all(row["resolved_in_product"] is False and row["implementation_sprint"] == 81 for row in rows)


def test_manifest_hashes_and_scope():
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
    assert report["status"] == "PASS"
    assert report["product_implementation_authorized"] is False


def test_negative_duplicate_conflated_with_stale():
    value = copy.deepcopy(matrix())
    value["idempotency_and_concurrency"]["operation_receipt_missing_expected_revision_stale"]["result"] = "ALREADY_APPLIED"
    assert "stale revision conflict" in validator.validate_matrix(value, evidence())


def test_negative_receipt_lookup_after_revision_compare():
    value = copy.deepcopy(matrix())
    value["authority_commit_order"] = [
        "COMPARE_EXPECTED_REVISION", "RESOLVE_OPERATION_RECEIPT", "COMPARE_CANONICAL_COMMAND_HASH",
        "VALIDATE_STATE_TRANSITION", "COMMIT_STATE_REVISION_RECORD_RECEIPT_AND_INTENTS_ATOMICALLY",
    ]
    assert "authority commit order" in validator.validate_matrix(value, evidence())


def test_negative_optional_receipt_field():
    value = copy.deepcopy(matrix())
    value["operation_receipt_authority"]["immutable_value"]["canonical_command_hash"] = "OPTIONAL"
    assert "receipt immutable value" in validator.validate_matrix(value, evidence())


def test_negative_caller_supplied_hash_trusted():
    value = copy.deepcopy(matrix())
    value["canonical_command_hash"]["caller_supplied_hash_trusted"] = True
    assert "canonical hash trust" in validator.validate_matrix(value, evidence())


def test_negative_raw_transition_concatenation():
    value = copy.deepcopy(matrix())
    value["transition_identity"]["raw_delimiter_concatenation"] = "ALLOWED"
    assert "transition encoding" in validator.validate_matrix(value, evidence())


def test_negative_timestamp_as_ordering_authority():
    value = copy.deepcopy(matrix())
    value["revision_contract"]["committed_at_ms_ordering_authority"] = True
    assert "revision ordering authority" in validator.validate_matrix(value, evidence())


def test_negative_heartbeat_consumes_revision():
    value = copy.deepcopy(matrix())
    row = next(row for row in value["operation_matrix"] if row["operation"] == "TASK_HEARTBEAT")
    row["consumes_revision"] = True
    assert "operation cardinality TASK_HEARTBEAT" in validator.validate_matrix(value, evidence())


def test_negative_legacy_path_silently_authorized():
    value = copy.deepcopy(matrix())
    row = next(row for row in value["operation_matrix"] if row["operation"] == "LEGACY_RUN_COMPLETE")
    row["decision_status"] = "DECIDED"
    row["mutation_class"] = "ACCEPTED_AUTHORITY_MUTATION"
    assert "legacy operation block" in validator.validate_matrix(value, evidence())


def test_negative_missing_operation_taxonomy_row():
    value = copy.deepcopy(matrix())
    value["operation_matrix"] = [row for row in value["operation_matrix"] if row["operation"] != "TASK_REQUEUE"]
    assert "operation taxonomy" in validator.validate_matrix(value, evidence())


def test_negative_projection_gap_applied():
    value = copy.deepcopy(matrix())
    value["projection_contract"]["revision_gap"] = "APPLY"
    assert "projection contract" in validator.validate_matrix(value, evidence())


def test_negative_product_source_scope():
    assert validator.validate_scope([*validator.PATHS, "hfa-core/src/hfa/lua/task_complete.lua"])


def test_negative_evidence_mutation():
    value = copy.deepcopy(evidence())
    value["canonical_transition_record_count"] = 1
    assert "evidence canonical record count" in validator.validate_evidence(value)

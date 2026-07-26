from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path
from typing import Any, Iterable

BASE = "956c3247d5ceaaa0697547a31950918cce38fcd9"
SOURCE = "5734ac60704f8546b8ce67e766e042ff2ce4c412"
ZIP_SHA = "7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588"
EVIDENCE_SHA = "40325ea57afab4d1c4c31e009de7f7eba4c369e025bf6ccaa73d9a52432f35cb"
BUNDLE_ALGORITHM = "sorted_utf8_lines:path\\0size_bytes\\0sha256\\n"
PATHS = {
    ".github/workflows/sprint80c3-canonical-transition-revision.yml",
    "docs/adr/sprint80/ADR-080C3-canonical-transition-revision.md",
    "docs/adr/sprint80/STATUS_UPDATE_80C2_80C3.md",
    "docs/adr/sprint80/evidence/transition_cardinality.run93.json",
    "docs/adr/sprint80/canonical_transition_revision_matrix.json",
    "docs/adr/sprint80/canonical_transition_revision_manifest.json",
    "scripts/sprint80c/validate_canonical_transition_revision.py",
    "tests/diagnostics/sprint80c/test_80c3_canonical_transition_revision.py",
}
SOURCE_BINDINGS = {
    "docs/adr/sprint80/ADR-080C1-runtime-truth-authority.md": "659f77af1c8a99cf63626318cb050ac711352b74",
    "docs/adr/sprint80/ADR-080C2-aggregate-boundary.md": "11be8587892d2fa64f5a97f649de4524246c480e",
    "scripts/sprint80/transition_cardinality.py": "fd242956c55b04534d0c1f371514d114c1ef7acd",
    "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py": "4a07352e33424e32332c916acdfd29e02b3eb71b",
}
FROZEN_OPERATIONS = {
    "task_admit", "task_dispatch", "task_claim", "task_heartbeat", "task_complete", "task_requeue",
    "legacy_run_completion_sequence", "worker_terminal_duplicate_ack", "operator_terminal_duplicate_cleanup_command",
}
TAXONOMY = {
    "TASK_ADMIT", "TASK_DISPATCH", "TASK_CLAIM", "TASK_HEARTBEAT", "TASK_COMPLETE", "TASK_FAIL",
    "TASK_REQUEUE", "TASK_CANCEL", "TASK_DEPENDENCY_APPLY", "RUN_CREATE", "RUN_TERMINATE",
    "LEGACY_RUN_COMPLETE", "TERMINAL_DUPLICATE_CLEANUP", "MESSAGE_APPEND", "MESSAGE_ACK",
}
FINDINGS = {"no_verified_canonical_transition_record", "no_aggregate_revision_evidence"}
RECEIPT_FIELDS = {
    "operation_id", "canonical_command_hash", "canonical_record_hash", "transition_id", "aggregate_revision",
    "operation_type", "committed_at_ms",
}
RECORD_FIELDS = {
    "schema_version", "transition_id", "aggregate_type", "canonical_aggregate_identity",
    "canonical_aggregate_identity_sha256", "from_revision", "to_revision", "operation_type", "operation_id",
    "canonical_command_hash", "canonical_record_hash", "previous_state", "next_state",
    "authoritative_metadata_changes", "child_effects", "causation_id", "correlation_id", "writer_id",
    "committed_at_ms", "durable_projection_intents",
}
OP_FIELDS = {
    "operation", "decision_status", "authority_owner", "lifecycle_state_before", "lifecycle_state_after",
    "mutation_class", "authoritative_metadata_mutation", "consumes_revision",
    "canonical_record_count_on_success", "operation_receipt_required", "duplicate_behavior",
    "stale_revision_behavior", "projection_intents", "evidence_binding",
}
MANIFEST_EXACT_REGISTER = {
    "base_branch": "baseline/local-import",
    "branch": "sprint/80c3-canonical-transition-revision",
    "exact_changed_files": 8,
    "human_architecture_acceptance": "PENDING",
    "merge_authorized": False,
}
EXPECTED_OPERATION_CONTRACTS = {'LEGACY_RUN_COMPLETE': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                         'authority_owner': 'LEGACY_RUN_COMPATIBILITY_PATH',
                         'canonical_record_count_on_success': 0,
                         'consumes_revision': False,
                         'decision_status': 'BLOCKED',
                         'duplicate_behavior': 'BLOCKED_UNTIL_MIGRATED_OR_EXPLICIT_COMPATIBILITY_CONTRACT',
                         'evidence_binding': 'run93:legacy_run_completion_sequence',
                         'lifecycle_state_after': 'done',
                         'lifecycle_state_before': 'running',
                         'mutation_class': 'UNSUPPORTED_LEGACY_OPERATION',
                         'operation': 'LEGACY_RUN_COMPLETE',
                         'operation_receipt_required': False,
                         'projection_intents': [],
                         'stale_revision_behavior': 'BLOCKED_UNTIL_MIGRATED_OR_EXPLICIT_COMPATIBILITY_CONTRACT'},
 'MESSAGE_ACK': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                 'authority_owner': 'TRANSPORT',
                 'canonical_record_count_on_success': 0,
                 'consumes_revision': False,
                 'decision_status': 'DECIDED',
                 'duplicate_behavior': 'TRANSPORT_ACK_DUPLICATE_NOOP',
                 'evidence_binding': 'run93:worker_terminal_duplicate_ack',
                 'lifecycle_state_after': 'NOT_APPLICABLE',
                 'lifecycle_state_before': 'NOT_APPLICABLE',
                 'mutation_class': 'TRANSPORT_ONLY_MUTATION',
                 'operation': 'MESSAGE_ACK',
                 'operation_receipt_required': False,
                 'projection_intents': ['STREAM_ACK'],
                 'stale_revision_behavior': 'NOT_APPLICABLE'},
 'MESSAGE_APPEND': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                    'authority_owner': 'TRANSPORT',
                    'canonical_record_count_on_success': 0,
                    'consumes_revision': False,
                    'decision_status': 'DECIDED',
                    'duplicate_behavior': 'TRANSPORT_IDEMPOTENCY_CONTRACT',
                    'evidence_binding': 'run93:task_dispatch:TaskRequested',
                    'lifecycle_state_after': 'NOT_APPLICABLE',
                    'lifecycle_state_before': 'NOT_APPLICABLE',
                    'mutation_class': 'TRANSPORT_ONLY_MUTATION',
                    'operation': 'MESSAGE_APPEND',
                    'operation_receipt_required': False,
                    'projection_intents': ['STREAM_APPEND'],
                    'stale_revision_behavior': 'NOT_APPLICABLE'},
 'RUN_CREATE': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                'authority_owner': 'RUN_AGGREGATE',
                'canonical_record_count_on_success': 1,
                'consumes_revision': True,
                'decision_status': 'DECIDED',
                'duplicate_behavior': 'ALREADY_APPLIED',
                'evidence_binding': 'NO_RUN93_DIRECT_OBSERVATION',
                'lifecycle_state_after': 'pending',
                'lifecycle_state_before': None,
                'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                'operation': 'RUN_CREATE',
                'operation_receipt_required': True,
                'projection_intents': ['RUN_STATUS_PROJECTION'],
                'stale_revision_behavior': 'AGGREGATE_ALREADY_EXISTS_CONFLICT'},
 'RUN_TERMINATE': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                   'authority_owner': 'RUN_AGGREGATE',
                   'canonical_record_count_on_success': 1,
                   'consumes_revision': True,
                   'decision_status': 'DECIDED',
                   'duplicate_behavior': 'ALREADY_APPLIED',
                   'evidence_binding': 'NO_RUN93_DIRECT_OBSERVATION',
                   'lifecycle_state_after': ['done', 'failed'],
                   'lifecycle_state_before': ['pending', 'running'],
                   'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                   'operation': 'RUN_TERMINATE',
                   'operation_receipt_required': True,
                   'projection_intents': ['RUN_RESULT_PROJECTION'],
                   'stale_revision_behavior': 'STALE_REVISION_CONFLICT'},
 'TASK_ADMIT': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                'authority_owner': 'TASK_AGGREGATE',
                'canonical_record_count_on_success': 1,
                'consumes_revision': True,
                'decision_status': 'DECIDED',
                'duplicate_behavior': 'ALREADY_APPLIED',
                'evidence_binding': 'run93:task_admit',
                'lifecycle_state_after': ['pending', 'ready'],
                'lifecycle_state_before': None,
                'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                'operation': 'TASK_ADMIT',
                'operation_receipt_required': True,
                'projection_intents': ['READY_QUEUE_IF_READY'],
                'stale_revision_behavior': 'AGGREGATE_ALREADY_EXISTS_CONFLICT'},
 'TASK_CANCEL': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                 'authority_owner': 'TASK_AGGREGATE',
                 'canonical_record_count_on_success': 1,
                 'consumes_revision': True,
                 'decision_status': 'DECIDED',
                 'duplicate_behavior': 'ALREADY_APPLIED',
                 'evidence_binding': 'NO_RUN93_DIRECT_OBSERVATION',
                 'lifecycle_state_after': 'skipped',
                 'lifecycle_state_before': ['pending', 'ready', 'scheduled', 'running'],
                 'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                 'operation': 'TASK_CANCEL',
                 'operation_receipt_required': True,
                 'projection_intents': ['TERMINAL_PROJECTION'],
                 'stale_revision_behavior': 'STALE_REVISION_CONFLICT'},
 'TASK_CLAIM': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                'authority_owner': 'TASK_AGGREGATE',
                'canonical_record_count_on_success': 1,
                'consumes_revision': True,
                'decision_status': 'DECIDED',
                'duplicate_behavior': 'ALREADY_APPLIED',
                'evidence_binding': 'run93:task_claim',
                'lifecycle_state_after': 'running',
                'lifecycle_state_before': 'scheduled',
                'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                'operation': 'TASK_CLAIM',
                'operation_receipt_required': True,
                'projection_intents': ['RUNNING_SET'],
                'stale_revision_behavior': 'STALE_REVISION_CONFLICT'},
 'TASK_COMPLETE': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                   'authority_owner': 'TASK_AGGREGATE',
                   'canonical_record_count_on_success': 1,
                   'consumes_revision': True,
                   'decision_status': 'DECIDED',
                   'duplicate_behavior': 'ALREADY_APPLIED',
                   'evidence_binding': 'run93:task_complete',
                   'lifecycle_state_after': 'done',
                   'lifecycle_state_before': 'running',
                   'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                   'operation': 'TASK_COMPLETE',
                   'operation_receipt_required': True,
                   'projection_intents': ['OUTPUT_PROJECTION', 'DEPENDENCY_FANOUT_INTENT'],
                   'stale_revision_behavior': 'STALE_REVISION_CONFLICT'},
 'TASK_DEPENDENCY_APPLY': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                           'authority_owner': 'CHILD_TASK_AGGREGATE',
                           'canonical_record_count_on_success': 1,
                           'consumes_revision': True,
                           'decision_status': 'DECIDED',
                           'duplicate_behavior': 'ALREADY_APPLIED',
                           'evidence_binding': 'ADR-080C2_LOGICAL_EDGE_CONTRACT',
                           'lifecycle_state_after': ['pending', 'ready', 'blocked_by_failure'],
                           'lifecycle_state_before': 'pending',
                           'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                           'operation': 'TASK_DEPENDENCY_APPLY',
                           'operation_receipt_required': True,
                           'projection_intents': ['READY_QUEUE_IF_READY'],
                           'stale_revision_behavior': 'STALE_REVISION_CONFLICT'},
 'TASK_DISPATCH': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                   'authority_owner': 'TASK_AGGREGATE',
                   'canonical_record_count_on_success': 1,
                   'consumes_revision': True,
                   'decision_status': 'DECIDED',
                   'duplicate_behavior': 'ALREADY_APPLIED',
                   'evidence_binding': 'run93:task_dispatch',
                   'lifecycle_state_after': 'scheduled',
                   'lifecycle_state_before': 'ready',
                   'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                   'operation': 'TASK_DISPATCH',
                   'operation_receipt_required': True,
                   'projection_intents': ['CONTROL_NOTIFICATION', 'TASK_REQUEST_MESSAGE'],
                   'stale_revision_behavior': 'STALE_REVISION_CONFLICT'},
 'TASK_FAIL': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
               'authority_owner': 'TASK_AGGREGATE',
               'canonical_record_count_on_success': 1,
               'consumes_revision': True,
               'decision_status': 'DECIDED',
               'duplicate_behavior': 'ALREADY_APPLIED',
               'evidence_binding': 'NO_RUN93_DIRECT_OBSERVATION',
               'lifecycle_state_after': 'failed',
               'lifecycle_state_before': 'running',
               'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
               'operation': 'TASK_FAIL',
               'operation_receipt_required': True,
               'projection_intents': ['DEPENDENCY_FAILURE_FANOUT_INTENT'],
               'stale_revision_behavior': 'STALE_REVISION_CONFLICT'},
 'TASK_HEARTBEAT': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                    'authority_owner': 'TASK_AGGREGATE',
                    'canonical_record_count_on_success': 0,
                    'consumes_revision': False,
                    'decision_status': 'DECIDED',
                    'duplicate_behavior': 'COORDINATION_DUPLICATE_NOOP',
                    'evidence_binding': 'run93:task_heartbeat',
                    'lifecycle_state_after': 'running',
                    'lifecycle_state_before': 'running',
                    'mutation_class': 'COORDINATION_ONLY_MUTATION',
                    'operation': 'TASK_HEARTBEAT',
                    'operation_receipt_required': False,
                    'projection_intents': ['LIVENESS_TTL'],
                    'stale_revision_behavior': 'NOT_APPLICABLE'},
 'TASK_REQUEUE': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                  'authority_owner': 'TASK_AGGREGATE',
                  'canonical_record_count_on_success': 1,
                  'consumes_revision': True,
                  'decision_status': 'DECIDED',
                  'duplicate_behavior': 'ALREADY_APPLIED',
                  'evidence_binding': 'run93:task_requeue',
                  'lifecycle_state_after': 'ready',
                  'lifecycle_state_before': 'running',
                  'mutation_class': 'ACCEPTED_AUTHORITY_MUTATION',
                  'operation': 'TASK_REQUEUE',
                  'operation_receipt_required': True,
                  'projection_intents': ['READY_QUEUE', 'REQUEUE_NOTIFICATION'],
                  'stale_revision_behavior': 'STALE_REVISION_CONFLICT'},
 'TERMINAL_DUPLICATE_CLEANUP': {'authoritative_metadata_mutation': 'ALLOWED_ONLY_IF_DECLARED_IN_CANONICAL_COMMAND',
                                'authority_owner': 'OPERATOR_COORDINATION',
                                'canonical_record_count_on_success': 0,
                                'consumes_revision': False,
                                'decision_status': 'DECIDED',
                                'duplicate_behavior': 'AUDITABLE_TRANSPORT_DUPLICATE_NOOP',
                                'evidence_binding': 'run93:operator_terminal_duplicate_cleanup_command',
                                'lifecycle_state_after': 'terminal',
                                'lifecycle_state_before': 'terminal',
                                'mutation_class': 'TRANSPORT_ONLY_MUTATION',
                                'operation': 'TERMINAL_DUPLICATE_CLEANUP',
                                'operation_receipt_required': False,
                                'projection_intents': ['AUDIT_INTENT', 'AUDIT_OUTCOME'],
                                'stale_revision_behavior': 'NOT_APPLICABLE'}}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def add(ok: bool, message: str, errors: list[str]) -> None:
    if not ok:
        errors.append(message)


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def bundle_index(entries: list[dict[str, Any]]) -> bytes:
    return "".join(
        f'{row["path"]}\0{row["size_bytes"]}\0{row["sha256"]}\n'
        for row in sorted(entries, key=lambda row: row["path"])
    ).encode("utf-8")


def validate_evidence(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    add(evidence.get("schema_version") == 2, "evidence schema", errors)
    add(evidence.get("operation_count") == 9, "evidence operation count", errors)
    add(evidence.get("canonical_transition_record_count") == 0, "evidence canonical record count", errors)
    add(evidence.get("aggregate_revision_evidence_count") == 0, "evidence revision count", errors)
    operations = evidence.get("operations") or []
    add({row.get("operation") for row in operations} == FROZEN_OPERATIONS, "frozen operation set", errors)
    add(all(row.get("canonical_transition_record_count") == 0 for row in operations), "per-operation canonical count", errors)
    add(all(row.get("aggregate_revision_evidence_count") == 0 for row in operations), "per-operation revision count", errors)
    return errors


def validate_matrix(matrix: dict[str, Any], evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    add(matrix.get("schema_version") == 2 and matrix.get("decision_id") == "ADR-080C3", "matrix identity", errors)
    add(matrix.get("decision_status") == "CORRECTED_TECHNICAL_RECOMMENDATION_READY_FOR_FINAL_REVIEW", "matrix status", errors)
    add(matrix.get("base_head") == BASE, "matrix base", errors)
    add(matrix.get("product_implementation_authorized") is False and matrix.get("sprint81_implementation_authorized") is False, "implementation flags", errors)
    add(matrix.get("merge_authorized") is False and matrix.get("human_architecture_acceptance") == "PENDING", "governance flags", errors)

    predecessors = matrix.get("predecessors") or {}
    expected_predecessors = {
        "ADR-080C1": ("9ad9503badd72afb0a935dbb8c02e828ea02d3e2", 5083135387, "9d7c7a6ad52a8a546a708dda048f28d4e23befc6"),
        "ADR-080C2": ("edb86b3560250c09cfc08ea28fa22aae88490382", 5083381947, BASE),
    }
    for key, (head, comment, merge) in expected_predecessors.items():
        row = predecessors.get(key) or {}
        add(row.get("status") == "ACCEPTED_MERGED_COMPLETE", f"{key} status", errors)
        add(row.get("accepted_head") == head and row.get("acceptance_comment_id") == comment and row.get("merge_commit") == merge, f"{key} acceptance binding", errors)

    frozen = matrix.get("evidence") or {}
    add(frozen == {
        "source_head": SOURCE, "artifact_sha256": ZIP_SHA, "transition_cardinality_sha256": EVIDENCE_SHA,
        "operation_count": 9, "canonical_transition_record_count": 0, "aggregate_revision_evidence_count": 0,
    }, "evidence register", errors)
    add(frozen.get("operation_count") == evidence.get("operation_count"), "evidence matrix binding", errors)

    identity = matrix.get("canonical_aggregate_identity") or {}
    add(identity.get("task") == "task:{run_id}:{task_id}" and identity.get("run") == "run:{run_id}", "canonical aggregate identity", errors)
    add(identity.get("raw_task_id_as_canonical_identity") == "FORBIDDEN", "raw task identity guard", errors)
    transition = matrix.get("transition_identity") or {}
    add(transition.get("format") == "ctr:v1:{canonical_aggregate_identity_sha256}:{aggregate_revision}:{operation_id_sha256}", "transition format", errors)
    add(transition.get("component_encoding") == "SHA256_OF_LENGTH_PREFIXED_UTF8_COMPONENT" and transition.get("raw_delimiter_concatenation") == "FORBIDDEN", "transition encoding", errors)
    add(transition.get("uniqueness_scope") == "GLOBAL" and transition.get("operation_id_scope") == "AGGREGATE_LOCAL_STABLE_IDEMPOTENCY_IDENTITY", "transition uniqueness", errors)

    preconditions = matrix.get("authority_entry_preconditions") or {}
    expected_preconditions = {
        "order": "OUTER_GATE_BEFORE_OPERATION_RECEIPT_LOOKUP",
        "validate_authenticated_writer": "REQUIRED",
        "validate_writer_operation_capability": "REQUIRED",
        "validate_claim_or_lease_fence_when_applicable": "REQUIRED",
        "validate_target_aggregate_identity": "REQUIRED",
        "accepted_predecessor_binding": "ADR-080C1_RUNTIME_TRUTH_AUTHORITY_AND_ADR-080C2_AGGREGATE_BOUNDARY",
        "precondition_failure": {
            "result": "AUTHORITY_ENTRY_REJECTED", "operation_receipt_disclosure": "FORBIDDEN",
            "aggregate_mutation": 0, "revision_increment": 0, "canonical_record_count": 0,
            "durable_conflict_record": "AS_REQUIRED_BY_SECURITY_AUDIT_POLICY",
        },
    }
    add(preconditions == expected_preconditions, "authority entry preconditions", errors)
    add(matrix.get("authority_evaluation_order") == ["VALIDATE_AUTHORITY_ENTRY_PRECONDITIONS", *matrix.get("authority_commit_order", [])], "authority evaluation order", errors)

    command_hash = matrix.get("canonical_command_hash") or {}
    expected_hash_members = [
        "aggregate_type", "canonical_aggregate_identity", "operation_type", "operation_id", "expected_revision",
        "intended_previous_state", "intended_next_state", "authoritative_payload",
        "authoritative_metadata_changes", "requested_child_effects", "requested_projection_intents", "causation_id",
    ]
    expected_jcs = {
        "standard": "RFC_8785_JCS", "unicode_input_normalization": "UTF8_NFC_BEFORE_JCS",
        "integer_float_encoding": "RFC_8785_ECMASCRIPT_NUMBER_SERIALIZATION",
        "negative_zero": "RFC_8785_NORMALIZATION", "non_finite_numbers": "FORBIDDEN",
        "duplicate_object_keys": "FORBIDDEN", "array_order": "PRESERVED",
        "binary_values": "BASE64URL_WITH_EXPLICIT_TYPE_TAG", "timestamp_representation": "INTEGER_MILLISECONDS_UTC",
    }
    add(command_hash.get("required") is True and command_hash.get("algorithm") == "SHA256", "canonical command hash required", errors)
    add(command_hash.get("serialization") == "RFC_8785_JCS" and command_hash.get("serialization_profile") == "CANONICAL_JSON_RFC_8785_JCS_WITH_UTF8_NFC_INPUT", "canonical hash serialization", errors)
    add(command_hash.get("canonical_json_standard") == expected_jcs, "canonical JSON exact standard", errors)
    add(command_hash.get("omitted_optional_fields") == "FORBIDDEN_USE_EXPLICIT_NULL" and command_hash.get("caller_supplied_hash_trusted") is False, "canonical hash trust", errors)
    add(command_hash.get("hash_members") == expected_hash_members, "canonical hash members", errors)
    add(set(command_hash.get("excluded_members") or []) == {"committed_at_ms", "writer_generated_metadata", "committed_revision", "transition_id"}, "canonical hash exclusions", errors)

    effect_binding = matrix.get("authoritative_effect_binding") or {}
    expected_effect_binding = {
        "model": "MODEL_A_HASH_ALL_REQUESTED_AUTHORITATIVE_EFFECTS",
        "hash_bound_command_fields": ["authoritative_metadata_changes", "requested_child_effects", "requested_projection_intents"],
        "record_effect_mapping": {
            "authoritative_metadata_changes": "EXACT_HASH_BOUND_COMMAND_VALUE",
            "child_effects": "EXACT_ACCEPTED_REQUESTED_CHILD_EFFECTS",
            "durable_projection_intents": "EXACT_ACCEPTED_REQUESTED_PROJECTION_INTENTS",
        },
        "caller_or_writer_local_effect_override": "FORBIDDEN",
        "same_operation_id_and_same_command_hash": {
            "immutable_record_effects_must_match": True, "canonical_record_hash_must_match": True,
        },
    }
    add(effect_binding == expected_effect_binding, "authoritative effect binding", errors)

    receipt = matrix.get("operation_receipt_authority") or {}
    add(receipt.get("key") == "oprcpt:v1:{canonical_aggregate_identity_sha256}:{operation_id_sha256}", "receipt key", errors)
    add(receipt.get("owner") == "AGGREGATE_AUTHORITY_COMMIT" and receipt.get("immutable") is True, "receipt authority", errors)
    receipt_values = receipt.get("immutable_value") or {}
    add(set(receipt_values) == RECEIPT_FIELDS and all(receipt_values[field] == "REQUIRED" for field in RECEIPT_FIELDS), "receipt immutable value", errors)
    add(receipt.get("second_lifecycle_truth") is False, "receipt not second truth", errors)

    expected_commit_order = [
        "RESOLVE_OPERATION_RECEIPT", "COMPARE_CANONICAL_COMMAND_HASH", "COMPARE_EXPECTED_REVISION",
        "VALIDATE_STATE_TRANSITION", "COMMIT_STATE_REVISION_RECORD_RECEIPT_AND_INTENTS_ATOMICALLY",
    ]
    add(matrix.get("authority_commit_order") == expected_commit_order, "authority commit order", errors)
    idem = matrix.get("idempotency_and_concurrency") or {}
    same = idem.get("operation_receipt_exists_same_payload") or {}
    add(same == {
        "result": "ALREADY_APPLIED", "return_existing_transition_id": "REQUIRED", "mutation": 0,
        "revision_increment": 0, "canonical_record_count": 0, "canonical_record_hash_match": "REQUIRED",
        "immutable_record_effects_match": "REQUIRED",
    }, "same operation duplicate", errors)
    different = idem.get("operation_receipt_exists_different_payload") or {}
    add(different.get("result") == "IDEMPOTENCY_CONFLICT" and different.get("mutation") == 0 and different.get("durable_conflict_record") == "REQUIRED" and different.get("reconciliation_candidate") == "REQUIRED", "idempotency conflict", errors)
    stale = idem.get("operation_receipt_missing_expected_revision_stale") or {}
    add(stale.get("result") == "STALE_REVISION_CONFLICT" and stale.get("mutation") == 0 and stale.get("canonical_record_count") == 0, "stale revision conflict", errors)
    future = idem.get("operation_receipt_missing_expected_revision_future") or {}
    add(future.get("result") == "FUTURE_REVISION_CONFLICT" and future.get("mutation") == 0 and future.get("reconciliation_candidate") == "REQUIRED", "future revision conflict", errors)
    add(idem.get("already_applied_proof") == "DURABLE_OPERATION_RECEIPT_ONLY", "already applied proof", errors)

    revision = matrix.get("revision_contract") or {}
    add(revision.get("initial_revision") == 0 and revision.get("first_committed_transition_revision") == 1, "revision origin", errors)
    add(revision.get("next_revision_rule") == "to_revision == from_revision + 1" and revision.get("monotonicity") == "STRICT_CONTIGUOUS", "revision sequence", errors)
    add(revision.get("gap") == revision.get("reuse") == revision.get("regression") == "FORBIDDEN", "revision prohibitions", errors)
    add(revision.get("expected_revision_required") is True and revision.get("ordering_authority") == "AGGREGATE_REVISION" and revision.get("committed_at_ms_ordering_authority") is False, "revision ordering authority", errors)

    creation = matrix.get("aggregate_creation_contract") or {}
    add(creation.get("aggregate_not_exists_logical_current_revision") == 0 and creation.get("required_expected_revision") == 0 and creation.get("committed_revision") == 1, "aggregate creation revisions", errors)
    add(creation.get("canonical_record_count") == 1 and creation.get("operation_receipt_count") == 1, "aggregate creation cardinality", errors)

    record = matrix.get("canonical_transition_record") or {}
    add(set(record.get("required_fields") or []) == RECORD_FIELDS, "record required fields", errors)
    add(record.get("revision_rule") == "to_revision == from_revision + 1" and record.get("aggregate_revision_alias") == "to_revision", "record revision rule", errors)
    expected_record_hash = {
        "algorithm": "SHA256", "serialization": "RFC_8785_JCS",
        "hash_scope": "ALL_IMMUTABLE_RECORD_FIELDS_EXCEPT_CANONICAL_RECORD_HASH",
        "caller_supplied_hash_trusted": False,
    }
    add(record.get("canonical_record_hash") == expected_record_hash, "canonical record hash", errors)
    add(record.get("append_only") is True and record.get("immutable") is True and record.get("owner") == "AGGREGATE_AUTHORITY_COMMIT", "record authority", errors)
    add(record.get("coordination_transport_projection_record") == "FORBIDDEN_UNLESS_ACCEPTED_AUTHORITY_MUTATION", "non-authority record guard", errors)

    storage = matrix.get("canonical_storage_authority") or {}
    store = storage.get("canonical_transition_store") or {}
    add(store.get("owner") == "AGGREGATE_AUTHORITY_COMMIT" and store.get("append_only") is True and store.get("immutable") is True and store.get("replay_source") is True, "canonical store authority", errors)
    add(store.get("physical_layout") == "DEFERRED_TO_SPRINT81", "physical layout deferral", errors)
    expected_index = {
        "owner": "AGGREGATE_AUTHORITY_COMMIT", "key": "transition_id",
        "same_transition_id_same_record": "ALREADY_PRESENT",
        "same_transition_id_different_record": "CANONICAL_RECORD_CORRUPTION_CONFLICT",
        "different_record_mutation": 0, "different_record_durable_conflict_record": "REQUIRED",
        "different_record_reconciliation_candidate": "REQUIRED",
    }
    add((storage.get("transition_uniqueness_index") or {}) == expected_index, "transition uniqueness index", errors)
    expected_collision = {
        "comparison_authority": "CANONICAL_RECORD_HASH",
        "same_transition_id_same_record": {"result": "ALREADY_PRESENT", "mutation": 0},
        "same_transition_id_different_record": {
            "result": "CANONICAL_RECORD_CORRUPTION_CONFLICT", "mutation": 0,
            "durable_conflict_record": "REQUIRED", "reconciliation_candidate": "REQUIRED",
        },
    }
    add(storage.get("canonical_record_collision") == expected_collision, "canonical record collision", errors)
    add((storage.get("operation_receipt_index") or {}) == {"owner": "AGGREGATE_AUTHORITY_COMMIT", "key": "canonical_aggregate_identity + operation_id"}, "operation receipt index", errors)
    atomicity = storage.get("atomicity") or {}
    add(atomicity.get("state_revision_record_receipt") == "ONE_AUTHORITY_COMMIT" and atomicity.get("authoritative_metadata_and_projection_intents") == "SAME_AUTHORITY_COMMIT" and atomicity.get("projection_delivery") == "OUTSIDE_AUTHORITY_COMMIT", "authority atomicity", errors)

    operations = matrix.get("operation_matrix") or []
    observed = {row.get("operation"): row for row in operations}
    add(set(observed) == TAXONOMY and len(operations) == 15, "operation taxonomy", errors)
    add(all(set(row) == OP_FIELDS for row in operations), "operation row fields", errors)
    add(observed == EXPECTED_OPERATION_CONTRACTS, "exact operation contract register", errors)

    mapping = matrix.get("frozen_operation_mapping") or {}
    add(set(mapping) == FROZEN_OPERATIONS and set(mapping.values()) <= TAXONOMY, "frozen operation mapping", errors)
    add({row.get("operation") for row in evidence.get("operations") or []} == set(mapping), "frozen mapping evidence parity", errors)

    expected_projection = {
        "ordering_authority": "AGGREGATE_REVISION", "replay_source": "CANONICAL_TRANSITION_STORE",
        "projection_application_receipt": {
            "owner": "PROJECTION", "key": "canonical_aggregate_identity",
            "value": {"applied_revision": "REQUIRED", "applied_transition_id": "REQUIRED", "applied_record_hash": "REQUIRED"},
        },
        "incoming_revision_equals_applied_revision": {
            "same_transition_id_and_record_hash": {"result": "DUPLICATE_NOOP", "projection_mutation": 0},
            "different_transition_id_or_record_hash": {
                "result": "PROJECTION_CORRUPTION_CONFLICT", "projection_mutation": 0,
                "durable_conflict_record": "REQUIRED", "reconciliation_candidate": "REQUIRED",
            },
        },
        "incoming_revision_older_than_applied_revision": {"result": "OLDER_REVISION_NOOP", "projection_mutation": 0},
        "incoming_revision_is_applied_revision_plus_one": {"result": "APPLY"},
        "incoming_revision_greater_than_applied_revision_plus_one": {
            "result": "GAP_FAIL_CLOSED_AND_REPLAY_REQUIRED", "projection_mutation": 0,
        },
    }
    add(matrix.get("projection_contract") == expected_projection, "projection contract", errors)

    findings = matrix.get("finding_dispositions") or []
    add({row.get("finding_id") for row in findings} == FINDINGS, "finding set", errors)
    add(all(row.get("decision_status") == "ARCHITECTURE_DECISION_ASSIGNED" and row.get("resolved_in_product") is False and row.get("implementation_sprint") == 81 for row in findings), "finding dispositions", errors)
    add({row.get("path"): row.get("blob_sha") for row in matrix.get("source_bindings") or []} == SOURCE_BINDINGS, "source binding register", errors)

    contracts = matrix.get("verification_contracts") or {}
    expected_true = {
        "canonical_aggregate_identity_decided", "canonical_command_hash_required", "operation_receipt_identity_decided",
        "predecessor_acceptance_bindings_exact", "receipt_lookup_before_revision_compare",
        "state_revision_record_receipt_one_commit", "strict_contiguous_revision", "transition_id_encoding_decided",
        "authoritative_effect_hash_binding_exact", "same_hash_same_immutable_effects",
        "canonical_record_collision_decided", "projection_same_revision_collision_decided",
        "operation_row_semantics_mutation_tests_present", "authority_entry_preconditions_decided",
        "canonical_hash_serialization_standard_exact", "manifest_governance_register_exactly_validated",
    }
    add(all(contracts.get(key) is True for key in expected_true), "verification exact flags", errors)
    add(contracts.get("technical_unresolved_choice") == 0 and contracts.get("operation_taxonomy_exact_count") == 15 and contracts.get("operation_taxonomy_exact_rows_machine_verified") == 15, "verification completeness", errors)
    add(contracts.get("product_source_mutation") == 0 and contracts.get("implementation_claims") == 0, "scope verification contract", errors)
    return errors


def validate_manifest(root: Path, manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    add(manifest.get("schema_version") == 2 and manifest.get("decision_id") == "ADR-080C3", "manifest identity", errors)
    add(manifest.get("base_head") == BASE and manifest.get("frozen_source_head") == SOURCE, "manifest source heads", errors)
    add(manifest.get("frozen_artifact_sha256") == ZIP_SHA and manifest.get("transition_cardinality_sha256") == EVIDENCE_SHA, "manifest evidence hashes", errors)
    add(manifest.get("bundle_index_algorithm") == BUNDLE_ALGORITHM, "manifest bundle algorithm", errors)
    add(manifest.get("product_source_mutation") == 0 and manifest.get("implementation_authorized") is False, "manifest scope flags", errors)
    add(all(manifest.get(key) == value for key, value in MANIFEST_EXACT_REGISTER.items()), "manifest exact governance register", errors)
    entries = manifest.get("artifacts") or []
    expected = PATHS - {"docs/adr/sprint80/canonical_transition_revision_manifest.json"}
    add({row.get("path") for row in entries} == expected and len(entries) == 7, "manifest artifact paths", errors)
    for row in entries:
        path = root / row["path"]
        add(path.exists(), f'missing artifact {row["path"]}', errors)
        if path.exists():
            add(path.stat().st_size == row.get("size_bytes"), f'artifact size {row["path"]}', errors)
            add(sha(path.read_bytes()) == row.get("sha256"), f'artifact hash {row["path"]}', errors)
    add(manifest.get("bundle_index_sha256") == sha(bundle_index(entries)), "manifest bundle index", errors)
    return errors


def validate_scope(paths: Iterable[str]) -> list[str]:
    observed = {str(path).replace("\\", "/") for path in paths}
    if observed == PATHS:
        return []
    return [json.dumps({"missing": sorted(PATHS - observed), "unexpected": sorted(observed - PATHS)}, sort_keys=True)]


def validate_source_bindings(root: Path) -> list[str]:
    errors: list[str] = []
    for path, expected_sha in SOURCE_BINDINGS.items():
        try:
            add(git(root, "rev-parse", f"{BASE}:{path}") == expected_sha, f"source binding {path}", errors)
        except subprocess.CalledProcessError:
            errors.append(f"source binding unreadable {path}")
    return errors


def validate_zip(path: Path, evidence_bytes: bytes) -> list[str]:
    errors: list[str] = []
    add(sha(path.read_bytes()) == ZIP_SHA, "frozen ZIP digest", errors)
    if errors:
        return errors
    with zipfile.ZipFile(path) as archive:
        add("transition_cardinality.json" in archive.namelist(), "frozen ZIP member", errors)
        frozen = archive.read("transition_cardinality.json")
    add(sha(frozen) == EVIDENCE_SHA and frozen == evidence_bytes, "frozen transition evidence bytes", errors)
    return errors


def validate_bundle(
    root: Path,
    *,
    evidence_zip: Path | None = None,
    changed_paths: Iterable[str] | None = None,
    verify_source_bindings: bool = False,
    verify_git_scope: bool = False,
) -> dict[str, Any]:
    evidence_path = root / "docs/adr/sprint80/evidence/transition_cardinality.run93.json"
    evidence_bytes = evidence_path.read_bytes()
    evidence = json.loads(evidence_bytes)
    matrix = read_json(root / "docs/adr/sprint80/canonical_transition_revision_matrix.json")
    manifest = read_json(root / "docs/adr/sprint80/canonical_transition_revision_manifest.json")
    errors: list[str] = []
    add(sha(evidence_bytes) == EVIDENCE_SHA, "repository evidence digest", errors)
    errors += validate_evidence(evidence)
    errors += validate_matrix(matrix, evidence)
    errors += validate_manifest(root, manifest)
    if changed_paths is not None:
        errors += validate_scope(changed_paths)
    if verify_git_scope:
        errors += validate_scope(git(root, "diff", "--name-only", f"{BASE}...HEAD").splitlines())
    if verify_source_bindings:
        errors += validate_source_bindings(root)
    historical = "NOT_RUN"
    if evidence_zip is not None:
        zip_errors = validate_zip(evidence_zip, evidence_bytes)
        errors += zip_errors
        historical = "PASS" if not zip_errors else "FAIL"
    return {
        "schema_version": 2,
        "status": "FAIL" if errors else "PASS",
        "technical_status": "FAIL" if errors else "PASS",
        "human_architecture_acceptance": "PENDING",
        "merge_authorized": False,
        "product_implementation_authorized": False,
        "base_head": BASE,
        "frozen_source_head": SOURCE,
        "frozen_artifact_sha256": ZIP_SHA,
        "transition_cardinality_sha256": EVIDENCE_SHA,
        "operation_count": evidence.get("operation_count"),
        "taxonomy_count": len(TAXONOMY),
        "finding_count": len(FINDINGS),
        "changed_path_count": len(PATHS),
        "historical_frozen_zip_verification": historical,
        "source_binding_verification": "PASS" if verify_source_bindings and not any("source binding" in error for error in errors) else ("NOT_RUN" if not verify_source_bindings else "FAIL"),
        "git_scope_verification": "PASS" if verify_git_scope and not any(error.startswith('{"missing"') for error in errors) else ("NOT_RUN" if not verify_git_scope else "FAIL"),
        "product_source_mutation": 0,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--evidence-zip", type=Path)
    parser.add_argument("--changed-path", action="append", default=[])
    parser.add_argument("--verify-source-bindings", action="store_true")
    parser.add_argument("--verify-git-scope", action="store_true")
    args = parser.parse_args()
    report = validate_bundle(
        args.repo_root.resolve(),
        evidence_zip=args.evidence_zip.resolve() if args.evidence_zip else None,
        changed_paths=args.changed_path or None,
        verify_source_bindings=args.verify_source_bindings,
        verify_git_scope=args.verify_git_scope,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["technical_status"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())

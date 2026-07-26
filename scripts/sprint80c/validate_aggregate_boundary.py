from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path
from typing import Any, Iterable

BASE = "9d7c7a6ad52a8a546a708dda048f28d4e23befc6"
SOURCE = "5734ac60704f8546b8ce67e766e042ff2ce4c412"
ZIP_SHA = "7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588"
EVIDENCE_SHA = "ac5ab01455a3e8acb6bc0043167849e398df393819f5b4cdf14e9770dea9f478"
FINDINGS = {"missing_remaining_counter_unlocks_fail_open", "ready_marker_strands_pending_child"}
PATHS = {
    ".github/workflows/sprint80c2-aggregate-boundary.yml",
    "docs/adr/sprint80/ADR-080C2-aggregate-boundary.md",
    "docs/adr/sprint80/evidence/aggregate_boundary.run93.json",
    "docs/adr/sprint80/aggregate_boundary_manifest.json",
    "docs/adr/sprint80/aggregate_boundary_matrix.json",
    "scripts/sprint80c/validate_aggregate_boundary.py",
    "tests/diagnostics/sprint80c/test_80c2_aggregate_boundary.py",
}
BINDINGS = {
    "docs/adr/sprint80/ADR-080C-decision-package.md": "ebc15272498fafbe115c164df4075257c090d539",
    "docs/adr/sprint80/ADR-080C1-runtime-truth-authority.md": "659f77af1c8a99cf63626318cb050ac711352b74",
    "scripts/sprint80/aggregate_boundary.py": "1d519c10f952907d4ac7a73e0854cff913a61eb6",
    "tests/diagnostics/sprint80/test_80_07_aggregate_boundary.py": "7462e70146923711937eacf801af7cee778ae0eb",
    "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py": "4a07352e33424e32332c916acdfd29e02b3eb71b",
    "hfa-core/src/hfa/lua/task_complete.lua": "8ee2b51cd94642f56d5e38680419a2e3988b26a9",
    "hfa-core/src/hfa/lua/task_admit.lua": "e5bd30953f4c7c3258e589d240113295f8bccadc",
    "hfa-core/src/hfa/dag/schema.py": "cd657e4b37975c3cf3080a221222c6f8caf56fec",
}
ZIP_MEMBERS = {
    "aggregate_boundary.json", "contracts_pytest.txt", "environment.json",
    "environment_after.json", "final_summary.json", "manifest.json",
    "preflight.json", "preflight_after.json", "reality_pytest.txt",
    "transition_cardinality.json", "truth_contradictions.json",
    "ttl_durability.json", "writer_inventory.json",
}
TERMINAL_STATES = {"done", "failed", "blocked_by_failure", "dead_lettered", "skipped"}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def readj(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def add_error(ok: bool, message: str, errors: list[str]) -> None:
    if not ok:
        errors.append(message)


def bundle_index(entries: list[dict[str, Any]]) -> bytes:
    return "".join(
        f'{row["path"]}\0{row["size_bytes"]}\0{row["sha256"]}\n'
        for row in sorted(entries, key=lambda value: value["path"])
    ).encode("utf-8")


def validate_evidence(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    add_error(evidence.get("schema_version") == 1, "evidence schema", errors)
    add_error(evidence.get("observation_count") == 8, "evidence count", errors)
    add_error(
        evidence.get("global_result") == "CROSS_TASK_MUTATION_WITHOUT_CHILD_AUTHORITY_RECORD",
        "evidence result",
        errors,
    )
    add_error(evidence.get("cross_task_mutation_observed") is True, "cross-task observation", errors)
    add_error(evidence.get("aggregate_revision_observed") is False, "aggregate revision observation", errors)
    add_error(evidence.get("child_authority_record_observed") is False, "child authority observation", errors)
    add_error(set(evidence.get("blocking_findings", [])) == FINDINGS, "finding set", errors)
    return errors


def validate_matrix(matrix: dict[str, Any], evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    add_error(matrix.get("schema_version") == 3 and matrix.get("decision_id") == "ADR-080C2", "matrix identity", errors)
    add_error(
        matrix.get("decision_status") == "CORRECTED_TECHNICAL_RECOMMENDATION_PREDECESSOR_ACCEPTANCE_PENDING",
        "decision status",
        errors,
    )
    add_error(matrix.get("product_implementation_authorized") is False, "implementation flag", errors)

    predecessor = matrix.get("predecessor_governance", {})
    add_error(predecessor.get("accepted_head_candidate") == "9ad9503badd72afb0a935dbb8c02e828ea02d3e2", "predecessor head", errors)
    add_error(predecessor.get("merge_commit") == BASE, "predecessor merge", errors)
    add_error(
        predecessor.get("acceptance_comment_id") is None
        and predecessor.get("human_architecture_acceptance") == "PENDING",
        "predecessor must remain pending without record",
        errors,
    )

    selected = matrix.get("selected_model", {})
    add_error(
        selected.get("name") == "INDEPENDENT_TASK_AGGREGATES_WITH_DURABLE_DEPENDENCY_PROCESS_MANAGER",
        "selected model",
        errors,
    )
    add_error(selected.get("logical_task_aggregate_identity") == "task:{run_id}:{task_id}", "logical identity", errors)
    add_error(selected.get("direct_parent_to_child_authority_mutation") == "FORBIDDEN", "cross-task mutation", errors)

    physical = matrix.get("physical_identity_strategy", {})
    add_error(physical.get("physical_key_layout") == "EXISTING_TASK_ID_KEYS_TEMPORARILY_RETAINED", "physical key strategy", errors)
    add_error(
        physical.get("required_invariant") == "TASK_ID_GLOBALLY_UNIQUE_ACROSS_ALL_RUNS"
        and physical.get("physical_rekey_in_80C2") == "NOT_SELECTED",
        "identity invariant",
        errors,
    )

    boundaries = matrix.get("transaction_boundaries", {})
    admission = boundaries.get("child_admission", {})
    admission_members = {
        "child_identity", "child_initial_state", "expected_parent_edge_set",
        "dependency_policy", "graph_identity", "graph_revision_or_topology_hash",
        "child_initial_revision", "one_canonical_admission_record",
    }
    add_error(admission.get("authority_scope") == "ONE_CHILD_TASK_AGGREGATE", "child admission scope", errors)
    add_error(set(admission.get("atomic_commit_members", [])) == admission_members, "child admission members", errors)
    add_error(admission.get("expected_parent_edge_set_owner") == "CHILD_TASK_AGGREGATE", "topology owner", errors)
    add_error(admission.get("expected_parent_edge_set_mutability") == "IMMUTABLE_AFTER_ADMISSION", "topology mutability", errors)
    add_error(admission.get("dependency_count_authority") is False, "dependency count authority", errors)

    child_apply = boundaries.get("child_dependency_application", {})
    add_error(
        "logical_edge_resolution" in child_apply.get("atomic_commit_members", [])
        and "outcome_aware_edge_receipt" in child_apply.get("atomic_commit_members", []),
        "logical-edge atomic members",
        errors,
    )

    identity = matrix.get("edge_identity_contract", {})
    add_error(identity.get("sole_command_idempotency_authority") == "edge_command_id", "single command identity", errors)
    add_error(identity.get("receipt_key_authority") == "edge_command_id", "receipt identity", errors)

    resolution = matrix.get("logical_edge_resolution_authority", {})
    add_error(resolution.get("key") == "logical_edge_id", "logical-edge resolution key", errors)
    add_error(resolution.get("owner") == "CHILD_TASK_AGGREGATE", "logical-edge resolution owner", errors)
    required_resolution_fields = {
        "accepted_edge_command_id", "accepted_parent_transition_id", "accepted_outcome",
        "graph_identity", "graph_revision_or_topology_hash", "applied_child_revision",
    }
    add_error(set((resolution.get("value") or {}).keys()) == required_resolution_fields, "logical-edge resolution value", errors)
    absent = resolution.get("when_logical_edge_resolution_absent", {})
    add_error(absent.get("validate_topology") == "REQUIRED", "logical-edge topology validation", errors)
    add_error(
        set(absent.get("atomic_commit", []))
        == {"logical_edge_resolution", "outcome_aware_edge_receipt", "child_state_effect", "child_revision", "one_canonical_transition_record"},
        "logical-edge atomic commit",
        errors,
    )
    same = resolution.get("when_same_edge_command_id_exists", {})
    add_error(
        same.get("result") == "ALREADY_APPLIED"
        and same.get("child_mutation") == 0
        and same.get("revision_increment") == 0
        and same.get("canonical_record_count") == 0,
        "same command no-op",
        errors,
    )
    different = resolution.get("when_logical_edge_resolved_by_different_command", {})
    add_error(
        different.get("result") == "CONTRADICTION"
        and different.get("child_mutation") == 0
        and different.get("durable_conflict_record") == "REQUIRED"
        and different.get("reconciliation_candidate") == "REQUIRED"
        and different.get("retry") == "STOP",
        "logical-edge contradiction",
        errors,
    )

    receipts = matrix.get("edge_receipt_contract", {})
    add_error(receipts.get("authority") == "IDEMPOTENT_APPLIED_EDGE_RECEIPTS_WITH_OUTCOME", "receipt authority", errors)
    add_error(set(receipts.get("allowed_outcomes", [])) == {"DEPENDENCY_SATISFIED", "DEPENDENCY_FAILED"}, "receipt outcomes", errors)
    add_error("logical_edge_id" in receipts.get("required_fields", []), "receipt logical edge", errors)

    policy = matrix.get("dependency_policy_contract", {})
    failed = policy.get("failed_edge", {})
    add_error(
        failed.get("child_disposition") == "blocked_by_failure"
        and failed.get("child_revision_increment") == 1
        and failed.get("canonical_transition_record") == "EXACTLY_ONE"
        and failed.get("process_manager_retry") == "STOP_AFTER_RECEIPT",
        "failed edge contract",
        errors,
    )

    terminal = matrix.get("terminal_state_command_disposition", {})
    add_error(set(terminal) == TERMINAL_STATES, "terminal vocabulary coverage", errors)
    add_error(terminal.get("done", {}).get("unsatisfied_edge_command") == "CONTRADICTION", "done disposition", errors)
    add_error(terminal.get("failed", {}).get("unsatisfied_edge_command") == "CONTRADICTION", "failed disposition", errors)
    blocked = terminal.get("blocked_by_failure", {})
    add_error(
        blocked.get("same_command") == "ALREADY_APPLIED"
        and blocked.get("different_edge_command") == "TERMINAL_CHILD_ALREADY_BLOCKED"
        and blocked.get("different_edge_child_mutation") == 0
        and blocked.get("different_edge_child_revision_increment") == 0
        and blocked.get("different_edge_canonical_transition_record_count") == 0
        and blocked.get("durable_disposition_record") == "REQUIRED"
        and blocked.get("disposition_owner") == "DEPENDENCY_PROCESS_MANAGER_COORDINATION_STATE"
        and blocked.get("disposition_identity") == "edge_command_id"
        and blocked.get("retry") == "STOP",
        "blocked child late-edge disposition",
        errors,
    )
    for state in ("dead_lettered", "skipped"):
        disposition = terminal.get(state, {})
        add_error(
            disposition.get("unsatisfied_edge_command") == "TERMINAL_CHILD_NOOP"
            and disposition.get("child_mutation") == 0
            and disposition.get("durable_disposition_record") == "REQUIRED"
            and set(disposition.get("required_authority_evidence", []))
            == {"terminal_transition_id", "child_state_authority_revision"}
            and disposition.get("retry") == "STOP",
            f"{state} disposition",
            errors,
        )

    findings = matrix.get("finding_dispositions", [])
    add_error({row.get("finding_id") for row in findings} == FINDINGS, "finding mapping", errors)
    add_error(all(row.get("resolved_in_product") is False and row.get("implementation_sprint") == 84 for row in findings), "finding disposition", errors)

    contracts = matrix.get("verification_contracts", {})
    add_error(contracts.get("logical_edge_resolution_authority_decided") is True, "resolution verification flag", errors)
    add_error(contracts.get("same_logical_edge_conflict_check_atomic") is True, "atomic conflict flag", errors)
    add_error(contracts.get("different_outcome_double_application_possible") is False, "double application flag", errors)
    add_error(contracts.get("blocked_child_late_edge_disposition_decided") is True, "blocked child flag", errors)
    add_error(contracts.get("terminal_state_vocabulary_mapping_complete") is True, "terminal mapping flag", errors)
    add_error(contracts.get("technical_unresolved_choice") == 0, "technical unresolved choice", errors)
    add_error(
        contracts.get("predecessor_human_acceptance_verified") is False
        and contracts.get("predecessor_acceptance_record_present") is False,
        "governance truth",
        errors,
    )

    add_error(
        {row.get("path"): row.get("blob_sha") for row in matrix.get("source_bindings", [])} == BINDINGS,
        "source binding register",
        errors,
    )
    add_error(matrix.get("immutable_evidence", {}).get("observation_count") == evidence.get("observation_count"), "evidence binding", errors)
    return errors


def validate_manifest(root: Path, manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    add_error(manifest.get("schema_version") == 3 and manifest.get("decision_id") == "ADR-080C2", "manifest identity", errors)
    entries = manifest.get("artifacts", [])
    expected = PATHS - {"docs/adr/sprint80/aggregate_boundary_manifest.json"}
    add_error({row.get("path") for row in entries} == expected and len(entries) == 6, "manifest paths", errors)
    for row in entries:
        path = root / row["path"]
        add_error(path.exists(), f'missing {row["path"]}', errors)
        if path.exists():
            add_error(path.stat().st_size == row["size_bytes"], f'size {row["path"]}', errors)
            add_error(sha(path.read_bytes()) == row["sha256"], f'hash {row["path"]}', errors)
    add_error(manifest.get("bundle_index_sha256") == sha(bundle_index(entries)), "bundle index", errors)
    add_error(manifest.get("predecessor_human_acceptance") == "PENDING", "manifest predecessor truth", errors)
    add_error(manifest.get("implementation_authorized") is False, "manifest implementation flag", errors)
    return errors


def validate_scope(paths: Iterable[str]) -> list[str]:
    observed = {str(path).replace("\\", "/") for path in paths}
    if observed == PATHS:
        return []
    return [json.dumps({"missing": sorted(PATHS - observed), "unexpected": sorted(observed - PATHS)}, sort_keys=True)]


def validate_sources(root: Path) -> list[str]:
    errors: list[str] = []
    for path, expected in BINDINGS.items():
        try:
            add_error(git(root, "rev-parse", f"{BASE}:{path}") == expected, f"source binding {path}", errors)
        except subprocess.CalledProcessError:
            errors.append(f"source binding unreadable {path}")
    return errors


def validate_zip(path: Path, evidence: bytes) -> list[str]:
    errors: list[str] = []
    add_error(sha(path.read_bytes()) == ZIP_SHA, "ZIP digest", errors)
    if errors:
        return errors
    with zipfile.ZipFile(path) as archive:
        add_error(set(archive.namelist()) == ZIP_MEMBERS, "ZIP members", errors)
        frozen = archive.read("aggregate_boundary.json")
    add_error(sha(frozen) == EVIDENCE_SHA and frozen == evidence, "frozen evidence bytes", errors)
    return errors


def validate_bundle(
    root: Path,
    *,
    evidence_zip: Path | None = None,
    changed_paths: Iterable[str] | None = None,
    verify_source_bindings: bool = False,
    verify_git_scope: bool = False,
) -> dict[str, Any]:
    evidence_bytes = (root / "docs/adr/sprint80/evidence/aggregate_boundary.run93.json").read_bytes()
    evidence = json.loads(evidence_bytes)
    matrix = readj(root / "docs/adr/sprint80/aggregate_boundary_matrix.json")
    manifest = readj(root / "docs/adr/sprint80/aggregate_boundary_manifest.json")
    errors: list[str] = []
    add_error(sha(evidence_bytes) == EVIDENCE_SHA, "repository evidence digest", errors)
    errors += validate_evidence(evidence)
    errors += validate_matrix(matrix, evidence)
    errors += validate_manifest(root, manifest)
    if changed_paths is not None:
        errors += validate_scope(changed_paths)
    if verify_git_scope:
        errors += validate_scope(git(root, "diff", "--name-only", f"{BASE}...HEAD").splitlines())
    if verify_source_bindings:
        errors += validate_sources(root)
    historical = "NOT_RUN"
    if evidence_zip:
        zip_errors = validate_zip(evidence_zip, evidence_bytes)
        errors += zip_errors
        historical = "PASS" if not zip_errors else "FAIL"
    blocker = matrix.get("predecessor_governance", {}).get("human_architecture_acceptance") != "ACCEPT"
    return {
        "schema_version": 3,
        "status": "FAIL" if errors else ("PASS_WITH_GOVERNANCE_BLOCKER" if blocker else "PASS"),
        "technical_status": "FAIL" if errors else "PASS",
        "governance_status": "BLOCKED" if blocker else "PASS",
        "governance_blockers": ["ADR-080C1 post-merge human architecture acceptance record is missing"] if blocker else [],
        "human_architecture_acceptance": "NOT_READY" if blocker else "PENDING",
        "predecessor_human_acceptance": "PENDING" if blocker else "ACCEPT",
        "base_head": BASE,
        "frozen_source_head": SOURCE,
        "frozen_artifact_sha256": ZIP_SHA,
        "aggregate_evidence_sha256": EVIDENCE_SHA,
        "observation_count": evidence.get("observation_count"),
        "finding_count": len(FINDINGS),
        "changed_path_count": len(PATHS),
        "historical_frozen_zip_verification": historical,
        "product_source_mutation": 0,
        "implementation_authorized": False,
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
    result = validate_bundle(
        args.repo_root.resolve(),
        evidence_zip=args.evidence_zip.resolve() if args.evidence_zip else None,
        changed_paths=args.changed_path or None,
        verify_source_bindings=args.verify_source_bindings,
        verify_git_scope=args.verify_git_scope,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["technical_status"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())

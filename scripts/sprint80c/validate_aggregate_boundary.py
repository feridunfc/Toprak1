from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path
from typing import Any, Iterable

EXPECTED_BASE_HEAD = "9d7c7a6ad52a8a546a708dda048f28d4e23befc6"
EXPECTED_SOURCE_HEAD = "5734ac60704f8546b8ce67e766e042ff2ce4c412"
EXPECTED_ZIP_SHA256 = "7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588"
EXPECTED_EVIDENCE_SHA256 = "ac5ab01455a3e8acb6bc0043167849e398df393819f5b4cdf14e9770dea9f478"
EXPECTED_FINDINGS = {"missing_remaining_counter_unlocks_fail_open", "ready_marker_strands_pending_child"}
EXPECTED_SCENARIOS = {
    "single_parent_done_unlocks_child", "first_parent_done_decrements_only",
    "duplicate_parent_completion_no_second_decrement", "second_parent_done_unlocks_child",
    "failed_parent_does_not_unlock_child", "nonpending_child_is_not_mutated",
    "missing_remaining_counter_unlocks_fail_open", "ready_marker_strands_pending_child",
}
EXPECTED_SOURCE_BINDINGS = {
    "docs/adr/sprint80/ADR-080C-decision-package.md": "ebc15272498fafbe115c164df4075257c090d539",
    "docs/adr/sprint80/ADR-080C1-runtime-truth-authority.md": "659f77af1c8a99cf63626318cb050ac711352b74",
    "scripts/sprint80/aggregate_boundary.py": "1d519c10f952907d4ac7a73e0854cff913a61eb6",
    "tests/diagnostics/sprint80/test_80_07_aggregate_boundary.py": "7462e70146923711937eacf801af7cee778ae0eb",
    "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py": "4a07352e33424e32332c916acdfd29e02b3eb71b",
    "hfa-core/src/hfa/lua/task_complete.lua": "8ee2b51cd94642f56d5e38680419a2e3988b26a9",
    "hfa-core/src/hfa/lua/task_admit.lua": "e5bd30953f4c7c3258e589d240113295f8bccadc",
    "hfa-core/src/hfa/dag/schema.py": "cd657e4b37975c3cf3080a221222c6f8caf56fec",
}
ALLOWED_CHANGED_PATHS = {
    ".github/workflows/sprint80c2-aggregate-boundary.yml",
    "docs/adr/sprint80/ADR-080C2-aggregate-boundary.md",
    "docs/adr/sprint80/evidence/aggregate_boundary.run93.json",
    "docs/adr/sprint80/aggregate_boundary_manifest.json",
    "docs/adr/sprint80/aggregate_boundary_matrix.json",
    "scripts/sprint80c/validate_aggregate_boundary.py",
    "tests/diagnostics/sprint80c/test_80c2_aggregate_boundary.py",
}
EXPECTED_ZIP_MEMBERS = {
    "aggregate_boundary.json", "contracts_pytest.txt", "environment.json", "environment_after.json",
    "final_summary.json", "manifest.json", "preflight.json", "preflight_after.json",
    "reality_pytest.txt", "transition_cardinality.json", "truth_contradictions.json",
    "ttl_durability.json", "writer_inventory.json",
}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def canonical_bundle_index(entries: list[dict[str, Any]]) -> bytes:
    rows = []
    for row in sorted(entries, key=lambda value: value["path"]):
        rows.append(f'{row["path"]}\0{row["size_bytes"]}\0{row["sha256"]}\n')
    return "".join(rows).encode("utf-8")


def validate_evidence(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if evidence.get("schema_version") != 1:
        errors.append("evidence schema_version must be 1")
    if evidence.get("observation_count") != 8:
        errors.append("evidence observation_count must be 8")
    if evidence.get("global_result") != "CROSS_TASK_MUTATION_WITHOUT_CHILD_AUTHORITY_RECORD":
        errors.append("evidence global result changed")
    if evidence.get("cross_task_mutation_observed") is not True:
        errors.append("cross-task mutation observation must remain true")
    if evidence.get("aggregate_revision_observed") is not False:
        errors.append("aggregate revision must remain unobserved")
    if evidence.get("child_authority_record_observed") is not False:
        errors.append("child authority record must remain unobserved")
    if set(evidence.get("blocking_findings") or []) != EXPECTED_FINDINGS:
        errors.append("blocking finding set mismatch")
    observations = evidence.get("observations") or []
    scenarios = {row.get("scenario") for row in observations}
    if scenarios != EXPECTED_SCENARIOS or len(observations) != 8:
        errors.append("evidence scenario set mismatch")
    return errors


def validate_matrix(matrix: dict[str, Any], evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if matrix.get("schema_version") != 1 or matrix.get("decision_id") != "ADR-080C2":
        errors.append("matrix identity mismatch")
    if matrix.get("decision_status") != "TECHNICAL_RECOMMENDATION_READY_FOR_HUMAN_REVIEW":
        errors.append("matrix is not review-ready")
    if matrix.get("product_implementation_authorized") is not False:
        errors.append("matrix must not authorize product implementation")
    if (matrix.get("base") or {}).get("head") != EXPECTED_BASE_HEAD:
        errors.append("matrix base head mismatch")
    immutable = matrix.get("immutable_evidence") or {}
    if immutable.get("source_head") != EXPECTED_SOURCE_HEAD:
        errors.append("frozen source head mismatch")
    if immutable.get("artifact_sha256") != EXPECTED_ZIP_SHA256:
        errors.append("frozen artifact digest mismatch")
    if immutable.get("aggregate_boundary_sha256") != EXPECTED_EVIDENCE_SHA256:
        errors.append("aggregate evidence digest mismatch")
    if immutable.get("observation_count") != len(evidence.get("observations") or []):
        errors.append("matrix observation count does not bind evidence")

    model = matrix.get("selected_model") or {}
    if model.get("name") != "INDEPENDENT_TASK_AGGREGATES_WITH_DURABLE_DEPENDENCY_PROCESS_MANAGER":
        errors.append("selected aggregate model mismatch")
    if model.get("task_aggregate_identity") != "task:{run_id}:{task_id}":
        errors.append("task aggregate identity mismatch")
    if model.get("parent_revision_owner") != "PARENT_TASK_AGGREGATE":
        errors.append("parent revision owner mismatch")
    if model.get("child_revision_owner") != "CHILD_TASK_AGGREGATE":
        errors.append("child revision owner mismatch")
    if model.get("direct_parent_to_child_authority_mutation") != "FORBIDDEN":
        errors.append("direct cross-task authority mutation must be forbidden")
    if model.get("distributed_transaction_across_tasks") is not False:
        errors.append("distributed transaction must not be selected")

    boundaries = matrix.get("transaction_boundaries") or {}
    parent = boundaries.get("parent_completion") or {}
    child = boundaries.get("child_dependency_application") or {}
    if parent.get("authority_scope") != "ONE_PARENT_TASK_AGGREGATE" or parent.get("child_authority_keys_mutated") != 0:
        errors.append("parent completion boundary is incomplete")
    if child.get("authority_scope") != "ONE_CHILD_TASK_AGGREGATE" or child.get("parent_authority_keys_mutated") != 0:
        errors.append("child dependency boundary is incomplete")
    if "one_canonical_transition_record" not in (parent.get("atomic_commit_members") or []):
        errors.append("parent boundary lacks canonical record")
    if "one_canonical_transition_record" not in (child.get("atomic_commit_members") or []):
        errors.append("child boundary lacks canonical record")

    dep = matrix.get("dependency_contract") or {}
    if dep.get("expected_dependency_authority") != "IMMUTABLE_EXPECTED_PARENT_EDGE_SET":
        errors.append("expected edge set must be authority")
    if dep.get("applied_dependency_authority") != "IDEMPOTENT_SATISFIED_EDGE_RECEIPTS":
        errors.append("edge receipts must be applied authority")
    if dep.get("remaining_deps") != "DERIVED_PROJECTION_NOT_AUTHORITY":
        errors.append("remaining counter cannot be authority")
    if dep.get("missing_expected_edge_metadata") != "FAIL_CLOSED_AND_EMIT_RECONCILIATION_CANDIDATE":
        errors.append("missing edge metadata must fail closed")
    if dep.get("duplicate_edge_command") != "ALREADY_APPLIED_NO_REVISION_NO_RECORD":
        errors.append("duplicate edge semantics mismatch")

    ready = matrix.get("ready_projection_contract") or {}
    if ready.get("authority") != "CHILD_STATE_AND_CHILD_REVISION":
        errors.append("ready authority mismatch")
    if ready.get("marker_can_authorize_or_suppress_state_transition") is not False:
        errors.append("ready marker cannot be authority")
    if ready.get("legacy_marker_conflict") != "FAIL_CLOSED_AND_EXPLICIT_RECONCILIATION_BEFORE_MIGRATION_CUTOVER":
        errors.append("legacy marker conflict behavior mismatch")

    record = matrix.get("canonical_child_effect_contract") or {}
    if record.get("every_new_edge_receipt_increments_child_revision") is not True:
        errors.append("new edge receipt must increment child revision")
    if record.get("every_new_edge_receipt_produces_exactly_one_canonical_transition_record") is not True:
        errors.append("new edge receipt must produce one canonical record")
    if record.get("duplicate_edge_receipt_produces_revision") is not False or record.get("duplicate_edge_receipt_produces_record") is not False:
        errors.append("duplicate edge must be a no-op")

    process = matrix.get("process_manager_contract") or {}
    if process.get("fanout_unit") != "ONE_IDEMPOTENT_COMMAND_PER_PARENT_CHILD_EDGE":
        errors.append("process-manager fanout unit mismatch")
    if process.get("direct_redis_child_key_mutation") is not False or process.get("task_truth_authority") is not False:
        errors.append("process manager cannot be task authority writer")

    findings = matrix.get("finding_dispositions") or []
    if {row.get("finding_id") for row in findings} != EXPECTED_FINDINGS or len(findings) != 2:
        errors.append("finding disposition set mismatch")
    for row in findings:
        if row.get("decision_status") != "ARCHITECTURE_DECISION_ASSIGNED" or row.get("resolved_in_product") is not False or row.get("implementation_sprint") != 84:
            errors.append(f'invalid finding disposition: {row.get("finding_id")}')

    deps = set(matrix.get("dependencies") or [])
    for required in (
        "ADR-080C1 accepted runtime truth ownership",
        "ADR-080C3 canonical transition and monotonic revision contract",
        "ADR-080C4 durable authority and reconstruction contract",
        "ADR-080C5 event-state atomicity and outbox contract",
        "ADR-080C6 stable writer identity and operation allowlist",
    ):
        if required not in deps:
            errors.append(f"missing dependency: {required}")

    bindings = matrix.get("source_bindings") or []
    actual_bindings = {row.get("path"): row.get("blob_sha") for row in bindings}
    if actual_bindings != EXPECTED_SOURCE_BINDINGS or len(bindings) != len(EXPECTED_SOURCE_BINDINGS):
        errors.append("source binding set mismatch")

    contracts = matrix.get("verification_contracts") or {}
    for key in ("selected_model_decided", "aggregate_identity_decided", "revision_owners_decided", "atomic_boundaries_decided"):
        if contracts.get(key) is not True:
            errors.append(f"verification contract {key} must be true")
    for key in ("direct_cross_task_authority_mutation", "remaining_counter_authority", "ready_marker_authority", "findings_resolved_in_product", "technical_unresolved_choice", "product_source_mutation", "implementation_claims"):
        if contracts.get(key) != 0:
            errors.append(f"verification contract {key} must be zero")
    if contracts.get("finding_count") != 2:
        errors.append("finding_count must be 2")
    return errors


def validate_manifest(repo_root: Path, manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if manifest.get("schema_version") != 1 or manifest.get("decision_id") != "ADR-080C2":
        errors.append("manifest identity mismatch")
    if manifest.get("base_head") != EXPECTED_BASE_HEAD:
        errors.append("manifest base head mismatch")
    entries = manifest.get("artifacts") or []
    expected = ALLOWED_CHANGED_PATHS - {"docs/adr/sprint80/aggregate_boundary_manifest.json"}
    if {row.get("path") for row in entries} != expected or len(entries) != 6:
        errors.append("manifest artifact path set mismatch")
    for row in entries:
        path = repo_root / str(row.get("path"))
        if not path.exists():
            errors.append(f"manifest file missing: {row.get('path')}")
            continue
        if path.stat().st_size != row.get("size_bytes"):
            errors.append(f"manifest size mismatch: {row.get('path')}")
        if sha256_file(path) != row.get("sha256"):
            errors.append(f"manifest sha mismatch: {row.get('path')}")
    calculated = sha256_bytes(canonical_bundle_index(entries))
    if manifest.get("bundle_index_algorithm") != r"sorted_utf8_lines:path\0size_bytes\0sha256\n":
        errors.append("bundle index algorithm mismatch")
    if manifest.get("bundle_index_sha256") != calculated:
        errors.append("bundle index digest mismatch")
    return errors


def validate_scope(paths: Iterable[str]) -> list[str]:
    observed = {str(value).replace("\\", "/") for value in paths}
    if observed == ALLOWED_CHANGED_PATHS:
        return []
    return [json.dumps({"missing": sorted(ALLOWED_CHANGED_PATHS-observed), "unexpected": sorted(observed-ALLOWED_CHANGED_PATHS)}, sort_keys=True)]


def git_output(repo_root: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=repo_root, text=True).strip()


def validate_git_scope(repo_root: Path) -> list[str]:
    paths = [line for line in git_output(repo_root, "diff", "--name-only", f"{EXPECTED_BASE_HEAD}...HEAD").splitlines() if line]
    return validate_scope(paths)


def validate_source_bindings(repo_root: Path) -> list[str]:
    errors: list[str] = []
    for path, expected in EXPECTED_SOURCE_BINDINGS.items():
        try:
            observed = git_output(repo_root, "rev-parse", f"{EXPECTED_BASE_HEAD}:{path}")
        except subprocess.CalledProcessError:
            errors.append(f"source binding unreadable: {path}")
            continue
        if observed != expected:
            errors.append(f"source binding mismatch: {path}")
    return errors


def validate_zip(zip_path: Path, evidence_bytes: bytes) -> list[str]:
    errors: list[str] = []
    if sha256_file(zip_path) != EXPECTED_ZIP_SHA256:
        return ["frozen ZIP digest mismatch"]
    with zipfile.ZipFile(zip_path) as archive:
        if set(archive.namelist()) != EXPECTED_ZIP_MEMBERS:
            errors.append("frozen ZIP member set mismatch")
        aggregate_bytes = archive.read("aggregate_boundary.json")
    if sha256_bytes(aggregate_bytes) != EXPECTED_EVIDENCE_SHA256:
        errors.append("aggregate evidence digest mismatch inside ZIP")
    if aggregate_bytes != evidence_bytes:
        errors.append("repository evidence bytes differ from frozen ZIP")
    return errors


def validate_bundle(repo_root: Path, *, evidence_zip: Path | None=None, changed_paths: Iterable[str] | None=None, verify_source_bindings: bool=False, verify_git_scope: bool=False) -> dict[str, Any]:
    evidence_path = repo_root / "docs/adr/sprint80/evidence/aggregate_boundary.run93.json"
    matrix_path = repo_root / "docs/adr/sprint80/aggregate_boundary_matrix.json"
    manifest_path = repo_root / "docs/adr/sprint80/aggregate_boundary_manifest.json"
    evidence_bytes = evidence_path.read_bytes()
    errors: list[str] = []
    if sha256_bytes(evidence_bytes) != EXPECTED_EVIDENCE_SHA256:
        errors.append("repository evidence SHA-256 mismatch")
    evidence = json.loads(evidence_bytes)
    matrix = load_json(matrix_path)
    manifest = load_json(manifest_path)
    errors += validate_evidence(evidence)
    errors += validate_matrix(matrix, evidence)
    errors += validate_manifest(repo_root, manifest)
    if changed_paths is not None:
        errors += validate_scope(changed_paths)
    if verify_git_scope:
        errors += validate_git_scope(repo_root)
    if verify_source_bindings:
        errors += validate_source_bindings(repo_root)
    historical = "NOT_RUN"
    if evidence_zip is not None:
        zip_errors = validate_zip(evidence_zip, evidence_bytes)
        errors += zip_errors
        historical = "PASS" if not zip_errors else "FAIL"
    return {
        "schema_version":1,
        "status":"PASS" if not errors else "FAIL",
        "base_head":EXPECTED_BASE_HEAD,
        "frozen_source_head":EXPECTED_SOURCE_HEAD,
        "frozen_artifact_sha256":EXPECTED_ZIP_SHA256,
        "aggregate_evidence_sha256":EXPECTED_EVIDENCE_SHA256,
        "observation_count":evidence.get("observation_count"),
        "finding_count":len(EXPECTED_FINDINGS),
        "changed_path_count":len(ALLOWED_CHANGED_PATHS),
        "historical_frozen_zip_verification":historical,
        "source_binding_verification":"PASS" if verify_source_bindings and not any("source binding" in e for e in errors) else ("NOT_RUN" if not verify_source_bindings else "FAIL"),
        "git_scope_verification":"PASS" if verify_git_scope and not any("missing" in e or "unexpected" in e for e in errors) else ("NOT_RUN" if not verify_git_scope else "FAIL"),
        "product_source_mutation":0,
        "implementation_authorized":False,
        "errors":errors,
    }


def main() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument("--repo-root",type=Path,default=Path.cwd())
    parser.add_argument("--evidence-zip",type=Path)
    parser.add_argument("--changed-path",action="append",default=[])
    parser.add_argument("--verify-source-bindings",action="store_true")
    parser.add_argument("--verify-git-scope",action="store_true")
    parser.add_argument("--out",type=Path)
    args=parser.parse_args()
    report=validate_bundle(args.repo_root.resolve(), evidence_zip=args.evidence_zip.resolve() if args.evidence_zip else None, changed_paths=args.changed_path or None, verify_source_bindings=args.verify_source_bindings, verify_git_scope=args.verify_git_scope)
    rendered=json.dumps(report,indent=2,sort_keys=True)+"\n"
    if args.out:
        args.out.parent.mkdir(parents=True,exist_ok=True)
        args.out.write_text(rendered,encoding="utf-8")
    print(rendered,end="")
    return 0 if report["status"]=="PASS" else 1

if __name__ == "__main__":
    raise SystemExit(main())

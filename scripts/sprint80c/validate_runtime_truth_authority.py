from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any, Iterable

EXPECTED_BASE_HEAD = "3439618ea7ad8cf8bdd0e49d660217fc455c787c"
EXPECTED_SOURCE_HEAD = "5734ac60704f8546b8ce67e766e042ff2ce4c412"
EXPECTED_ZIP_SHA256 = "7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588"
EXPECTED_TRUTH_SHA256 = "037fcd91c5aeec88039dd17705a05678790ab7f7969647a1cb7e624fcfda9d07"
EXPECTED_CALLERS = {
    "control_api_run_state",
    "run_recovery_missing_state",
    "run_recovery_terminal_guard",
    "task_recovery_missing_state",
    "worker_task_duplicate_guard_nonterminal",
    "worker_task_duplicate_guard_terminal",
    "legacy_worker_run_guard",
    "scheduler_dag_dispatch",
}
EXPECTED_OPERATION_CLASSES = {
    "read_query",
    "admission",
    "dispatch",
    "claim",
    "heartbeat",
    "completion_failure",
    "requeue",
    "recovery",
    "reconciliation",
}
EXPECTED_ZIP_MEMBERS = {
    "aggregate_boundary.json",
    "contracts_pytest.txt",
    "environment.json",
    "environment_after.json",
    "final_summary.json",
    "manifest.json",
    "preflight.json",
    "preflight_after.json",
    "reality_pytest.txt",
    "transition_cardinality.json",
    "truth_contradictions.json",
    "ttl_durability.json",
    "writer_inventory.json",
}
ALLOWED_CHANGED_PATHS = {
    "docs/adr/sprint80/ADR-080C1-runtime-truth-authority.md",
    "docs/adr/sprint80/runtime_truth_authority_matrix.json",
    "docs/adr/sprint80/runtime_truth_authority_manifest.json",
    "docs/adr/sprint80/evidence/truth_contradictions.run93.json",
    "scripts/sprint80c/validate_runtime_truth_authority.py",
    "tests/diagnostics/sprint80c/test_80c1_runtime_truth_authority.py",
}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_evidence(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if evidence.get("schema_version") != 1:
        errors.append("truth evidence schema_version must be 1")
    if evidence.get("observation_count") != 9:
        errors.append("truth evidence observation_count must be 9")
    if evidence.get("global_truth_policy") != "INCONSISTENT_BY_CALLER":
        errors.append("frozen global truth policy changed")
    observations = evidence.get("observations") or []
    if len(observations) != 9:
        errors.append("truth evidence must contain exactly 9 observations")
    callers = {row.get("caller") for row in observations}
    if callers != EXPECTED_CALLERS:
        errors.append(f"caller set mismatch: {sorted(callers)}")
    scenarios = [row.get("scenario") for row in observations]
    if len(scenarios) != len(set(scenarios)):
        errors.append("truth evidence scenarios must be unique")
    return errors


def validate_matrix(matrix: dict[str, Any], evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if matrix.get("schema_version") != 1:
        errors.append("matrix schema_version must be 1")
    if matrix.get("decision_id") != "ADR-080C1":
        errors.append("matrix decision_id must be ADR-080C1")
    if matrix.get("decision_status") != "TECHNICAL_RECOMMENDATION_READY_FOR_HUMAN_REVIEW":
        errors.append("matrix decision status is not review-ready")
    if matrix.get("product_implementation_authorized") is not False:
        errors.append("matrix must not authorize product implementation")
    base = matrix.get("base") or {}
    if base.get("head") != EXPECTED_BASE_HEAD:
        errors.append("matrix base head mismatch")
    immutable = matrix.get("immutable_evidence") or {}
    if immutable.get("source_head") != EXPECTED_SOURCE_HEAD:
        errors.append("matrix frozen source head mismatch")
    if immutable.get("artifact_sha256") != EXPECTED_ZIP_SHA256:
        errors.append("matrix artifact digest mismatch")
    if immutable.get("truth_contradictions_sha256") != EXPECTED_TRUTH_SHA256:
        errors.append("matrix truth evidence digest mismatch")

    operation_classes = matrix.get("operation_classes") or []
    observed_ops = {row.get("operation_class") for row in operation_classes}
    if observed_ops != EXPECTED_OPERATION_CLASSES:
        errors.append(f"operation class mismatch: {sorted(observed_ops)}")
    for row in operation_classes:
        if not row.get("authority"):
            errors.append(f"operation lacks authority: {row.get('operation_class')}")
        if not row.get("conflict_behavior"):
            errors.append(f"operation lacks conflict behavior: {row.get('operation_class')}")
        if not row.get("writer_role"):
            errors.append(f"operation lacks writer role: {row.get('operation_class')}")

    caller_rows = matrix.get("caller_decisions") or []
    matrix_callers = {row.get("caller") for row in caller_rows}
    if matrix_callers != EXPECTED_CALLERS:
        errors.append(f"matrix caller set mismatch: {sorted(matrix_callers)}")
    evidence_scenarios = {row.get("scenario") for row in evidence.get("observations") or []}
    matrix_scenarios = {
        scenario
        for row in caller_rows
        for scenario in (row.get("scenarios") or [])
    }
    if matrix_scenarios != evidence_scenarios:
        errors.append("matrix does not map every frozen scenario exactly")
    if sum(len(row.get("scenarios") or []) for row in caller_rows) != 9:
        errors.append("matrix scenario mapping cardinality must be 9")
    for row in caller_rows:
        if not row.get("target_authority") or not row.get("target_behavior"):
            errors.append(f"caller decision incomplete: {row.get('caller')}")

    policy = matrix.get("global_policy") or {}
    expected_policy = {
        "task_lifecycle_authority": "TASK_AGGREGATE",
        "run_lifecycle_authority": "RUN_AGGREGATE",
        "cross_plane_conflict_mutation": "FAIL_CLOSED",
        "missing_authority_record": "FAIL_CLOSED",
        "repair_path": "EXPLICIT_RECONCILIATION",
        "silent_auto_repair": False,
    }
    for key, value in expected_policy.items():
        if policy.get(key) != value:
            errors.append(f"global policy mismatch for {key}")

    contracts = matrix.get("verification_contracts") or {}
    for key in (
        "all_frozen_observations_mapped",
        "all_expected_callers_mapped",
        "all_operation_classes_decided",
    ):
        if contracts.get(key) is not True:
            errors.append(f"verification contract {key} must be true")
    for key in (
        "unclassified_truth_reader",
        "unclassified_truth_writer",
        "technical_unresolved_choice",
        "implementation_claims",
        "product_source_mutation",
    ):
        if contracts.get(key) != 0:
            errors.append(f"verification contract {key} must be zero")

    bindings = matrix.get("source_bindings") or []
    if len(bindings) < 15:
        errors.append("source binding set is incomplete")
    if len({row.get("path") for row in bindings}) != len(bindings):
        errors.append("source binding paths must be unique")
    for row in bindings:
        blob = str(row.get("blob_sha") or "")
        if len(blob) != 40 or any(c not in "0123456789abcdef" for c in blob):
            errors.append(f"invalid source blob sha: {row.get('path')}")
    return errors


def validate_manifest(repo_root: Path, manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if manifest.get("schema_version") != 1:
        errors.append("manifest schema_version must be 1")
    if manifest.get("base_head") != EXPECTED_BASE_HEAD:
        errors.append("manifest base head mismatch")
    artifacts = manifest.get("artifacts") or []
    paths = {row.get("path") for row in artifacts}
    expected = ALLOWED_CHANGED_PATHS - {"docs/adr/sprint80/runtime_truth_authority_manifest.json"}
    if paths != expected:
        errors.append(f"manifest path set mismatch: {sorted(paths)}")
    for row in artifacts:
        rel = row.get("path")
        path = repo_root / rel
        if not path.exists():
            errors.append(f"manifest file missing: {rel}")
            continue
        if path.stat().st_size != row.get("size_bytes"):
            errors.append(f"manifest size mismatch: {rel}")
        if sha256_file(path) != row.get("sha256"):
            errors.append(f"manifest sha mismatch: {rel}")
    return errors


def validate_zip(zip_path: Path, committed_evidence_bytes: bytes) -> list[str]:
    errors: list[str] = []
    if sha256_file(zip_path) != EXPECTED_ZIP_SHA256:
        errors.append("run93 zip digest mismatch")
        return errors
    with zipfile.ZipFile(zip_path) as archive:
        names = set(archive.namelist())
        if names != EXPECTED_ZIP_MEMBERS:
            errors.append(f"run93 zip member set mismatch: {sorted(names)}")
        truth_bytes = archive.read("truth_contradictions.json")
    if sha256_bytes(truth_bytes) != EXPECTED_TRUTH_SHA256:
        errors.append("truth_contradictions.json digest mismatch inside zip")
    if json.loads(truth_bytes) != json.loads(committed_evidence_bytes):
        errors.append("committed truth evidence differs from frozen zip")
    return errors


def validate_scope(changed_paths: Iterable[str]) -> list[str]:
    paths = {str(path).replace("\\", "/") for path in changed_paths}
    if paths != ALLOWED_CHANGED_PATHS:
        return [
            "changed path set mismatch: "
            + json.dumps(
                {
                    "missing": sorted(ALLOWED_CHANGED_PATHS - paths),
                    "unexpected": sorted(paths - ALLOWED_CHANGED_PATHS),
                },
                sort_keys=True,
            )
        ]
    return []


def validate_bundle(
    repo_root: Path,
    *,
    evidence_zip: Path | None = None,
    changed_paths: Iterable[str] | None = None,
) -> dict[str, Any]:
    evidence_path = repo_root / "docs/adr/sprint80/evidence/truth_contradictions.run93.json"
    matrix_path = repo_root / "docs/adr/sprint80/runtime_truth_authority_matrix.json"
    manifest_path = repo_root / "docs/adr/sprint80/runtime_truth_authority_manifest.json"
    evidence_bytes = evidence_path.read_bytes()
    errors: list[str] = []
    if sha256_bytes(evidence_bytes) != EXPECTED_TRUTH_SHA256:
        errors.append("committed truth evidence sha mismatch")
    evidence = json.loads(evidence_bytes)
    matrix = load_json(matrix_path)
    manifest = load_json(manifest_path)
    errors.extend(validate_evidence(evidence))
    errors.extend(validate_matrix(matrix, evidence))
    errors.extend(validate_manifest(repo_root, manifest))
    if evidence_zip is not None:
        errors.extend(validate_zip(evidence_zip, evidence_bytes))
    if changed_paths is not None:
        errors.extend(validate_scope(changed_paths))
    return {
        "schema_version": 1,
        "status": "PASS" if not errors else "FAIL",
        "base_head": EXPECTED_BASE_HEAD,
        "frozen_source_head": EXPECTED_SOURCE_HEAD,
        "artifact_sha256": EXPECTED_ZIP_SHA256,
        "truth_evidence_sha256": EXPECTED_TRUTH_SHA256,
        "observation_count": evidence.get("observation_count"),
        "caller_count": len(EXPECTED_CALLERS),
        "operation_class_count": len(EXPECTED_OPERATION_CLASSES),
        "changed_path_count": len(list(changed_paths)) if changed_paths is not None else None,
        "product_source_mutation": 0,
        "implementation_authorized": False,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--evidence-zip", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--changed-path", action="append", default=[])
    args = parser.parse_args()
    report = validate_bundle(
        args.repo_root.resolve(),
        evidence_zip=args.evidence_zip.resolve() if args.evidence_zip else None,
        changed_paths=args.changed_path or None,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

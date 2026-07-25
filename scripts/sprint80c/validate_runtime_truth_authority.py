from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path
from typing import Any, Iterable, Mapping

BASE = "3439618ea7ad8cf8bdd0e49d660217fc455c787c"
SOURCE = "5734ac60704f8546b8ce67e766e042ff2ce4c412"
ZIP_SHA = "7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588"
TRUTH_SHA = "037fcd91c5aeec88039dd17705a05678790ab7f7969647a1cb7e624fcfda9d07"
CHECK_NAME = "Sprint 80C.1 Runtime Truth Authority / runtime-truth-authority"
BUNDLE_INDEX_ALGORITHM = "sorted_utf8_lines:path\\0size_bytes\\0sha256\\n"
EVIDENCE_RELATIVE_PATH = "docs/adr/sprint80/evidence/truth_contradictions.run93.json"

EXPECTED_CALLERS = {
    "control_api_run_state", "run_recovery_missing_state",
    "run_recovery_terminal_guard", "task_recovery_missing_state",
    "worker_task_duplicate_guard_nonterminal",
    "worker_task_duplicate_guard_terminal", "legacy_worker_run_guard",
    "scheduler_dag_dispatch",
}
EXPECTED_SCENARIOS = {
    "api_run_done_task_running", "api_run_running_task_done",
    "run_missing_task_terminal", "run_terminal_task_running_recovery",
    "task_missing_run_terminal", "run_done_task_running_duplicate_guard",
    "run_running_task_done_worker_guard",
    "run_done_task_running_legacy_worker_guard",
    "run_terminal_task_ready_scheduler",
}
EXPECTED_OPERATIONS = {
    "read_query", "admission", "dispatch", "claim", "heartbeat",
    "completion_failure", "requeue", "recovery", "reconciliation",
}
EXPECTED_FINDINGS = {
    "inconsistent_by_caller",
    "contradictory_mutation_not_guaranteed_blocked",
}
ALLOWED_CHANGED_PATHS = {
    ".github/workflows/sprint80c1-runtime-truth-authority.yml",
    "docs/adr/sprint80/ADR-080C1-runtime-truth-authority.md",
    EVIDENCE_RELATIVE_PATH,
    "docs/adr/sprint80/runtime_truth_authority_manifest.json",
    "docs/adr/sprint80/runtime_truth_authority_matrix.json",
    "scripts/sprint80c/validate_runtime_truth_authority.py",
    "tests/diagnostics/sprint80c/test_80c1_runtime_truth_authority.py",
}
ZIP_MEMBERS = {
    "aggregate_boundary.json", "contracts_pytest.txt", "environment.json",
    "environment_after.json", "final_summary.json", "manifest.json",
    "preflight.json", "preflight_after.json", "reality_pytest.txt",
    "transition_cardinality.json", "truth_contradictions.json",
    "ttl_durability.json", "writer_inventory.json",
}
EXPECTED_SOURCE_BINDINGS = {
    "docs/adr/sprint80/ADR-080C-decision-package.md": "ebc15272498fafbe115c164df4075257c090d539",
    "tests/diagnostics/sprint80/test_80_06_truth_contradictions.py": "ba3e1b8e89c64d3aa78de7aa5144a31e031850de",
    "tests/diagnostics/sprint80/test_80_04_transition_cardinality.py": "4a07352e33424e32332c916acdfd29e02b3eb71b",
    "hfa-control/src/hfa_control/service.py": "75ec7a1aab7445e9f293cec45f509dd701fc22cb",
    "hfa-control/src/hfa_control/recovery.py": "b4dcaf72e0583d3fa697e5b6fa766d24f467ae2b",
    "hfa-control/src/hfa_control/task_recovery.py": "d1355f3cccd8e1ee17320dbdebe4558b78dd50f0",
    "hfa-worker/src/hfa_worker/idempotency.py": "49a9f9a3be9184a3dc934f088cb2f07b0b1fb28e",
    "hfa-worker/src/hfa_worker/runtime/terminal_duplicate_delivery.py": "eec7ed91d7d386ef9832c4268b541d1384cd7b43",
    "hfa-core/src/hfa/runtime/state_store.py": "e25cda0df5bcb54c974f43987e713775e6b58143",
    "hfa-core/src/hfa/lua/task_admit.lua": "e5bd30953f4c7c3258e589d240113295f8bccadc",
    "hfa-core/src/hfa/lua/task_dispatch_commit.lua": "f43f431eead940b07d995320dfd1a8548ac1e1a4",
    "hfa-core/src/hfa/lua/task_claim_start.lua": "50edc573cafb2b75a60e2cf7acbcb41dabf8c2ad",
    "hfa-core/src/hfa/lua/task_heartbeat.lua": "0efde7a88fbc988b524b810f691d77ee3d1fa0ff",
    "hfa-core/src/hfa/lua/task_complete.lua": "8ee2b51cd94642f56d5e38680419a2e3988b26a9",
    "hfa-core/src/hfa/lua/task_requeue.lua": "f8ee723fdedcb0175fd9f596831af84299065f27",
}
EXPECTED_BASE_HEAD = BASE
EXPECTED_SOURCE_HEAD = SOURCE
EXPECTED_ZIP_SHA256 = ZIP_SHA
EXPECTED_TRUTH_SHA256 = TRUTH_SHA
EXPECTED_OPERATION_CLASSES = EXPECTED_OPERATIONS


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args], capture_output=True, text=True
    )
    if result.returncode:
        raise RuntimeError(
            f"git {' '.join(args)} failed: {(result.stderr or result.stdout).strip()}"
        )
    return result.stdout.strip()


def git_blob_sha(root: Path, base: str, path: str) -> str:
    return _git(root, "rev-parse", f"{base}:{path}")


def git_changed_paths(root: Path, base: str) -> list[str]:
    return [
        line.replace("\\", "/")
        for line in _git(root, "diff", "--name-only", f"{base}...HEAD").splitlines()
        if line.strip()
    ]


def validate_evidence_bytes(data: bytes) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    if sha256_bytes(data) != TRUTH_SHA:
        errors.append("committed truth evidence sha mismatch")
    try:
        evidence = json.loads(data)
    except json.JSONDecodeError as exc:
        return {}, [f"committed truth evidence JSON invalid: {exc}"]
    expected = {
        "schema_version": 1,
        "observation_count": 9,
        "global_truth_policy": "INCONSISTENT_BY_CALLER",
    }
    for key, value in expected.items():
        if evidence.get(key) != value:
            errors.append(f"committed truth evidence mismatch for {key}")
    observations = evidence.get("observations") or []
    callers = {row.get("caller") for row in observations}
    scenarios = [row.get("scenario") for row in observations]
    if len(observations) != 9 or callers != EXPECTED_CALLERS:
        errors.append("committed truth evidence caller/cardinality mismatch")
    if len(scenarios) != len(set(scenarios)) or set(scenarios) != EXPECTED_SCENARIOS:
        errors.append("committed truth evidence scenario set mismatch")
    return evidence, errors


def _operation(matrix: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    return next(
        row for row in matrix.get("operation_classes", [])
        if row.get("operation_class") == name
    )


def validate_matrix(
    matrix: dict[str, Any], evidence: dict[str, Any] | None = None, *,
    repo_root: Path | None = None, verify_source_bindings: bool = False,
) -> list[str]:
    errors: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            errors.append(message)

    require(matrix.get("schema_version") == 2, "matrix schema_version must be 2")
    require(matrix.get("decision_id") == "ADR-080C1", "matrix decision_id mismatch")
    require(
        matrix.get("decision_status")
        == "CORRECTED_TECHNICAL_RECOMMENDATION_READY_FOR_RE_REVIEW",
        "matrix decision status is not corrected/review-ready",
    )
    require(matrix.get("human_architecture_acceptance") == "PENDING",
            "human architecture acceptance must remain pending")
    require(matrix.get("product_implementation_authorized") is False,
            "matrix must not authorize product implementation")
    require((matrix.get("base") or {}).get("head") == BASE,
            "matrix base head mismatch")

    immutable = matrix.get("immutable_evidence") or {}
    immutable_expected = {
        "source_head": SOURCE,
        "artifact_sha256": ZIP_SHA,
        "truth_contradictions_sha256": TRUTH_SHA,
        "repository_evidence_path": EVIDENCE_RELATIVE_PATH,
        "repository_evidence_sha256": TRUTH_SHA,
        "historical_frozen_zip_verification": "ALREADY_FROZEN_AND_DIGEST_PINNED",
        "observation_count": 9,
        "caller_count": 8,
        "global_truth_policy": "INCONSISTENT_BY_CALLER",
    }
    for key, value in immutable_expected.items():
        require(immutable.get(key) == value, f"immutable evidence mismatch for {key}")

    policy = matrix.get("global_policy") or {}
    policy_expected = {
        "task_lifecycle_truth_owner": "TASK_STATE_AUTHORITY",
        "run_lifecycle_truth_owner": "RUN_STATE_AUTHORITY",
        "cross_plane_conflict_mutation": "FAIL_CLOSED",
        "missing_authority_record": "FAIL_CLOSED",
        "repair_path": "EXPLICIT_RECONCILIATION",
        "silent_auto_repair": False,
    }
    for key, value in policy_expected.items():
        require(policy.get(key) == value, f"global policy mismatch for {key}")
    require(
        not ({"task_lifecycle_authority", "run_lifecycle_authority"} & set(policy)),
        "80C.1 must not freeze TASK_AGGREGATE/RUN_AGGREGATE authority terminology",
    )

    deferred = matrix.get("aggregate_boundary_deferral") or {}
    require(deferred.get("prejudged_by_80C1") is False,
            "aggregate boundary must not be prejudged by 80C.1")
    for key in (
        "transaction_aggregate_boundary", "aggregate_identity",
        "revision_owner", "atomic_mutation_boundary",
    ):
        require((deferred.get(key) or {}).get("status") == "DEFERRED_TO_ADR_080C2",
                f"{key} must be deferred to ADR-080C2")
    require(
        "ADR-080C2 aggregate identity, revision owner and atomic boundary"
        in set(matrix.get("dependencies") or []),
        "ADR-080C2 dependency is missing",
    )

    operations = matrix.get("operation_classes") or []
    require(len(operations) == 9, "operation class cardinality must be exactly 9")
    require({row.get("operation_class") for row in operations} == EXPECTED_OPERATIONS,
            "operation class set mismatch")
    for row in operations:
        for field in ("truth_owner", "conflict_behavior", "writer_role"):
            require(bool(row.get(field)), f"operation lacks {field}: {row.get('operation_class')}")

    heartbeat = _operation(matrix, "heartbeat")
    require(heartbeat.get("terminal_run_behavior") == "REJECT",
            "terminal RUN heartbeat behavior must be REJECT")
    require(heartbeat.get("liveness_ttl_refresh") == 0,
            "terminal RUN heartbeat must refresh zero liveness TTL")
    require(heartbeat.get("running_zset_refresh") == 0,
            "terminal RUN heartbeat must refresh zero running ZSET score")
    require(heartbeat.get("reconciliation_candidate") == "REQUIRED",
            "terminal RUN heartbeat must require reconciliation")
    require(
        "run truth exists and is not terminal"
        in set(heartbeat.get("cross_plane_preconditions") or []),
        "heartbeat must require nonterminal RUN truth",
    )

    completion = _operation(matrix, "completion_failure")
    contract = completion.get("terminal_run_contract") or {}
    for key, value in {
        "normal_completion": "REJECT",
        "normal_failure": "REJECT",
        "child_effects": 0,
        "output_commit": 0,
        "normal_ack_authorization": 0,
        "next_path": "EXPLICIT_RECONCILIATION_OR_CANCELLATION_COMMAND",
    }.items():
        require(contract.get(key) == value,
                f"terminal RUN completion contract mismatch for {key}")
    cleanup = completion.get("transport_duplicate_cleanup") or {}
    require(cleanup.get("lifecycle_transition") is False,
            "transport duplicate cleanup must not be a lifecycle transition")
    require(
        cleanup.get("ack_exception")
        == "ALLOW_ONLY_WITH_EXPLICIT_TASK_ID_RUN_ID_AND_TERMINAL_TASK_EVIDENCE",
        "transport duplicate cleanup ACK exception mismatch",
    )

    callers = matrix.get("caller_decisions") or []
    require(len(callers) == 8, "caller decision cardinality must be exactly 8")
    require({row.get("caller") for row in callers} == EXPECTED_CALLERS,
            "matrix caller set mismatch")
    scenarios = [s for row in callers for s in (row.get("scenarios") or [])]
    evidence_scenarios = {
        row.get("scenario") for row in ((evidence or {}).get("observations") or [])
    } or EXPECTED_SCENARIOS
    require(len(scenarios) == 9 and set(scenarios) == evidence_scenarios,
            "matrix must map the committed evidence scenario set exactly")
    for row in callers:
        require(bool(row.get("target_truth_owner") and row.get("target_behavior")),
                f"caller decision incomplete: {row.get('caller')}")

    findings = matrix.get("finding_dispositions") or []
    require(len(findings) == 2, "finding disposition cardinality must be 2")
    require({row.get("finding_id") for row in findings} == EXPECTED_FINDINGS,
            "finding disposition set mismatch")
    for row in findings:
        fid = row.get("finding_id")
        require(row.get("decision_status") == "ARCHITECTURE_DECISION_ASSIGNED",
                f"finding decision status invalid: {fid}")
        require(row.get("resolved_in_product") is False,
                f"finding must remain unresolved: {fid}")
        require(row.get("implementation_sprint") == 82,
                f"finding implementation sprint must be 82: {fid}")
        require(bool(row.get("verification_contract")),
                f"finding lacks verification contract: {fid}")

    contracts = matrix.get("verification_contracts") or {}
    for key in (
        "all_frozen_observations_mapped", "all_expected_callers_mapped",
        "all_operation_classes_decided", "heartbeat_terminal_run_behavior_decided",
        "completion_terminal_run_behavior_decided", "dependency_on_80C2_explicit",
    ):
        require(contracts.get(key) is True,
                f"verification contract {key} must be true")
    expected_contract_values = {
        "aggregate_boundary_prejudged": False,
        "expected_truth_findings": 2,
        "mapped_truth_findings": 2,
        "resolved_in_product": 0,
        "unclassified_truth_reader": 0,
        "unclassified_truth_writer": 0,
        "technical_unresolved_choice": 0,
        "implementation_claims": 0,
        "product_source_mutation": 0,
    }
    for key, value in expected_contract_values.items():
        require(contracts.get(key) == value,
                f"verification contract {key} must be {value}")

    bindings = matrix.get("source_bindings") or []
    declared = {str(row.get("path")): str(row.get("blob_sha")) for row in bindings}
    require(len(bindings) == len(EXPECTED_SOURCE_BINDINGS),
            "source binding cardinality mismatch")
    require(len(declared) == len(bindings), "duplicate source binding path")
    require(set(declared) == set(EXPECTED_SOURCE_BINDINGS),
            "source binding path set mismatch")
    for path, expected in EXPECTED_SOURCE_BINDINGS.items():
        require(declared.get(path) == expected, f"declared source blob mismatch: {path}")
        if verify_source_bindings and repo_root is not None:
            try:
                require(git_blob_sha(repo_root, BASE, path) == expected,
                        f"git source binding mismatch: {path}")
            except RuntimeError as exc:
                errors.append(str(exc))
    if verify_source_bindings and repo_root is None:
        errors.append("repo_root is required for source binding verification")
    return errors


def bundle_index_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    return "".join(
        f"{row['path']}\0{row['size_bytes']}\0{row['sha256']}\n"
        for row in sorted(rows, key=lambda item: str(item["path"]))
    ).encode("utf-8")


def validate_manifest(root: Path, manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_paths = ALLOWED_CHANGED_PATHS - {
        "docs/adr/sprint80/runtime_truth_authority_manifest.json"
    }
    expected = {
        "schema_version": 2,
        "base_head": BASE,
        "frozen_source_head": SOURCE,
        "frozen_artifact_sha256": ZIP_SHA,
        "truth_evidence_sha256": TRUTH_SHA,
        "historical_frozen_zip_verification": "ALREADY_FROZEN_AND_DIGEST_PINNED",
        "package_specific_check_name": CHECK_NAME,
        "bundle_index_algorithm": BUNDLE_INDEX_ALGORITHM,
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            errors.append(f"manifest mismatch for {key}")
    rows = manifest.get("artifacts") or []
    if len(rows) != 6 or {row.get("path") for row in rows} != expected_paths:
        errors.append("manifest artifact path/cardinality mismatch")
    for row in rows:
        path = root / str(row.get("path"))
        if not path.exists():
            errors.append(f"manifest file missing: {row.get('path')}")
            continue
        if path.stat().st_size != row.get("size_bytes"):
            errors.append(f"manifest size mismatch: {row.get('path')}")
        if sha256_file(path) != row.get("sha256"):
            errors.append(f"manifest sha mismatch: {row.get('path')}")
    if manifest.get("bundle_index_sha256") != sha256_bytes(bundle_index_bytes(rows)):
        errors.append("manifest bundle index sha mismatch")
    return errors


def validate_zip(path: Path, committed_evidence_bytes: bytes) -> list[str]:
    errors: list[str] = []
    if sha256_file(path) != ZIP_SHA:
        return ["run93 zip digest mismatch"]
    with zipfile.ZipFile(path) as archive:
        if set(archive.namelist()) != ZIP_MEMBERS:
            errors.append("run93 zip member set mismatch")
        truth = archive.read("truth_contradictions.json")
    if sha256_bytes(truth) != TRUTH_SHA:
        errors.append("truth evidence digest mismatch inside zip")
    if truth != committed_evidence_bytes:
        errors.append("committed truth evidence differs byte-for-byte from frozen ZIP")
    return errors


def validate_scope(paths: Iterable[str]) -> list[str]:
    actual = {str(path).replace("\\", "/") for path in paths}
    if actual == ALLOWED_CHANGED_PATHS:
        return []
    return ["changed path set mismatch: " + json.dumps({
        "missing": sorted(ALLOWED_CHANGED_PATHS - actual),
        "unexpected": sorted(actual - ALLOWED_CHANGED_PATHS),
    }, sort_keys=True)]


def validate_bundle(
    root: Path, *, evidence_zip: Path | None = None,
    changed_paths: Iterable[str] | None = None,
    verify_source_bindings: bool = False,
    verify_git_scope: bool = False,
) -> dict[str, Any]:
    evidence_bytes = (root / EVIDENCE_RELATIVE_PATH).read_bytes()
    evidence, errors = validate_evidence_bytes(evidence_bytes)
    matrix = load_json(root / "docs/adr/sprint80/runtime_truth_authority_matrix.json")
    manifest = load_json(root / "docs/adr/sprint80/runtime_truth_authority_manifest.json")
    matrix_errors = validate_matrix(
        matrix, evidence, repo_root=root,
        verify_source_bindings=verify_source_bindings,
    )
    manifest_errors = validate_manifest(root, manifest)
    errors.extend(matrix_errors)
    errors.extend(manifest_errors)
    historical = "ALREADY_FROZEN_AND_DIGEST_PINNED"
    if evidence_zip:
        zip_errors = validate_zip(evidence_zip, evidence_bytes)
        errors.extend(zip_errors)
        historical = "PASS" if not zip_errors else "FAIL"
    effective: list[str] | None = None
    if verify_git_scope:
        try:
            effective = git_changed_paths(root, BASE)
        except RuntimeError as exc:
            errors.append(str(exc))
    elif changed_paths is not None:
        effective = list(changed_paths)
    scope_errors = validate_scope(effective) if effective is not None else []
    errors.extend(scope_errors)
    source_binding_failed = any(
        "source binding" in error or "git rev-parse" in error
        for error in errors
    )
    return {
        "schema_version": 2,
        "status": "PASS" if not errors else "FAIL",
        "base_head": BASE,
        "frozen_source_head": SOURCE,
        "artifact_sha256": ZIP_SHA,
        "truth_evidence_sha256": TRUTH_SHA,
        "historical_frozen_zip_verification": historical,
        "current_pr_bundle_verification": "PASS" if not manifest_errors else "FAIL",
        "source_binding_verification": (
            "PASS" if verify_source_bindings and not source_binding_failed
            else "NOT_RUN" if not verify_source_bindings else "FAIL"
        ),
        "git_scope_verification": (
            "PASS" if verify_git_scope and not scope_errors
            else "NOT_RUN" if not verify_git_scope else "FAIL"
        ),
        "observation_count": evidence.get("observation_count"),
        "caller_count": len(EXPECTED_CALLERS),
        "operation_class_count": len(EXPECTED_OPERATIONS),
        "finding_count": len(EXPECTED_FINDINGS),
        "changed_path_count": len(effective) if effective is not None else None,
        "product_source_mutation": 0,
        "implementation_authorized": False,
        "human_architecture_acceptance": "PENDING",
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--evidence-zip", type=Path)
    parser.add_argument("--out", type=Path)
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
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

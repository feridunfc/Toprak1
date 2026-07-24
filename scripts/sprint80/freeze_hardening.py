from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

REQUIRED_EVIDENCE = frozenset(
    {
        "preflight.json",
        "environment.json",
        "writer_inventory.json",
        "transition_cardinality.json",
        "ttl_durability.json",
        "truth_contradictions.json",
        "aggregate_boundary.json",
        "final_summary.json",
        "reality_pytest.txt",
        "contracts_pytest.txt",
        "preflight_after.json",
        "environment_after.json",
    }
)

REQUIRED_FINDING_FIELDS = frozenset(
    {
        "finding_id",
        "source_artifact",
        "status",
        "severity",
        "evidence_reference",
        "ADR_dependency",
        "product_fix_deferred_to",
    }
)

EXPECTED_FINDING_IDS_BY_CATEGORY: dict[str, frozenset[str]] = {
    "event_state_parity": frozenset(
        {
            "event_first_orphan",
            "state_first_missing_transport",
            "committed_state_missing_background_audit",
        }
    ),
    "cardinality": frozenset(
        {
            "no_verified_canonical_transition_record",
            "no_aggregate_revision_evidence",
        }
    ),
    "durability": frozenset(
        {
            "run_state_expires_without_reconstruction",
            "run_result_expires_without_reconstruction",
            "dag_task_state_expires",
            "dag_task_meta_expires",
            "task_output_expires",
            "operator_audit_approximate_trim_only",
        }
    ),
    "truth": frozenset(
        {
            "inconsistent_by_caller",
            "contradictory_mutation_not_guaranteed_blocked",
        }
    ),
    "aggregate_boundary": frozenset(
        {
            "missing_remaining_counter_unlocks_fail_open",
            "ready_marker_strands_pending_child",
        }
    ),
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _finding(
    finding_id: str,
    *,
    source_artifact: str,
    severity: str,
    evidence_reference: str,
    adr_dependency: str,
    product_fix_deferred_to: str = "SPRINT_80C_POST_ADR_IMPLEMENTATION",
) -> dict[str, str]:
    return {
        "finding_id": finding_id,
        "source_artifact": source_artifact,
        "status": "ACCEPTED_GAP",
        "severity": severity,
        "evidence_reference": evidence_reference,
        "ADR_dependency": adr_dependency,
        "product_fix_deferred_to": product_fix_deferred_to,
    }


def _state_family_rows(report: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(row.get("state_family", "")): row
        for row in report.get("observations", [])
    }


def _scenario_rows(report: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(row.get("scenario", "")): row
        for row in report.get("observations", [])
    }


def build_accepted_findings(
    *,
    transition_cardinality: Mapping[str, Any],
    ttl_durability: Mapping[str, Any],
    truth_contradictions: Mapping[str, Any],
    aggregate_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    categories: dict[str, list[dict[str, str]]] = {
        "event_state_parity": [
            _finding(
                "event_first_orphan",
                source_artifact="reality_pytest.txt",
                severity="critical",
                evidence_reference=(
                    "tests/diagnostics/sprint80/test_80_03_event_state_parity.py::"
                    "test_real_eventstore_append_survives_forced_state_failure"
                ),
                adr_dependency="EVENT_STATE_ATOMICITY_ADR",
            ),
            _finding(
                "state_first_missing_transport",
                source_artifact="reality_pytest.txt",
                severity="critical",
                evidence_reference=(
                    "tests/diagnostics/sprint80/test_80_03_event_state_parity.py::"
                    "test_state_first_admission_can_leave_missing_transport"
                ),
                adr_dependency="EVENT_STATE_ATOMICITY_ADR",
            ),
            _finding(
                "committed_state_missing_background_audit",
                source_artifact="reality_pytest.txt",
                severity="high",
                evidence_reference=(
                    "tests/diagnostics/sprint80/test_80_03_event_state_parity.py::"
                    "test_committed_state_survives_background_audit_failure"
                ),
                adr_dependency="EVENT_STATE_ATOMICITY_ADR",
            ),
        ],
        "cardinality": [],
        "durability": [],
        "truth": [],
        "aggregate_boundary": [],
    }

    if int(transition_cardinality.get("canonical_transition_record_count", -1)) == 0:
        categories["cardinality"].append(
            _finding(
                "no_verified_canonical_transition_record",
                source_artifact="transition_cardinality.json",
                severity="critical",
                evidence_reference="#/canonical_transition_record_count",
                adr_dependency="CANONICAL_TRANSITION_RECORD_ADR",
            )
        )
    if int(transition_cardinality.get("aggregate_revision_evidence_count", -1)) == 0:
        categories["cardinality"].append(
            _finding(
                "no_aggregate_revision_evidence",
                source_artifact="transition_cardinality.json",
                severity="high",
                evidence_reference="#/aggregate_revision_evidence_count",
                adr_dependency="CANONICAL_TRANSITION_RECORD_ADR",
            )
        )

    ttl_rows = _state_family_rows(ttl_durability)
    ttl_blocking = set(ttl_durability.get("blocking_findings", []))
    durability_specs = (
        (
            "run_state",
            "run_state_expires_without_reconstruction",
            "critical",
        ),
        (
            "run_result",
            "run_result_expires_without_reconstruction",
            "high",
        ),
        ("dag_task_state", "dag_task_state_expires", "critical"),
        ("dag_task_meta", "dag_task_meta_expires", "critical"),
        ("task_output", "task_output_expires", "high"),
        (
            "operator_audit_stream",
            "operator_audit_approximate_trim_only",
            "high",
        ),
    )
    for state_family, finding_id, severity in durability_specs:
        row = ttl_rows.get(state_family, {})
        if (
            state_family in ttl_blocking
            and row.get("finding_status") == "BLOCKING_GAP"
            and bool(row.get("blocking_finding"))
        ):
            categories["durability"].append(
                _finding(
                    finding_id,
                    source_artifact="ttl_durability.json",
                    severity=severity,
                    evidence_reference=f"#/observations[state_family={state_family}]",
                    adr_dependency="DURABILITY_AND_RECONSTRUCTION_ADR",
                )
            )

    if truth_contradictions.get("global_truth_policy") == "INCONSISTENT_BY_CALLER":
        categories["truth"].append(
            _finding(
                "inconsistent_by_caller",
                source_artifact="truth_contradictions.json",
                severity="critical",
                evidence_reference="#/global_truth_policy",
                adr_dependency="RUN_TASK_TRUTH_ADR",
            )
        )
    truth_scenarios = _scenario_rows(truth_contradictions)
    scheduler = truth_scenarios.get("run_terminal_task_ready_scheduler", {})
    if (
        scheduler.get("deterministic_winner") == "TASK_WINS"
        and bool(scheduler.get("mutation_attempted"))
        and str(scheduler.get("returned_status", "")).startswith("committed:")
    ):
        categories["truth"].append(
            _finding(
                "contradictory_mutation_not_guaranteed_blocked",
                source_artifact="truth_contradictions.json",
                severity="critical",
                evidence_reference=(
                    "#/observations[scenario=run_terminal_task_ready_scheduler]"
                ),
                adr_dependency="RUN_TASK_TRUTH_ADR",
            )
        )

    aggregate_scenarios = _scenario_rows(aggregate_boundary)
    aggregate_blocking = set(aggregate_boundary.get("blocking_findings", []))
    aggregate_specs = (
        (
            "missing_remaining_counter_unlocks_fail_open",
            "critical",
        ),
        ("ready_marker_strands_pending_child", "high"),
    )
    for finding_id, severity in aggregate_specs:
        row = aggregate_scenarios.get(finding_id, {})
        if (
            finding_id in aggregate_blocking
            and row.get("finding_status") == "BLOCKING_GAP"
        ):
            categories["aggregate_boundary"].append(
                _finding(
                    finding_id,
                    source_artifact="aggregate_boundary.json",
                    severity=severity,
                    evidence_reference=f"#/observations[scenario={finding_id}]",
                    adr_dependency="ADR-80.2_AGGREGATE_BOUNDARY",
                )
            )

    observed_ids_by_category = {
        category: {row["finding_id"] for row in rows}
        for category, rows in categories.items()
    }
    missing_by_category = {
        category: sorted(expected - observed_ids_by_category.get(category, set()))
        for category, expected in EXPECTED_FINDING_IDS_BY_CATEGORY.items()
    }
    malformed: list[str] = []
    for rows in categories.values():
        for row in rows:
            if not REQUIRED_FINDING_FIELDS <= row.keys() or any(
                not row.get(field) for field in REQUIRED_FINDING_FIELDS
            ):
                malformed.append(row.get("finding_id", "<missing-finding-id>"))

    missing_ids = sorted(
        finding_id
        for rows in missing_by_category.values()
        for finding_id in rows
    )
    all_rows = [row for rows in categories.values() for row in rows]
    category_counts = Counter(
        category for category, rows in categories.items() for _ in rows
    )
    return {
        "schema_version": 1,
        "finding_count": len(all_rows),
        "category_counts": dict(sorted(category_counts.items())),
        "categories": categories,
        "expected_finding_ids_by_category": {
            category: sorted(ids)
            for category, ids in sorted(EXPECTED_FINDING_IDS_BY_CATEGORY.items())
        },
        "missing_expected_finding_ids_by_category": missing_by_category,
        "missing_expected_finding_ids": missing_ids,
        "malformed_finding_ids": sorted(malformed),
        "complete": not missing_ids and not malformed,
    }


def harden_summary_payload(
    summary: Mapping[str, Any],
    *,
    transition_cardinality: Mapping[str, Any],
    ttl_durability: Mapping[str, Any],
    truth_contradictions: Mapping[str, Any],
    aggregate_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    payload = dict(summary)
    accepted_findings = build_accepted_findings(
        transition_cardinality=transition_cardinality,
        ttl_durability=ttl_durability,
        truth_contradictions=truth_contradictions,
        aggregate_boundary=aggregate_boundary,
    )
    payload["schema_version"] = max(int(payload.get("schema_version", 1)), 2)
    payload["accepted_findings"] = accepted_findings
    closure_gates = dict(payload.get("closure_gates", {}))
    closure_gates["accepted_finding_register"] = {
        "status": "PASS" if accepted_findings["complete"] else "FAIL",
        "expected_count": sum(
            len(ids) for ids in EXPECTED_FINDING_IDS_BY_CATEGORY.values()
        ),
        "observed_count": accepted_findings["finding_count"],
        "unresolved_count": len(
            accepted_findings["missing_expected_finding_ids"]
        )
        + len(accepted_findings["malformed_finding_ids"]),
        "missing_finding_ids": accepted_findings[
            "missing_expected_finding_ids"
        ],
        "malformed_finding_ids": accepted_findings["malformed_finding_ids"],
    }
    payload["closure_gates"] = closure_gates
    if not accepted_findings["complete"]:
        payload["closure_status"] = "BLOCKED"
    payload["freeze_hardening"] = {
        "mandatory_evidence_set": sorted(REQUIRED_EVIDENCE),
        "mandatory_evidence_count": len(REQUIRED_EVIDENCE),
        "finding_register_complete": accepted_findings["complete"],
        "writer_id_pinning": "P2_DEFERRED",
        "standalone_manifest_validation": "P2_DEFERRED",
    }
    return payload


def validate_manifest(
    directory: Path,
    manifest: Mapping[str, Any],
    *,
    expected_branch: str | None = None,
    expected_head: str | None = None,
) -> dict[str, Any]:
    artifact_rows = list(manifest.get("artifacts", []))
    artifact_paths = {str(row.get("path", "")) for row in artifact_rows}
    missing = sorted(REQUIRED_EVIDENCE - artifact_paths)
    unexpected = sorted(artifact_paths - REQUIRED_EVIDENCE)
    if missing or unexpected:
        raise ValueError(
            f"mandatory evidence mismatch: missing={missing} unexpected={unexpected}"
        )
    if len(artifact_rows) != len(REQUIRED_EVIDENCE):
        raise ValueError(
            f"manifest row count mismatch: {len(artifact_rows)} != {len(REQUIRED_EVIDENCE)}"
        )
    if expected_branch is not None and manifest.get("branch") != expected_branch:
        raise ValueError(
            f"manifest branch mismatch: {manifest.get('branch')} != {expected_branch}"
        )
    if expected_head is not None and manifest.get("head") != expected_head:
        raise ValueError(
            f"manifest head mismatch: {manifest.get('head')} != {expected_head}"
        )

    for row in artifact_rows:
        path = directory / str(row["path"])
        if not path.exists():
            raise ValueError(f"manifest artifact missing: {path.name}")
        size = path.stat().st_size
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if size != int(row["size_bytes"]):
            raise ValueError(f"manifest size mismatch: {path.name}")
        if digest != row["sha256"]:
            raise ValueError(f"manifest digest mismatch: {path.name}")

    return {
        "status": "VALID",
        "required_evidence_count": len(REQUIRED_EVIDENCE),
        "artifact_count": len(artifact_rows),
        "missing": missing,
        "unexpected": unexpected,
    }


def harden_summary_file(directory: Path, summary_path: Path) -> dict[str, Any]:
    payload = harden_summary_payload(
        _read_json(summary_path),
        transition_cardinality=_read_json(directory / "transition_cardinality.json"),
        ttl_durability=_read_json(directory / "ttl_durability.json"),
        truth_contradictions=_read_json(directory / "truth_contradictions.json"),
        aggregate_boundary=_read_json(directory / "aggregate_boundary.json"),
    )
    _write_json(summary_path, payload)
    return payload


def validate_summary(summary: Mapping[str, Any]) -> None:
    register = summary.get("accepted_findings", {})
    gate = summary.get("closure_gates", {}).get(
        "accepted_finding_register", {}
    )
    if not register.get("complete"):
        raise ValueError("accepted finding register is incomplete")
    if gate.get("status") != "PASS" or int(gate.get("unresolved_count", -1)) != 0:
        raise ValueError("accepted finding register closure gate failed")
    if int(register.get("finding_count", -1)) != sum(
        len(ids) for ids in EXPECTED_FINDING_IDS_BY_CATEGORY.values()
    ):
        raise ValueError("accepted finding count mismatch")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Harden Sprint 80B.8 freeze evidence and final finding register"
    )
    parser.add_argument("--directory", default="local_out/sprint80")
    parser.add_argument("--summary", default="local_out/sprint80/final_summary.json")
    parser.add_argument("--manifest", default="local_out/sprint80/manifest.json")
    parser.add_argument("--harden-summary", action="store_true")
    parser.add_argument("--validate-manifest", action="store_true")
    parser.add_argument("--validate-summary", action="store_true")
    args = parser.parse_args(argv)

    directory = Path(args.directory)
    summary_path = Path(args.summary)
    manifest_path = Path(args.manifest)

    summary: dict[str, Any] | None = None
    if args.harden_summary:
        summary = harden_summary_file(directory, summary_path)
    elif args.validate_summary:
        summary = _read_json(summary_path)

    validation: dict[str, Any] | None = None
    if args.validate_manifest:
        if summary is None and summary_path.exists():
            summary = _read_json(summary_path)
        validation = validate_manifest(
            directory,
            _read_json(manifest_path),
            expected_branch=(str(summary.get("branch", "")) if summary else None),
            expected_head=(str(summary.get("head", "")) if summary else None),
        )

    if args.validate_summary:
        if summary is None:
            summary = _read_json(summary_path)
        validate_summary(summary)

    print(
        json.dumps(
            {
                "summary_hardened": bool(args.harden_summary),
                "summary_validated": bool(args.validate_summary),
                "manifest_validation": validation,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

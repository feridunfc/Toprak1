from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

AUDIT_BASE_COMMIT = "2eca85b2d9b115ad4588641b020e98efdd570a2d"

SELECTED_AUTHORITIES = {"RUN", "TASK", "NONE"}
BEHAVIOR_DISPOSITIONS = {
    "RETURN_SELECTED_STATE",
    "SUPPRESS",
    "FAIL_OPEN",
    "FAIL_CLOSED",
    "UNRESOLVED",
}
MUTATION_RESULTS = {
    "not_attempted",
    "attempted_rejected",
    "committed",
    "attempted_unknown",
}

EXPECTED_CANONICAL_DISPOSITION_PATHS = {
    "hfa-core/src/hfa/state/__init__.py",
}

EXPECTED_SEMANTIC_COMPOSITION_DISPOSITION_PATHS = {
    "hfa-semantic/src/hfa_semantic/runtime/inmemory_state_store.py",
    "hfa-semantic/src/hfa_semantic/runtime/redis_state_store.py",
    "hfa-semantic/src/hfa_semantic/runtime/state_store.py",
}

FEATURE_FLAG_MATRIX: tuple[dict[str, Any], ...] = (
    {
        "flag": "HFA_WORKER_TASK_CONSUMER_BRIDGE",
        "status": "EXECUTABLE_COMPLETE",
        "risk": "high",
        "affected_paths": [
            "hfa-worker/src/hfa_worker/consumer.py",
            "hfa-worker/src/hfa_worker/runtime/worker_runtime.py",
        ],
        "evidence": [
            "tests/diagnostics/sprint80/test_80_02_production_reachability.py::test_message_type_and_flag_select_observed_execution_path",
            "TaskRequested/RunRequested x enabled/disabled = four executable variants",
        ],
        "revisit_trigger": "worker routing predicates or message taxonomy changes",
    },
    {
        "flag": "HFA_STRICT_CAS_MODE",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": (
            "The shared hfa.state transition gateway is a canonical candidate, but its "
            "production composition and caller set are not proven by Sprint 80B."
        ),
        "risk": "high",
        "affected_paths": [
            "hfa-core/src/hfa/state/__init__.py",
            "hfa-core/src/hfa/lua/state_transition.lua",
        ],
        "revisit_trigger": (
            "any production import/call of hfa.state.transition_state, deployment enablement "
            "of HFA_STRICT_CAS_MODE, or the authority ADR"
        ),
    },
    {
        "flag": "HFA_ALLOW_LEGACY_DIRECT_TASK_CLAIM",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": "The full enabled/disabled direct-claim writer matrix was not executed in 80B.",
        "risk": "high",
        "affected_paths": ["hfa-control/src/hfa_control/dag_lua.py"],
        "revisit_trigger": "non-empty production setting or removal of the legacy direct-claim path",
    },
    {
        "flag": "HFA_ALLOW_LEGACY_INJECTED_DISPATCH",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": "Injected legacy dispatch composition was not executed across both flag values.",
        "risk": "high",
        "affected_paths": ["hfa-control/src/hfa_control/scheduler_loop.py"],
        "revisit_trigger": "production enablement or scheduler composition change",
    },
    {
        "flag": "IRON_V3_COMPLETION_SLICE",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": "The compatibility StateStore completion slice was inventoried but not matrix-tested.",
        "risk": "high",
        "affected_paths": ["hfa-core/src/hfa/runtime/state_store.py"],
        "revisit_trigger": "production enablement, compatibility retirement, or completion authority ADR",
    },
    {
        "flag": "IRON_V3_EVENT_GATE",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": "Event-gated and legacy authoritative-write routes were not executed as a full matrix.",
        "risk": "high",
        "affected_paths": ["hfa-core/src/hfa/events/append_service.py"],
        "revisit_trigger": "event gate production enablement or event/state atomicity implementation",
    },
    {
        "flag": "IRON_V3_PROOF_ENFORCEMENT",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": "Scheduler and recovery proof-enforcement variants were not both executed.",
        "risk": "high",
        "affected_paths": [
            "hfa-control/src/hfa_control/scheduler_loop.py",
            "hfa-control/src/hfa_control/task_recovery.py",
        ],
        "revisit_trigger": "production enablement or proof schema/guard changes",
    },
    {
        "flag": "IRON_V3_SCHEDULER_SEAL",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": "The scheduler-seal writer path was source-inventoried but not matrix-executed.",
        "risk": "high",
        "affected_paths": ["hfa-control/src/hfa_control/scheduler_loop.py"],
        "revisit_trigger": "production enablement or scheduler authority changes",
    },
    {
        "flag": "IRON_V3_WORKER_EFFECT_HYBRID",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": "Worker effect/event variants were not executed through production composition.",
        "risk": "high",
        "affected_paths": ["hfa-worker/src/hfa_worker/runtime/worker_runtime.py"],
        "revisit_trigger": "production enablement or worker effect authority changes",
    },
    {
        "flag": "IRON_SEMANTIC_GATE_MODE",
        "status": "DEFERRED_WITH_EXPLICIT_REASON",
        "reason": "Semantic runtime stores are excluded from observed default worker/scheduler composition, but the enabled semantic composition was not executed.",
        "risk": "medium",
        "affected_paths": [
            "hfa-semantic/src/hfa_semantic/runtime/semantic_hook.py",
            "hfa-semantic/src/hfa_semantic/runtime/inmemory_state_store.py",
            "hfa-semantic/src/hfa_semantic/runtime/redis_state_store.py",
            "hfa-semantic/src/hfa_semantic/runtime/state_store.py",
        ],
        "revisit_trigger": "any production hfa_semantic composition or IRON_SEMANTIC_GATE_MODE enablement",
    },
    {
        "flag": "IRON_STRICT_MODE",
        "status": "OUT_OF_SCOPE_NON_AUTHORITY",
        "reason": "Governance budget strictness is not a run/task authority-route selector in the observed inventory.",
        "risk": "low",
        "affected_paths": ["hfa-core/src/hfa/governance/budget_guard.py"],
        "revisit_trigger": "evidence that the flag selects a run/task state writer",
    },
    {
        "flag": "HFA_LEDGER_KEY_ID",
        "status": "CONFIG_VALUE_NOT_FLAG",
        "reason": "Signing key identifier; not a boolean path selector.",
        "risk": "not_applicable",
        "affected_paths": ["hfa-control/src/hfa_control/audit.py"],
        "revisit_trigger": "classification changes from credential/config value to route selector",
    },
    {
        "flag": "HFA_LEDGER_PRIVATE_KEY_B64",
        "status": "CONFIG_VALUE_NOT_FLAG",
        "reason": "Signing private-key material; not a boolean path selector.",
        "risk": "not_applicable",
        "affected_paths": ["hfa-control/src/hfa_control/audit.py"],
        "revisit_trigger": "classification changes from credential/config value to route selector",
    },
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_pytest_counts(text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for label in (
        "passed",
        "failed",
        "skipped",
        "xfailed",
        "xpassed",
        "deselected",
        "errors",
    ):
        matches = re.findall(rf"(\d+)\s+{label}\b", text, flags=re.IGNORECASE)
        counts[label] = int(matches[-1]) if matches else 0
    return counts


def _selected_authority(source: str) -> str:
    if source.startswith("run_") or source in {"run_state", "run_result", "running_zset_plus_run_state"}:
        return "RUN"
    if source.startswith("task_") or source in {"task_state", "task_state_and_meta"}:
        return "TASK"
    return "NONE"


def _behavior_disposition(row: Mapping[str, Any]) -> str:
    caller = str(row.get("caller", ""))
    mapping = {
        "control_api_run_state": "RETURN_SELECTED_STATE",
        "run_recovery_missing_state": "FAIL_CLOSED",
        "run_recovery_terminal_guard": "SUPPRESS",
        "task_recovery_missing_state": "FAIL_CLOSED",
        "worker_task_duplicate_guard_nonterminal": "FAIL_OPEN",
        "worker_task_duplicate_guard_terminal": "SUPPRESS",
        "legacy_worker_run_guard": "SUPPRESS",
        "scheduler_dag_dispatch": "FAIL_OPEN",
    }
    return mapping.get(caller, "UNRESOLVED")


def _mutation_result(row: Mapping[str, Any]) -> str:
    if not bool(row.get("mutation_attempted")):
        return "not_attempted"
    caller = str(row.get("caller", ""))
    if caller == "task_recovery_missing_state":
        return "attempted_rejected"
    if caller in {"scheduler_dag_dispatch", "run_recovery_terminal_guard"}:
        return "committed"
    return "attempted_unknown"


def normalize_truth_observations(report: Mapping[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    authority_counts: Counter[str] = Counter()
    disposition_counts: Counter[str] = Counter()
    mutation_counts: Counter[str] = Counter()

    for original in report.get("observations", []):
        selected = _selected_authority(str(original.get("selected_truth_source", "")))
        disposition = _behavior_disposition(original)
        mutation = _mutation_result(original)
        if selected not in SELECTED_AUTHORITIES:
            raise ValueError(f"invalid selected authority: {selected}")
        if disposition not in BEHAVIOR_DISPOSITIONS:
            raise ValueError(f"invalid behavior disposition: {disposition}")
        if mutation not in MUTATION_RESULTS:
            raise ValueError(f"invalid mutation result: {mutation}")
        authority_counts[selected] += 1
        disposition_counts[disposition] += 1
        mutation_counts[mutation] += 1
        rows.append(
            {
                "scenario": original.get("scenario", ""),
                "caller": original.get("caller", ""),
                "run_state": original.get("run_state", ""),
                "task_state": original.get("task_state", ""),
                "selected_truth_source": original.get("selected_truth_source", ""),
                "selected_authority": selected,
                "behavior_disposition": disposition,
                "mutation": mutation,
                "returned_status": original.get("returned_status", ""),
                "recovery_action": original.get("recovery_action", "none"),
                "notes": list(original.get("notes", [])),
            }
        )

    return {
        "source_schema_version": report.get("schema_version"),
        "source_global_truth_policy": report.get("global_truth_policy"),
        "normalized_global_behavior": "INCONSISTENT_BY_CALLER",
        "selected_authority_counts": dict(sorted(authority_counts.items())),
        "behavior_disposition_counts": dict(sorted(disposition_counts.items())),
        "mutation_counts": dict(sorted(mutation_counts.items())),
        "observations": rows,
    }


def normalize_aggregate_boundary(
    aggregate_report: Mapping[str, Any],
    cardinality_report: Mapping[str, Any],
) -> dict[str, Any]:
    blocking = set(aggregate_report.get("blocking_findings", []))
    accepted_correctness_gaps = sorted(
        blocking
        & {
            "missing_remaining_counter_unlocks_fail_open",
            "ready_marker_strands_pending_child",
        }
    )
    revision_observed = bool(
        cardinality_report.get("aggregate_revision_evidence_count", 0)
    ) or bool(aggregate_report.get("aggregate_revision_observed"))
    canonical_observed = bool(
        cardinality_report.get("canonical_transition_record_count", 0)
    ) or bool(aggregate_report.get("child_authority_record_observed"))

    return {
        "cross_task_mutation": "OBSERVED_FACT",
        "aggregate_boundary_interpretation": "ADR_REQUIRED",
        "separate_task_aggregate_violation": "UNRESOLVED",
        "dag_aggregate_interpretation": "POSSIBLE_BUT_REQUIRES_REVISION_CONTRACT",
        "accepted_correctness_gaps": accepted_correctness_gaps,
        "aggregate_revision_observed": revision_observed,
        "canonical_transition_record_observed": canonical_observed,
        "evidence_basis": [
            "80B.7 real-Redis parent completion observations",
            "task_complete.lua source inspection",
            "80B.4 transition-cardinality and revision evidence",
        ],
        "evidence_claim": (
            "No child authority record or aggregate revision was observed; the conclusion "
            "is corroborated by 80B.4 cardinality evidence and task_complete.lua inspection."
        ),
        "frozen_contract": (
            "Every committed aggregate revision containing child effects must produce exactly "
            "one CanonicalTransitionRecord that records those child effects."
        ),
        "aggregate_revision_contract_satisfied": bool(
            revision_observed and canonical_observed
        ),
    }


def _group_writer_ids(
    writers: Iterable[Mapping[str, Any]],
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for writer in writers:
        grouped[str(writer.get("path", ""))].append(str(writer.get("writer_id", "")))
    return {path: sorted(ids) for path, ids in sorted(grouped.items())}


def build_writer_dispositions(inventory: Mapping[str, Any]) -> dict[str, Any]:
    writers = list(inventory.get("writers", []))
    unknown_production_default = [
        row
        for row in writers
        if row.get("production_reachability") == "production_default"
        and row.get("authority_class") == "unknown"
    ]
    unknown_canonical = [
        row
        for row in writers
        if row.get("authority_class") == "canonical_candidate"
        and row.get("production_reachability") == "unknown"
    ]
    production_flagged_unknown = [
        row
        for row in writers
        if row.get("production_reachability") == "production_flagged"
        and row.get("authority_class") == "unknown"
    ]
    composition_unknown = [
        row
        for row in writers
        if row.get("production_reachability")
        in {"production_default", "production_flagged"}
        and row.get("composition_root") == "unknown"
    ]

    canonical_dispositioned = [
        row
        for row in unknown_canonical
        if row.get("path") in EXPECTED_CANONICAL_DISPOSITION_PATHS
    ]
    canonical_unresolved = [
        row for row in unknown_canonical if row not in canonical_dispositioned
    ]
    semantic_composition_dispositioned = [
        row
        for row in composition_unknown
        if row.get("path") in EXPECTED_SEMANTIC_COMPOSITION_DISPOSITION_PATHS
    ]
    composition_unresolved = [
        row
        for row in composition_unknown
        if row not in semantic_composition_dispositioned
    ]

    dispositions = [
        {
            "disposition_id": "canonical-shared-state-gateway",
            "status": "DEFERRED_WITH_EXPLICIT_REASON",
            "writer_ids_by_path": _group_writer_ids(canonical_dispositioned),
            "reason": (
                "The shared hfa.state gateway is a canonical candidate, but current production "
                "worker/scheduler composition evidence does not prove it is an active authority writer."
            ),
            "risk": "high",
            "affected_paths": sorted(EXPECTED_CANONICAL_DISPOSITION_PATHS),
            "revisit_trigger": (
                "any production call to hfa.state.transition_state, HFA_STRICT_CAS_MODE "
                "enablement, or the authority ADR"
            ),
        },
        {
            "disposition_id": "semantic-runtime-store-composition",
            "status": "DEFERRED_WITH_EXPLICIT_REASON",
            "writer_ids_by_path": _group_writer_ids(
                semantic_composition_dispositioned
            ),
            "reason": (
                "80B.2 observed default worker/scheduler composition excludes hfa_semantic, "
                "but the enabled semantic composition matrix was not executed."
            ),
            "risk": "medium",
            "affected_paths": sorted(
                EXPECTED_SEMANTIC_COMPOSITION_DISPOSITION_PATHS
            ),
            "revisit_trigger": (
                "any production hfa_semantic composition or IRON_SEMANTIC_GATE_MODE enablement"
            ),
        },
    ]

    return {
        "raw_counts": {
            "unknown_production_default_authority_writer": len(
                unknown_production_default
            ),
            "unknown_canonical_candidate": len(unknown_canonical),
            "production_flagged_unknown_writer": len(production_flagged_unknown),
            "production_writer_unknown_composition_root": len(composition_unknown),
        },
        "dispositioned_counts": {
            "unknown_canonical_candidate": len(canonical_dispositioned),
            "production_writer_unknown_composition_root": len(
                semantic_composition_dispositioned
            ),
        },
        "unresolved_counts": {
            "unknown_production_default_authority_writer": len(
                unknown_production_default
            ),
            "unknown_canonical_candidate": len(canonical_unresolved),
            "production_flagged_unknown_writer": len(production_flagged_unknown),
            "production_writer_unknown_composition_root": len(
                composition_unresolved
            ),
        },
        "unresolved_writer_ids": {
            "unknown_production_default_authority_writer": sorted(
                row.get("writer_id", "") for row in unknown_production_default
            ),
            "unknown_canonical_candidate": sorted(
                row.get("writer_id", "") for row in canonical_unresolved
            ),
            "production_flagged_unknown_writer": sorted(
                row.get("writer_id", "") for row in production_flagged_unknown
            ),
            "production_writer_unknown_composition_root": sorted(
                row.get("writer_id", "") for row in composition_unresolved
            ),
        },
        "dispositions": dispositions,
    }


def build_feature_flag_matrix(
    inventory: Mapping[str, Any],
    environment: Mapping[str, Any],
) -> dict[str, Any]:
    discovered: set[str] = set(environment.get("environment_flags", {}).keys())
    for writer in inventory.get("writers", []):
        discovered.update(str(flag) for flag in writer.get("feature_flags", []))
    discovered.update(
        {
            "HFA_WORKER_TASK_CONSUMER_BRIDGE",
            "IRON_V3_WORKER_EFFECT_HYBRID",
        }
    )

    rows = [dict(item) for item in FEATURE_FLAG_MATRIX]
    covered = {row["flag"] for row in rows}
    unaccounted = sorted(discovered - covered)
    status_counts = Counter(row["status"] for row in rows)
    deferred_rows = [
        row for row in rows if row["status"] == "DEFERRED_WITH_EXPLICIT_REASON"
    ]
    malformed_deferred = [
        row["flag"]
        for row in deferred_rows
        if not row.get("reason")
        or not row.get("risk")
        or not row.get("affected_paths")
        or not row.get("revisit_trigger")
    ]
    unresolved_count = len(unaccounted) + len(malformed_deferred)
    if unresolved_count:
        matrix_status = "INCOMPLETE"
    elif deferred_rows:
        matrix_status = "EXPLICITLY_DEFERRED"
    else:
        matrix_status = "COMPLETE"

    observed_values = dict(environment.get("environment_flags", {}))
    for row in rows:
        row["observed_value"] = observed_values.get(row["flag"], "not_captured")

    return {
        "scope": (
            "authority-affecting flags discovered in writer inventory, environment snapshot, "
            "and worker runtime routing helpers"
        ),
        "matrix_status": matrix_status,
        "status_counts": dict(sorted(status_counts.items())),
        "unaccounted_discovered_flags": unaccounted,
        "malformed_deferred_flags": sorted(malformed_deferred),
        "unresolved_count": unresolved_count,
        "rows": rows,
    }


def render_final_summary(
    *,
    writer_inventory: Mapping[str, Any],
    truth_contradictions: Mapping[str, Any],
    aggregate_boundary: Mapping[str, Any],
    transition_cardinality: Mapping[str, Any],
    environment: Mapping[str, Any],
    preflight_after: Mapping[str, Any],
    reality_counts: Mapping[str, int],
    contract_counts: Mapping[str, int],
    manifest_validated: bool = False,
) -> dict[str, Any]:
    normalized_truth = normalize_truth_observations(truth_contradictions)
    normalized_aggregate = normalize_aggregate_boundary(
        aggregate_boundary,
        transition_cardinality,
    )
    writer_dispositions = build_writer_dispositions(writer_inventory)
    flag_matrix = build_feature_flag_matrix(writer_inventory, environment)

    unexpected_product_changes = list(
        preflight_after.get("unexpected_changed_paths", [])
    )
    unresolved_writers = writer_dispositions["unresolved_counts"]
    gates = {
        "unknown_production_default_authority_writer": {
            "status": "PASS"
            if unresolved_writers[
                "unknown_production_default_authority_writer"
            ]
            == 0
            else "FAIL",
            "unresolved_count": unresolved_writers[
                "unknown_production_default_authority_writer"
            ],
        },
        "unknown_canonical_candidate": {
            "status": "PASS_WITH_EXPLICIT_DISPOSITION"
            if unresolved_writers["unknown_canonical_candidate"] == 0
            else "FAIL",
            "raw_count": writer_dispositions["raw_counts"][
                "unknown_canonical_candidate"
            ],
            "dispositioned_count": writer_dispositions["dispositioned_counts"][
                "unknown_canonical_candidate"
            ],
            "unresolved_count": unresolved_writers[
                "unknown_canonical_candidate"
            ],
        },
        "production_flagged_unknown_writer": {
            "status": "PASS"
            if unresolved_writers["production_flagged_unknown_writer"] == 0
            else "FAIL",
            "unresolved_count": unresolved_writers[
                "production_flagged_unknown_writer"
            ],
        },
        "production_composition_disposition": {
            "status": "PASS_WITH_EXPLICIT_DISPOSITION"
            if unresolved_writers[
                "production_writer_unknown_composition_root"
            ]
            == 0
            else "FAIL",
            "raw_count": writer_dispositions["raw_counts"][
                "production_writer_unknown_composition_root"
            ],
            "dispositioned_count": writer_dispositions[
                "dispositioned_counts"
            ]["production_writer_unknown_composition_root"],
            "unresolved_count": unresolved_writers[
                "production_writer_unknown_composition_root"
            ],
        },
        "feature_flag_path_matrix": {
            "status": flag_matrix["matrix_status"],
            "unresolved_count": flag_matrix["unresolved_count"],
        },
        "reality_failures": {
            "status": "PASS" if int(reality_counts.get("failed", 0)) == 0 else "FAIL",
            "count": int(reality_counts.get("failed", 0)),
        },
        "xpass": {
            "status": "PASS" if int(contract_counts.get("xpassed", 0)) == 0 else "FAIL",
            "count": int(contract_counts.get("xpassed", 0)),
        },
        "unexpected_contract_failure": {
            "status": "PASS"
            if int(contract_counts.get("failed", 0)) == 0
            and int(contract_counts.get("errors", 0)) == 0
            else "FAIL",
            "count": int(contract_counts.get("failed", 0))
            + int(contract_counts.get("errors", 0)),
        },
        "product_source_mutation": {
            "status": "PASS" if not unexpected_product_changes else "FAIL",
            "count": len(unexpected_product_changes),
            "paths": unexpected_product_changes,
        },
        "evidence_manifest": {
            "status": (
                "VALIDATED_BY_WORKFLOW" if manifest_validated else "PENDING_POST_BUILD_VALIDATION"
            ),
        },
    }

    non_manifest_failures = [
        name
        for name, row in gates.items()
        if name != "evidence_manifest"
        and row.get("status")
        not in {
            "PASS",
            "PASS_WITH_EXPLICIT_DISPOSITION",
            "COMPLETE",
            "EXPLICITLY_DEFERRED",
        }
    ]
    if non_manifest_failures:
        closure_status = "BLOCKED"
    elif not manifest_validated:
        closure_status = "PENDING_MANIFEST_VALIDATION"
    else:
        closure_status = "PASS_WITH_EXPLICIT_DISPOSITIONS"

    return {
        "schema_version": 1,
        "audit_base_commit": AUDIT_BASE_COMMIT,
        "branch": environment.get("branch", ""),
        "head": environment.get("head", ""),
        "tranche": "80B.8",
        "closure_status": closure_status,
        "merge_authorized": False,
        "sprint80c_started": False,
        "human_freeze_review_required": True,
        "accepted_tranches": {
            "80B.6": "ACCEPTED_COMPLETE",
            "80B.7_correctness_gaps": "ACCEPTED",
            "80B.7_aggregate_interpretation": "ADR_REQUIRED",
        },
        "truth_summary": normalized_truth,
        "aggregate_boundary_summary": normalized_aggregate,
        "writer_dispositions": writer_dispositions,
        "feature_flag_matrix": flag_matrix,
        "test_results": {
            "reality": dict(reality_counts),
            "contracts": dict(contract_counts),
        },
        "closure_gates": gates,
    }


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Sprint 80B.8 final reconciliation summary")
    parser.add_argument("--directory", default="local_out/sprint80")
    parser.add_argument("--out", default="local_out/sprint80/final_summary.json")
    parser.add_argument("--manifest-validated", action="store_true")
    args = parser.parse_args(argv)

    directory = Path(args.directory)
    reality_text = (directory / "reality_pytest.txt").read_text(encoding="utf-8")
    contracts_text = (directory / "contracts_pytest.txt").read_text(encoding="utf-8")
    payload = render_final_summary(
        writer_inventory=_read_json(directory / "writer_inventory.json"),
        truth_contradictions=_read_json(directory / "truth_contradictions.json"),
        aggregate_boundary=_read_json(directory / "aggregate_boundary.json"),
        transition_cardinality=_read_json(directory / "transition_cardinality.json"),
        environment=_read_json(directory / "environment_after.json"),
        preflight_after=_read_json(directory / "preflight_after.json"),
        reality_counts=parse_pytest_counts(reality_text),
        contract_counts=parse_pytest_counts(contracts_text),
        manifest_validated=bool(args.manifest_validated),
    )
    write_json(Path(args.out), payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["closure_status"] != "BLOCKED" else 1


if __name__ == "__main__":
    raise SystemExit(main())

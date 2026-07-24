from __future__ import annotations

import json
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

import pytest


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _first_existing(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError(", ".join(str(path) for path in paths))


@pytest.fixture(scope="module")
def final_summary_model(
    sprint80_module_loader: Callable[[str], ModuleType],
) -> ModuleType:
    return sprint80_module_loader("final_summary")


@pytest.fixture(scope="module")
def final_summary_report(
    repo_root: Path,
    final_summary_model: ModuleType,
) -> dict[str, Any]:
    directory = repo_root / "local_out/sprint80"
    return final_summary_model.render_final_summary(
        writer_inventory=_read_json(directory / "writer_inventory.json"),
        truth_contradictions=_read_json(directory / "truth_contradictions.json"),
        aggregate_boundary=_read_json(directory / "aggregate_boundary.json"),
        transition_cardinality=_read_json(directory / "transition_cardinality.json"),
        environment=_read_json(
            _first_existing(
                directory / "environment_after.json",
                directory / "environment.json",
            )
        ),
        preflight_after=_read_json(
            _first_existing(
                directory / "preflight_after.json",
                directory / "preflight.json",
            )
        ),
        reality_counts={"failed": 0, "errors": 0},
        contract_counts={"failed": 0, "errors": 0, "xpassed": 0},
        manifest_validated=True,
    )


def _truth_row(report: dict[str, Any], caller: str) -> dict[str, Any]:
    return next(
        row
        for row in report["truth_summary"]["observations"]
        if row["caller"] == caller
    )


@pytest.mark.sprint80_reality
def test_truth_summary_separates_authority_disposition_and_mutation(
    final_summary_report: dict[str, Any],
):
    truth = final_summary_report["truth_summary"]
    assert truth["normalized_global_behavior"] == "INCONSISTENT_BY_CALLER"
    assert truth["selected_authority_counts"] == {"RUN": 5, "TASK": 4}
    assert "NONE" not in truth["selected_authority_counts"]
    for row in truth["observations"]:
        assert row["selected_authority"] in {"RUN", "TASK"}
        assert row["behavior_disposition"] in {
            "RETURN_SELECTED_STATE",
            "SUPPRESS",
            "FAIL_OPEN",
            "FAIL_CLOSED",
        }
        assert row["mutation"] in {
            "not_attempted",
            "attempted_rejected",
            "committed",
        }


@pytest.mark.sprint80_reality
def test_fail_closed_is_behavior_not_truth_winner(
    final_summary_report: dict[str, Any],
):
    task_recovery = _truth_row(
        final_summary_report,
        "task_recovery_missing_state",
    )
    run_recovery = _truth_row(
        final_summary_report,
        "run_recovery_missing_state",
    )
    assert task_recovery["selected_authority"] == "TASK"
    assert task_recovery["behavior_disposition"] == "FAIL_CLOSED"
    assert task_recovery["mutation"] == "attempted_rejected"
    assert run_recovery["selected_authority"] == "RUN"
    assert run_recovery["behavior_disposition"] == "FAIL_CLOSED"
    assert run_recovery["mutation"] == "not_attempted"


@pytest.mark.sprint80_reality
def test_scheduler_is_fail_open_with_committed_task_mutation(
    final_summary_report: dict[str, Any],
):
    scheduler = _truth_row(final_summary_report, "scheduler_dag_dispatch")
    assert scheduler["selected_authority"] == "TASK"
    assert scheduler["behavior_disposition"] == "FAIL_OPEN"
    assert scheduler["mutation"] == "committed"


@pytest.mark.sprint80_reality
def test_aggregate_interpretation_is_adr_dependent(
    final_summary_report: dict[str, Any],
):
    aggregate = final_summary_report["aggregate_boundary_summary"]
    assert aggregate["cross_task_mutation"] == "OBSERVED_FACT"
    assert aggregate["aggregate_boundary_interpretation"] == "ADR_REQUIRED"
    assert aggregate["separate_task_aggregate_violation"] == "UNRESOLVED"
    assert aggregate["accepted_correctness_gaps"] == [
        "missing_remaining_counter_unlocks_fail_open",
        "ready_marker_strands_pending_child",
    ]
    assert aggregate["aggregate_revision_observed"] is False
    assert aggregate["canonical_transition_record_observed"] is False
    assert "80B.4" in aggregate["evidence_claim"]
    assert "task_complete.lua" in aggregate["evidence_claim"]


@pytest.mark.sprint80_reality
def test_remaining_writer_candidates_have_explicit_dispositions(
    final_summary_report: dict[str, Any],
):
    writers = final_summary_report["writer_dispositions"]
    assert writers["raw_counts"] == {
        "unknown_production_default_authority_writer": 0,
        "unknown_canonical_candidate": 4,
        "production_flagged_unknown_writer": 0,
        "production_writer_unknown_composition_root": 26,
    }
    assert writers["unresolved_counts"] == {
        "unknown_production_default_authority_writer": 0,
        "unknown_canonical_candidate": 0,
        "production_flagged_unknown_writer": 0,
        "production_writer_unknown_composition_root": 0,
    }
    for disposition in writers["dispositions"]:
        assert disposition["status"] == "DEFERRED_WITH_EXPLICIT_REASON"
        assert disposition["reason"]
        assert disposition["risk"]
        assert disposition["affected_paths"]
        assert disposition["revisit_trigger"]


@pytest.mark.sprint80_reality
def test_feature_flag_matrix_is_complete_or_explicitly_deferred(
    final_summary_report: dict[str, Any],
):
    matrix = final_summary_report["feature_flag_matrix"]
    assert matrix["matrix_status"] == "EXPLICITLY_DEFERRED"
    assert matrix["unresolved_count"] == 0
    assert matrix["unaccounted_discovered_flags"] == []
    assert matrix["malformed_deferred_flags"] == []
    bridge = next(
        row
        for row in matrix["rows"]
        if row["flag"] == "HFA_WORKER_TASK_CONSUMER_BRIDGE"
    )
    assert bridge["status"] == "EXECUTABLE_COMPLETE"
    deferred = [
        row
        for row in matrix["rows"]
        if row["status"] == "DEFERRED_WITH_EXPLICIT_REASON"
    ]
    assert deferred
    for row in deferred:
        assert row["reason"]
        assert row["risk"]
        assert row["affected_paths"]
        assert row["revisit_trigger"]


@pytest.mark.sprint80_reality
@pytest.mark.sprint80_contract
def test_80b8_closure_gates_have_no_unresolved_items(
    final_summary_report: dict[str, Any],
):
    assert final_summary_report["closure_status"] == (
        "PASS_WITH_EXPLICIT_DISPOSITIONS"
    )
    assert final_summary_report["merge_authorized"] is False
    assert final_summary_report["sprint80c_started"] is False
    gates = final_summary_report["closure_gates"]
    assert gates["unknown_production_default_authority_writer"][
        "unresolved_count"
    ] == 0
    assert gates["unknown_canonical_candidate"]["unresolved_count"] == 0
    assert gates["production_flagged_unknown_writer"]["unresolved_count"] == 0
    assert gates["production_composition_disposition"]["unresolved_count"] == 0
    assert gates["feature_flag_path_matrix"]["unresolved_count"] == 0
    assert gates["reality_failures"]["count"] == 0
    assert gates["xpass"]["count"] == 0
    assert gates["unexpected_contract_failure"]["count"] == 0
    assert gates["product_source_mutation"]["count"] == 0
    assert gates["evidence_manifest"]["status"] == "VALIDATED_BY_WORKFLOW"


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Committed child effects have no observed aggregate revision and no exactly-one "
        "CanonicalTransitionRecord recording those effects"
    ),
)
def test_committed_aggregate_revision_with_child_effects_has_one_canonical_record(
    final_summary_report: dict[str, Any],
):
    aggregate = final_summary_report["aggregate_boundary_summary"]
    assert aggregate["aggregate_revision_contract_satisfied"] is True

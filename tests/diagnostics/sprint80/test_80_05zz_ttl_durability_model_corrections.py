from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from collections import Counter
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

import pytest

EXPECTED_DURABILITY_COUNTS = {
    "audit_history": 2,
    "durable_truth_candidate": 1,
    "ephemeral_coordination": 9,
    "reconstructable_projection": 2,
    "retained_execution_truth": 4,
    "transport_retention": 4,
}

EXPECTED_BLOCKING = {
    "run_state",
    "run_result",
    "dag_task_state",
    "dag_task_meta",
    "task_output",
    "operator_audit_stream",
}

EVIDENCE_INSPECTION_FAMILIES = {
    "run_state",
    "run_result",
    "dag_task_state",
    "dag_task_meta",
    "task_output",
    "event_store_list",
}


def _load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load diagnostic module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _row(report: dict[str, Any], family: str) -> dict[str, Any]:
    return next(item for item in report["observations"] if item["state_family"] == family)


def _pttl_values(row: dict[str, Any]) -> list[int]:
    return [int(value) for _stage, value in row["pttl_by_transition"]]


def _finding_status(row: dict[str, Any]) -> str:
    if row["blocking_finding"]:
        return "BLOCKING_GAP"
    if row["classification_confidence"] == "source_verified" and not row["runtime_key_observed"]:
        return "SOURCE_VERIFIED_ONLY"
    if row["durability_class"] == "ephemeral_coordination":
        if row["reconstructable"] is True and row["recovery_path_executed"]:
            return "EXPECTED_EPHEMERAL"
        if row["reconstructable"] is True:
            return "NOT_TESTED"
    if row["reconstructable"] is None:
        return "UNRESOLVED"
    if not row["reconstruction_assessed"]:
        return "NOT_TESTED"
    return "PASS"


def _apply_model_corrections(report: dict[str, Any], model: ModuleType) -> dict[str, Any]:
    operator = _row(report, "operator_audit_stream")
    operator["durability_class"] = "audit_history"
    operator["classification_confidence"] = "runtime_observed"
    operator["runtime_key_observed"] = True
    operator["reconstruction_assessed"] = True
    operator["recovery_path_executed"] = False
    operator["reconstruction_tested"] = False
    operator["assessment_basis"] = [
        "dedicated operator audit stream observed",
        "retention uses approximate MAXLEN",
        "no external durable archive observed",
    ]

    assessment_basis = {
        "run_state": [
            "run state expired under accelerated TTL",
            "no revision-bound reconstruction source observed",
        ],
        "run_result": [
            "run result expired under accelerated TTL",
            "surviving result surfaces do not reproduce exact payload and provenance",
        ],
        "dag_task_state": [
            "terminal task state expired",
            "surviving EventStore history is not revision-bound or transaction-coupled",
        ],
        "dag_task_meta": [
            "task metadata expired",
            "ownership, fencing and completion metadata are not exactly reconstructable from surviving history",
        ],
        "task_output": [
            "inline output expired",
            "external payload storage is not a general reconstruction source for inline output",
        ],
        "event_store_list": [
            "history survived task expiry",
            "history lacks exact revision, output and provenance reconstruction semantics",
        ],
    }

    for row in report["observations"]:
        values = _pttl_values(row)
        runtime_observed = any(value != -2 for value in values)
        row["runtime_key_observed"] = runtime_observed

        if row["state_family"] in EVIDENCE_INSPECTION_FAMILIES:
            row["classification_confidence"] = "evidence_inspection"
            row["reconstruction_assessed"] = True
            row["recovery_path_executed"] = False
            row["reconstruction_tested"] = False
            row["assessment_basis"] = assessment_basis[row["state_family"]]
        elif row["classification_confidence"] in {"", "observed", "unspecified", None}:
            row["classification_confidence"] = (
                "runtime_observed" if runtime_observed else "source_verified"
            )

        row.setdefault("reconstruction_assessed", row.get("reconstructable") is not None)
        row.setdefault("recovery_path_executed", bool(row.get("reconstruction_tested")))
        row.setdefault("assessment_basis", [])

        row["blocking_finding"] = model.classify_blocking(
            authority_class=row["authority_class"],
            durability_class=row["durability_class"],
            retention_mechanism=row["retention_mechanism"],
            pttl_by_transition=row["pttl_by_transition"],
            recovery_source=row["recovery_source"],
            reconstructable=row["reconstructable"],
            external_durable_archive=False,
        )
        row["finding_status"] = _finding_status(row)

    report["schema_version"] = 2
    report["blocking_findings"] = sorted(
        row["state_family"] for row in report["observations"] if row["blocking_finding"]
    )
    report["blocking_finding_count"] = len(report["blocking_findings"])
    report["durability_class_counts"] = dict(
        sorted(Counter(row["durability_class"] for row in report["observations"]).items())
    )
    report["retention_mechanism_counts"] = dict(
        sorted(Counter(row["retention_mechanism"] for row in report["observations"]).items())
    )
    report["finding_status_counts"] = dict(
        sorted(Counter(row["finding_status"] for row in report["observations"]).items())
    )
    return report


async def _build_corrected_report(
    *,
    redis_url: str,
    repo_root: Path,
    model: ModuleType,
) -> dict[str, Any]:
    base = _load_module(
        repo_root / "tests/diagnostics/sprint80/test_80_05_ttl_durability.py",
        "sprint80_ttl_model_base",
    )
    requeue = _load_module(
        repo_root / "tests/diagnostics/sprint80/test_80_05z_ttl_durability_corrections.py",
        "sprint80_ttl_model_requeue",
    )

    await base._build_report(redis_url=redis_url, repo_root=repo_root, model=model)
    await requeue._run_isolated_requeue_probe(redis_url=redis_url, repo_root=repo_root)

    report_path = repo_root / "local_out/sprint80/ttl_durability.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    corrected = _apply_model_corrections(report, model)
    model.write_json(report_path, corrected)
    return corrected


@pytest.fixture(scope="module")
def corrected_ttl_durability_report(
    repo_root: Path,
    sprint80_module_loader: Callable[[str], ModuleType],
) -> dict[str, Any]:
    redis_url = os.getenv("SPRINT80_REDIS_URL", "")
    if not redis_url:
        pytest.skip("SPRINT80_REDIS_URL is required for TTL model correction")
    return asyncio.run(
        _build_corrected_report(
            redis_url=redis_url,
            repo_root=repo_root,
            model=sprint80_module_loader("ttl_durability"),
        )
    )


@pytest.mark.sprint80_reality
def test_operator_audit_is_classified_as_audit_history(
    corrected_ttl_durability_report: dict[str, Any],
):
    audit = _row(corrected_ttl_durability_report, "operator_audit_stream")
    assert audit["authority_class"] == "audit"
    assert audit["durability_class"] == "audit_history"
    assert audit["retention_mechanism"] == "stream_maxlen_approximate"


@pytest.mark.sprint80_reality
def test_approximate_audit_history_without_archive_is_blocking(
    corrected_ttl_durability_report: dict[str, Any],
):
    audit = _row(corrected_ttl_durability_report, "operator_audit_stream")
    assert audit["blocking_finding"] is True
    assert audit["finding_status"] == "BLOCKING_GAP"


@pytest.mark.sprint80_reality
def test_reconstruction_tested_requires_executed_recovery_path(
    corrected_ttl_durability_report: dict[str, Any],
):
    for family in (
        "run_state",
        "run_result",
        "dag_task_state",
        "dag_task_meta",
        "task_output",
    ):
        row = _row(corrected_ttl_durability_report, family)
        assert row["reconstructable"] is False
        assert row["reconstruction_assessed"] is True
        assert row["recovery_path_executed"] is False
        assert row["reconstruction_tested"] is False
        assert row["classification_confidence"] == "evidence_inspection"
        assert row["assessment_basis"]


@pytest.mark.sprint80_reality
def test_completion_stream_retention_is_runtime_observed_or_source_verified(
    corrected_ttl_durability_report: dict[str, Any],
):
    completion = _row(corrected_ttl_durability_report, "completion_stream")
    values = {stage: int(value) for stage, value in completion["pttl_by_transition"]}
    assert values["requeue_after"] == -1
    assert completion["runtime_key_observed"] is True
    assert completion["classification_confidence"] == "runtime_observed"


@pytest.mark.sprint80_reality
def test_classification_confidence_is_never_implicitly_observed(
    corrected_ttl_durability_report: dict[str, Any],
):
    allowed = {"runtime_observed", "source_verified", "evidence_inspection"}
    for row in corrected_ttl_durability_report["observations"]:
        assert row["classification_confidence"] in allowed
        assert row["classification_confidence"] != "observed"


@pytest.mark.sprint80_reality
def test_finding_status_separates_blocking_unresolved_and_not_tested(
    corrected_ttl_durability_report: dict[str, Any],
):
    statuses = {
        row["state_family"]: row["finding_status"]
        for row in corrected_ttl_durability_report["observations"]
    }
    assert statuses["run_state"] == "BLOCKING_GAP"
    assert statuses["operator_audit_stream"] == "BLOCKING_GAP"
    assert statuses["ready_queue"] == "UNRESOLVED"
    assert statuses["remaining_dependencies"] == "NOT_TESTED"
    assert statuses["worker_reservation"] == "EXPECTED_EPHEMERAL"


@pytest.mark.sprint80_reality
def test_corrected_durability_counts_and_blocking_families(
    corrected_ttl_durability_report: dict[str, Any],
):
    assert corrected_ttl_durability_report["durability_class_counts"] == EXPECTED_DURABILITY_COUNTS
    assert set(corrected_ttl_durability_report["blocking_findings"]) == EXPECTED_BLOCKING


@pytest.mark.sprint80_contract
@pytest.mark.xfail(
    strict=True,
    reason="Operator audit history still relies on approximate MAXLEN without an external durable archive",
)
def test_corrected_audit_history_has_durable_non_trimming_archive(
    corrected_ttl_durability_report: dict[str, Any],
):
    audit = _row(corrected_ttl_durability_report, "operator_audit_stream")
    assert audit["retention_mechanism"] != "stream_maxlen_approximate"
    assert audit["reconstructable"] is True
    assert audit["recovery_path_executed"] is True

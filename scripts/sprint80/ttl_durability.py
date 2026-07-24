from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

AUDIT_BASE_COMMIT = "2eca85b2d9b115ad4588641b020e98efdd570a2d"

DURABILITY_CLASSES = {
    "durable_truth_candidate",
    "retained_execution_truth",
    "ephemeral_coordination",
    "reconstructable_projection",
    "audit_history",
    "transport_retention",
    "unknown",
}

RETENTION_MECHANISMS = {
    "expire_ttl",
    "stream_maxlen_exact",
    "stream_maxlen_approximate",
    "list_retention_unbounded",
    "no_expiry",
    "manual_cleanup",
    "external_artifact_store",
    "unknown",
}

CLASSIFICATION_CONFIDENCES = {
    "runtime_observed",
    "source_verified",
    "evidence_inspection",
    "unknown",
    "unspecified",
}

FINDING_STATUSES = {
    "PASS",
    "BLOCKING_GAP",
    "UNRESOLVED",
    "EXPECTED_EPHEMERAL",
    "SOURCE_VERIFIED_ONLY",
    "NOT_TESTED",
}

PTTL_ABSENT = -2
PTTL_NO_EXPIRY = -1


@dataclass(frozen=True)
class DurabilityObservation:
    state_family: str
    key_pattern: str
    authority_class: str
    durability_class: str
    retention_mechanism: str
    pttl_by_transition: tuple[tuple[str, int], ...]
    ttl_refreshed_by: tuple[str, ...]
    ttl_removed_by: tuple[str, ...]
    expiry_observed: bool
    history_survives_expiry: bool
    recovery_source: str
    reconstructable: bool | None
    reconstruction_tested: bool
    classification_confidence: str
    blocking_finding: bool
    reconstruction_assessed: bool = False
    recovery_path_executed: bool = False
    assessment_basis: tuple[str, ...] = ()
    runtime_key_observed: bool = False
    finding_status: str = "NOT_TESTED"
    notes: tuple[str, ...] = ()


def pttl_state(value: int) -> str:
    if value == PTTL_ABSENT:
        return "absent"
    if value == PTTL_NO_EXPIRY:
        return "present_without_expiry"
    if value >= 0:
        return "present_with_expiry"
    return "invalid"


def has_ttl(values: Mapping[str, int] | Iterable[tuple[str, int]]) -> bool:
    pairs = dict(values)
    return any(value >= 0 for value in pairs.values())


def classify_blocking(
    *,
    authority_class: str,
    durability_class: str,
    retention_mechanism: str,
    pttl_by_transition: Mapping[str, int] | Iterable[tuple[str, int]],
    recovery_source: str,
    reconstructable: bool | None,
    external_durable_archive: bool = False,
) -> bool:
    ttl_present = has_ttl(pttl_by_transition)
    recovery_unknown = recovery_source in {"", "unknown", "none"}

    if authority_class == "canonical_candidate" and ttl_present and recovery_unknown:
        return True

    if (
        durability_class == "audit_history"
        and retention_mechanism == "stream_maxlen_approximate"
        and not external_durable_archive
    ):
        return True

    if durability_class == "retained_execution_truth" and ttl_present and reconstructable is False:
        return True

    return False


def classify_finding_status(
    *,
    blocking_finding: bool,
    authority_class: str,
    durability_class: str,
    reconstructable: bool | None,
    reconstruction_assessed: bool,
    recovery_path_executed: bool,
    classification_confidence: str,
    runtime_key_observed: bool,
) -> str:
    if blocking_finding:
        return "BLOCKING_GAP"
    if classification_confidence == "source_verified" and not runtime_key_observed:
        return "SOURCE_VERIFIED_ONLY"
    if durability_class == "ephemeral_coordination":
        if reconstructable is True and recovery_path_executed:
            return "EXPECTED_EPHEMERAL"
        if reconstructable is True:
            return "NOT_TESTED"
    if reconstructable is None:
        return "UNRESOLVED"
    if not reconstruction_assessed:
        return "NOT_TESTED"
    return "PASS"


def make_observation(
    *,
    state_family: str,
    key_pattern: str,
    authority_class: str,
    durability_class: str,
    retention_mechanism: str,
    pttl_by_transition: Mapping[str, int],
    ttl_refreshed_by: Iterable[str] = (),
    ttl_removed_by: Iterable[str] = (),
    expiry_observed: bool = False,
    history_survives_expiry: bool = False,
    recovery_source: str = "unknown",
    reconstructable: bool | None = None,
    reconstruction_tested: bool = False,
    reconstruction_assessed: bool | None = None,
    recovery_path_executed: bool | None = None,
    assessment_basis: Iterable[str] = (),
    runtime_key_observed: bool | None = None,
    classification_confidence: str = "unspecified",
    finding_status: str | None = None,
    external_durable_archive: bool = False,
    notes: Iterable[str] = (),
) -> DurabilityObservation:
    if durability_class not in DURABILITY_CLASSES:
        raise ValueError(f"invalid durability_class: {durability_class}")
    if retention_mechanism not in RETENTION_MECHANISMS:
        raise ValueError(f"invalid retention_mechanism: {retention_mechanism}")
    if classification_confidence not in CLASSIFICATION_CONFIDENCES:
        raise ValueError(f"invalid classification_confidence: {classification_confidence}")

    ordered_pttl = tuple((name, int(value)) for name, value in pttl_by_transition.items())
    observed = (
        any(value != PTTL_ABSENT for _, value in ordered_pttl)
        if runtime_key_observed is None
        else bool(runtime_key_observed)
    )
    assessed = reconstructable is not None if reconstruction_assessed is None else bool(reconstruction_assessed)
    path_executed = bool(reconstruction_tested) if recovery_path_executed is None else bool(recovery_path_executed)
    blocking = classify_blocking(
        authority_class=authority_class,
        durability_class=durability_class,
        retention_mechanism=retention_mechanism,
        pttl_by_transition=ordered_pttl,
        recovery_source=recovery_source,
        reconstructable=reconstructable,
        external_durable_archive=external_durable_archive,
    )
    status = finding_status or classify_finding_status(
        blocking_finding=blocking,
        authority_class=authority_class,
        durability_class=durability_class,
        reconstructable=reconstructable,
        reconstruction_assessed=assessed,
        recovery_path_executed=path_executed,
        classification_confidence=classification_confidence,
        runtime_key_observed=observed,
    )
    if status not in FINDING_STATUSES:
        raise ValueError(f"invalid finding_status: {status}")

    return DurabilityObservation(
        state_family=state_family,
        key_pattern=key_pattern,
        authority_class=authority_class,
        durability_class=durability_class,
        retention_mechanism=retention_mechanism,
        pttl_by_transition=ordered_pttl,
        ttl_refreshed_by=tuple(ttl_refreshed_by),
        ttl_removed_by=tuple(ttl_removed_by),
        expiry_observed=bool(expiry_observed),
        history_survives_expiry=bool(history_survives_expiry),
        recovery_source=recovery_source,
        reconstructable=reconstructable,
        reconstruction_tested=bool(reconstruction_tested),
        classification_confidence=classification_confidence,
        blocking_finding=blocking,
        reconstruction_assessed=assessed,
        recovery_path_executed=path_executed,
        assessment_basis=tuple(assessment_basis),
        runtime_key_observed=observed,
        finding_status=status,
        notes=tuple(notes),
    )


def render_report(observations: Iterable[DurabilityObservation], *, lifecycle: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(observations)
    blocking = [row.state_family for row in rows if row.blocking_finding]
    durability_counts = Counter(row.durability_class for row in rows)
    retention_counts = Counter(row.retention_mechanism for row in rows)
    finding_status_counts = Counter(row.finding_status for row in rows)
    return {
        "schema_version": 2,
        "audit_base_commit": AUDIT_BASE_COMMIT,
        "observation_count": len(rows),
        "lifecycle": dict(lifecycle),
        "observations": [asdict(row) for row in rows],
        "blocking_findings": sorted(blocking),
        "blocking_finding_count": len(blocking),
        "durability_class_counts": dict(sorted(durability_counts.items())),
        "retention_mechanism_counts": dict(sorted(retention_counts.items())),
        "finding_status_counts": dict(sorted(finding_status_counts.items())),
    }


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

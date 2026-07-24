from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

AUDIT_BASE_COMMIT = "2eca85b2d9b115ad4588641b020e98efdd570a2d"

FINDING_STATUSES = {
    "PASS",
    "BLOCKING_GAP",
    "EXPECTED_NOOP",
    "UNRESOLVED",
}

BOUNDARY_EFFECTS = {
    "CHILD_UNLOCKED",
    "DEPENDENCY_DECREMENTED",
    "NO_CHILD_EFFECT",
    "CHILD_STRANDED",
    "FAIL_OPEN_UNLOCK",
}


@dataclass(frozen=True)
class AggregateBoundaryObservation:
    scenario: str
    parent_state_before: str
    parent_state_after: str
    parent_terminal_state_requested: str
    completion_status: str
    completion_committed: bool
    unlocked_count: int
    child_state_before: str
    child_state_after: str
    child_remaining_before: int | None
    child_remaining_after: int | None
    child_ready_emitted_before: bool
    child_ready_emitted_after: bool
    ready_queue_member_before: bool
    ready_queue_member_after: bool
    boundary_effect: str
    mutation_scope: str
    child_transition_source: str
    aggregate_revision_observed: bool
    canonical_transition_record_observed: bool
    finding_status: str
    notes: tuple[str, ...] = ()


def make_observation(
    *,
    scenario: str,
    parent_state_before: str,
    parent_state_after: str,
    parent_terminal_state_requested: str,
    completion_status: str,
    completion_committed: bool,
    unlocked_count: int,
    child_state_before: str,
    child_state_after: str,
    child_remaining_before: int | None,
    child_remaining_after: int | None,
    child_ready_emitted_before: bool,
    child_ready_emitted_after: bool,
    ready_queue_member_before: bool,
    ready_queue_member_after: bool,
    boundary_effect: str,
    finding_status: str,
    mutation_scope: str = "parent_completion_lua_mutates_child_keys",
    child_transition_source: str = "task_complete.lua",
    aggregate_revision_observed: bool = False,
    canonical_transition_record_observed: bool = False,
    notes: Iterable[str] = (),
) -> AggregateBoundaryObservation:
    if boundary_effect not in BOUNDARY_EFFECTS:
        raise ValueError(f"invalid boundary_effect: {boundary_effect}")
    if finding_status not in FINDING_STATUSES:
        raise ValueError(f"invalid finding_status: {finding_status}")
    return AggregateBoundaryObservation(
        scenario=scenario,
        parent_state_before=parent_state_before,
        parent_state_after=parent_state_after,
        parent_terminal_state_requested=parent_terminal_state_requested,
        completion_status=completion_status,
        completion_committed=bool(completion_committed),
        unlocked_count=int(unlocked_count),
        child_state_before=child_state_before,
        child_state_after=child_state_after,
        child_remaining_before=child_remaining_before,
        child_remaining_after=child_remaining_after,
        child_ready_emitted_before=bool(child_ready_emitted_before),
        child_ready_emitted_after=bool(child_ready_emitted_after),
        ready_queue_member_before=bool(ready_queue_member_before),
        ready_queue_member_after=bool(ready_queue_member_after),
        boundary_effect=boundary_effect,
        mutation_scope=mutation_scope,
        child_transition_source=child_transition_source,
        aggregate_revision_observed=bool(aggregate_revision_observed),
        canonical_transition_record_observed=bool(canonical_transition_record_observed),
        finding_status=finding_status,
        notes=tuple(notes),
    )


def render_report(
    observations: Iterable[AggregateBoundaryObservation],
    *,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = list(observations)
    status_counts = Counter(row.finding_status for row in rows)
    effect_counts = Counter(row.boundary_effect for row in rows)
    blocking = sorted(
        row.scenario for row in rows if row.finding_status == "BLOCKING_GAP"
    )
    cross_task_mutation_observed = any(
        row.child_state_before != row.child_state_after
        or row.child_remaining_before != row.child_remaining_after
        or row.child_ready_emitted_before != row.child_ready_emitted_after
        or row.ready_queue_member_before != row.ready_queue_member_after
        for row in rows
    )
    child_authority_record_observed = any(
        row.canonical_transition_record_observed for row in rows
    )
    aggregate_revision_observed = any(row.aggregate_revision_observed for row in rows)

    return {
        "schema_version": 1,
        "audit_base_commit": AUDIT_BASE_COMMIT,
        "observation_count": len(rows),
        "global_result": (
            "CROSS_TASK_MUTATION_WITHOUT_CHILD_AUTHORITY_RECORD"
            if cross_task_mutation_observed
            and not child_authority_record_observed
            and not aggregate_revision_observed
            else "UNRESOLVED"
        ),
        "transaction_boundary": "single_redis_lua_call",
        "cross_task_mutation_observed": cross_task_mutation_observed,
        "child_authority_record_observed": child_authority_record_observed,
        "aggregate_revision_observed": aggregate_revision_observed,
        "blocking_findings": blocking,
        "blocking_finding_count": len(blocking),
        "finding_status_counts": dict(sorted(status_counts.items())),
        "boundary_effect_counts": dict(sorted(effect_counts.items())),
        "observations": [asdict(row) for row in rows],
        "metadata": dict(metadata or {}),
    }


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

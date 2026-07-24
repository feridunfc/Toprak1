from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

AUDIT_BASE_COMMIT = "2eca85b2d9b115ad4588641b020e98efdd570a2d"

TRUTH_SOURCES = {
    "run_state",
    "run_state_and_meta",
    "run_result",
    "running_zset_plus_run_state",
    "task_state",
    "task_state_and_meta",
    "task_running_zset_plus_task_meta_then_task_state",
    "none",
    "mixed",
}

WINNERS = {
    "RUN_WINS",
    "TASK_WINS",
    "INCONSISTENT_BY_CALLER",
    "FAIL_CLOSED",
    "UNRESOLVED",
}

FAIL_MODES = {
    "fail_open",
    "fail_closed",
    "not_applicable",
}


@dataclass(frozen=True)
class TruthContradictionObservation:
    scenario: str
    run_state: str
    task_state: str
    caller: str
    selected_truth_source: str
    returned_status: str
    mutation_attempted: bool
    fail_open_or_closed: str
    deterministic_winner: str
    recovery_action: str
    notes: tuple[str, ...] = ()


def make_observation(
    *,
    scenario: str,
    run_state: str,
    task_state: str,
    caller: str,
    selected_truth_source: str,
    returned_status: str,
    mutation_attempted: bool,
    fail_open_or_closed: str,
    deterministic_winner: str,
    recovery_action: str = "none",
    notes: Iterable[str] = (),
) -> TruthContradictionObservation:
    if selected_truth_source not in TRUTH_SOURCES:
        raise ValueError(f"invalid selected_truth_source: {selected_truth_source}")
    if deterministic_winner not in WINNERS:
        raise ValueError(f"invalid deterministic_winner: {deterministic_winner}")
    if fail_open_or_closed not in FAIL_MODES:
        raise ValueError(f"invalid fail_open_or_closed: {fail_open_or_closed}")
    return TruthContradictionObservation(
        scenario=scenario,
        run_state=run_state,
        task_state=task_state,
        caller=caller,
        selected_truth_source=selected_truth_source,
        returned_status=returned_status,
        mutation_attempted=bool(mutation_attempted),
        fail_open_or_closed=fail_open_or_closed,
        deterministic_winner=deterministic_winner,
        recovery_action=recovery_action,
        notes=tuple(notes),
    )


def render_report(
    observations: Iterable[TruthContradictionObservation],
    *,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows = list(observations)
    source_counts = Counter(row.selected_truth_source for row in rows)
    winner_counts = Counter(row.deterministic_winner for row in rows)
    caller_counts = Counter(row.caller for row in rows)
    fail_mode_counts = Counter(row.fail_open_or_closed for row in rows)

    decisive_winners = {
        row.deterministic_winner
        for row in rows
        if row.deterministic_winner in {"RUN_WINS", "TASK_WINS", "FAIL_CLOSED"}
    }
    global_truth_policy = (
        "INCONSISTENT_BY_CALLER" if len(decisive_winners) > 1 else next(iter(decisive_winners), "UNRESOLVED")
    )

    return {
        "schema_version": 1,
        "audit_base_commit": AUDIT_BASE_COMMIT,
        "observation_count": len(rows),
        "global_truth_policy": global_truth_policy,
        "observations": [asdict(row) for row in rows],
        "selected_truth_source_counts": dict(sorted(source_counts.items())),
        "deterministic_winner_counts": dict(sorted(winner_counts.items())),
        "caller_counts": dict(sorted(caller_counts.items())),
        "fail_mode_counts": dict(sorted(fail_mode_counts.items())),
        "metadata": dict(metadata or {}),
    }


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

from .model import CriterionResult, LoopContract, Outcome, canonical_hash, thaw


def verify(
    contract: LoopContract,
    rows: list[CriterionResult],
    *,
    loop_id: str,
    attempt_id: str,
    task_id: str,
    run_id: str,
    input_hash: str,
    now_ms: int,
    generator_principal: str,
) -> tuple[dict[str, Outcome], tuple[str, ...], str]:
    outcomes: dict[str, Outcome] = {}
    reasons: list[str] = []
    normalized_rows: list[dict] = []

    for row in rows:
        if row.criterion_id in outcomes:
            outcomes[row.criterion_id] = Outcome.UNKNOWN
            reasons.append(f"duplicate:{row.criterion_id}")
            continue

        valid = all([
            row.loop_id == loop_id,
            row.attempt_id == attempt_id,
            row.task_id == task_id,
            row.run_id == run_id,
            row.input_hash == input_hash,
            bool(row.evidence_refs),
            row.produced_at_ms is not None,
            row.produced_at_ms is not None and row.produced_at_ms <= now_ms + 30_000,
            row.valid_until_ms is None or (
                row.produced_at_ms is not None and row.valid_until_ms >= row.produced_at_ms
            ),
            row.evaluator_principal != generator_principal,
        ])
        outcomes[row.criterion_id] = row.outcome if valid else Outcome.UNKNOWN
        if not valid:
            reasons.append(f"invalid_provenance:{row.criterion_id}")
        normalized_rows.append(thaw(row))

    for criterion_id in contract.required_criteria:
        if criterion_id not in outcomes:
            outcomes[criterion_id] = Outcome.UNKNOWN
            reasons.append(f"missing:{criterion_id}")

    for criterion_id in set(outcomes) - set(contract.required_criteria):
        reasons.append(f"unknown:{criterion_id}")

    evidence_set_hash = canonical_hash(sorted(normalized_rows, key=lambda item: item["criterion_id"]))
    return outcomes, tuple(sorted(reasons)), evidence_set_hash

from .model import CriterionResult, Outcome, LoopContract

def verify(contract: LoopContract, rows: list[CriterionResult], *, loop_id: str, attempt_id: str, task_id: str, run_id: str, input_hash: str, now_ms: int, generator_principal: str) -> tuple[dict[str, Outcome], tuple[str, ...]]:
    outcomes: dict[str, Outcome] = {}
    reasons: list[str] = []
    for row in rows:
        if row.criterion_id in outcomes:
            outcomes[row.criterion_id] = Outcome.UNKNOWN
            reasons.append(f'duplicate:{row.criterion_id}')
            continue
        valid = all([
            row.loop_id == loop_id, row.attempt_id == attempt_id, row.task_id == task_id,
            row.run_id == run_id, row.input_hash == input_hash, bool(row.evaluator_principal),
            bool(row.evaluator_version), bool(row.independence_scope), bool(row.evidence_refs),
            len(row.evidence_hash) == 64, bool(row.source_transition_id), row.source_task_revision >= 0,
            bool(row.canonical_claim_operation_id), bool(row.claim_epoch_or_fence), row.execution_generation >= 0,
            row.produced_at_ms is not None, row.produced_at_ms is not None and row.produced_at_ms <= now_ms + 30000,
            row.valid_until_ms is None or (row.produced_at_ms is not None and row.valid_until_ms >= row.produced_at_ms),
            row.evaluator_principal != generator_principal,
        ])
        outcomes[row.criterion_id] = row.outcome if valid else Outcome.UNKNOWN
        if not valid: reasons.append(f'invalid_provenance:{row.criterion_id}')
    for criterion_id in contract.required_criteria:
        if criterion_id not in outcomes:
            outcomes[criterion_id] = Outcome.UNKNOWN
            reasons.append(f'missing:{criterion_id}')
    for criterion_id in set(outcomes) - set(contract.required_criteria):
        reasons.append(f'unknown:{criterion_id}')
    return outcomes, tuple(reasons)

from .model import Recommendation, Outcome, LoopContract

def recommend(contract: LoopContract, results: dict[str, Outcome], defect_type: str | None = None) -> tuple[Recommendation, str]:
    values = [results.get(criterion_id, Outcome.UNKNOWN) for criterion_id in contract.required_criteria]
    if any(value is Outcome.UNKNOWN for value in values): return Recommendation.BLOCK, 'EVIDENCE_UNKNOWN'
    if all(value is Outcome.PASS for value in values): return Recommendation.ACCEPT, 'ALL_REQUIRED_CRITERIA_PASS'
    recommendation = {
        'transient': Recommendation.RETRY_RECOMMENDED,
        'artifact_defect': Recommendation.REWORK_RECOMMENDED,
        'wrong_plan': Recommendation.REPLAN_RECOMMENDED,
        'capability_mismatch': Recommendation.ESCALATE,
        'missing_input': Recommendation.BLOCK,
    }.get(defect_type, Recommendation.UNKNOWN)
    return recommendation, 'CRITERIA_FAILED'

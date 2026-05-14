
import pytest
from hfa_control.scheduler_scoring import SchedulerScoring, ScoringCandidate

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_combined_scoring_prefers_best_overall():
    candidates = [
        ScoringCandidate("t1", "w1", "task1", vruntime=20, inflight=2, worker_load=5, capacity=1),
        ScoringCandidate("t2", "w2", "task2", vruntime=10, inflight=1, worker_load=3, capacity=2),
        ScoringCandidate("t3", "w3", "task3", vruntime=10, inflight=1, worker_load=1, capacity=2),
    ]

    best = SchedulerScoring.choose_best(candidates)

    assert best.worker_id == "w3"
    assert best.tenant_id == "t3"


@pytest.mark.integration
async def test_empty_candidates_returns_none():
    assert SchedulerScoring.choose_best([]) is None


from hfa_control.scheduler_scoring import SchedulerScoring, ScoringCandidate


def test_lower_vruntime_wins():
    c1 = ScoringCandidate("t1", "w1", "task1", vruntime=10, inflight=0, worker_load=0, capacity=1)
    c2 = ScoringCandidate("t2", "w2", "task2", vruntime=5, inflight=0, worker_load=0, capacity=1)

    best = SchedulerScoring.choose_best([c1, c2])
    assert best.tenant_id == "t2"


def test_inflight_breaks_tie():
    c1 = ScoringCandidate("t1", "w1", "task1", vruntime=10, inflight=3, worker_load=0, capacity=1)
    c2 = ScoringCandidate("t2", "w2", "task2", vruntime=10, inflight=1, worker_load=0, capacity=1)

    best = SchedulerScoring.choose_best([c1, c2])
    assert best.tenant_id == "t2"


def test_worker_load_breaks_tie():
    c1 = ScoringCandidate("t1", "w1", "task1", vruntime=10, inflight=1, worker_load=5, capacity=1)
    c2 = ScoringCandidate("t2", "w2", "task2", vruntime=10, inflight=1, worker_load=1, capacity=1)

    best = SchedulerScoring.choose_best([c1, c2])
    assert best.worker_id == "w2"


def test_capacity_preferred():
    c1 = ScoringCandidate("t1", "w1", "task1", vruntime=10, inflight=1, worker_load=1, capacity=1)
    c2 = ScoringCandidate("t2", "w2", "task2", vruntime=10, inflight=1, worker_load=1, capacity=5)

    best = SchedulerScoring.choose_best([c1, c2])
    assert best.worker_id == "w2"

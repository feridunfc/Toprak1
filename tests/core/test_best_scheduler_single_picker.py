
from types import SimpleNamespace

from hfa_control.scheduler import Scheduler


class Score:
    def __init__(self, value):
        self.value = value

    def as_sort_key(self):
        return (self.value,)


def test_single_picker_ignores_bad_candidates():
    sched = Scheduler()
    candidates = [
        SimpleNamespace(schedulable=False, worker_id="w0", decision_score=Score(0)),
        SimpleNamespace(schedulable=True, worker_id="", decision_score=Score(1)),
        SimpleNamespace(schedulable=True, worker_id="w2", decision_score=None),
        SimpleNamespace(schedulable=True, worker_id="w3", decision_score=Score(3)),
        SimpleNamespace(schedulable=True, worker_id="w1", decision_score=Score(1)),
    ]
    best = sched._pick_best_worker(candidates)
    assert best.worker_id == "w1"

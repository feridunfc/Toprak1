
from pathlib import Path


def test_scheduler_has_single_pick_best_worker_definition():
    text = Path("hfa-control/src/hfa_control/scheduler.py").read_text(encoding="utf-8")
    assert text.count("def _pick_best_worker(") == 1

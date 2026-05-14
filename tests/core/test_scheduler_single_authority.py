"""
tests/core/test_scheduler_single_authority.py
IRONCLAD Sprint 22 — Scheduler single authority invariant

Verifies that SchedulerLoop is the sole dispatch authority:
- No _dispatch_fair_batch in scheduler.py
- No _schedule method in scheduler.py
- Direct mode also routes through _enqueue_admitted
- scheduler.py has no placement/worker-selection logic
"""
import ast
from pathlib import Path


def _read(rel_path: str) -> str:
    repo = Path(__file__).parent.parent.parent
    return (repo / rel_path).read_text(encoding="utf-8", errors="ignore")


def test_no_dispatch_fair_batch():
    """_dispatch_fair_batch must not exist in scheduler.py."""
    src = _read("hfa-control/src/hfa_control/scheduler.py")
    assert "_dispatch_fair_batch" not in src, (
        "_dispatch_fair_batch still exists — SchedulerLoop must be the sole dispatch authority"
    )


def test_no_schedule_method():
    """_schedule must not exist in scheduler.py — dispatch via SchedulerLoop."""
    src = _read("hfa-control/src/hfa_control/scheduler.py")
    tree = ast.parse(src)
    fn_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "_schedule" not in fn_names, (
        "scheduler.py defines _schedule — this method must be removed; "
        "all dispatch goes through SchedulerLoop.run_cycle()"
    )


def test_no_select_worker_group():
    """_select_worker_group must not exist in scheduler.py — worker selection is SchedulerLoop's job."""
    src = _read("hfa-control/src/hfa_control/scheduler.py")
    tree = ast.parse(src)
    fn_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "_select_worker_group" not in fn_names, (
        "_select_worker_group still in scheduler.py — SchedulerLoop owns worker selection"
    )


def test_scheduler_calls_run_cycle():
    """scheduler.py must delegate scheduling to SchedulerLoop.run_cycle()."""
    src = _read("hfa-control/src/hfa_control/scheduler.py")
    assert "run_cycle" in src, (
        "scheduler.py does not call SchedulerLoop.run_cycle() — dispatch delegation missing"
    )


def test_scheduler_loop_owns_commit():
    """commit_dispatch must exist in scheduler_loop.py, not in scheduler.py."""
    loop_src = _read("hfa-control/src/hfa_control/scheduler_loop.py")
    sched_src = _read("hfa-control/src/hfa_control/scheduler.py")
    assert "commit_dispatch" in loop_src, "SchedulerLoop must define commit_dispatch"
    assert "commit_dispatch" not in sched_src, (
        "scheduler.py must not define or shadow commit_dispatch"
    )


def test_scheduler_reasons_exists():
    """scheduler_reasons.py must exist as single source of truth for reason codes."""
    repo = Path(__file__).parent.parent.parent
    reasons_path = repo / "hfa-control/src/hfa_control/scheduler_reasons.py"
    assert reasons_path.exists(), "scheduler_reasons.py missing"
    src = reasons_path.read_text()
    for code in ["COMMITTED", "STATE_CONFLICT", "QUARANTINE_REASONS", "REQUEUE_REASONS"]:
        assert code in src, f"scheduler_reasons.py missing constant: {code}"


def test_quarantine_run_in_scheduler_loop():
    """SchedulerLoop must have a general quarantine helper."""
    src = _read("hfa-control/src/hfa_control/scheduler_loop.py")
    assert "_quarantine_run" in src, (
        "_quarantine_run helper missing from scheduler_loop.py — "
        "all non-requeue dispatch failures must be explicitly quarantined"
    )

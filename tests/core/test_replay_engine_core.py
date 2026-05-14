
from hfa_control.replay_engine import ReplayEngine


def test_replay_happy_path_completed():
    events = [
        {"event_type": "TASK_ADMITTED", "timestamp_ms": 1},
        {"event_type": "TASK_SCHEDULED", "timestamp_ms": 2, "worker_id": "w1"},
        {"event_type": "TASK_CLAIMED", "timestamp_ms": 3, "worker_id": "w1"},
        {"event_type": "TASK_COMPLETED", "timestamp_ms": 4, "worker_id": "w1"},
    ]
    summary = ReplayEngine.rebuild_state("run-1", events)
    assert summary.final_state == "completed"
    assert summary.attempts == 1
    assert summary.claim_count == 1
    assert summary.completed_by == "w1"
    assert summary.history_length == 4


def test_replay_requeue_path_counts_attempts_correctly():
    events = [
        {"event_type": "TASK_ADMITTED", "timestamp_ms": 1},
        {"event_type": "TASK_SCHEDULED", "timestamp_ms": 2, "worker_id": "w1"},
        {"event_type": "TASK_CLAIMED", "timestamp_ms": 3, "worker_id": "w1"},
        {"event_type": "TASK_REQUEUED", "timestamp_ms": 4, "worker_id": "w1"},
        {"event_type": "TASK_SCHEDULED", "timestamp_ms": 5, "worker_id": "w2"},
        {"event_type": "TASK_CLAIMED", "timestamp_ms": 6, "worker_id": "w2"},
        {"event_type": "TASK_COMPLETED", "timestamp_ms": 7, "worker_id": "w2"},
    ]
    summary = ReplayEngine.rebuild_state("run-2", events)
    assert summary.final_state == "completed"
    assert summary.attempts == 2
    assert summary.requeue_count == 1
    assert summary.claimed_workers == ["w1", "w2"]
    assert summary.completed_by == "w2"


def test_duplicate_claim_same_worker_does_not_inflate_attempts():
    events = [
        {"event_type": "TASK_ADMITTED", "timestamp_ms": 1},
        {"event_type": "TASK_CLAIMED", "timestamp_ms": 2, "worker_id": "w1"},
        {"event_type": "TASK_CLAIMED", "timestamp_ms": 3, "worker_id": "w1"},
    ]
    summary = ReplayEngine.rebuild_state("run-3", events)
    assert summary.attempts == 1
    assert summary.claim_count == 2
    assert summary.claimed_workers == ["w1"]


def test_unknown_event_is_recorded_not_crashing():
    events = [
        {"event_type": "TASK_ADMITTED", "timestamp_ms": 1},
        {"event_type": "TASK_TELEPORTED", "timestamp_ms": 2},
    ]
    summary = ReplayEngine.rebuild_state("run-4", events)
    assert summary.final_state == "admitted"
    assert summary.unknown_events == ["TASK_TELEPORTED"]


def test_strict_mismatch_detection():
    events = [
        {"event_type": "TASK_ADMITTED", "timestamp_ms": 1},
        {"event_type": "TASK_FAILED", "timestamp_ms": 2},
    ]
    summary = ReplayEngine.rebuild_state("run-5", events)
    assert ReplayEngine.detect_mismatch(summary, "failed") is True
    assert ReplayEngine.detect_mismatch(summary, "running") is False

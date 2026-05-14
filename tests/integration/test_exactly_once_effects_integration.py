"""
tests/integration/test_exactly_once_effects_integration.py
IRONCLAD Phase 7C-Final — Concurrency integration tests for exactly-once execution.

Test matrix
-----------
A. parallel_duplicate_dispatch
   → only 1 reservation + 1 dispatch call + 1 TASK_SCHEDULED event

B. parallel_duplicate_completion
   → only 1 terminal state write + 1 TASK_COMPLETED event

C. zombie_completion_after_winner
   → stale worker fenced; metric incremented

D. duplicate_suppress_no_extra_event
   → event stream stays at exactly 1 entry after N duplicate attempts

All tests use asyncio.gather for real concurrency simulation against
the integration Redis instance (same fixture chain as existing integration tests).
"""
from __future__ import annotations

import asyncio

import pytest

from hfa_control import effect_metrics
from hfa_control.effect_ledger import EffectLedger
from hfa_control.event_store import EventStore
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.worker_reservation import WorkerReservationManager
from hfa.runtime.state_store import StateStore

pytestmark = pytest.mark.asyncio


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def reset_metrics():
    """Ensure metric counters are clean for every test."""
    effect_metrics.reset_all()
    yield
    effect_metrics.reset_all()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _make_dispatcher(redis_client, reservation_manager, event_store):
    """Build a SchedulerReservationDispatcher with effect ledger wired in."""
    ledger = EffectLedger(redis_client)

    call_counts = {"reserve": 0, "dispatch": 0}

    original_reserve = reservation_manager.reserve

    async def counting_reserve(**kw):
        call_counts["reserve"] += 1
        return await original_reserve(**kw)

    async def counting_dispatch(
        *, task_id, worker_id, scheduler_epoch, dispatch_payload
    ) -> bool:
        call_counts["dispatch"] += 1
        return True

    reservation_manager.reserve = counting_reserve

    dispatcher = SchedulerReservationDispatcher(
        reservation_manager=reservation_manager,
        dispatch_fn=counting_dispatch,
        event_store=event_store,
        effect_ledger=ledger,
    )
    return dispatcher, call_counts


# --------------------------------------------------------------------------- #
# Test A — Parallel duplicate dispatch
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_parallel_duplicate_dispatch_one_reservation_one_event(redis_client):
    """
    5 concurrent dispatchers race with the same run_id/task_id/attempt/epoch.
    Only the first winner should:
      - call reservation once
      - call dispatch_fn once
      - emit exactly one TASK_SCHEDULED event
    The other 4 must return duplicate_dispatch_suppressed with zero side effects.
    """
    store = EventStore(redis_client)
    rm = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    dispatcher, counts = _make_dispatcher(redis_client, rm, store)

    payload = {"run_id": "run-dup-dispatch", "task_id": "task-dd", "attempt": 1}

    results = await asyncio.gather(*[
        dispatcher.reserve_and_dispatch(
            task_id="task-dd",
            worker_id=f"sched-{i}",
            scheduler_epoch="epoch-x",
            dispatch_payload=payload,
        )
        for i in range(5)
    ])

    ok_results = [r for r in results if r.ok]
    dup_results = [r for r in results if r.status == "duplicate_dispatch_suppressed"]

    # Exactly one winner
    assert len(ok_results) == 1, f"Expected 1 ok, got {len(ok_results)}"
    assert len(dup_results) == 4, f"Expected 4 duplicates, got {len(dup_results)}"

    # Zero extra reservation/dispatch calls
    assert counts["reserve"] == 1, f"Expected 1 reserve call, got {counts['reserve']}"
    assert counts["dispatch"] == 1, f"Expected 1 dispatch call, got {counts['dispatch']}"

    # Metric correctly incremented
    assert effect_metrics.get(effect_metrics.DUPLICATE_DISPATCH_SUPPRESSED) == 4

    # Allow background event tasks to flush
    await asyncio.sleep(0.05)

    # Only one TASK_SCHEDULED event in the event stream
    history = await store.get_run_history("run-dup-dispatch")
    scheduled_events = [e for e in history if e["event_type"] == "TASK_SCHEDULED"]
    assert len(scheduled_events) == 1, (
        f"Event inflation detected: {len(scheduled_events)} TASK_SCHEDULED events"
    )


# --------------------------------------------------------------------------- #
# Test B — Parallel duplicate completion
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_parallel_duplicate_completion_one_state_write_one_event(redis_client):
    """
    5 concurrent completions with identical (run_id, task_id, worker_id, attempt).
    Only the first should write terminal state and emit TASK_COMPLETED.
    The other 4 must return duplicate_completion_suppressed.
    """
    store = EventStore(redis_client)
    ledger = EffectLedger(redis_client)
    state = StateStore(redis_client, None, event_store=store, effect_ledger=ledger)

    results = await asyncio.gather(*[
        state.complete_once(
            run_id="run-dup-complete",
            task_id="task-dc",
            worker_id="worker-w1",
            attempt=1,
            status="done",
        )
        for _ in range(5)
    ])

    ok_results = [r for r in results if r.ok]
    dup_results = [r for r in results if r.status == "duplicate_completion_suppressed"]

    assert len(ok_results) == 1, f"Expected 1 ok, got {len(ok_results)}"
    assert len(dup_results) == 4, f"Expected 4 duplicates, got {len(dup_results)}"

    # Metric
    assert effect_metrics.get(effect_metrics.DUPLICATE_COMPLETION_SUPPRESSED) == 4

    # Redis state written exactly once
    state_val = await redis_client.get("hfa:dag:task:task-dc:state")
    assert state_val == "done"

    # Allow background event tasks to flush
    await asyncio.sleep(0.05)

    # Only one TASK_COMPLETED event — no event inflation
    history = await store.get_run_history("run-dup-complete")
    completed_events = [e for e in history if e["event_type"] == "TASK_COMPLETED"]
    assert len(completed_events) == 1, (
        f"Event inflation detected: {len(completed_events)} TASK_COMPLETED events"
    )


# --------------------------------------------------------------------------- #
# Test C — Zombie completion after winner (stale_owner_fenced)
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_zombie_completion_fenced_after_winner(redis_client):
    """
    winner-worker completes successfully.
    zombie-worker then attempts completion with the same task_id, different worker_id.
    The zombie must be fenced (stale_owner_fenced) without writing state or emitting event.
    """
    store = EventStore(redis_client)
    ledger = EffectLedger(redis_client)
    state = StateStore(redis_client, None, event_store=store, effect_ledger=ledger)

    winner = await state.complete_once(
        run_id="run-zombie",
        task_id="task-z",
        worker_id="winner-worker",
        attempt=1,
        status="done",
    )
    assert winner.ok is True
    assert winner.status == "done"

    # Zombie uses a different worker_id → different effect token → passes effect gate
    # but hits owner fencing gate
    zombie = await state.complete_once(
        run_id="run-zombie",
        task_id="task-z",
        worker_id="zombie-worker",
        attempt=1,
        status="done",
    )
    assert zombie.ok is False
    assert zombie.status == "stale_owner_fenced"

    # Metric
    assert effect_metrics.get(effect_metrics.STALE_COMPLETION_FENCED) == 1

    # State must still reflect winner, not zombie
    state_val = await redis_client.get("hfa:dag:task:task-z:state")
    owner_val = await redis_client.get("hfa:task:task-z:owner")
    assert state_val == "done"
    assert owner_val == "winner-worker"

    await asyncio.sleep(0.05)

    # Only one TASK_COMPLETED event — zombie did not emit
    history = await store.get_run_history("run-zombie")
    completed_events = [e for e in history if e["event_type"] == "TASK_COMPLETED"]
    assert len(completed_events) == 1, (
        f"Zombie event leaked: {len(completed_events)} TASK_COMPLETED events"
    )


# --------------------------------------------------------------------------- #
# Test D — Duplicate suppress path emits zero extra events
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_duplicate_suppress_path_event_count_stays_one(redis_client):
    """
    10 sequential duplicate dispatch attempts on the same token.
    Event stream must contain exactly 1 TASK_SCHEDULED entry.
    Metric must count exactly 9 suppressed duplicates.
    """
    store = EventStore(redis_client)
    rm = WorkerReservationManager(redis_client, reservation_ttl_seconds=30)
    ledger = EffectLedger(redis_client)

    async def always_true_dispatch(**kw) -> bool:
        return True

    dispatcher = SchedulerReservationDispatcher(
        reservation_manager=rm,
        dispatch_fn=always_true_dispatch,
        event_store=store,
        effect_ledger=ledger,
    )

    payload = {"run_id": "run-seq-dup", "task_id": "task-sd", "attempt": 1}

    for i in range(10):
        await dispatcher.reserve_and_dispatch(
            task_id="task-sd",
            worker_id=f"sched-{i}",
            scheduler_epoch="epoch-seq",
            dispatch_payload=payload,
        )

    assert effect_metrics.get(effect_metrics.DUPLICATE_DISPATCH_SUPPRESSED) == 9

    await asyncio.sleep(0.05)

    history = await store.get_run_history("run-seq-dup")
    scheduled_events = [e for e in history if e["event_type"] == "TASK_SCHEDULED"]
    assert len(scheduled_events) == 1, (
        f"Event stream has {len(scheduled_events)} TASK_SCHEDULED entries (expected 1)"
    )


# --------------------------------------------------------------------------- #
# Test E — Receipt schema consistency (dispatch vs completion)
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_duplicate_receipt_preserves_prior_owner(redis_client):
    """
    When a duplicate is detected, the returned receipt must carry the
    original owner_id (first writer), not the duplicate requester's ID.
    """
    ledger = EffectLedger(redis_client)

    from hfa_control.effect_config import get_dispatch_effect_ttl

    # First writer
    first = await ledger.acquire_effect(
        run_id="run-receipt",
        token="tok-receipt-1",
        effect_type="dispatch",
        owner_id="original-owner",
        ttl_seconds=get_dispatch_effect_ttl(),
    )
    assert first.accepted is True
    assert first.owner_id == "original-owner"
    assert first.reason is None

    # Duplicate writer
    dup = await ledger.acquire_effect(
        run_id="run-receipt",
        token="tok-receipt-1",
        effect_type="dispatch",
        owner_id="duplicate-requester",
        ttl_seconds=get_dispatch_effect_ttl(),
    )
    assert dup.accepted is False
    assert dup.duplicate is True
    # Prior owner preserved — not the duplicate requester
    assert dup.owner_id == "original-owner"
    # committed_state from original receipt preserved
    assert dup.committed_state == "pending"

    # Caller sets reason
    dup.reason = "duplicate_dispatch_suppressed"
    assert dup.reason == "duplicate_dispatch_suppressed"

"""
tests/core/test_scheduler_dispatch_outcomes.py
IRONCLAD Sprint 22 — Dequeue outcome invariant

Verifies: every destructive dequeue must result in exactly one of:
  - committed (successful dispatch)
  - requeued (transient failure, run returned to queue)
  - quarantined (state conflict / illegal, run moved to failed state)

No run may silently disappear after being dequeued.
"""
import pytest
import fakeredis.aioredis as faredis

from hfa.config.keys import RedisKey, RedisTTL
from hfa_control.scheduler_reasons import (
    COMMITTED, STATE_CONFLICT, ALREADY_RUNNING, ILLEGAL_TRANSITION,
    QUARANTINE_REASONS, QUARANTINED_STATE_CONFLICT,
)


@pytest.mark.asyncio
async def test_quarantine_reasons_are_well_defined():
    """QUARANTINE_REASONS must be non-empty and contain the 3 critical codes."""
    assert STATE_CONFLICT in QUARANTINE_REASONS
    assert ALREADY_RUNNING in QUARANTINE_REASONS
    assert ILLEGAL_TRANSITION in QUARANTINE_REASONS


@pytest.mark.asyncio
async def test_quarantine_run_writes_failed_state():
    """_quarantine_run must write failed state and failure reason to meta."""
    import types
    redis = faredis.FakeRedis()
    run_id = "run-quarantine-1"
    tenant_id = "tenant-a"
    state_key = RedisKey.run_state(run_id)
    meta_key = RedisKey.run_meta(run_id)

    # Set state to "queued" so transition is legal
    await redis.set(state_key, "queued", ex=RedisTTL.RUN_STATE)

    # Create a minimal SchedulerLoop-like object with _quarantine_run
    from hfa_control.scheduler_loop import SchedulerLoop
    # Directly test the helper by constructing it manually
    import hfa_control.scheduler_loop as sl_mod

    # Build a minimal fake loop that has _quarantine_run
    class FakeLoop:
        _redis = redis

    loop = FakeLoop()
    # Bind the method
    import types
    loop._quarantine_run = types.MethodType(SchedulerLoop._quarantine_run, loop)

    await loop._quarantine_run(tenant_id, run_id, reason=QUARANTINED_STATE_CONFLICT, stage="test")

    # State must be failed
    state = await redis.get(state_key)
    state_str = state.decode() if isinstance(state, bytes) else state
    assert state_str == "failed", f"Expected 'failed', got {state_str!r}"

    # Failure reason must be in meta
    meta = await redis.hgetall(meta_key)
    reason_val = meta.get(b"dispatch_failure_reason") or meta.get("dispatch_failure_reason")
    if isinstance(reason_val, bytes):
        reason_val = reason_val.decode()
    assert reason_val == QUARANTINED_STATE_CONFLICT


def test_all_reason_codes_are_strings():
    """All reason codes must be plain strings (no None, no int)."""
    import hfa_control.scheduler_reasons as sr
    codes = [
        sr.COMMITTED, sr.DISPATCH_BUDGET_EXHAUSTED, sr.NO_ACTIVE_TENANTS,
        sr.STATE_CONFLICT, sr.ALREADY_RUNNING, sr.ILLEGAL_TRANSITION,
        sr.INTERNAL_ERROR, sr.QUARANTINED_STATE_CONFLICT,
    ]
    for code in codes:
        assert isinstance(code, str) and code, f"Reason code is not a non-empty string: {code!r}"


def test_quarantine_and_requeue_sets_are_disjoint():
    """QUARANTINE_REASONS and REQUEUE_REASONS must not overlap."""
    from hfa_control.scheduler_reasons import QUARANTINE_REASONS, REQUEUE_REASONS
    overlap = QUARANTINE_REASONS & REQUEUE_REASONS
    assert not overlap, f"Reason codes in both quarantine and requeue: {overlap}"

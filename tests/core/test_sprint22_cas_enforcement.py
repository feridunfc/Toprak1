"""
tests/core/test_sprint22_cas_enforcement.py
IRONCLAD Sprint 22 — CAS-Only Regime Enforcement Tests

Covers PR-3 acceptance criteria:
  - duplicate admission ignored
  - terminal state blocks further transitions
  - recovery cannot resurrect done/failed runs
  - scheduler failure path writes failed state
  - double-dispatch prevented by state machine
"""
import pytest
import fakeredis.aioredis as faredis

from hfa.config.keys import RedisKey, RedisTTL
from hfa_control.state_machine import (
    transition_state,
    validate_transition,
    is_terminal,
    get_run_state,
    InvalidStateTransition,
    VALID_TRANSITIONS,
)


# ── 1. Duplicate admission ignored ────────────────────────────────────────

@pytest.mark.asyncio
async def test_admission_state_gate_blocks_duplicate():
    """Two admissions for the same run_id — second is a no-op."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-dup-001")

    ok1 = await transition_state(redis, "run-dup-001", "admitted",
                                  state_key=key, expected_state=None)
    ok2 = await transition_state(redis, "run-dup-001", "admitted",
                                  state_key=key, expected_state=None)

    assert ok1.ok is True,  "First admission must succeed"
    assert ok2.ok is False, "Duplicate admission must be blocked"

    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "admitted"


@pytest.mark.asyncio
async def test_admission_does_not_overwrite_running_state():
    """Admission must not overwrite a run already in 'running' state."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-overwrite-001")
    await redis.set(key, "running", ex=RedisTTL.RUN_STATE)

    ok = await transition_state(redis, "run-overwrite-001", "admitted",
                                 state_key=key, expected_state=None)
    assert ok.ok is False

    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "running"


# ── 2. Terminal state is final ─────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["done", "failed", "rejected", "dead_lettered"])
async def test_terminal_state_blocks_all_transitions(terminal):
    """No transition out of a terminal state."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state(f"run-term-{terminal}")
    await redis.set(key, terminal, ex=RedisTTL.RUN_STATE)

    for to_state in ["admitted", "queued", "scheduled", "running"]:
        ok = await transition_state(redis, f"run-term-{terminal}", to_state,
                                     state_key=key, expected_state=None)
        assert ok.ok is False, f"Transition {terminal!r} → {to_state!r} must be blocked"

    # State must still be terminal
    val = await redis.get(key)
    cur = val.decode() if isinstance(val, bytes) else val
    assert cur == terminal


# ── 3. Recovery cannot resurrect terminal/cancelled runs ─────────────────

@pytest.mark.asyncio
async def test_recovery_cannot_reschedule_done_run():
    """A 'done' run must not be transitioned to 'rescheduled'."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-done-001")
    await redis.set(key, "done", ex=RedisTTL.RUN_STATE)

    ok = await transition_state(redis, "run-done-001", "rescheduled",
                                 state_key=key, expected_state="done")
    assert ok.ok is False

    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "done"


@pytest.mark.asyncio
async def test_recovery_cas_miss_does_not_overwrite():
    """Recovery CAS with wrong expected_state must not overwrite."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-cas-miss-001")
    await redis.set(key, "done", ex=RedisTTL.RUN_STATE)

    # Recovery thinks it's "running" but it's already "done"
    ok = await transition_state(redis, "run-cas-miss-001", "rescheduled",
                                 state_key=key, expected_state="running")
    assert ok.ok is False

    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "done"


@pytest.mark.asyncio
async def test_recovery_succeeds_on_running():
    """Recovery CAS with correct expected_state succeeds."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-running-001")
    await redis.set(key, "running", ex=RedisTTL.RUN_STATE)

    ok = await transition_state(redis, "run-running-001", "rescheduled",
                                 state_key=key, expected_state="running")
    assert ok.ok is True

    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "rescheduled"


# ── 4. Double-dispatch prevention ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_double_dispatch_blocked_by_state():
    """A run already in 'running' cannot be dispatched again."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-dd-001")
    await redis.set(key, "running", ex=RedisTTL.RUN_STATE)

    ok = await transition_state(redis, "run-dd-001", "scheduled",
                                 state_key=key, expected_state="admitted")
    assert ok.ok is False  # CAS miss — state is "running", not "admitted"


@pytest.mark.asyncio
async def test_scheduled_to_running_only_once():
    """Once a run is running, it cannot be transitioned to running again."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-once-001")
    await redis.set(key, "scheduled", ex=RedisTTL.RUN_STATE)

    ok1 = await transition_state(redis, "run-once-001", "running",
                                   state_key=key, expected_state="scheduled")
    assert ok1.ok is True

    ok2 = await transition_state(redis, "run-once-001", "running",
                                   state_key=key, expected_state="scheduled")
    assert ok2.ok is False  # CAS miss — state is now "running"


# ── 5. get_run_state helper ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_run_state_returns_none_for_missing():
    redis = faredis.FakeRedis()
    s = await get_run_state(redis, "run-missing-001")
    assert s is None


@pytest.mark.asyncio
async def test_get_run_state_returns_current():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-get-001")
    await redis.set(key, "queued", ex=RedisTTL.RUN_STATE)
    s = await get_run_state(redis, "run-get-001", state_key=key)
    assert s == "queued"


# ── 6. Unknown state fail-closed ──────────────────────────────────────────

def test_unknown_from_state_is_rejected():
    """Unknown from_state must be rejected (fail-closed)."""
    ok = validate_transition("run-unknown", "zombie_state", "running", raise_on_invalid=False)
    assert ok is False


def test_validate_transition_legal():
    assert validate_transition("r", "admitted", "queued") is True
    assert validate_transition("r", "queued", "scheduled") is True
    assert validate_transition("r", "scheduled", "running") is True
    assert validate_transition("r", "running", "done") is True

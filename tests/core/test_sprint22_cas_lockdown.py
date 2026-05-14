"""
tests/core/test_sprint22_cas_lockdown.py
IRONCLAD Sprint 22 — CAS-Only regime enforcement tests

Validates that:
- transition_state rejects illegal transitions
- duplicate admission is blocked
- terminal state runs cannot be resurrected
- scheduler failure path writes failed state
- recovery cannot hortlat a done/cancelled run
"""
import pytest
import fakeredis.aioredis as faredis

from hfa.config.keys import RedisKey, RedisTTL
from hfa_control.state_machine import (
    transition_state, validate_transition,
    InvalidStateTransition, is_terminal,
    TERMINAL_STATES,
)


# ── transition_state core CAS semantics ───────────────────────────────────

@pytest.mark.asyncio
async def test_transition_state_initial_write_succeeds():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-cas-init")
    ok = await transition_state(redis, "run-cas-init", "admitted",
                                state_key=key, expected_state=None)
    assert ok.ok is True
    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "admitted"


@pytest.mark.asyncio
async def test_transition_state_blocks_duplicate_initial():
    """expected_state=None blocks if key already exists (idempotent guard)."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-dup")
    await redis.set(key, "admitted", ex=RedisTTL.RUN_STATE)
    ok = await transition_state(redis, "run-dup", "admitted",
                                state_key=key, expected_state=None)
    assert ok.ok is False  # duplicate admission blocked


@pytest.mark.asyncio
async def test_transition_state_valid_progression():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-prog")
    await redis.set(key, "queued", ex=RedisTTL.RUN_STATE)
    ok = await transition_state(redis, "run-prog", "scheduled",
                                state_key=key, expected_state="queued")
    assert ok.ok is True
    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "scheduled"


@pytest.mark.asyncio
async def test_transition_state_cas_miss_does_not_write():
    """CAS miss: expected differs from actual — no write."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-miss")
    await redis.set(key, "running", ex=RedisTTL.RUN_STATE)
    ok = await transition_state(redis, "run-miss", "scheduled",
                                state_key=key, expected_state="queued")
    assert ok.ok is False
    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "running"  # unchanged


@pytest.mark.asyncio
async def test_transition_state_terminal_blocks_all_transitions():
    redis = faredis.FakeRedis()
    for terminal in TERMINAL_STATES:
        run_id = f"run-terminal-{terminal}"
        key = RedisKey.run_state(run_id)
        await redis.set(key, terminal, ex=RedisTTL.RUN_STATE)
        for to_state in ("admitted", "queued", "scheduled", "running"):
            ok = await transition_state(redis, run_id, to_state,
                                        state_key=key)
            assert ok.ok is False, f"{terminal} → {to_state} must be blocked"


@pytest.mark.asyncio
async def test_transition_state_unknown_from_blocks():
    """Unknown from_state is now fail-closed (not forward-compat allow)."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-unknown")
    await redis.set(key, "weird_state", ex=RedisTTL.RUN_STATE)
    ok = await transition_state(redis, "run-unknown", "scheduled",
                                state_key=key)
    assert ok.ok is False  # fail-closed


# ── Resurrection prevention ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_done_run_cannot_be_rescheduled():
    """A done run must not be resurrected by recovery."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-done")
    await redis.set(key, "done", ex=RedisTTL.RUN_STATE)
    ok = await transition_state(redis, "run-done", "rescheduled",
                                state_key=key, expected_state="done")
    assert ok.ok is False
    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "done"


@pytest.mark.asyncio
async def test_failed_run_cannot_be_admitted():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-failed")
    await redis.set(key, "failed", ex=RedisTTL.RUN_STATE)
    ok = await transition_state(redis, "run-failed", "admitted",
                                state_key=key, expected_state=None)
    # expected_state=None with existing key → blocks
    assert ok.ok is False


# ── is_terminal utility ────────────────────────────────────────────────────

def test_is_terminal_for_all_terminal_states():
    for s in TERMINAL_STATES:
        assert is_terminal(s) is True


def test_is_terminal_false_for_active_states():
    for s in ("admitted", "queued", "scheduled", "running", "rescheduled"):
        assert is_terminal(s) is False


def test_is_terminal_none_is_false():
    assert is_terminal(None) is False


# ── validate_transition edge cases ────────────────────────────────────────

def test_validate_running_to_rescheduled():
    assert validate_transition("r", "running", "rescheduled") is True


def test_validate_rescheduled_to_failed():
    assert validate_transition("r", "rescheduled", "failed") is True


def test_validate_illegal_done_to_running_returns_false():
    result = validate_transition("r", "done", "running", raise_on_invalid=False)
    assert result is False


def test_validate_unknown_state_returns_false():
    result = validate_transition("r", "cosmic_ray_state", "scheduled",
                                 raise_on_invalid=False)
    assert result is False


# ── Regression: admission duplicate ───────────────────────────────────────

@pytest.mark.asyncio
async def test_admission_duplicate_write_blocked():
    """
    Second admission attempt with state already set must be blocked.
    This simulates a duplicate admission after network retry.
    """
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-adm-dup")
    # Simulate first admission already wrote state
    await redis.set(key, "admitted", ex=RedisTTL.RUN_STATE)
    # Second attempt with expected_state=None (initial-write guard)
    ok = await transition_state(redis, "run-adm-dup", "admitted",
                                state_key=key, expected_state=None)
    assert ok.ok is False, "Duplicate admission must be blocked"
    # State must remain the original admitted, not double-written
    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "admitted"


# ── Regression: recovery resurrection ─────────────────────────────────────

@pytest.mark.asyncio
async def test_recovery_cannot_reschedule_done_run():
    """
    A run that completed (done) must not be resurrected by recovery.
    """
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-done-res")
    await redis.set(key, "done", ex=RedisTTL.RUN_STATE)
    # Recovery tries to reschedule with expected_state="running"
    ok = await transition_state(redis, "run-done-res", "rescheduled",
                                state_key=key, expected_state="running")
    assert ok.ok is False, "CAS miss: run is done, not running"
    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "done"


@pytest.mark.asyncio
async def test_recovery_cannot_reschedule_cancelled_run():
    """
    A run in rejected/failed state must not be rescheduled.
    """
    redis = faredis.FakeRedis()
    for terminal in ("rejected", "failed"):
        run_id = f"run-cancel-{terminal}"
        key = RedisKey.run_state(run_id)
        await redis.set(key, terminal, ex=RedisTTL.RUN_STATE)
        ok = await transition_state(redis, run_id, "rescheduled",
                                    state_key=key, expected_state="running")
        assert ok.ok is False, f"CAS miss: run is {terminal}, not running"


# ── Regression: scheduler dispatch commit race ─────────────────────────────

@pytest.mark.asyncio
async def test_scheduler_cas_miss_on_already_running():
    """
    If a run transitions to running by another actor between pick and commit,
    transition_state must reject the scheduler's commit attempt.
    """
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-race")
    # Simulate run was queued, then raced to running by another scheduler
    await redis.set(key, "running", ex=RedisTTL.RUN_STATE)
    # Scheduler tries to commit from queued (stale view)
    ok = await transition_state(redis, "run-race", "scheduled",
                                state_key=key, expected_state="queued")
    assert ok.ok is False, "Stale scheduler view must be rejected by CAS"
    val = await redis.get(key)
    assert (val.decode() if isinstance(val, bytes) else val) == "running"


# ── Regression: DLQ replay idempotence ────────────────────────────────────

@pytest.mark.asyncio
async def test_dlq_replay_requires_dead_lettered_state():
    """
    DLQ replay must fail if the run is NOT in dead_lettered state.
    Prevents replaying active runs or done runs.
    """
    redis = faredis.FakeRedis()
    for state in ("admitted", "running", "done", "failed"):
        run_id = f"run-replay-{state}"
        key = RedisKey.run_state(run_id)
        await redis.set(key, state, ex=RedisTTL.RUN_STATE)
        ok = await transition_state(redis, run_id, "admitted",
                                    state_key=key, expected_state="dead_lettered")
        assert ok.ok is False, f"Replay from {state} must be blocked"


# ── TransitionResult structured fields ────────────────────────────────────

@pytest.mark.asyncio
async def test_transition_result_has_reason_code():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-struct-1")
    await redis.set(key, "done", ex=RedisTTL.RUN_STATE)

    from hfa_control.state_machine import TransitionResult
    result = await transition_state(redis, "run-struct-1", "running",
                                    state_key=key)
    assert result.ok is False
    assert result.reason in (
        TransitionResult.TERMINAL_BLOCKED,
        TransitionResult.CAS_MISS,
        TransitionResult.ILLEGAL_TRANSITION,
        TransitionResult.INITIAL_WRITE_BLOCKED,
    )
    assert result.to_state == "running"
    assert result.run_id == "run-struct-1"


@pytest.mark.asyncio
async def test_transition_result_committed_has_correct_fields():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-struct-2")
    await redis.set(key, "queued", ex=RedisTTL.RUN_STATE)

    from hfa_control.state_machine import TransitionResult
    result = await transition_state(redis, "run-struct-2", "scheduled",
                                    state_key=key, expected_state="queued")
    assert result.ok is True
    assert result.reason == TransitionResult.COMMITTED
    assert result.to_state == "scheduled"
    assert result.run_id == "run-struct-2"


@pytest.mark.asyncio
async def test_transition_result_bool_compat():
    """TransitionResult must behave as bool for existing if/if-not patterns."""
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-bool-1")
    result = await transition_state(redis, "run-bool-1", "admitted",
                                    state_key=key, expected_state=None)
    assert bool(result) is True   # works as True
    assert result            # works in if context

    result2 = await transition_state(redis, "run-bool-1", "admitted",
                                     state_key=key, expected_state=None)
    assert bool(result2) is False  # blocked
    assert not result2        # works in if-not context


# ── Tightened transitions: scheduled→admitted and running→scheduled blocked ─

def test_scheduled_to_admitted_no_longer_valid():
    """scheduled→admitted is now closed (use rescheduled path)."""
    from hfa_control.state_machine import validate_transition
    result = validate_transition("r", "scheduled", "admitted", raise_on_invalid=False)
    assert result is False


def test_running_to_scheduled_no_longer_valid():
    """running→scheduled is now closed (use rescheduled path)."""
    from hfa_control.state_machine import validate_transition
    result = validate_transition("r", "running", "scheduled", raise_on_invalid=False)
    assert result is False


def test_reschedule_loop_still_valid():
    """The rescheduled path must still work: running→rescheduled→admitted."""
    from hfa_control.state_machine import validate_transition
    assert validate_transition("r", "running", "rescheduled") is True
    assert validate_transition("r", "rescheduled", "admitted") is True
